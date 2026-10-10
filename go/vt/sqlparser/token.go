/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

import (
	"fmt"
	"strconv"
	"strings"

	"vitess.io/vitess/go/sqltypes"
)

const (
	eofChar = 0x100
)

// Tokenizer is the struct used to generate SQL
// tokens for the parser.
type Tokenizer struct {
	AllowComments       bool
	SkipSpecialComments bool
	SkipToEnd           bool
	LastError           error
	ParseTrees          []Statement
	BindVars            map[string]struct{}

	lastTokenType      int
	lastToken          string
	posVarIndex        int
	partialDDL         Statement
	multi              bool
	inVersionedComment bool // true when scanning inside a MySQL versioned comment (/*!...*/)

	Pos       int
	buf       string
	parser    *Parser
	currStart int // start position of current token (set in Scan after skipBlank)
}

// The token types that scan returns for a token whose value needs more work
// than taking it from the input: escapes to decode or a '?' to number. The
// value scan returns with them is the text between the quotes, as written.
// Scan does that work and returns the token's type instead.
const (
	// singleQuotedEscaped is a STRING in single quotes that holds escapes.
	singleQuotedEscaped = -1 - iota
	// doubleQuotedEscaped is a STRING in double quotes that holds escapes.
	doubleQuotedEscaped
	// nationalEscaped is an NCHAR_STRING that holds escapes.
	nationalEscaped
	// backquotedEscaped is an ID in backticks that holds a doubled backtick.
	backquotedEscaped
	// positionalArg is a VALUE_ARG written as '?'.
	positionalArg
)

// location tracks the byte-offset span [start, end) of a grammar symbol
// in the input buffer. Used by the parser's %locations feature.
type location struct {
	start int
	end   int
}

// yyLocDefault merges locations during reductions. For non-empty
// productions (n > 0), it spans from the first RHS symbol's start to
// the last RHS symbol's end. For empty productions (n == 0), it
// creates a zero-width span at the predecessor's end position.
func yyLocDefault(cur *location, rhs []yySymType, n int) {
	if n > 0 {
		cur.start = rhs[1].yyloc.start
		cur.end = rhs[n].yyloc.end
	} else {
		cur.start = rhs[0].yyloc.end
		cur.end = rhs[0].yyloc.end
	}
}

// NewStringTokenizer creates a new Tokenizer for the
// sql string.
func (p *Parser) NewStringTokenizer(sql string) *Tokenizer {
	return &Tokenizer{
		buf:      sql,
		BindVars: make(map[string]struct{}),
		parser:   p,
	}
}

// Lex returns the next token form the Tokenizer.
// This function is used by go yacc.
func (tkn *Tokenizer) Lex(lval *yySymType) int {
	if tkn.SkipToEnd {
		// We need to check the last token type to
		// prevent us from skipping the next query in a multi
		// parse mode. If we don't check the last token, we
		// will skip the next query.
		if tkn.lastTokenType == ';' {
			tkn.SkipToEnd = false
		} else {
			return tkn.skipStatement()
		}
	}

	typ, val := tkn.Scan()
	for typ == COMMENT {
		if tkn.AllowComments {
			break
		}
		typ, val = tkn.Scan()
	}
	if typ == 0 || typ == ';' || typ == LEX_ERROR {
		// If encounter end of statement or invalid token,
		// we should not accept partially parsed DDLs. They
		// should instead result in parser errors. See the
		// Parse function to see how this is handled.
		tkn.partialDDL = nil
	}
	lval.yyloc.start = tkn.currStart
	lval.yyloc.end = tkn.Pos
	lval.setstr(val)
	tkn.lastTokenType = typ
	tkn.lastToken = val
	return typ
}

// PositionedErr holds context related to parser errors
type PositionedErr struct {
	Err  string
	Pos  int
	Near string
}

func (p PositionedErr) Error() string {
	if p.Near != "" {
		return fmt.Sprintf("%s at position %v near '%s'", p.Err, p.Pos, p.Near)
	}
	return fmt.Sprintf("%s at position %v", p.Err, p.Pos)
}

// GetInputExpression extracts the original input text between start and end positions.
// The lexer's skipBlank ensures start/end already exclude leading/trailing whitespace.
//
// Note that this returns a slice into the original input buffer,
// so it will keep the original input buffer alive in memory until
// the returned string is no longer referenced.
func (tkn *Tokenizer) GetInputExpression(start, end int) string {
	if start < 0 || end < 0 || start >= end || end > len(tkn.buf) {
		return ""
	}
	return tkn.buf[start:end]
}

// Error is called by go yacc if there's a parsing error.
func (tkn *Tokenizer) Error(err string) {
	tkn.LastError = PositionedErr{Err: err, Pos: tkn.Pos + 1, Near: tkn.lastToken}

	// Try and re-sync to the next statement
	tkn.skipStatement()
}

// Scan scans the tokenizer for the next token and returns
// the token type and an optional value.
func (tkn *Tokenizer) Scan() (int, string) {
	typ, val := tkn.scan()
	switch typ {
	case singleQuotedEscaped:
		return STRING, decodeString(val, '\'')
	case doubleQuotedEscaped:
		return STRING, decodeString(val, '"')
	case nationalEscaped:
		return NCHAR_STRING, decodeString(val, '\'')
	case backquotedEscaped:
		return ID, decodeBackquoted(val)
	case positionalArg:
		tkn.posVarIndex++
		buf := make([]byte, 0, 8)
		buf = append(buf, ":v"...)
		buf = strconv.AppendInt(buf, int64(tkn.posVarIndex), 10)
		return VALUE_ARG, string(buf)
	}
	return typ, val
}

// scannedType returns the token type of a token that scan returned as typ.
func scannedType(typ int) int {
	switch typ {
	case singleQuotedEscaped, doubleQuotedEscaped:
		return STRING
	case nationalEscaped:
		return NCHAR_STRING
	case backquotedEscaped:
		return ID
	case positionalArg:
		return VALUE_ARG
	}
	return typ
}

// scan is Scan without the work that only the value of a token needs. For a
// token whose value needs decoding or numbering, it returns one of the token
// types above, and Scan does that work. Callers that need only where the
// tokens are and what they are call scan and skip it.
func (tkn *Tokenizer) scan() (int, string) {
	for {
		tkn.skipBlank()
		// If inside a versioned comment and we've reached the closing */,
		// skip past it and resume normal scanning.
		if tkn.inVersionedComment && tkn.cur() == '*' && tkn.peek(1) == '/' {
			tkn.skip(2)
			tkn.inVersionedComment = false
			tkn.skipBlank()
		}
		tkn.currStart = tkn.Pos
		switch ch := tkn.cur(); {
		case ch == '@':
			tokenID := AT_ID
			tkn.skip(1)
			if tkn.cur() == '@' {
				tokenID = AT_AT_ID
				tkn.skip(1)
			}
			var tID int
			var tBytes string
			if tkn.cur() == '`' {
				tkn.skip(1)
				tID, tBytes = tkn.scanLiteralIdentifier()
			} else if tokenID == AT_ID && (tkn.cur() == '\'' || tkn.cur() == '"') && !tkn.quotedNameIsIdentifier() {
				// MySQL reads a quoted name after a single '@' to its closing
				// quote, so nothing inside the quotes is a comment. Vitess
				// reads the name as an identifier that includes the quotes,
				// which stops at the first character an identifier cannot
				// hold. Rather than read such a name in part, reject it.
				return LEX_ERROR, ""
			} else if tkn.cur() == eofChar {
				return LEX_ERROR, ""
			} else {
				tID, tBytes = tkn.scanIdentifier(true)
			}
			if tID == LEX_ERROR {
				return tID, ""
			}
			if tID == backquotedEscaped {
				tBytes = decodeBackquoted(tBytes)
			}
			return tokenID, tBytes
		case isLetter(ch):
			if ch == 'X' || ch == 'x' {
				if tkn.peek(1) == '\'' {
					tkn.skip(2)
					return tkn.scanHex()
				}
			}
			if ch == 'B' || ch == 'b' {
				if tkn.peek(1) == '\'' {
					tkn.skip(2)
					return tkn.scanBitLiteral()
				}
			}
			// N'literal' is a string in the national character set. MySQL
			// recognizes the introducer only before a single quote: N"…" is the
			// identifier N followed by a double-quoted token, whatever the mode
			// makes of that token.
			if ch == 'N' || ch == 'n' {
				if tkn.peek(1) == '\'' {
					tkn.skip(2)
					return tkn.scanString('\'', NCHAR_STRING)
				}
			}
			return tkn.scanIdentifier(false)
		case isDigit(ch):
			return tkn.scanNumber()
		case ch == ':':
			return tkn.scanBindVarOrAssignmentExpression()
		case ch == ';':
			if tkn.multi {
				// In multi mode, ';' is treated as EOF. So, we don't advance.
				// Repeated calls to Scan will keep returning 0 until ParseNext
				// forces the advance.
				return 0, ""
			}
			tkn.skip(1)
			return ';', ""
		case ch == eofChar:
			if tkn.inVersionedComment {
				// Unclosed versioned comment — report a lex error.
				tkn.inVersionedComment = false
				return LEX_ERROR, ""
			}
			return 0, ""
		default:
			if ch == '.' && isDigit(tkn.peek(1)) {
				return tkn.scanNumber()
			}

			tkn.skip(1)
			switch ch {
			case '=', ',', '(', ')', '+', '*', '%', '^', '~':
				return int(ch), ""
			case '&':
				if tkn.cur() == '&' {
					tkn.skip(1)
					return AND, ""
				}
				return int(ch), ""
			case '|':
				if tkn.cur() == '|' {
					tkn.skip(1)
					return OR, ""
				}
				return int(ch), ""
			case '?':
				return positionalArg, ""
			case '.':
				return int(ch), ""
			case '/':
				if tkn.inVersionedComment {
					if tkn.cur() == '*' {
						// Nested /* ... */ comment inside a versioned comment:
						// consume and discard it (spec allows one level of nesting).
						tkn.skip(1)
						if tok, val := tkn.scanCommentType2(); tok == LEX_ERROR {
							return tok, val
						}
						continue
					}
					// Not a comment start — return / as division operator.
					return int(ch), ""
				}
				switch tkn.cur() {
				case '/':
					tkn.skip(1)
					return tkn.scanCommentType1(2)
				case '*':
					tkn.skip(1)
					if tkn.cur() == '!' && !tkn.SkipSpecialComments {
						tkn.skip(1)
						if tok, val := tkn.scanMySQLSpecificComment(); tok == LEX_ERROR {
							return tok, val
						}
						continue
					}
					return tkn.scanCommentType2()
				default:
					return int(ch), ""
				}
			case '#':
				return tkn.scanCommentType1(1)
			case '-':
				switch tkn.cur() {
				case '-':
					nextChar := tkn.peek(1)
					if nextChar == ' ' || nextChar == '\t' || nextChar == '\n' || nextChar == '\v' || nextChar == '\f' || nextChar == '\r' || nextChar == eofChar {
						tkn.skip(1)
						return tkn.scanCommentType1(2)
					}
				case '>':
					tkn.skip(1)
					if tkn.cur() == '>' {
						tkn.skip(1)
						return JSON_UNQUOTE_EXTRACT_OP, ""
					}
					return JSON_EXTRACT_OP, ""
				}
				return int(ch), ""
			case '<':
				switch tkn.cur() {
				case '>':
					tkn.skip(1)
					return NE, ""
				case '<':
					tkn.skip(1)
					return SHIFT_LEFT, ""
				case '=':
					tkn.skip(1)
					switch tkn.cur() {
					case '>':
						tkn.skip(1)
						return NULL_SAFE_EQUAL, ""
					default:
						return LE, ""
					}
				default:
					return int(ch), ""
				}
			case '>':
				switch tkn.cur() {
				case '=':
					tkn.skip(1)
					return GE, ""
				case '>':
					tkn.skip(1)
					return SHIFT_RIGHT, ""
				default:
					return int(ch), ""
				}
			case '!':
				if tkn.cur() == '=' {
					tkn.skip(1)
					return NE, ""
				}
				return int(ch), ""
			case '\'', '"':
				return tkn.scanString(ch, STRING)
			case '`':
				return tkn.scanLiteralIdentifier()
			default:
				return LEX_ERROR, string(byte(ch))
			}
		}
	}
}

// skipStatement scans until end of statement.
func (tkn *Tokenizer) skipStatement() int {
	tkn.SkipToEnd = false
	for {
		typ, _ := tkn.Scan()
		if typ == 0 || typ == ';' || typ == LEX_ERROR {
			return typ
		}
	}
}

// skipBlank skips the cursor while it finds whitespace
func (tkn *Tokenizer) skipBlank() {
	ch := tkn.cur()
	for ch == ' ' || ch == '\t' || ch == '\n' || ch == '\v' || ch == '\f' || ch == '\r' {
		tkn.skip(1)
		ch = tkn.cur()
	}
}

// quotedNameIsIdentifier reports whether the quoted name at the cursor, read
// to its closing quote as MySQL reads it, consists of characters that
// scanIdentifier reads as part of a variable name, so that scanIdentifier
// reads the whole name.
func (tkn *Tokenizer) quotedNameIsIdentifier() bool {
	delim := tkn.buf[tkn.Pos]
	for i := tkn.Pos + 1; i < len(tkn.buf); i++ {
		ch := uint16(tkn.buf[i])
		if ch == uint16(delim) && (i+1 == len(tkn.buf) || tkn.buf[i+1] != delim) {
			return true
		}
		if !isLetter(ch) && !isDigit(ch) && !isCarat(ch) {
			return false
		}
		if ch == uint16(delim) {
			// A doubled quote stands for the quote itself.
			i++
		}
	}
	return false
}

// scanIdentifier scans a language keyword or @-encased variable
func (tkn *Tokenizer) scanIdentifier(isVariable bool) (int, string) {
	start := tkn.Pos
	tkn.skip(1)

	for {
		ch := tkn.cur()
		if !isLetter(ch) && !isDigit(ch) && (!isVariable || !isCarat(ch)) {
			break
		}
		tkn.skip(1)
	}
	keywordName := tkn.buf[start:tkn.Pos]
	if keywordID, found := keywordLookupTable.LookupString(keywordName); found {
		return keywordID, keywordName
	}
	return ID, keywordName
}

// scanHex scans a hex numeral; assumes x' or X' has already been scanned
func (tkn *Tokenizer) scanHex() (int, string) {
	start := tkn.Pos
	tkn.scanMantissa(16)
	hex := tkn.buf[start:tkn.Pos]
	if tkn.cur() != '\'' {
		return LEX_ERROR, hex
	}
	tkn.skip(1)
	if len(hex)%2 != 0 {
		return LEX_ERROR, hex
	}
	return HEX, hex
}

// scanBitLiteral scans a binary numeric literal; assumes b' or B' has already been scanned
func (tkn *Tokenizer) scanBitLiteral() (int, string) {
	start := tkn.Pos
	tkn.scanMantissa(2)
	bit := tkn.buf[start:tkn.Pos]
	if tkn.cur() != '\'' {
		return LEX_ERROR, bit
	}
	tkn.skip(1)
	return BIT_LITERAL, bit
}

// scanLiteralIdentifier scans an identifier enclosed by backticks. If the identifier
// is a simple literal, it'll be returned as a slice of the input buffer. If it
// holds a doubled backtick, which stands for one backtick, it is returned as
// backquotedEscaped, for Scan to decode.
func (tkn *Tokenizer) scanLiteralIdentifier() (int, string) {
	start := tkn.Pos
	doubled := false
	for {
		switch tkn.cur() {
		case '`':
			if tkn.peek(1) == '`' {
				doubled = true
				tkn.skip(2)
				continue
			}
			if tkn.Pos == start {
				return LEX_ERROR, ""
			}
			tkn.skip(1)
			if !doubled {
				return ID, tkn.buf[start : tkn.Pos-1]
			}
			return backquotedEscaped, tkn.buf[start : tkn.Pos-1]
		case eofChar:
			// Premature EOF.
			if !doubled {
				return LEX_ERROR, tkn.buf[start:tkn.Pos]
			}
			return LEX_ERROR, decodeBackquoted(tkn.buf[start:tkn.Pos])
		default:
			tkn.skip(1)
		}
	}
}

// decodeBackquoted decodes the doubled backticks in the text of an identifier
// quoted with backticks.
func decodeBackquoted(text string) string {
	return strings.ReplaceAll(text, "``", "`")
}

// scanBindVarOrAssignmentExpression scans a bind variable or an assignment expression; assumes a ':' has been scanned right before
func (tkn *Tokenizer) scanBindVarOrAssignmentExpression() (int, string) {
	start := tkn.Pos
	token := VALUE_ARG

	tkn.skip(1)
	// If : is followed by a digit, then it is an offset value arg. Example - :1, :10
	if isDigit(tkn.cur()) {
		tkn.scanMantissa(10)
		return OFFSET_ARG, tkn.buf[start+1 : tkn.Pos]
	}

	// If : is followed by a =, then it is an assignment operator
	if tkn.cur() == '=' {
		tkn.skip(1)
		return ASSIGNMENT_OPT, ""
	}

	// If : is followed by another : it is a list arg. Example ::v1, ::list
	if tkn.cur() == ':' {
		token = LIST_ARG
		tkn.skip(1)
	}
	if !isLetter(tkn.cur()) {
		return LEX_ERROR, tkn.buf[start:tkn.Pos]
	}
	// If : is followed by a letter, it is a bindvariable. Example :v1, :v2
	for {
		ch := tkn.cur()
		if !isLetter(ch) && !isDigit(ch) && ch != '.' {
			break
		}
		tkn.skip(1)
	}
	return token, tkn.buf[start:tkn.Pos]
}

// scanMantissa scans a sequence of numeric characters with the same base.
// This is a helper function only called from the numeric scanners
func (tkn *Tokenizer) scanMantissa(base int) {
	for digitVal(tkn.cur()) < base {
		tkn.skip(1)
	}
}

// scanNumber scans any SQL numeric literal, either floating point or integer
func (tkn *Tokenizer) scanNumber() (int, string) {
	start := tkn.Pos
	token := INTEGRAL

	if tkn.cur() == '.' {
		token = DECIMAL
		tkn.skip(1)
		tkn.scanMantissa(10)
		goto exponent
	}

	// 0x construct.
	if tkn.cur() == '0' {
		tkn.skip(1)
		if tkn.cur() == 'x' || tkn.cur() == 'X' {
			token = HEXNUM
			tkn.skip(1)
			tkn.scanMantissa(16)
			goto exit
		}
		if tkn.cur() == 'b' || tkn.cur() == 'B' {
			token = BITNUM
			tkn.skip(1)
			tkn.scanMantissa(2)
			goto exit
		}
	}

	tkn.scanMantissa(10)

	if tkn.cur() == '.' {
		token = DECIMAL
		tkn.skip(1)
		tkn.scanMantissa(10)
	}

exponent:
	if tkn.cur() == 'e' || tkn.cur() == 'E' {
		token = FLOAT
		tkn.skip(1)
		if tkn.cur() == '+' || tkn.cur() == '-' {
			tkn.skip(1)
		}
		tkn.scanMantissa(10)
	}

exit:
	if isLetter(tkn.cur()) {
		// A letter cannot immediately follow a float number.
		if token == FLOAT || token == DECIMAL {
			return LEX_ERROR, tkn.buf[start:tkn.Pos]
		}
		// A letter seen after a few numbers means that we should parse this
		// as an identifier and not a number.
		for {
			ch := tkn.cur()
			if !isLetter(ch) && !isDigit(ch) {
				break
			}
			tkn.skip(1)
		}
		return ID, tkn.buf[start:tkn.Pos]
	}

	return token, tkn.buf[start:tkn.Pos]
}

// scanString scans a string surrounded by the given `delim`, which can be
// either single or double quotes. Assumes that the given delimiter has just
// been scanned. A string without escapes is returned as a slice of the input
// buffer. A string with escapes is returned as one of the token types for
// Scan to decode: singleQuotedEscaped, doubleQuotedEscaped or nationalEscaped.
func (tkn *Tokenizer) scanString(delim uint16, typ int) (int, string) {
	start := tkn.Pos
	escaped := false
	for {
		switch tkn.cur() {
		case delim:
			if tkn.peek(1) != delim {
				tkn.skip(1)
				if !escaped {
					return typ, tkn.buf[start : tkn.Pos-1]
				}
				return escapedStringType(delim, typ), tkn.buf[start : tkn.Pos-1]
			}
			escaped = true
			tkn.skip(1)
		case '\\':
			escaped = true
			tkn.skip(1)
			if tkn.cur() == eofChar {
				// String terminates mid escape character.
				return LEX_ERROR, decodeString(tkn.buf[start:tkn.Pos], byte(delim))
			}
		case eofChar:
			if !escaped {
				return LEX_ERROR, tkn.buf[start:tkn.Pos]
			}
			return LEX_ERROR, decodeString(tkn.buf[start:tkn.Pos], byte(delim))
		}
		tkn.skip(1)
	}
}

// escapedStringType returns the token type that scan returns for a string of
// type typ in the given quotes that holds escapes.
func escapedStringType(delim uint16, typ int) int {
	switch {
	case typ == NCHAR_STRING:
		return nationalEscaped
	case delim == '"':
		return doubleQuotedEscaped
	}
	return singleQuotedEscaped
}

// decodeString decodes the backslash escapes and the doubled delimiters in
// the text of a string literal. A backslash at the end of the text, where an
// unterminated string stops, is dropped.
func decodeString(text string, delim byte) string {
	var buf strings.Builder
	buf.Grow(len(text))
	for {
		i := 0
		for i < len(text) && text[i] != '\\' && text[i] != delim {
			i++
		}
		buf.WriteString(text[:i])
		if i+1 >= len(text) {
			return buf.String()
		}
		ch := text[i+1]
		if text[i] == '\\' {
			// Preserve escaping of % and _
			if ch == '%' || ch == '_' {
				buf.WriteByte('\\')
			} else if decoded := sqltypes.SQLDecodeMap[ch]; decoded != sqltypes.DontEscape {
				ch = decoded
			}
		}
		// A delimiter here is always doubled: a single one ends the string.
		buf.WriteByte(ch)
		text = text[i+2:]
	}
}

// scanCommentType1 scans a SQL line-comment, which is applied until the end
// of the line. The given prefix length varies based on whether the comment
// is started with '//', '--' or '#'.
func (tkn *Tokenizer) scanCommentType1(prefixLen int) (int, string) {
	start := tkn.Pos - prefixLen
	for tkn.cur() != eofChar {
		if tkn.cur() == '\n' {
			tkn.skip(1)
			break
		}
		tkn.skip(1)
	}
	return COMMENT, tkn.buf[start:tkn.Pos]
}

// scanCommentType2 scans a '/*' delimited comment; assumes the opening
// prefix has already been scanned
func (tkn *Tokenizer) scanCommentType2() (int, string) {
	start := tkn.Pos - 2
	end := strings.Index(tkn.buf[tkn.Pos:], "*/")
	if end < 0 {
		tkn.Pos = len(tkn.buf)
		return LEX_ERROR, tkn.buf[start:]
	}
	tkn.skip(end + 2)
	return COMMENT, tkn.buf[start:tkn.Pos]
}

// scanMySQLSpecificComment handles a MySQL versioned comment (/*!NNNNN ... */).
// If the server version satisfies the comment's version, inVersionedComment is
// set so that Scan reads inner tokens normally. Otherwise the entire comment
// body is skipped. Returns LEX_ERROR if the comment is malformed; the caller
// should continue scanning in all other cases.
func (tkn *Tokenizer) scanMySQLSpecificComment() (int, string) {
	start := tkn.Pos - 3

	// Read up to 5 version digits inline.
	versionStart := tkn.Pos
	for i := 0; i < 5 && isDigit(tkn.cur()); i++ {
		tkn.skip(1)
	}
	versionStr := tkn.buf[versionStart:tkn.Pos]
	if len(versionStr) < 5 {
		// Fewer than 5 digits: no version, digits are part of the content.
		versionStr = ""
		tkn.Pos = versionStart
	}

	if tkn.parser.version >= versionStr {
		// Version satisfied — Scan() will read inner tokens and detect
		// the closing */ via the inVersionedComment flag.
		tkn.inVersionedComment = true
		return 0, ""
	}

	// Version not satisfied — skip the entire comment.
	// Track one level of /* ... */ nesting so that a nested comment's
	// closing */ does not prematurely end the versioned comment.
	for {
		if tkn.cur() == '/' && tkn.peek(1) == '*' {
			// Nested /* ... */ comment — consume and discard it.
			tkn.skip(2)
			if tok, val := tkn.scanCommentType2(); tok == LEX_ERROR {
				return tok, val
			}
			continue
		}
		if tkn.cur() == '*' {
			tkn.skip(1)
			if tkn.cur() == '/' {
				tkn.skip(1)
				break
			}
			continue
		}
		if tkn.cur() == eofChar {
			return LEX_ERROR, tkn.buf[start:tkn.Pos]
		}
		tkn.skip(1)
	}
	return 0, ""
}

func (tkn *Tokenizer) cur() uint16 {
	return tkn.peek(0)
}

func (tkn *Tokenizer) skip(dist int) {
	tkn.Pos += dist
}

func (tkn *Tokenizer) peek(dist int) uint16 {
	if tkn.Pos+dist >= len(tkn.buf) {
		return eofChar
	}
	return uint16(tkn.buf[tkn.Pos+dist])
}

// reset clears posVarIndex to reset the index count we assign to variables for a new query.
func (tkn *Tokenizer) reset() {
	tkn.posVarIndex = 0
}

func isLetter(ch uint16) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_' || ch == '$'
}

func isCarat(ch uint16) bool {
	return ch == '.' || ch == '\'' || ch == '"' || ch == '`'
}

func digitVal(ch uint16) int {
	switch {
	case '0' <= ch && ch <= '9':
		return int(ch) - '0'
	case 'a' <= ch && ch <= 'f':
		return int(ch) - 'a' + 10
	case 'A' <= ch && ch <= 'F':
		return int(ch) - 'A' + 10
	}
	return 16 // larger than any legal digit val
}

func isDigit(ch uint16) bool {
	return '0' <= ch && ch <= '9'
}
