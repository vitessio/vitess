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
	ParseTree           Statement
	BindVars            map[string]struct{}

	lastToken      string
	posVarIndex    int
	partialDDL     Statement
	multi          bool
	specialComment *Tokenizer

	Pos int
	buf string
}

// NewStringTokenizer creates a new Tokenizer for the
// sql string.
func NewStringTokenizer(sql string) *Tokenizer {
	checkParserVersionFlag()

	return &Tokenizer{
		buf:      sql,
		BindVars: make(map[string]struct{}),
	}
}

// Lex returns the next token form the Tokenizer.
// This function is used by go yacc.
func (tkn *Tokenizer) Lex(lval *yySymType) int {
	if tkn.SkipToEnd {
		return tkn.skipStatement()
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
	lval.str = val
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

// Error is called by go yacc if there's a parsing error.
func (tkn *Tokenizer) Error(err string) {
	tkn.LastError = PositionedErr{Err: err, Pos: tkn.Pos + 1, Near: tkn.lastToken}

	// Try and re-sync to the next statement
	tkn.skipStatement()
}

// Scan scans the tokenizer for the next token and returns
// the token type and an optional value.
func (tkn *Tokenizer) Scan() (int, string) {
	if tkn.specialComment != nil {
		// Enter specialComment scan mode.
		// for scanning such kind of comment: /*! MySQL-specific code */
		specialComment := tkn.specialComment
		tok, val := specialComment.Scan()
		if tok != 0 {
			// return the specialComment scan result as the result
			return tok, val
		}
		// leave specialComment scan mode after all stream consumed.
		tkn.specialComment = nil
	}

	tkn.skipBlank()
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
		} else if tkn.cur() == eofChar {
			return LEX_ERROR, ""
		} else {
			tID, tBytes = tkn.scanIdentifier(true)
		}
		if tID == LEX_ERROR {
			return tID, ""
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
		// N\'literal' is used to create a string in the national character set
		if ch == 'N' || ch == 'n' {
			nxt := tkn.peek(1)
			if nxt == '\'' || nxt == '"' {
				tkn.skip(2)
				return tkn.scanString(nxt, NCHAR_STRING)
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
			tkn.posVarIndex++
			buf := make([]byte, 0, 8)
			buf = append(buf, ":v"...)
			buf = strconv.AppendInt(buf, int64(tkn.posVarIndex), 10)
			return VALUE_ARG, string(buf)
		case '.':
			return int(ch), ""
		case '/':
			switch tkn.cur() {
			case '/':
				tkn.skip(1)
				return tkn.scanCommentType1(2)
			case '*':
				tkn.skip(1)
				if tkn.cur() == '!' && !tkn.SkipSpecialComments {
					tkn.skip(1)
					return tkn.scanMySQLSpecificComment()
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
				if nextChar == ' ' || nextChar == '\n' || nextChar == '\t' || nextChar == '\r' || nextChar == eofChar {
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
	for ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t' {
		tkn.skip(1)
		ch = tkn.cur()
	}
}

// scanIdentifier scans a language keyword or @-encased variable
func (tkn *Tokenizer) scanIdentifier(isVariable bool) (int, string) {
	start := tkn.Pos
	tkn.skip(1)

	for {
		ch := tkn.cur()
		if !isLetter(ch) && !isDigit(ch) && !(isVariable && isCarat(ch)) {
			break
		}
		tkn.skip(1)
	}
	keywordName := tkn.buf[start:tkn.Pos]
	if keywordID, found := keywordLookupTable.LookupString(keywordName); found {
		return keywordID, keywordName
	}
	// dual must always be case-insensitive
	if keywordASCIIMatch(keywordName, "dual") {
		return ID, "dual"
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

// scanLiteralIdentifierSlow scans an identifier surrounded by backticks which may
// contain escape sequences instead of it. This method is only called from
// scanLiteralIdentifier once the first escape sequence is found in the identifier.
// The provided `buf` contains the contents of the identifier that have been scanned
// so far.
func (tkn *Tokenizer) scanLiteralIdentifierSlow(buf *strings.Builder) (int, string) {
	backTickSeen := true
	for {
		if backTickSeen {
			if tkn.cur() != '`' {
				break
			}
			backTickSeen = false
			buf.WriteByte('`')
			tkn.skip(1)
			continue
		}
		// The previous char was not a backtick.
		switch tkn.cur() {
		case '`':
			backTickSeen = true
		case eofChar:
			// Premature EOF.
			return LEX_ERROR, buf.String()
		default:
			buf.WriteByte(byte(tkn.cur()))
			// keep scanning
		}
		tkn.skip(1)
	}
	return ID, buf.String()
}

// scanLiteralIdentifier scans an identifier enclosed by backticks. If the identifier
// is a simple literal, it'll be returned as a slice of the input buffer. If the identifier
// contains escape sequences, this function will fall back to scanLiteralIdentifierSlow
func (tkn *Tokenizer) scanLiteralIdentifier() (int, string) {
	start := tkn.Pos
	for {
		switch tkn.cur() {
		case '`':
			if tkn.peek(1) != '`' {
				if tkn.Pos == start {
					return LEX_ERROR, ""
				}
				tkn.skip(1)
				return ID, tkn.buf[start : tkn.Pos-1]
			}

			var buf strings.Builder
			buf.WriteString(tkn.buf[start:tkn.Pos])
			tkn.skip(1)
			return tkn.scanLiteralIdentifierSlow(&buf)
		case eofChar:
			// Premature EOF.
			return LEX_ERROR, tkn.buf[start:tkn.Pos]
		default:
			tkn.skip(1)
		}
	}
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
// been scanned. If the skin contains any escape sequences, this function
// will fall back to scanStringSlow
func (tkn *Tokenizer) scanString(delim uint16, typ int) (int, string) {
	start := tkn.Pos

	for {
		switch tkn.cur() {
		case delim:
			if tkn.peek(1) != delim {
				tkn.skip(1)
				return typ, tkn.buf[start : tkn.Pos-1]
			}
			fallthrough

		case '\\':
			var buffer strings.Builder
			buffer.WriteString(tkn.buf[start:tkn.Pos])
			return tkn.scanStringSlow(&buffer, delim, typ)

		case eofChar:
			return LEX_ERROR, tkn.buf[start:tkn.Pos]
		}

		tkn.skip(1)
	}
}

// scanString scans a string surrounded by the given `delim` and containing escape
// sequencse. The given `buffer` contains the contents of the string that have
// been scanned so far.
func (tkn *Tokenizer) scanStringSlow(buffer *strings.Builder, delim uint16, typ int) (int, string) {
	for {
		ch := tkn.cur()
		if ch == eofChar {
			// Unterminated string.
			return LEX_ERROR, buffer.String()
		}

		if ch != delim && ch != '\\' {
			// Scan ahead to the next interesting character.
			start := tkn.Pos
			for ; tkn.Pos < len(tkn.buf); tkn.Pos++ {
				ch = uint16(tkn.buf[tkn.Pos])
				if ch == delim || ch == '\\' {
					break
				}
			}

			buffer.WriteString(tkn.buf[start:tkn.Pos])
			if tkn.Pos >= len(tkn.buf) {
				// Reached the end of the buffer without finding a delim or
				// escape character.
				tkn.skip(1)
				continue
			}
		}
		tkn.skip(1) // Read one past the delim or escape character.

		if ch == '\\' {
			if tkn.cur() == eofChar {
				// String terminates mid escape character.
				return LEX_ERROR, buffer.String()
			}
			// Preserve escaping of % and _
			if tkn.cur() == '%' || tkn.cur() == '_' {
				buffer.WriteByte('\\')
				ch = tkn.cur()
			} else if decodedChar := sqltypes.SQLDecodeMap[byte(tkn.cur())]; decodedChar == sqltypes.DontEscape {
				ch = tkn.cur()
			} else {
				ch = uint16(decodedChar)
			}
		} else if ch == delim && tkn.cur() != delim {
			// Correctly terminated string, which is not a double delim.
			break
		}

		buffer.WriteByte(byte(ch))
		tkn.skip(1)
	}

	return typ, buffer.String()
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
	for {
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
	return COMMENT, tkn.buf[start:tkn.Pos]
}

// scanMySQLSpecificComment scans a MySQL comment pragma, which always starts with '//*`
func (tkn *Tokenizer) scanMySQLSpecificComment() (int, string) {
	start := tkn.Pos - 3
	for {
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

	commentVersion, sql := ExtractMysqlComment(tkn.buf[start:tkn.Pos])

	if mySQLParserVersion >= commentVersion {
		// Only add the special comment to the tokenizer if the version of MySQL is higher or equal to the comment version
		tkn.specialComment = NewStringTokenizer(sql)
	}

	return tkn.Scan()
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

// reset clears any internal state.
func (tkn *Tokenizer) reset() {
	tkn.ParseTree = nil
	tkn.partialDDL = nil
	tkn.specialComment = nil
	tkn.posVarIndex = 0
	tkn.SkipToEnd = false
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
