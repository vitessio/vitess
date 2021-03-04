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

	"vitess.io/vitess/go/bytes2"
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
	lastToken           []byte
	LastError           error
	posVarIndex         int
	ParseTree           Statement
	partialDDL          Statement
	nesting             int
	multi               bool
	specialComment      *Tokenizer

	Pos int
	buf []byte
}

// NewStringTokenizer creates a new Tokenizer for the
// sql string.
func NewStringTokenizer(sql string) *Tokenizer {
	buf := []byte(sql)
	return &Tokenizer{
		buf: buf,
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
	lval.bytes = val
	tkn.lastToken = val
	return typ
}

// PositionedErr holds context related to parser errors
type PositionedErr struct {
	Err  string
	Pos  int
	Near []byte
}

func (p PositionedErr) Error() string {
	if p.Near != nil {
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
func (tkn *Tokenizer) Scan() (int, []byte) {
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
		var tBytes []byte
		if tkn.cur() == '`' {
			tkn.skip(1)
			tID, tBytes = tkn.scanLiteralIdentifier()
		} else {
			tID, tBytes = tkn.scanIdentifier(true)
		}
		if tID == LEX_ERROR {
			return tID, nil
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
		return tkn.scanIdentifier(false)
	case isDigit(ch):
		return tkn.scanNumber()
	case ch == ':':
		return tkn.scanBindVar()
	case ch == ';':
		if tkn.multi {
			// In multi mode, ';' is treated as EOF. So, we don't advance.
			// Repeated calls to Scan will keep returning 0 until ParseNext
			// forces the advance.
			return 0, nil
		}
		tkn.skip(1)
		return ';', nil
	case ch == eofChar:
		return 0, nil
	default:
		if ch == '.' && isDigit(tkn.peek(1)) {
			return tkn.scanNumber()
		}

		tkn.skip(1)
		switch ch {
		case '=', ',', '(', ')', '+', '*', '%', '^', '~':
			return int(ch), nil
		case '&':
			if tkn.cur() == '&' {
				tkn.skip(1)
				return AND, nil
			}
			return int(ch), nil
		case '|':
			if tkn.cur() == '|' {
				tkn.skip(1)
				return OR, nil
			}
			return int(ch), nil
		case '?':
			tkn.posVarIndex++
			var buf bytes2.Buffer
			fmt.Fprintf(&buf, ":v%d", tkn.posVarIndex)
			return VALUE_ARG, buf.Bytes()
		case '.':
			return int(ch), nil
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
				return int(ch), nil
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
					return JSON_UNQUOTE_EXTRACT_OP, nil
				}
				return JSON_EXTRACT_OP, nil
			}
			return int(ch), nil
		case '<':
			switch tkn.cur() {
			case '>':
				tkn.skip(1)
				return NE, nil
			case '<':
				tkn.skip(1)
				return SHIFT_LEFT, nil
			case '=':
				tkn.skip(1)
				switch tkn.cur() {
				case '>':
					tkn.skip(1)
					return NULL_SAFE_EQUAL, nil
				default:
					return LE, nil
				}
			default:
				return int(ch), nil
			}
		case '>':
			switch tkn.cur() {
			case '=':
				tkn.skip(1)
				return GE, nil
			case '>':
				tkn.skip(1)
				return SHIFT_RIGHT, nil
			default:
				return int(ch), nil
			}
		case '!':
			if tkn.cur() == '=' {
				tkn.skip(1)
				return NE, nil
			}
			return int(ch), nil
		case '\'', '"':
			return tkn.scanString(ch, STRING)
		case '`':
			return tkn.scanLiteralIdentifier()
		default:
			return LEX_ERROR, []byte{byte(ch)}
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

func (tkn *Tokenizer) skipBlank() {
	ch := tkn.cur()
	for ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t' {
		tkn.skip(1)
		ch = tkn.cur()
	}
}

func (tkn *Tokenizer) scanIdentifier(isVariable bool) (int, []byte) {
	start := tkn.Pos
	tkn.skip(1)

	for {
		ch := tkn.cur()
		if !isLetter(ch) && !isDigit(ch) && ch != '@' && !(isVariable && isCarat(ch)) {
			break
		}
		if ch == '@' {
			isVariable = true
		}
		tkn.skip(1)
	}
	keywordName := tkn.buf[start:tkn.Pos]

	if keywordID, found := keywordLookupTable.Lookup(keywordName); found {
		return keywordID, keywordName
	}
	// dual must always be case-insensitive
	if keywordASCIIMatch(keywordName, "dual") {
		return ID, []byte("dual")
	}
	return ID, keywordName
}

func (tkn *Tokenizer) scanHex() (int, []byte) {
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

func (tkn *Tokenizer) scanBitLiteral() (int, []byte) {
	start := tkn.Pos
	tkn.scanMantissa(2)
	bit := tkn.buf[start:tkn.Pos]
	if tkn.cur() != '\'' {
		return LEX_ERROR, bit
	}
	tkn.skip(1)
	return BIT_LITERAL, bit
}

func (tkn *Tokenizer) scanLiteralIdentifierSlow(start int) (int, []byte) {
	var buf bytes2.Buffer
	buf.Write(tkn.buf[start:tkn.Pos])

	tkn.skip(1)

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
			return LEX_ERROR, buf.Bytes()
		default:
			buf.WriteByte(byte(tkn.cur()))
			// keep scanning
		}
		tkn.skip(1)
	}
	return ID, buf.Bytes()
}

func (tkn *Tokenizer) scanLiteralIdentifier() (int, []byte) {
	start := tkn.Pos
	for {
		switch tkn.cur() {
		case '`':
			if tkn.peek(1) != '`' {
				if tkn.Pos == start {
					return LEX_ERROR, nil
				}
				tkn.skip(1)
				return ID, tkn.buf[start : tkn.Pos-1]
			}
			return tkn.scanLiteralIdentifierSlow(start)
		case eofChar:
			// Premature EOF.
			return LEX_ERROR, tkn.buf[start:tkn.Pos]
		default:
			tkn.skip(1)
		}
	}
}

func (tkn *Tokenizer) scanBindVar() (int, []byte) {
	start := tkn.Pos
	token := VALUE_ARG

	tkn.skip(1)
	if tkn.cur() == ':' {
		token = LIST_ARG
		tkn.skip(1)
	}
	if !isLetter(tkn.cur()) {
		return LEX_ERROR, tkn.buf[start:tkn.Pos]
	}
	for {
		ch := tkn.cur()
		if !isLetter(ch) && !isDigit(ch) && ch != '.' {
			break
		}
		tkn.skip(1)
	}
	return token, tkn.buf[start:tkn.Pos]
}

func (tkn *Tokenizer) scanMantissa(base int) {
	for digitVal(tkn.cur()) < base {
		tkn.skip(1)
	}
}

func (tkn *Tokenizer) scanNumber() (int, []byte) {
	start := tkn.Pos
	token := INTEGRAL

	if tkn.cur() == '.' {
		token = FLOAT
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
	}

	tkn.scanMantissa(10)

	if tkn.cur() == '.' {
		token = FLOAT
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
	// A letter cannot immediately follow a number.
	if isLetter(tkn.cur()) {
		return LEX_ERROR, tkn.buf[start:tkn.Pos]
	}

	return token, tkn.buf[start:tkn.Pos]
}

func (tkn *Tokenizer) scanString(delim uint16, typ int) (int, []byte) {
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
			var buffer bytes2.Buffer
			buffer.Write(tkn.buf[start:tkn.Pos])
			return tkn.scanStringSlow(&buffer, delim, typ)

		case eofChar:
			return LEX_ERROR, tkn.buf[start:tkn.Pos]
		}

		tkn.skip(1)
	}
}

func (tkn *Tokenizer) scanStringSlow(buffer *bytes2.Buffer, delim uint16, typ int) (int, []byte) {
	for {
		ch := tkn.cur()
		if ch == eofChar {
			// Unterminated string.
			return LEX_ERROR, buffer.Bytes()
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

			buffer.Write(tkn.buf[start:tkn.Pos])
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
				return LEX_ERROR, buffer.Bytes()
			}
			if decodedChar := sqltypes.SQLDecodeMap[byte(tkn.cur())]; decodedChar == sqltypes.DontEscape {
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

	return typ, buffer.Bytes()
}

func (tkn *Tokenizer) scanCommentType1(prefixLen int) (int, []byte) {
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

func (tkn *Tokenizer) scanCommentType2() (int, []byte) {
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

func (tkn *Tokenizer) scanMySQLSpecificComment() (int, []byte) {
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

	// TODO: do not cast to string
	commentVersion, sql := ExtractMysqlComment(string(tkn.buf[start:tkn.Pos]))

	if MySQLVersion >= commentVersion {
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
	tkn.nesting = 0
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
