// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/youtube/vitess/go/sqltypes"
)

const EOFCHAR = 0x100

type Tokenizer struct {
	InStream      *strings.Reader
	AllowComments bool
	ForceEOF      bool
	lastChar      uint16
	position      int
	errorToken    []byte
	LastError     string
	posVarIndex   int
	ParseTree     Statement
}

func NewStringTokenizer(s string) *Tokenizer {
	b := strings.NewReader(s)
	return &Tokenizer{InStream: b}
}

var keywords = map[string]int{
	"select": SELECT,
	"insert": INSERT,
	"update": UPDATE,
	"delete": DELETE,
	"from":   FROM,
	"where":  WHERE,
	"group":  GROUP,
	"having": HAVING,
	"order":  ORDER,
	"by":     BY,
	"limit":  LIMIT,
	"for":    FOR,

	"union":     UNION,
	"all":       ALL,
	"minus":     MINUS,
	"except":    EXCEPT,
	"intersect": INTERSECT,

	"join":          JOIN,
	"straight_join": STRAIGHT_JOIN,
	"left":          LEFT,
	"right":         RIGHT,
	"inner":         INNER,
	"outer":         OUTER,
	"cross":         CROSS,
	"natural":       NATURAL,
	"use":           USE,
	"force":         FORCE,
	"on":            ON,
	"into":          INTO,

	"distinct":  DISTINCT,
	"case":      CASE,
	"when":      WHEN,
	"then":      THEN,
	"else":      ELSE,
	"end":       END,
	"as":        AS,
	"and":       AND,
	"or":        OR,
	"not":       NOT,
	"exists":    EXISTS,
	"in":        IN,
	"is":        IS,
	"like":      LIKE,
	"between":   BETWEEN,
	"null":      NULL,
	"asc":       ASC,
	"desc":      DESC,
	"values":    VALUES,
	"duplicate": DUPLICATE,
	"key":       KEY,
	"default":   DEFAULT,
	"set":       SET,
	"lock":      LOCK,

	"create": CREATE,
	"alter":  ALTER,
	"rename": RENAME,
	"drop":   DROP,
	"table":  TABLE,
	"index":  INDEX,
	"view":   VIEW,
	"to":     TO,
	"ignore": IGNORE,
	"if":     IF,
	"unique": UNIQUE,
	"using":  USING,
}

func (tkn *Tokenizer) Lex(lval *yySymType) int {
	parseNode := tkn.Scan()
	tkn.errorToken = parseNode.Value
	for parseNode.Type == COMMENT {
		if tkn.AllowComments {
			break
		}
		parseNode = tkn.Scan()
	}
	lval.node = parseNode
	return parseNode.Type
}

func (tkn *Tokenizer) Error(err string) {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	if tkn.errorToken != nil {
		fmt.Fprintf(buf, "%s at position %v near %s", err, tkn.position, tkn.errorToken)
	} else {
		fmt.Fprintf(buf, "%s at position %v", err, tkn.position)
	}
	tkn.LastError = buf.String()
}

func (tkn *Tokenizer) Scan() (parseNode *Node) {
	if tkn.ForceEOF {
		return NewSimpleParseNode(0, "")
	}

	if tkn.lastChar == 0 {
		tkn.next()
	}
	tkn.skipBlank()
	switch ch := tkn.lastChar; {
	case isLetter(ch):
		return tkn.scanIdentifier(ID)
	case isDigit(ch):
		return tkn.scanNumber(false)
	case ch == ':':
		return tkn.scanBindVar(VALUE_ARG)
	default:
		tkn.next()
		switch ch {
		case EOFCHAR:
			return NewSimpleParseNode(0, "")
		case '=', ',', ';', '(', ')', '+', '*', '%', '&', '|', '^', '~':
			return NewSimpleParseNode(int(ch), string(ch))
		case '?':
			tkn.posVarIndex++
			return NewSimpleParseNode(VALUE_ARG, fmt.Sprintf(":v%d", tkn.posVarIndex))
		case '.':
			if isDigit(tkn.lastChar) {
				return tkn.scanNumber(true)
			} else {
				return NewSimpleParseNode(int(ch), string(ch))
			}
		case '/':
			switch tkn.lastChar {
			case '/':
				tkn.next()
				return tkn.scanCommentType1("//")
			case '*':
				tkn.next()
				return tkn.scanCommentType2()
			default:
				return NewSimpleParseNode(int(ch), string(ch))
			}
		case '-':
			if tkn.lastChar == '-' {
				tkn.next()
				return tkn.scanCommentType1("--")
			} else {
				return NewSimpleParseNode(int(ch), string(ch))
			}
		case '<':
			switch tkn.lastChar {
			case '>':
				tkn.next()
				return NewSimpleParseNode(NE, "<>")
			case '=':
				tkn.next()
				switch tkn.lastChar {
				case '>':
					tkn.next()
					return NewSimpleParseNode(NULL_SAFE_EQUAL, "<=>")
				default:
					return NewSimpleParseNode(LE, "<=")
				}
			default:
				return NewSimpleParseNode(int(ch), string(ch))
			}
		case '>':
			if tkn.lastChar == '=' {
				tkn.next()
				return NewSimpleParseNode(GE, ">=")
			} else {
				return NewSimpleParseNode(int(ch), string(ch))
			}
		case '!':
			if tkn.lastChar == '=' {
				tkn.next()
				return NewSimpleParseNode(NE, "!=")
			} else {
				return NewSimpleParseNode(LEX_ERROR, "!")
			}
		case '\'', '"':
			return tkn.scanString(ch)
		case '`':
			tok := tkn.scanString(ch)
			tok.Type = ID
			return tok
		default:
			return NewSimpleParseNode(LEX_ERROR, fmt.Sprintf("%c", ch))
		}
	}
}

func (tkn *Tokenizer) skipBlank() {
	ch := tkn.lastChar
	for ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t' {
		tkn.next()
		ch = tkn.lastChar
	}
}

func (tkn *Tokenizer) scanIdentifier(Type int) *Node {
	buffer := bytes.NewBuffer(make([]byte, 0, 8))
	buffer.WriteByte(byte(tkn.lastChar))
	for tkn.next(); isLetter(tkn.lastChar) || isDigit(tkn.lastChar); tkn.next() {
		buffer.WriteByte(byte(tkn.lastChar))
	}
	lowered := bytes.ToLower(buffer.Bytes())
	if keywordId, found := keywords[string(lowered)]; found {
		return NewParseNode(keywordId, lowered)
	}
	return NewParseNode(Type, buffer.Bytes())
}

func (tkn *Tokenizer) scanBindVar(Type int) *Node {
	buffer := bytes.NewBuffer(make([]byte, 0, 8))
	buffer.WriteByte(byte(tkn.lastChar))
	for tkn.next(); isLetter(tkn.lastChar) || isDigit(tkn.lastChar) || tkn.lastChar == '.'; tkn.next() {
		buffer.WriteByte(byte(tkn.lastChar))
	}
	if buffer.Len() == 1 {
		return NewParseNode(LEX_ERROR, buffer.Bytes())
	}
	return NewParseNode(Type, buffer.Bytes())
}

func (tkn *Tokenizer) scanMantissa(base int, buffer *bytes.Buffer) {
	for digitVal(tkn.lastChar) < base {
		tkn.ConsumeNext(buffer)
	}
}

func (tkn *Tokenizer) scanNumber(seenDecimalPoint bool) *Node {
	buffer := bytes.NewBuffer(make([]byte, 0, 8))
	if seenDecimalPoint {
		buffer.WriteByte('.')
		tkn.scanMantissa(10, buffer)
		goto exponent
	}

	if tkn.lastChar == '0' {
		// int or float
		tkn.ConsumeNext(buffer)
		if tkn.lastChar == 'x' || tkn.lastChar == 'X' {
			// hexadecimal int
			tkn.ConsumeNext(buffer)
			tkn.scanMantissa(16, buffer)
		} else {
			// octal int or float
			seenDecimalDigit := false
			tkn.scanMantissa(8, buffer)
			if tkn.lastChar == '8' || tkn.lastChar == '9' {
				// illegal octal int or float
				seenDecimalDigit = true
				tkn.scanMantissa(10, buffer)
			}
			if tkn.lastChar == '.' || tkn.lastChar == 'e' || tkn.lastChar == 'E' {
				goto fraction
			}
			// octal int
			if seenDecimalDigit {
				return NewParseNode(LEX_ERROR, buffer.Bytes())
			}
		}
		goto exit
	}

	// decimal int or float
	tkn.scanMantissa(10, buffer)

fraction:
	if tkn.lastChar == '.' {
		tkn.ConsumeNext(buffer)
		tkn.scanMantissa(10, buffer)
	}

exponent:
	if tkn.lastChar == 'e' || tkn.lastChar == 'E' {
		tkn.ConsumeNext(buffer)
		if tkn.lastChar == '+' || tkn.lastChar == '-' {
			tkn.ConsumeNext(buffer)
		}
		tkn.scanMantissa(10, buffer)
	}

exit:
	return NewParseNode(NUMBER, buffer.Bytes())
}

func (tkn *Tokenizer) scanString(delim uint16) *Node {
	buffer := bytes.NewBuffer(make([]byte, 0, 8))
	for {
		ch := tkn.lastChar
		tkn.next()
		if ch == delim {
			if tkn.lastChar == delim {
				tkn.next()
			} else {
				break
			}
		} else if ch == '\\' {
			if tkn.lastChar == EOFCHAR {
				return NewParseNode(LEX_ERROR, buffer.Bytes())
			}
			if decodedChar := sqltypes.SqlDecodeMap[byte(tkn.lastChar)]; decodedChar == sqltypes.DONTESCAPE {
				ch = tkn.lastChar
			} else {
				ch = uint16(decodedChar)
			}
			tkn.next()
		}
		if ch == EOFCHAR {
			return NewParseNode(LEX_ERROR, buffer.Bytes())
		}
		buffer.WriteByte(byte(ch))
	}
	return NewParseNode(STRING, buffer.Bytes())
}

func (tkn *Tokenizer) scanCommentType1(prefix string) *Node {
	buffer := bytes.NewBuffer(make([]byte, 0, 8))
	buffer.WriteString(prefix)
	for tkn.lastChar != EOFCHAR {
		if tkn.lastChar == '\n' {
			tkn.ConsumeNext(buffer)
			break
		}
		tkn.ConsumeNext(buffer)
	}
	return NewParseNode(COMMENT, buffer.Bytes())
}

func (tkn *Tokenizer) scanCommentType2() *Node {
	buffer := bytes.NewBuffer(make([]byte, 0, 8))
	buffer.WriteString("/*")
	for {
		if tkn.lastChar == '*' {
			tkn.ConsumeNext(buffer)
			if tkn.lastChar == '/' {
				tkn.ConsumeNext(buffer)
				break
			}
			continue
		}
		if tkn.lastChar == EOFCHAR {
			return NewParseNode(LEX_ERROR, buffer.Bytes())
		}
		tkn.ConsumeNext(buffer)
	}
	return NewParseNode(COMMENT, buffer.Bytes())
}

func (tkn *Tokenizer) ConsumeNext(buffer *bytes.Buffer) {
	if tkn.lastChar == EOFCHAR {
		// This should never happen.
		panic("unexpected EOF")
	}
	buffer.WriteByte(byte(tkn.lastChar))
	tkn.next()
}

func (tkn *Tokenizer) next() {
	if ch, err := tkn.InStream.ReadByte(); err != nil {
		// Only EOF is possible.
		tkn.lastChar = EOFCHAR
	} else {
		tkn.lastChar = uint16(ch)
	}
	tkn.position++
}

func isLetter(ch uint16) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_' || ch == '@'
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
