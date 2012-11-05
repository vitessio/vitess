// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"bytes"
	"fmt"
	"io"
	"unicode"

	"code.google.com/p/vitess/go/sqltypes"
)

const EOFCHAR = 0x100

type Tokenizer struct {
	InStream      io.ByteReader
	AllowComments bool
	ForceEOF      bool
	lastChar      uint16
	position      int
	lastToken     *Node
	LastError     string
	ParseTree     *Node
}

func NewStringTokenizer(s string) *Tokenizer {
	b := bytes.NewBufferString(s)
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
	"on":            ON,
	"into":          INTO,

	"distinct":  DISTINCT,
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

	"create": CREATE,
	"alter":  ALTER,
	"rename": RENAME,
	"drop":   DROP,
	"table":  TABLE,
	"index":  INDEX,
	"to":     TO,
	"ignore": IGNORE,
	"if":     IF,
	"unique": UNIQUE,
	"using":  USING,
}

func (self *Tokenizer) Lex(lval *yySymType) int {
	parseNode := self.Scan()
	for parseNode.Type == COMMENT {
		if self.AllowComments {
			break
		}
		parseNode = self.Scan()
	}
	self.lastToken = parseNode
	lval.node = parseNode
	return parseNode.Type
}

func (self *Tokenizer) Error(err string) {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	fmt.Fprintf(buf, "Error at position %v: %s", self.position, string(self.lastToken.Value))
	self.LastError = buf.String()
}

func (self *Tokenizer) Scan() (parseNode *Node) {
	defer func() {
		if x := recover(); x != nil {
			err := x.(ParserError)
			parseNode = NewSimpleParseNode(LEX_ERROR, err.Error())
		}
	}()

	if self.ForceEOF {
		return NewSimpleParseNode(0, "")
	}

	if self.lastChar == 0 {
		self.Next()
	}
	self.skipBlank()
	switch ch := self.lastChar; {
	case isLetter(ch):
		return self.scanIdentifier(ID)
	case isDigit(ch):
		return self.scanNumber(false)
	case ch == ':':
		return self.scanBindVar(VALUE_ARG)
	default:
		self.Next()
		switch ch {
		case EOFCHAR:
			return NewSimpleParseNode(0, "")
		case '=', ',', ';', '(', ')', '+', '*', '%', '&', '|', '^', '~':
			return NewSimpleParseNode(int(ch), string(ch))
		case '.':
			if isDigit(self.lastChar) {
				return self.scanNumber(true)
			} else {
				return NewSimpleParseNode(int(ch), string(ch))
			}
		case '/':
			switch self.lastChar {
			case '/':
				self.Next()
				return self.scanCommentType1("//")
			case '*':
				self.Next()
				return self.scanCommentType2()
			default:
				return NewSimpleParseNode(int(ch), string(ch))
			}
		case '-':
			if self.lastChar == '-' {
				self.Next()
				return self.scanCommentType1("--")
			} else {
				return NewSimpleParseNode(int(ch), string(ch))
			}
		case '<':
			switch self.lastChar {
			case '>':
				self.Next()
				return NewSimpleParseNode(NE, "<>")
			case '=':
				self.Next()
				switch self.lastChar {
				case '>':
					self.Next()
					return NewSimpleParseNode(NULL_SAFE_EQUAL, "<=>")
				default:
					return NewSimpleParseNode(LE, "<=")
				}
			default:
				return NewSimpleParseNode(int(ch), string(ch))
			}
		case '>':
			if self.lastChar == '=' {
				self.Next()
				return NewSimpleParseNode(GE, ">=")
			} else {
				return NewSimpleParseNode(int(ch), string(ch))
			}
		case '!':
			if self.lastChar == '=' {
				self.Next()
				return NewSimpleParseNode(NE, "!=")
			} else {
				return NewSimpleParseNode(LEX_ERROR, "Unexpected character '!'")
			}
		case '\'', '"':
			return self.scanString(ch)
		case '`':
			tok := self.scanString(ch)
			tok.Type = ID
			return tok
		default:
			return NewSimpleParseNode(LEX_ERROR, fmt.Sprintf("Unexpected character '%c'", ch))
		}
	}
	return NewSimpleParseNode(LEX_ERROR, "Internal Error")
}

func (self *Tokenizer) skipBlank() {
	ch := self.lastChar
	for ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t' {
		self.Next()
		ch = self.lastChar
	}
}

func (self *Tokenizer) scanIdentifier(Type int) *Node {
	buffer := bytes.NewBuffer(make([]byte, 0, 8))
	buffer.WriteByte(byte(unicode.ToLower(rune(self.lastChar))))
	for self.Next(); isLetter(self.lastChar) || isDigit(self.lastChar); self.Next() {
		buffer.WriteByte(byte(unicode.ToLower(rune(self.lastChar))))
	}
	if keywordId, found := keywords[buffer.String()]; found {
		return NewParseNode(keywordId, buffer.Bytes())
	}
	return NewParseNode(Type, buffer.Bytes())
}

func (self *Tokenizer) scanBindVar(Type int) *Node {
	buffer := bytes.NewBuffer(make([]byte, 0, 8))
	buffer.WriteByte(byte(unicode.ToLower(rune(self.lastChar))))
	for self.Next(); isLetter(self.lastChar) || isDigit(self.lastChar) || self.lastChar == '.'; self.Next() {
		buffer.WriteByte(byte(self.lastChar))
	}
	if buffer.Len() == 1 {
		return NewParseNode(LEX_ERROR, buffer.Bytes())
	}
	if keywordId, found := keywords[buffer.String()]; found {
		return NewParseNode(keywordId, buffer.Bytes())
	}
	return NewParseNode(Type, buffer.Bytes())
}

func (self *Tokenizer) scanMantissa(base int, buffer *bytes.Buffer) {
	for digitVal(self.lastChar) < base {
		self.ConsumeNext(buffer)
	}
}

func (self *Tokenizer) scanNumber(seenDecimalPoint bool) *Node {
	buffer := bytes.NewBuffer(make([]byte, 0, 8))
	if seenDecimalPoint {
		self.scanMantissa(10, buffer)
		goto exponent
	}

	if self.lastChar == '0' {
		// int or float
		self.ConsumeNext(buffer)
		if self.lastChar == 'x' || self.lastChar == 'X' {
			// hexadecimal int
			self.ConsumeNext(buffer)
			self.scanMantissa(16, buffer)
		} else {
			// octal int or float
			seenDecimalDigit := false
			self.scanMantissa(8, buffer)
			if self.lastChar == '8' || self.lastChar == '9' {
				// illegal octal int or float
				seenDecimalDigit = true
				self.scanMantissa(10, buffer)
			}
			if self.lastChar == '.' || self.lastChar == 'e' || self.lastChar == 'E' {
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
	self.scanMantissa(10, buffer)

fraction:
	if self.lastChar == '.' {
		self.ConsumeNext(buffer)
		self.scanMantissa(10, buffer)
	}

exponent:
	if self.lastChar == 'e' || self.lastChar == 'E' {
		self.ConsumeNext(buffer)
		if self.lastChar == '+' || self.lastChar == '-' {
			self.ConsumeNext(buffer)
		}
		self.scanMantissa(10, buffer)
	}

exit:
	return NewParseNode(NUMBER, buffer.Bytes())
}

func (self *Tokenizer) scanString(delim uint16) *Node {
	buffer := bytes.NewBuffer(make([]byte, 0, 8))
	for {
		ch := self.lastChar
		self.Next()
		if ch == delim {
			if self.lastChar == delim {
				self.Next()
			} else {
				break
			}
		} else if ch == '\\' {
			if self.lastChar == EOFCHAR {
				return NewParseNode(LEX_ERROR, buffer.Bytes())
			}
			if decodedChar := sqltypes.SqlDecodeMap[byte(self.lastChar)]; decodedChar == sqltypes.DONTESCAPE {
				ch = self.lastChar
			} else {
				ch = uint16(decodedChar)
			}
			self.Next()
		}
		if ch == EOFCHAR {
			return NewParseNode(LEX_ERROR, buffer.Bytes())
		}
		buffer.WriteByte(byte(ch))
	}
	return NewParseNode(STRING, buffer.Bytes())
}

func (self *Tokenizer) scanCommentType1(prefix string) *Node {
	buffer := bytes.NewBuffer(make([]byte, 0, 8))
	buffer.WriteString(prefix)
	for self.lastChar != EOFCHAR {
		if self.lastChar == '\n' {
			self.ConsumeNext(buffer)
			break
		}
		self.ConsumeNext(buffer)
	}
	return NewParseNode(COMMENT, buffer.Bytes())
}

func (self *Tokenizer) scanCommentType2() *Node {
	buffer := bytes.NewBuffer(make([]byte, 0, 8))
	buffer.WriteString("/*")
	for {
		if self.lastChar == '*' {
			self.ConsumeNext(buffer)
			if self.lastChar == '/' {
				self.ConsumeNext(buffer)
				buffer.WriteByte(' ')
				break
			}
		}
		self.ConsumeNext(buffer)
	}
	return NewParseNode(COMMENT, buffer.Bytes())
}

func (self *Tokenizer) ConsumeNext(buffer *bytes.Buffer) {
	// Never consume an EOF
	if self.lastChar == EOFCHAR {
		panic(NewParserError("Unexpected EOF"))
	}
	buffer.WriteByte(byte(self.lastChar))
	self.Next()
}

func (self *Tokenizer) Next() {
	if ch, err := self.InStream.ReadByte(); err != nil {
		if err != io.EOF {
			panic(NewParserError("%s", err.Error()))
		} else {
			self.lastChar = EOFCHAR
		}
	} else {
		self.lastChar = uint16(ch)
	}
	self.position++
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
