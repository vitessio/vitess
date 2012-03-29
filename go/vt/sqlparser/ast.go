/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package sqlparser

import (
	"bytes"
	"fmt"
)

type ParserError struct {
	Message string
}

func NewParserError(format string, args ...interface{}) ParserError {
	return ParserError{fmt.Sprintf(format, args...)}
}

func (self ParserError) Error() string {
	return self.Message
}

func handleError(err *error) {
	if x := recover(); x != nil {
		*err = x.(ParserError)
	}
}

type Node struct {
	Type  int
	Value []byte
	Sub   []*Node
}

func Parse(sql string) (*Node, error) {
	tokenizer := NewStringTokenizer(sql)
	if yyParse(tokenizer) != 0 {
		return nil, NewParserError("%s", tokenizer.LastError)
	}
	return tokenizer.ParseTree, nil
}

func NewSimpleParseNode(Type int, value string) *Node {
	return &Node{Type: Type, Value: []byte(value)}
}

func NewParseNode(Type int, value []byte) *Node {
	return &Node{Type: Type, Value: value}
}

func (self *Node) PushTwo(left *Node, right *Node) *Node {
	self.Push(left)
	return self.Push(right)
}

func (self *Node) Push(value *Node) *Node {
	if self.Sub == nil {
		self.Sub = make([]*Node, 0, 2)
	}
	self.Sub = append(self.Sub, value)
	return self
}

func (self *Node) Pop() *Node {
	self.Sub = self.Sub[:len(self.Sub)-1]
	return self
}

func (self *Node) At(index int) *Node {
	return self.Sub[index]
}

func (self *Node) Set(index int, val *Node) {
	self.Sub[index] = val
}

func (self *Node) Len() int {
	return len(self.Sub)
}

func (self *Node) String() (out string) {
	buf := NewTrackedBuffer()
	self.Format(buf)
	return buf.String()
}

func (self *Node) Format(buf *TrackedBuffer) {
	switch self.Type {
	case SELECT:
		Fprintf(buf, "select %v%v%v from %v%v%v%v%v%v%v",
			self.At(SELECT_COMMENT_OFFSET),
			self.At(SELECT_DISTINCT_OFFSET),
			self.At(SELECT_EXPR_OFFSET),
			self.At(SELECT_FROM_OFFSET),
			self.At(SELECT_WHERE_OFFSET),
			self.At(SELECT_GROUP_OFFSET),
			self.At(SELECT_HAVING_OFFSET),
			self.At(SELECT_ORDER_OFFSET),
			self.At(SELECT_LIMIT_OFFSET),
			self.At(SELECT_FOR_UPDATE_OFFSET),
		)
	case INSERT:
		Fprintf(buf, "insert %vinto %v%v %v%v",
			self.At(INSERT_COMMENT_OFFSET),
			self.At(INSERT_TABLE_OFFSET),
			self.At(INSERT_COLUMN_LIST_OFFSET),
			self.At(INSERT_VALUES_OFFSET),
			self.At(INSERT_ON_DUP_OFFSET),
		)
	case UPDATE:
		Fprintf(buf, "update %v%v set %v%v%v%v",
			self.At(UPDATE_COMMENT_OFFSET),
			self.At(UPDATE_TABLE_OFFSET),
			self.At(UPDATE_LIST_OFFSET),
			self.At(UPDATE_WHERE_OFFSET),
			self.At(UPDATE_ORDER_OFFSET),
			self.At(UPDATE_LIMIT_OFFSET),
		)
	case DELETE:
		Fprintf(buf, "delete %vfrom %v%v%v%v",
			self.At(DELETE_COMMENT_OFFSET),
			self.At(DELETE_TABLE_OFFSET),
			self.At(DELETE_WHERE_OFFSET),
			self.At(DELETE_ORDER_OFFSET),
			self.At(DELETE_LIMIT_OFFSET),
		)
	case SET:
		Fprintf(buf, "set %v%v", self.At(0), self.At(1))
	case CREATE, ALTER, DROP:
		Fprintf(buf, "%s table %v", self.Value, self.At(0))
	case RENAME:
		Fprintf(buf, "%s table %v %v", self.Value, self.At(0), self.At(1))
	case TABLE_EXPR:
		Fprintf(buf, "%v", self.At(0))
		if self.At(1).Len() == 1 {
			Fprintf(buf, " as %v", self.At(1).At(0))
		}
		Fprintf(buf, "%v", self.At(2))
	case USE:
		if self.Len() != 0 {
			Fprintf(buf, " use index %v", self.At(0))
		}
	case WHERE, HAVING:
		if self.Len() > 0 {
			Fprintf(buf, " %s %v", self.Value, self.At(0))
		}
	case ORDER, GROUP:
		if self.Len() > 0 {
			Fprintf(buf, " %s by %v", self.Value, self.At(0))
		}
	case LIMIT:
		if self.Len() > 0 {
			Fprintf(buf, " %s %v", self.Value, self.At(0))
			if self.Len() > 1 {
				Fprintf(buf, ", %v", self.At(1))
			}
		}
	case COLUMN_LIST:
		if self.Len() > 0 {
			Fprintf(buf, "(%v", self.At(0))
			for i := 1; i < self.Len(); i++ {
				Fprintf(buf, ", %v", self.At(i))
			}
			buf.WriteByte(')')
		}
	case NODE_LIST:
		if self.Len() > 0 {
			Fprintf(buf, "%v", self.At(0))
			for i := 1; i < self.Len(); i++ {
				Fprintf(buf, ", %v", self.At(i))
			}
		}
	case COMMENT_LIST:
		if self.Len() > 0 {
			for i := 0; i < self.Len(); i++ {
				Fprintf(buf, "%v", self.At(i))
			}
		}
	case JOIN, LEFT, RIGHT, CROSS, NATURAL:
		Fprintf(buf, "%v %s %v", self.At(0), self.Value, self.At(1))
		if self.Len() > 2 {
			Fprintf(buf, " on %v", self.At(2))
		}
	case DUPLICATE:
		if self.Len() != 0 {
			Fprintf(buf, " on duplicate key update %v", self.At(0))
		}
	case NUMBER, NULL, ID, SELECT_STAR, NO_DISTINCT, COMMENT, FOR_UPDATE, NOT_FOR_UPDATE, TABLE:
		Fprintf(buf, "%s", self.Value)
	case VALUE_ARG:
		buf.bind_locations = append(buf.bind_locations, BindLocation{buf.Len(), len(self.Value)})
		Fprintf(buf, "%s", self.Value)
	case STRING:
		buf.WriteByte('\'')
		for _, ch := range self.Value {
			if encodedChar, ok := escapeEncodeMap[ch]; ok {
				buf.WriteByte('\\')
				buf.WriteByte(encodedChar)
			} else {
				buf.WriteByte(ch)
			}
		}
		buf.WriteByte('\'')
	case '+', '-', '*', '/', '%', '&', '|', '^', '.':
		Fprintf(buf, "%v%s%v", self.At(0), self.Value, self.At(1))
	case '=', '>', '<', GE, LE, NE, NULL_SAFE_EQUAL, AS, AND, OR, UNION, UNION_ALL, MINUS, EXCEPT, INTERSECT, LIKE, NOT_LIKE, IN, NOT_IN:
		Fprintf(buf, "%v %s %v", self.At(0), self.Value, self.At(1))
	case '(':
		Fprintf(buf, "(%v)", self.At(0))
	case EXISTS:
		Fprintf(buf, "%s (%v)", self.Value, self.At(0))
	case FUNCTION:
		if self.Len() == 2 { // DISTINCT
			Fprintf(buf, "%s(%v%v)", self.Value, self.At(0), self.At(1))
		} else {
			Fprintf(buf, "%s(%v)", self.Value, self.At(0))
		}
	case UPLUS, UMINUS, '~':
		Fprintf(buf, "%s%v", self.Value, self.At(0))
	case NOT, VALUES:
		Fprintf(buf, "%s %v", self.Value, self.At(0))
	case ASC, DESC, IS_NULL, IS_NOT_NULL:
		Fprintf(buf, "%v %s", self.At(0), self.Value)
	case BETWEEN, NOT_BETWEEN:
		Fprintf(buf, "%v %s %v and %v", self.At(0), self.Value, self.At(1), self.At(2))
	case DISTINCT:
		Fprintf(buf, "%s ", self.Value)
	default:
		Fprintf(buf, "Unknown: %s", self.Value)
	}
}

// Mimics fmt.Fprintf, but limited to Value & Node
func Fprintf(buf *TrackedBuffer, format string, values ...interface{}) {
	end := len(format)
	fieldnum := 0
	for i := 0; i < end; {
		lasti := i
		for i < end && format[i] != '%' {
			i++
		}
		if i > lasti {
			buf.WriteString(format[lasti:i])
		}
		if i >= end {
			break
		}
		i++ // '%'
		switch format[i] {
		case 's':
			nodeValue := values[fieldnum].([]byte)
			buf.Write(nodeValue)
		case 'v':
			node := values[fieldnum].(*Node)
			node.Format(buf)
		default:
			panic("unexpected")
		}
		fieldnum++
		i++
	}
}

func (self *Node) TreeString() string {
	buf := bytes.NewBuffer(make([]byte, 0, 8))
	self.NodeString(0, buf)
	return buf.String()
}

func (self *Node) NodeString(level int, buf *bytes.Buffer) {
	for i := 0; i < level; i++ {
		buf.WriteString("|-")
	}
	buf.Write(self.Value)
	buf.WriteByte('\n')
	for i := 0; i < self.Len(); i++ {
		self.At(i).NodeString(level+1, buf)
	}
}
