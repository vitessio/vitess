// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"bytes"
	"fmt"

	"code.google.com/p/vitess/go/sqltypes"
)

type ParserError struct {
	Message string
}

func NewParserError(format string, args ...interface{}) ParserError {
	return ParserError{fmt.Sprintf(format, args...)}
}

func (err ParserError) Error() string {
	return err.Message
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

func (node *Node) PushTwo(left *Node, right *Node) *Node {
	node.Push(left)
	return node.Push(right)
}

func (node *Node) Push(value *Node) *Node {
	if node.Sub == nil {
		node.Sub = make([]*Node, 0, 2)
	}
	node.Sub = append(node.Sub, value)
	return node
}

func (node *Node) Pop() *Node {
	node.Sub = node.Sub[:len(node.Sub)-1]
	return node
}

func (node *Node) At(index int) *Node {
	return node.Sub[index]
}

func (node *Node) Set(index int, val *Node) {
	node.Sub[index] = val
}

func (node *Node) Len() int {
	return len(node.Sub)
}

func (node *Node) String() (out string) {
	buf := NewTrackedBuffer(nil)
	FormatNode(buf, node)
	return buf.String()
}

func (node *Node) TreeString() string {
	buf := bytes.NewBuffer(make([]byte, 0, 8))
	node.NodeString(0, buf)
	return buf.String()
}

func (node *Node) NodeString(level int, buf *bytes.Buffer) {
	for i := 0; i < level; i++ {
		buf.WriteString("|-")
	}
	buf.Write(node.Value)
	buf.WriteByte('\n')
	for i := 0; i < node.Len(); i++ {
		node.At(i).NodeString(level+1, buf)
	}
}

func FormatNode(buf *TrackedBuffer, node *Node) {
	switch node.Type {
	case SELECT:
		buf.Fprintf("select %v%v%v from %v%v%v%v%v%v%v",
			node.At(SELECT_COMMENT_OFFSET),
			node.At(SELECT_DISTINCT_OFFSET),
			node.At(SELECT_EXPR_OFFSET),
			node.At(SELECT_FROM_OFFSET),
			node.At(SELECT_WHERE_OFFSET),
			node.At(SELECT_GROUP_OFFSET),
			node.At(SELECT_HAVING_OFFSET),
			node.At(SELECT_ORDER_OFFSET),
			node.At(SELECT_LIMIT_OFFSET),
			node.At(SELECT_FOR_UPDATE_OFFSET),
		)
	case INSERT:
		buf.Fprintf("insert %vinto %v%v %v%v",
			node.At(INSERT_COMMENT_OFFSET),
			node.At(INSERT_TABLE_OFFSET),
			node.At(INSERT_COLUMN_LIST_OFFSET),
			node.At(INSERT_VALUES_OFFSET),
			node.At(INSERT_ON_DUP_OFFSET),
		)
	case UPDATE:
		buf.Fprintf("update %v%v set %v%v%v%v",
			node.At(UPDATE_COMMENT_OFFSET),
			node.At(UPDATE_TABLE_OFFSET),
			node.At(UPDATE_LIST_OFFSET),
			node.At(UPDATE_WHERE_OFFSET),
			node.At(UPDATE_ORDER_OFFSET),
			node.At(UPDATE_LIMIT_OFFSET),
		)
	case DELETE:
		buf.Fprintf("delete %vfrom %v%v%v%v",
			node.At(DELETE_COMMENT_OFFSET),
			node.At(DELETE_TABLE_OFFSET),
			node.At(DELETE_WHERE_OFFSET),
			node.At(DELETE_ORDER_OFFSET),
			node.At(DELETE_LIMIT_OFFSET),
		)
	case SET:
		buf.Fprintf("set %v%v", node.At(0), node.At(1))
	case CREATE, ALTER, DROP:
		buf.Fprintf("%s table %v", node.Value, node.At(0))
	case RENAME:
		buf.Fprintf("%s table %v %v", node.Value, node.At(0), node.At(1))
	case TABLE_EXPR:
		buf.Fprintf("%v", node.At(0))
		if node.At(1).Len() == 1 {
			buf.Fprintf(" as %v", node.At(1).At(0))
		}
		buf.Fprintf("%v", node.At(2))
	case USE:
		if node.Len() != 0 {
			buf.Fprintf(" use index %v", node.At(0))
		}
	case WHERE, HAVING:
		if node.Len() > 0 {
			buf.Fprintf(" %s %v", node.Value, node.At(0))
		}
	case ORDER, GROUP:
		if node.Len() > 0 {
			buf.Fprintf(" %s by %v", node.Value, node.At(0))
		}
	case LIMIT:
		if node.Len() > 0 {
			buf.Fprintf(" %s %v", node.Value, node.At(0))
			if node.Len() > 1 {
				buf.Fprintf(", %v", node.At(1))
			}
		}
	case COLUMN_LIST:
		if node.Len() > 0 {
			buf.Fprintf("(%v", node.At(0))
			for i := 1; i < node.Len(); i++ {
				buf.Fprintf(", %v", node.At(i))
			}
			buf.WriteByte(')')
		}
	case NODE_LIST:
		if node.Len() > 0 {
			buf.Fprintf("%v", node.At(0))
			for i := 1; i < node.Len(); i++ {
				buf.Fprintf(", %v", node.At(i))
			}
		}
	case COMMENT_LIST:
		if node.Len() > 0 {
			for i := 0; i < node.Len(); i++ {
				buf.Fprintf("%v", node.At(i))
			}
		}
	case WHEN_LIST:
		buf.Fprintf("%v", node.At(0))
		for i := 1; i < node.Len(); i++ {
			buf.Fprintf(" %v", node.At(i))
		}
	case JOIN, STRAIGHT_JOIN, LEFT, RIGHT, CROSS, NATURAL:
		buf.Fprintf("%v %s %v", node.At(0), node.Value, node.At(1))
		if node.Len() > 2 {
			buf.Fprintf(" on %v", node.At(2))
		}
	case DUPLICATE:
		if node.Len() != 0 {
			buf.Fprintf(" on duplicate key update %v", node.At(0))
		}
	case NUMBER, NULL, SELECT_STAR, NO_DISTINCT, COMMENT, FOR_UPDATE, NOT_FOR_UPDATE, TABLE:
		buf.Fprintf("%s", node.Value)
	case ID:
		if _, ok := keywords[string(node.Value)]; ok {
			buf.Fprintf("`%s`", node.Value)
		} else {
			buf.Fprintf("%s", node.Value)
		}
	case VALUE_ARG:
		buf.bindLocations = append(buf.bindLocations, BindLocation{buf.Len(), len(node.Value)})
		buf.Fprintf("%s", node.Value)
	case STRING:
		s := sqltypes.MakeString(node.Value)
		s.EncodeSql(buf)
	case '+', '-', '*', '/', '%', '&', '|', '^', '.':
		buf.Fprintf("%v%s%v", node.At(0), node.Value, node.At(1))
	case CASE_WHEN:
		buf.Fprintf("case %v end", node.At(0))
	case CASE:
		buf.Fprintf("case %v %v end", node.At(0), node.At(1))
	case WHEN:
		buf.Fprintf("when %v then %v", node.At(0), node.At(1))
	case ELSE:
		buf.Fprintf("else %v", node.At(0))
	case '=', '>', '<', GE, LE, NE, NULL_SAFE_EQUAL, AS, AND, OR, UNION, UNION_ALL, MINUS, EXCEPT, INTERSECT, LIKE, NOT_LIKE, IN, NOT_IN:
		buf.Fprintf("%v %s %v", node.At(0), node.Value, node.At(1))
	case '(':
		buf.Fprintf("(%v)", node.At(0))
	case EXISTS:
		buf.Fprintf("%s (%v)", node.Value, node.At(0))
	case FUNCTION:
		if node.Len() == 2 { // DISTINCT
			buf.Fprintf("%s(%v%v)", node.Value, node.At(0), node.At(1))
		} else {
			buf.Fprintf("%s(%v)", node.Value, node.At(0))
		}
	case UPLUS, UMINUS, '~':
		buf.Fprintf("%s%v", node.Value, node.At(0))
	case NOT, VALUES:
		buf.Fprintf("%s %v", node.Value, node.At(0))
	case ASC, DESC, IS_NULL, IS_NOT_NULL:
		buf.Fprintf("%v %s", node.At(0), node.Value)
	case BETWEEN, NOT_BETWEEN:
		buf.Fprintf("%v %s %v and %v", node.At(0), node.Value, node.At(1), node.At(2))
	case DISTINCT:
		buf.Fprintf("%s ", node.Value)
	default:
		buf.Fprintf("Unknown: %s", node.Value)
	}
}

// TrackedBuffer is used to rebuild a query from the ast.
// bindLocations keeps track of locations in the buffer that
// use bind variables for efficient future substitutions.
// nodeFormatter is the formatting function the buffer will
// use to format a node. By default(nil), it's FormatNode.
// But you can supply a different formatting function if you
// want to generate a query that's different from the default.
type TrackedBuffer struct {
	*bytes.Buffer
	bindLocations []BindLocation
	nodeFormatter func(buf *TrackedBuffer, node *Node)
}

func NewTrackedBuffer(nodeFormatter func(buf *TrackedBuffer, node *Node)) *TrackedBuffer {
	if nodeFormatter == nil {
		nodeFormatter = FormatNode
	}
	buf := &TrackedBuffer{
		Buffer:        bytes.NewBuffer(make([]byte, 0, 128)),
		bindLocations: make([]BindLocation, 0, 4),
		nodeFormatter: nodeFormatter,
	}
	return buf
}

// Mimics fmt.Fprintf, but limited to Value & Node
func (buf *TrackedBuffer) Fprintf(format string, values ...interface{}) {
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
			buf.nodeFormatter(buf, node)
		default:
			panic("unexpected")
		}
		fieldnum++
		i++
	}
}

func (buf *TrackedBuffer) ParsedQuery() *ParsedQuery {
	return &ParsedQuery{buf.String(), buf.bindLocations}
}
