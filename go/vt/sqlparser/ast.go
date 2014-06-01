// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"bytes"
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
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

func Parse(sql string) (Statement, error) {
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

func (node *Node) LowerCase() {
	node.Value = bytes.ToLower(node.Value)
}

func (node *Node) String() (out string) {
	buf := NewTrackedBuffer(nil)
	buf.Fprintf("%v", node)
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

// Format generates the SQL for the current node.
func (node *Node) Format(buf *TrackedBuffer) {
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
			node.At(SELECT_LOCK_OFFSET),
		)
	case TABLE_EXPR:
		buf.Fprintf("%v", node.At(0))
		if node.At(1).Len() == 1 {
			buf.Fprintf(" as %v", node.At(1).At(0))
		}
		buf.Fprintf("%v", node.At(2))
	case USE, FORCE:
		if node.Len() != 0 {
			buf.Fprintf(" %s index %v", node.Value, node.At(0))
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
	case COLUMN_LIST, INDEX_LIST:
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
		for i := 0; i < node.Len(); i++ {
			buf.Fprintf("%v ", node.At(i))
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
	case NUMBER, NULL, SELECT_STAR, NO_DISTINCT, COMMENT, NO_LOCK, TABLE, FOR_UPDATE, LOCK_IN_SHARE_MODE:
		buf.Fprintf("%s", node.Value)
	case ID:
		if _, ok := keywords[string(node.Value)]; ok {
			buf.Fprintf("`%s`", node.Value)
		} else {
			buf.Fprintf("%s", node.Value)
		}
	case VALUE_ARG:
		buf.WriteArg(string(node.Value[1:]))
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

// AnonymizedFormatter is a formatter that
// anonymizes all values in the SQL.
func AnonymizedFormatter(buf *TrackedBuffer, node SQLNode) {
	switch node := node.(type) {
	case *Node:
		switch node.Type {
		case STRING, NUMBER:
			buf.Fprintf("?")
		default:
			node.Format(buf)
		}
	default:
		node.Format(buf)
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
	nodeFormatter func(buf *TrackedBuffer, node SQLNode)
}

func NewTrackedBuffer(nodeFormatter func(buf *TrackedBuffer, node SQLNode)) *TrackedBuffer {
	buf := &TrackedBuffer{
		Buffer:        bytes.NewBuffer(make([]byte, 0, 128)),
		bindLocations: make([]BindLocation, 0, 4),
		nodeFormatter: nodeFormatter,
	}
	return buf
}

// Fprintf mimics fmt.Fprintf, but limited to Node(%v), Node.Value(%s) and string(%s).
// It also allows a %a for a value argument, in which case it adds tracking info for
// future substitutions.
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
			switch v := values[fieldnum].(type) {
			case []byte:
				buf.Write(v)
			case string:
				buf.WriteString(v)
			default:
				panic(fmt.Sprintf("unexpected type %T", v))
			}
		case 'v':
			node := values[fieldnum].(SQLNode)
			if buf.nodeFormatter == nil {
				node.Format(buf)
			} else {
				buf.nodeFormatter(buf, node)
			}
		case 'a':
			buf.WriteArg(values[fieldnum].(string))
		default:
			panic("unexpected")
		}
		fieldnum++
		i++
	}
}

// WriteArg writes a value argument into the buffer. arg should not contain
// the ':' prefix. It also adds tracking info for future substitutions.
func (buf *TrackedBuffer) WriteArg(arg string) {
	buf.bindLocations = append(buf.bindLocations, BindLocation{buf.Len(), len(arg) + 1})
	buf.WriteByte(':')
	buf.WriteString(arg)
}

func (buf *TrackedBuffer) ParsedQuery() *ParsedQuery {
	return &ParsedQuery{buf.String(), buf.bindLocations}
}
