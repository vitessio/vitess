// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

// Statement is the interface that needs to be
// satisfied by SQL statement nodes. statement()
// is a dummy function used for verifying that a
// node is a Statement.
type Statement interface {
	statement()
	Format(buf *TrackedBuffer)
}

// SelectStatement is the interface that needs to be
// satisfied by all select statements, including
// unions. They need to implement the dummy
// selectStatement function.
type SelectStatement interface {
	selectStatement()
	statement()
	Format(buf *TrackedBuffer)
}

// Select represents a SELECT statement.
type Select struct {
	Comments    Comments
	Distinct    Distinct
	SelectExprs SelectExprs
	From        *Node
	Where       *Node
	GroupBy     *Node
	Having      *Node
	OrderBy     *Node
	Limit       *Node
	Lock        *Node
}

func (*Select) statement() {}

func (*Select) selectStatement() {}

func (stmt *Select) Format(buf *TrackedBuffer) {
	buf.Fprintf("select %v%v%v from %v%v%v%v%v%v%v",
		stmt.Comments, stmt.Distinct, stmt.SelectExprs,
		stmt.From, stmt.Where,
		stmt.GroupBy, stmt.Having, stmt.OrderBy,
		stmt.Limit, stmt.Lock)
}

// Union represents a UNION statement.
type Union struct {
	Type             []byte
	Select1, Select2 SelectStatement
}

func (*Union) statement() {}

func (*Union) selectStatement() {}

func (stmt *Union) Format(buf *TrackedBuffer) {
	buf.Fprintf("%v %s %v", stmt.Select1, stmt.Type, stmt.Select2)
}

func selectNode(statement SelectStatement) *Node {
	switch stmt := statement.(type) {
	case *Select:
		n := NewSimpleParseNode(SELECT, "select")
		n.Push(stmt.Comments.Node())
		n.Push(stmt.Distinct.Node())
		n.Push(stmt.SelectExprs.Node())
		n.Push(stmt.From)
		n.Push(stmt.Where)
		n.Push(stmt.GroupBy)
		n.Push(stmt.Having)
		n.Push(stmt.OrderBy)
		n.Push(stmt.Limit)
		n.Push(stmt.Lock)
		return n
	case *Union:
		n := NewParseNode(UNION, stmt.Type)
		n.PushTwo(selectNode(stmt.Select1), selectNode(stmt.Select2))
		return n
	}
	panic("unreachable")
}

func newSelect(node *Node) SelectStatement {
	switch node.Type {
	case SELECT:
		return &Select{
			Comments:    newComments(node.At(0)),
			Distinct:    newDistinct(node.At(1)),
			SelectExprs: newSelectExprsNode(node.At(2)),
			From:        node.At(3),
			Where:       node.At(4),
			GroupBy:     node.At(5),
			Having:      node.At(6),
			OrderBy:     node.At(7),
			Limit:       node.At(8),
			Lock:        node.At(9),
		}
	case UNION:
		return &Union{
			Type:    node.Value,
			Select1: newSelect(node.At(0)),
			Select2: newSelect(node.At(1)),
		}
	}
	panic("unreachable")
}

// Insert represents an INSERT statement.
type Insert struct {
	Comments Comments
	Table    *Node
	Columns  *Node
	Values   *Node
	OnDup    *Node
}

func (*Insert) statement() {}

func (stmt *Insert) Format(buf *TrackedBuffer) {
	buf.Fprintf("insert %vinto %v%v %v%v",
		stmt.Comments,
		stmt.Table, stmt.Columns, stmt.Values, stmt.OnDup)
}

// Update represents an UPDATE statement.
type Update struct {
	Comments Comments
	Table    *Node
	List     *Node
	Where    *Node
	OrderBy  *Node
	Limit    *Node
}

func (*Update) statement() {}

func (stmt *Update) Format(buf *TrackedBuffer) {
	buf.Fprintf("update %v%v set %v%v%v%v",
		stmt.Comments, stmt.Table,
		stmt.List, stmt.Where, stmt.OrderBy, stmt.Limit)
}

// Delete represents a DELETE statement.
type Delete struct {
	Comments Comments
	Table    *Node
	Where    *Node
	OrderBy  *Node
	Limit    *Node
}

func (*Delete) statement() {}

func (stmt *Delete) Format(buf *TrackedBuffer) {
	buf.Fprintf("delete %vfrom %v%v%v%v",
		stmt.Comments,
		stmt.Table, stmt.Where, stmt.OrderBy, stmt.Limit)
}

// Set represents a SET statement.
type Set struct {
	Comments Comments
	Updates  *Node
}

func (*Set) statement() {}

func (stmt *Set) Format(buf *TrackedBuffer) {
	buf.Fprintf("set %v%v", stmt.Comments, stmt.Updates)
}

// DDLSimple represents a CREATE, ALTER or DROP statement.
type DDLSimple struct {
	Action int
	Table  *Node
}

func (*DDLSimple) statement() {}

func (stmt *DDLSimple) Format(buf *TrackedBuffer) {
	switch stmt.Action {
	case CREATE:
		buf.Fprintf("create table %v", stmt.Table)
	case ALTER:
		buf.Fprintf("alter table %v", stmt.Table)
	case DROP:
		buf.Fprintf("drop table %v", stmt.Table)
	default:
		panic("unreachable")
	}
}

// Rename represents a RENAME statement.
type Rename struct {
	OldName, NewName *Node
}

func (*Rename) statement() {}

func (stmt *Rename) Format(buf *TrackedBuffer) {
	buf.Fprintf("rename table %v %v", stmt.OldName, stmt.NewName)
}

// Comments represents a list of comments.
type Comments []Comment

func (comments Comments) Format(buf *TrackedBuffer) {
	for _, c := range comments {
		c.Format(buf)
	}
}

func (comments Comments) Node() *Node {
	node := NewSimpleParseNode(COMMENT_LIST, "")
	for _, c := range comments {
		node.Push(NewParseNode(COMMENT, []byte(c)))
	}
	return node
}

func newComments(node *Node) Comments {
	if node.Type != COMMENT_LIST {
		panic("unreachable: not a comment list")
	}
	var comments Comments
	for i := 0; i < node.Len(); i++ {
		comments = append(comments, Comment(node.At(i).Value))
	}
	return comments
}

// Comment represents one comment.
type Comment []byte

func (comment Comment) Format(buf *TrackedBuffer) {
	buf.Fprintf("%s ", []byte(comment))
}

// Distinct specifies if DISTINCT was used.
type Distinct bool

func (distinct Distinct) Format(buf *TrackedBuffer) {
	if distinct {
		buf.Fprintf("distinct ")
	}
}

func (distinct Distinct) Node() *Node {
	if distinct {
		return NewSimpleParseNode(DISTINCT, "distinct")
	}
	return NewSimpleParseNode(NO_DISTINCT, "")
}

func newDistinct(node *Node) Distinct {
	switch node.Type {
	case DISTINCT:
		return Distinct(true)
	case NO_DISTINCT:
		return Distinct(false)
	}
	panic("not a distinct node")
}

// SelectExprs represents SELECT expressions.
type SelectExprs []*Node

func (exprs SelectExprs) Format(buf *TrackedBuffer) {
	for i, sel := range exprs {
		if i == 0 {
			buf.Fprintf("%v", sel)
		} else {
			buf.Fprintf(", %v", sel)
		}
	}
}

func (exprs SelectExprs) Node() *Node {
	node := NewSimpleParseNode(NODE_LIST, "")
	for _, sel := range exprs {
		node.Push(sel)
	}
	return node
}

func newSelectExprsNode(node *Node) SelectExprs {
	var exprs SelectExprs
	for i := 0; i < node.Len(); i++ {
		exprs = append(exprs, node.At(i))
	}
	return exprs
}
