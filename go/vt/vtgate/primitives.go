// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"errors"
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// This is a V3 file. Do not intermix with V2.

// Primitive represents a self-contained operation that can be performed by the
// VTGate execution engine. Primitives can be linked into trees to construct
// execution plans of arbitrary complexity. After an initial, naive tree is
// generated for a given query plan, the optimizer's job is to transform the
// tree to maximize execution efficiency.
type Primitive interface {
	// Parent returns the destination Primitive to which this primitive sends its
	// rows after doing its own processing, or nil if it has no parent.
	Parent() Primitive
	// Children returns the ordered list of source Primitives from which this
	// primitive receives rows to process, or nil if it has no children.
	Children() []Primitive
	// SetParent sets the parent.
	SetParent(parent Primitive)
	// SetChildren sets the list of children.
	SetChildren(children []Primitive)
}

// primitiveNode maintains the up and down pointers for the primitive tree.
// Primitives embed this to share common tree code by composition.
type primitiveNode struct {
	parent   Primitive
	children []Primitive
}

// Parent implements Primitive.
func (n *primitiveNode) Parent() Primitive {
	return n.parent
}

// Children implements Primitive.
func (n *primitiveNode) Children() []Primitive {
	return n.children
}

// SetParent implements Primitive.
func (n *primitiveNode) SetParent(parent Primitive) {
	n.parent = parent
}

// SetChildren implements Primitive.
func (n *primitiveNode) SetChildren(children []Primitive) {
	n.children = children
}

// Select is a Primitive that extracts columns from its input, evaluates
// expressions, and renames columns.
//
// Select is not executable (it does not implement Plan).
type Select struct {
	// primitiveNode maintains the parent and children links.
	//
	// For Select, the children are as follows:
	//   [0]  -> FROM clause
	//   [1:] -> Subqueries
	primitiveNode

	// Exprs is a list of column expressions.
	//
	// Since Select is not executable, these are only stored to track the mapping
	// between the Select primitive's inputs and outputs (used while optimizing),
	// and to generate a query if the Select is pushed into a Route.
	Exprs sqlparser.SelectExprs
}

// reparentPrimitives attaches the given children to the parent, setting the
// parent and child pointers as appropriate on both sides.
func reparentPrimitives(parent Primitive, children ...Primitive) {
	parent.SetChildren(children)
	for _, child := range children {
		child.SetParent(parent)
	}
}

// buildPrimitiveTree creates a naive but correct primitive tree to execute the
// query. Usually this tree is passed through the optimizer before using
// MakePlan() to create a plan for the tree.
//
// Note that although this naive tree would produce the correct result if
// executed, some primitives in the tree may not actually be implemented in
// VTGate, which means execution will fail.
//
// For example, the Scan primitive is not executable (it doesn't implement the
// Plan interface), because actually doing a full table scan from VTGate is
// never a good idea. Any primitive tree from which the optimizer is unable to
// eliminate all Scans will thus fail when passed to MakePlan().
func buildPrimitiveTree(query string, vschema *VSchema) (Primitive, error) {
	statement, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	b := &primitiveTreeBuilder{vschema: vschema}
	switch statement := statement.(type) {
	case *sqlparser.Select:
		return b.selectTree(statement)
	case *sqlparser.Insert:
		return b.insertTree(statement)
	case *sqlparser.Update:
		return b.updateTree(statement)
	case *sqlparser.Delete:
		return b.deleteTree(statement)
	case *sqlparser.Union, *sqlparser.Set, *sqlparser.DDL, *sqlparser.Other:
		return nil, fmt.Errorf("can't build primitive tree for unsupported construct: %T", statement)
	default:
		panic("unexpected sqlparser.Statement type")
	}
}

type primitiveTreeBuilder struct {
	vschema *VSchema
}

// selectTree builds a primitive tree for a SELECT statement.
func (b *primitiveTreeBuilder) selectTree(sel *sqlparser.Select) (Primitive, error) {
	from, err := b.tableExprsTree(sel.From)
	if err != nil {
		return nil, err
	}
	// TODO: rest of select...
}

// tableExprsTree builds a primitive tree for a list of table expressions.
func (b *primitiveTreeBuilder) tableExprsTree(tableExprs sqlparser.TableExprs) (Primitive, error) {
	if len(tableExprs) != 1 {
		return nil, errors.New("can't build primitive tree for table expression: ',' operator is not supported (use 'JOIN' instead)")
	}
	return b.tableExprTree(tableExprs[0])
}

// tableExprTree builds a primitive tree for a table expression.
func (b *primitiveTreeBuilder) tableExprTree(tableExpr sqlparser.TableExpr) (Primitive, error) {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		// A table name or a subquery.
		return b.aliasedTableExprTree(tableExpr)
	case *sqlparser.ParenTableExpr:
		// Unwrap from the parentheses, and try again.
		return b.tableExprsTree(tableExpr.Exprs)
	case *sqlparser.JoinTableExpr:
		return b.joinTree(tableExpr)
	default:
		panic("unexpected sqlparser.TableExpr type")
	}
}

// joinTree builds a primitive tree for a JOIN table expression.
func (b *primitiveTreeBuilder) joinTree(joinExpr *sqlparser.JoinTableExpr) (Primitive, error) {
	var join Primitive

	switch joinExpr.Join {
	case sqlparser.JoinStr, sqlparser.StraightJoinStr:
		join = &Join{On: joinExpr.On}
	case sqlparser.LeftJoinStr:
		join = &Join{On: joinExpr.On, IsLeft: true}
	case sqlparser.RightJoinStr:
		// This can be supported if we convert it to a LEFT JOIN.
		convertToLeftJoin(joinExpr)
		join = &Join{On: joinExpr.On, IsLeft: true}
	default:
		return nil, fmt.Errorf("can't build primitive tree for unsupported JOIN type: %s", joinExpr.Join)
	}

	// Build the left and right children of the Join.
	left, err := b.tableExprTree(joinExpr.LeftExpr)
	if err != nil {
		return nil, err
	}
	right, err := b.tableExprTree(joinExpr.RightExpr)
	if err != nil {
		return nil, err
	}
	reparentPrimitives(join, left, right)

	return join, nil
}
