// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

// A builder is used to build a primitive. The top-level
// builder will be a tree that points to other builders.
// Each builder builds an engine.Primitive.
// The primitives themselves will mirror the same tree.
type builder interface {
	// Symtab returns the associated symtab.
	Symtab() *symtab
	// SetSymtab sets the symtab for the current node and
	// its non-subquery children.
	SetSymtab(*symtab)
	// Order is a number that signifies execution order.
	// A lower Order number Route is executed before a
	// higher one. For a node that contains other nodes,
	// the Order represents the highest order of the leaf
	// nodes. This function is used to travel from a root
	// node to a target node.
	Order() int
	// SetOrder sets the order for the underlying routes.
	SetOrder(int)
	// Primitve returns the underlying primitive.
	Primitive() engine.Primitive
	// Leftmost returns the leftmost route.
	Leftmost() *route
	// Join joins the two builder objects. The outcome
	// can be a new builder or a modified one.
	Join(rhs builder, ajoin *sqlparser.JoinTableExpr) (builder, error)
	// SetRHS marks all routes under this node as RHS due
	// to a left join. Such nodes have restrictions on what
	// can be pushed into them. This should not propagate
	// to subqueries.
	SetRHS()
	// PushSelect pushes the select expression through the tree
	// all the way to the route that colsym points to.
	// PushSelect is similar to SupplyCol except that it always
	// adds a new column, whereas SupplyCol can reuse an existing
	// column. The function must return a colsym for the expression
	// and the column number of the result.
	PushSelect(expr *sqlparser.NonStarExpr, rb *route) (colsym *colsym, colnum int, err error)
	// PushOrderByNull pushes the special case ORDER By NULL to
	// all routes. It's safe to push down this clause because it's
	// just on optimization hint.
	PushOrderByNull()
	// PushMisc pushes miscelleaneous constructs to all the routes.
	// This should not propagate to subqueries.
	PushMisc(sel *sqlparser.Select)
	// Wireup performs the wire-up work. Nodes should be traversed
	// from right to left because the rhs nodes can request vars from
	// the lhs nodes.
	Wireup(bldr builder, jt *jointab) error
	// SupplyVar finds the common root between from and to. If it's
	// the common root, it supplies the requested var to the rhs tree.
	SupplyVar(from, to int, col *sqlparser.ColName, varname string)
	// SupplyCol will be used for the wire-up process. This function
	// takes a column reference as input, changes the primitive
	// to supply the requested column and returns the column number of
	// the result for it. The request is passed down recursively
	// as needed.
	SupplyCol(ref colref) int
}

// VSchema defines the interface for this package to fetch
// info about tables.
type VSchema interface {
	Find(keyspace, tablename string) (table *vindexes.Table, err error)
}

// Build builds a plan for a query based on the specified vschema.
// It's the main entry point for this package.
func Build(query string, vschema VSchema) (*engine.Plan, error) {
	statement, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	plan := &engine.Plan{
		Original: query,
	}
	switch statement := statement.(type) {
	case *sqlparser.Select:
		plan.Instructions, err = buildSelectPlan(statement, vschema)
	case *sqlparser.Insert:
		plan.Instructions, err = buildInsertPlan(statement, vschema)
	case *sqlparser.Update:
		plan.Instructions, err = buildUpdatePlan(statement, vschema)
	case *sqlparser.Delete:
		plan.Instructions, err = buildDeletePlan(statement, vschema)
	case *sqlparser.Union, *sqlparser.Set, *sqlparser.DDL, *sqlparser.Other:
		return nil, errors.New("unsupported construct")
	default:
		panic("unexpected statement type")
	}
	if err != nil {
		return nil, err
	}
	return plan, nil
}
