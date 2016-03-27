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

// planBuilder represents any object that's used to
// build a plan. The top-level planBuilder will be a
// tree that points to other planBuilder objects.
// Each Builder object builds a Plan object, and they
// will mirror the same tree. Once all the plans are built,
// the builder objects will be discarded, and only
// the Plan objects will remain. Because of the near-equivalent
// meaning of a planBuilder object and its plan, the variable
// names are overloaded. The separation exists only because the
// information in the planBuilder objects are not ultimately
// required externally.
// For example, a route variable usually refers to a
// routeBuilder object, which in turn, has a Route field.
// This should not cause confusion because we almost never
// reference the inner Route directly.
type planBuilder interface {
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
	// Primitve returns the primitive built by the builder.
	Primitive() engine.Primitive
	// Leftmost returns the leftmost route.
	Leftmost() *routeBuilder
	// Join joins the two planBuilder objects. The outcome
	// can be a new planBuilder or a modified one.
	Join(rhs planBuilder, Join *sqlparser.JoinTableExpr) (planBuilder, error)
	// SetRHS marks all routes under this node as RHS.
	SetRHS()
	// PushSelect pushes the select expression through the tree
	// all the way to the route that colsym points to.
	// PushSelect is similar to SupplyCol except that it always
	// adds a new column, whereas SupplyCol can reuse an existing
	// column. This is required because the ORDER BY clause
	// may refer to columns by number. The function must return
	// a colsym for the expression and the column number of the result.
	PushSelect(expr *sqlparser.NonStarExpr, route *routeBuilder) (colsym *colsym, colnum int, err error)
	// PushMisc pushes miscelleaneous constructs to all the routes.
	PushMisc(sel *sqlparser.Select)
	// Wireup performs the wire-up work. Nodes should be traversed
	// from right to left because the rhs nodes can request vars from
	// the lhs nodes.
	Wireup(plan planBuilder, jt *jointab) error
	// SupplyVar finds the common root between from and to. If it's
	// the common root, it supplies the requested var to the rhs tree.
	SupplyVar(from, to int, col *sqlparser.ColName, varname string)
	// SupplyCol will be used for the wire-up process. This function
	// takes a column reference as input, changes the plan
	// to supply the requested column and returns the column number of
	// the result for it. The request is passed down recursively
	// as needed.
	SupplyCol(col *sqlparser.ColName) int
}

// BuildPlan builds a plan for a query based on the specified vschema.
// It's the main entry point for this package.
func BuildPlan(query string, vschema *vindexes.VSchema) (*engine.Plan, error) {
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
