/*
Copyright 2017 Google Inc.

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
	// all the way to the route that resultColumn points to.
	// PushSelect is similar to SupplyCol except that it always
	// adds a new column, whereas SupplyCol can reuse an existing
	// column. The function must return a resultColumn for the expression
	// and the column number of the result.
	PushSelect(expr *sqlparser.NonStarExpr, rb *route) (rc *resultColumn, colnum int, err error)
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
	// to supply the requested column and returns the resultColumn and
	// column number of the result. The request is passed down recursively
	// as needed.
	SupplyCol(c *column) (rc *resultColumn, colnum int)
}

// VSchema defines the interface for this package to fetch
// info about tables.
type VSchema interface {
	Find(keyspace, tablename sqlparser.TableIdent) (table *vindexes.Table, err error)
}

// Build builds a plan for a query based on the specified vschema.
// It's the main entry point for this package.
func Build(query string, vschema VSchema) (*engine.Plan, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	return BuildFromStmt(query, stmt, vschema)
}

// BuildFromStmt builds a plan based on the AST provided.
// TODO(sougou): The query input is trusted as the source
// of the AST. Maybe this function just returns instructions
// and engine.Plan can be built by the caller.
func BuildFromStmt(query string, stmt sqlparser.Statement, vschema VSchema) (*engine.Plan, error) {
	var err error
	plan := &engine.Plan{
		Original: query,
	}
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		plan.Instructions, err = buildSelectPlan(stmt, vschema)
	case *sqlparser.Insert:
		plan.Instructions, err = buildInsertPlan(stmt, vschema)
	case *sqlparser.Update:
		plan.Instructions, err = buildUpdatePlan(stmt, vschema)
	case *sqlparser.Delete:
		plan.Instructions, err = buildDeletePlan(stmt, vschema)
	case *sqlparser.Union:
		plan.Instructions, err = buildUnionPlan(stmt, vschema)
	case *sqlparser.Set:
		return nil, errors.New("unsupported construct: set")
	case *sqlparser.Show:
		return nil, errors.New("unsupported construct: show")
	case *sqlparser.DDL:
		return nil, errors.New("unsupported construct: ddl")
	case *sqlparser.Other:
		return nil, errors.New("unsupported construct: other")
	default:
		panic("unexpected statement type")
	}
	if err != nil {
		return nil, err
	}
	return plan, nil
}
