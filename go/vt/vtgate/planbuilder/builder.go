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
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

// builder defines the interface that a primitive must
// satisfy.
type builder interface {
	// Symtab returns the associated symtab.
	Symtab() *symtab

	// MaxOrder must return the order number of the
	// highest columnOriginator within the current sub-tree.
	// The originators are numbered by their execution order.
	// When two trees are brought together into a join,
	// the MaxOrder from the left is used as starting point
	// to renumber the originators on the right. Execution order
	// always goes from left to right. A node that contains
	// sub-nodes (like a join), can cache these values
	// and use them for efficient b-tree style traversal.
	MaxOrder() int

	// SetOrder reassigns order for the primitive and its sub-primitives.
	// The input is the max order of the previous primitive that should
	// execute before this one.
	SetOrder(int)

	// Primitve returns the underlying primitive.
	Primitive() engine.Primitive

	// Leftmost returns the leftmost columnOriginator.
	Leftmost() columnOriginator

	// ResultColumns returns the list of result columns the
	// primitive returns.
	ResultColumns() []*resultColumn

	// PushFilter pushes a WHERE or HAVING clause expression
	// to the specified origin.
	PushFilter(filter sqlparser.Expr, whereType string, origin columnOriginator) error

	// PushSelect pushes the select expression to the specified
	// originator. If successful, the originator must create
	// a resultColumn entry and return it. The top level caller
	// must accumulate these result columns and set the symtab
	// after analysis.
	PushSelect(expr *sqlparser.AliasedExpr, origin columnOriginator) (rc *resultColumn, colnum int, err error)

	// PushOrderByNull pushes the special case ORDER By NULL to
	// all primitives. It's safe to push down this clause because it's
	// just on optimization hint.
	PushOrderByNull()

	// PushOrderByRand pushes the special case ORDER BY RAND() to
	// all primitives.
	PushOrderByRand()

	// SetUpperLimit is an optimization hint that tells that primitive
	// that it does not need to return more than the specified number of rows.
	// A primitive that cannot perform this can ignore the request.
	SetUpperLimit(count *sqlparser.SQLVal)

	// PushMisc pushes miscelleaneous constructs to all the primitives.
	PushMisc(sel *sqlparser.Select)

	// Wireup performs the wire-up work. Nodes should be traversed
	// from right to left because the rhs nodes can request vars from
	// the lhs nodes.
	Wireup(bldr builder, jt *jointab) error

	// SupplyVar finds the common root between from and to. If it's
	// the common root, it supplies the requested var to the rhs tree.
	// If the primitive already has the column in its list, it should
	// just supply it to the 'to' node. Otherwise, it should request
	// for it by calling SupplyCol on the 'from' sub-tree to request the
	// column, and then supply it to the 'to' node.
	SupplyVar(from, to int, col *sqlparser.ColName, varname string)

	// SupplyCol is meant to be used for the wire-up process. This function
	// changes the primitive to supply the requested column and returns
	// the resultColumn and column number of the result. SupplyCol
	// is different from PushSelect because it may reuse an existing
	// resultColumn, whereas PushSelect guarantees the addition of a new
	// result column and returns a distinct symbol for it.
	SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colnum int)
}

// columnOriginator is a builder that originates
// column symbols.
type columnOriginator interface {
	builder
	Order() int
}

// VSchema defines the interface for this package to fetch
// info about tables.
type VSchema interface {
	FindTable(tablename sqlparser.TableName) (*vindexes.Table, error)
	FindTableOrVindex(tablename sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, error)
	DefaultKeyspace() (*vindexes.Keyspace, error)
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
	case *sqlparser.OtherRead:
		return nil, errors.New("unsupported construct: other read")
	case *sqlparser.OtherAdmin:
		return nil, errors.New("unsupported construct: other admin")
	default:
		panic(fmt.Sprintf("BUG: unexpected statement type: %T", stmt))
	}
	if err != nil {
		return nil, err
	}
	return plan, nil
}
