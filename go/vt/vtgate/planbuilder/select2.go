// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"encoding/json"
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// PlanBuilder represents any object that's used to
// build a plan.
type PlanBuilder interface{}

// JoinBuilder is used to build a Join primitive.
type JoinBuilder struct {
	IsLeft      bool
	Left, Right PlanBuilder
}

// RouteBuilder is used to build a Route primitive.
type RouteBuilder struct {
	From     sqlparser.TableExpr
	ChunkNum int
	Route    *Route
}

// Route represents a Route primitive.
type Route struct {
	PlanID   PlanID
	Keyspace *Keyspace
	Vindex   Vindex      `json:",omitempty"`
	Values   interface{} `json:",omitempty"`
}

// MarshalJSON marshals RouteBuilder into a readable form.
func (rtb *RouteBuilder) MarshalJSON() ([]byte, error) {
	marshalRoute := struct {
		From     string `json:",omitempty"`
		ChunkNum int    `json:",omitempty"`
		Route    *Route
	}{
		From:     sqlparser.String(rtb.From),
		ChunkNum: rtb.ChunkNum,
		Route:    rtb.Route,
	}
	return json.Marshal(marshalRoute)
}

// buildSelectPlan2 is the new function to build a Select plan. It will be renamed
// after the old one is removed.
func buildSelectPlan2(sel *sqlparser.Select, schema *Schema) (PlanBuilder, *SymbolTable, error) {
	return processTableExprs(sel.From, schema)
}

// processTableExprs is the root function to process the FROM clause. It produces a PlanBuilder
// and the associated SymbolTable. The PlanBuilder is a tree of JoinBuilder and RouteBuilder
// objects, where all the chunks are identified, and each chunk is grouped under one RouteBuilder.
func processTableExprs(tableExprs sqlparser.TableExprs, schema *Schema) (PlanBuilder, *SymbolTable, error) {
	if len(tableExprs) != 1 {
		return nil, nil, errors.New("no list")
	}
	return processTableExpr(tableExprs[0], schema)
}

// processTableExpr produces a PlanBuilder subtree and SymbolTable for the given TableExpr.
func processTableExpr(tableExpr sqlparser.TableExpr, schema *Schema) (PlanBuilder, *SymbolTable, error) {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		return processAliasedTable(tableExpr, schema)
	case *sqlparser.ParenTableExpr:
		planBuilder, symbols, err := processTableExprs(tableExpr.Exprs, schema)
		if route, ok := planBuilder.(*RouteBuilder); ok {
			route.From = tableExpr
		}
		return planBuilder, symbols, err
	case *sqlparser.JoinTableExpr:
		return processJoin(tableExpr, schema)
	}
	panic("unreachable")
}

// processAliasedTable produces a PlanBuilder subtree and SymbolTable for the given AliasedTableExpr.
func processAliasedTable(tableExpr *sqlparser.AliasedTableExpr, schema *Schema) (PlanBuilder, *SymbolTable, error) {
	switch expr := tableExpr.Expr.(type) {
	case *sqlparser.TableName:
		route, table, err := getTablePlan(expr, schema)
		if err != nil {
			return nil, nil, err
		}
		planBuilder := &RouteBuilder{
			From:  tableExpr,
			Route: route,
		}
		alias := expr.Name
		if tableExpr.As != "" {
			alias = tableExpr.As
		}
		symbols := NewSymbolTable(alias, table, planBuilder)
		return planBuilder, symbols, nil
	case *sqlparser.Subquery:
		return nil, nil, errors.New("no subqueries")
	}
	panic("unreachable")
}

// getTablePlan produces the initial Route for the specified TableName. It also returns
// the associated vschema info (*Table) so that it can be used to create the symbol table
// entry.
func getTablePlan(tableName *sqlparser.TableName, schema *Schema) (*Route, *Table, error) {
	if tableName.Qualifier != "" {
		return nil, nil, errors.New("tablename qualifier not allowed")
	}
	table, reason := schema.FindTable(string(tableName.Name))
	if reason != "" {
		return nil, nil, errors.New(reason)
	}
	if table.Keyspace.Sharded {
		return &Route{
			PlanID:   SelectScatter,
			Keyspace: table.Keyspace,
		}, table, nil
	}
	return &Route{
		PlanID:   SelectUnsharded,
		Keyspace: table.Keyspace,
	}, table, nil
}

// processJoin produces a PlanBuilder subtree and SymbolTable for the given Join. If the left and
// right node can be merged into one chunk, then it's a RouteBuilder. Otherwise, it's a JoinBuilder.
func processJoin(join *sqlparser.JoinTableExpr, schema *Schema) (PlanBuilder, *SymbolTable, error) {
	switch join.Join {
	case sqlparser.JoinStr, sqlparser.StraightJoinStr, sqlparser.LeftJoinStr:
	default:
		return nil, nil, errors.New("unsupported join")
	}
	lplanBuilder, lsymbols, err := processTableExpr(join.LeftExpr, schema)
	if err != nil {
		return nil, nil, err
	}
	rplanBuilder, rsymbols, err := processTableExpr(join.RightExpr, schema)
	if err != nil {
		return nil, nil, err
	}
	switch lplanBuilder := lplanBuilder.(type) {
	case *JoinBuilder:
		return makeChunk(lplanBuilder, lsymbols, rplanBuilder, rsymbols, join, schema)
	case *RouteBuilder:
		switch rplanBuilder := rplanBuilder.(type) {
		case *JoinBuilder:
			return makeChunk(lplanBuilder, lsymbols, rplanBuilder, rsymbols, join, schema)
		case *RouteBuilder:
			return joinRoutes(lplanBuilder, lsymbols, rplanBuilder, rsymbols, join, schema)
		}
	}
	panic("unreachable")
}

// makeChink creates a new chunk for the join operation. This function is called when
// two chunks cannot be merged into one.
func makeChunk(lplanBuilder PlanBuilder, lsymbols *SymbolTable, rplanBuilder PlanBuilder, rsymbols *SymbolTable, join *sqlparser.JoinTableExpr, schema *Schema) (PlanBuilder, *SymbolTable, error) {
	err := lsymbols.AddChunk(rsymbols)
	if err != nil {
		return nil, nil, err
	}
	isLeft := false
	if join.Join == sqlparser.LeftJoinStr {
		isLeft = true
	}
	return &JoinBuilder{
		IsLeft: isLeft,
		Left:   lplanBuilder,
		Right:  rplanBuilder,
	}, lsymbols, nil
}

// joinRoutes attempts to join to RouteBuilder objects into one. If it's not possible,
// it produces a joined RouteBuilder. Otherwise, it's a JoinBuilder.
func joinRoutes(lplanBuilder *RouteBuilder, lsymbols *SymbolTable, rplanBuilder *RouteBuilder, rsymbols *SymbolTable, join *sqlparser.JoinTableExpr, schema *Schema) (PlanBuilder, *SymbolTable, error) {
	switch lplanBuilder.Route.PlanID {
	case SelectUnsharded:
		switch rplanBuilder.Route.PlanID {
		case SelectUnsharded:
			// Two Routes from the same unsharded keyspace can be merged.
			if lplanBuilder.Route.Keyspace.Name == rplanBuilder.Route.Keyspace.Name {
				lplanBuilder.From = join
				err := lsymbols.Merge(rsymbols, lplanBuilder)
				if err != nil {
					return nil, nil, err
				}
				return lplanBuilder, lsymbols, nil
			}
		}
	}
	return makeChunk(lplanBuilder, lsymbols, rplanBuilder, rsymbols, join, schema)
}
