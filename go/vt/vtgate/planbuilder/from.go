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
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

// This file has functions to analyze the FROM clause.

// processTableExprs analyzes the FROM clause. It produces a builder
// with all the routes identified.
func processTableExprs(tableExprs sqlparser.TableExprs, vschema VSchema) (builder, error) {
	if len(tableExprs) == 1 {
		return processTableExpr(tableExprs[0], vschema)
	}

	lplan, err := processTableExpr(tableExprs[0], vschema)
	if err != nil {
		return nil, err
	}
	rplan, err := processTableExprs(tableExprs[1:], vschema)
	if err != nil {
		return nil, err
	}
	return joinBuilders(lplan, rplan, nil)
}

// processTableExpr produces a builder subtree for the given TableExpr.
func processTableExpr(tableExpr sqlparser.TableExpr, vschema VSchema) (builder, error) {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		return processAliasedTable(tableExpr, vschema)
	case *sqlparser.ParenTableExpr:
		bldr, err := processTableExprs(tableExpr.Exprs, vschema)
		// If it's a route, preserve the parenthesis so things
		// don't associate differently when more things are pushed
		// into it. FROM a, (b, c) should not become FROM a, b, c.
		if rb, ok := bldr.(*route); ok {
			sel := rb.Select.(*sqlparser.Select)
			sel.From = sqlparser.TableExprs{&sqlparser.ParenTableExpr{Exprs: sel.From}}
		}
		return bldr, err
	case *sqlparser.JoinTableExpr:
		return processJoin(tableExpr, vschema)
	}
	panic(fmt.Sprintf("BUG: unexpected table expression type: %T", tableExpr))
}

// processAliasedTable produces a builder subtree for the given AliasedTableExpr.
// If the expression is a subquery, then the primitive will create a table
// for it in the symtab. If the subquery is a route, then we build a route
// primitive with the subquery in the From clause, because a route is more
// versatile than a subquery. If a subquery becomes a route, then any result
// columns that represent underlying vindex columns are also exposed as
// vindex columns.
func processAliasedTable(tableExpr *sqlparser.AliasedTableExpr, vschema VSchema) (builder, error) {
	switch expr := tableExpr.Expr.(type) {
	case sqlparser.TableName:
		return buildTablePrimitive(tableExpr, expr, vschema)
	case *sqlparser.Subquery:
		var err error
		var subplan builder
		switch stmt := expr.Select.(type) {
		case *sqlparser.Select:
			subplan, err = processSelect(stmt, vschema, nil)
		case *sqlparser.Union:
			subplan, err = processUnion(stmt, vschema, nil)
		default:
			panic(fmt.Sprintf("BUG: unexpected SELECT type: %T", stmt))
		}
		if err != nil {
			return nil, err
		}

		subroute, ok := subplan.(*route)
		if !ok {
			return newSubquery(tableExpr.As, subplan, vschema), nil
		}

		// Since a route is more versatile than a subquery, we
		// build a route primitive that has the subquery in its
		// FROM clause. This allows for other constructs to be
		// later pushed into it.
		table := &vindexes.Table{
			Keyspace: subroute.ERoute.Keyspace,
		}
		for _, rc := range subroute.ResultColumns() {
			if rc.column.Vindex == nil {
				continue
			}
			// Check if a colvindex of the same name already exists.
			// Dups are not allowed in subqueries in this situation.
			for _, colVindex := range table.ColumnVindexes {
				if colVindex.Columns[0].Equal(rc.alias) {
					return nil, fmt.Errorf("duplicate column aliases: %v", rc.alias)
				}
			}
			table.ColumnVindexes = append(table.ColumnVindexes, &vindexes.ColumnVindex{
				Columns: []sqlparser.ColIdent{rc.alias},
				Vindex:  rc.column.Vindex,
			})
		}
		rb := newRoute(
			&sqlparser.Select{From: sqlparser.TableExprs([]sqlparser.TableExpr{tableExpr})},
			subroute.ERoute,
			subroute.condition,
			vschema,
		)
		// AddVindexTable can never fail because symtab is empty.
		_ = rb.symtab.AddVindexTable(sqlparser.TableName{Name: tableExpr.As}, table, rb)
		subroute.Redirect = rb
		return rb, nil
	}
	panic(fmt.Sprintf("BUG: unexpected table expression type: %T", tableExpr.Expr))
}

// buildTablePrimitive builds a primitive based on the table name.
func buildTablePrimitive(tableExpr *sqlparser.AliasedTableExpr, tableName sqlparser.TableName, vschema VSchema) (builder, error) {
	alias := tableName
	if !tableExpr.As.IsEmpty() {
		alias = sqlparser.TableName{Name: tableExpr.As}
	}
	rb := newRoute(
		&sqlparser.Select{From: sqlparser.TableExprs([]sqlparser.TableExpr{tableExpr})},
		nil,
		nil,
		vschema,
	)
	if systemTable(tableName.Qualifier.String()) {
		ks, err := vschema.DefaultKeyspace()
		if err != nil {
			return nil, err
		}
		rb.ERoute = engine.NewRoute(engine.ExecDBA, ks)
		return rb, nil
	}

	table, vindex, err := vschema.FindTableOrVindex(tableName)
	if err != nil {
		return nil, err
	}
	if vindex != nil {
		return newVindexFunc(alias, vindex, vschema), nil
	}
	// AddVindexTable can never fail because symtab is empty.
	_ = rb.symtab.AddVindexTable(alias, table, rb)

	if !table.Keyspace.Sharded {
		rb.ERoute = engine.NewRoute(engine.SelectUnsharded, table.Keyspace)
		return rb, nil
	}
	if table.Pinned == nil {
		rb.ERoute = engine.NewRoute(engine.SelectScatter, table.Keyspace)
		return rb, nil
	}
	// Pinned tables have their keyspace ids already assigned.
	// Use the Binary vindex, which is the identity function
	// for keyspace id. Currently only dual tables are pinned.
	route := engine.NewRoute(engine.SelectEqualUnique, table.Keyspace)
	route.Vindex, _ = vindexes.NewBinary("binary", nil)
	route.Values = []sqltypes.PlanValue{{Value: sqltypes.MakeTrusted(sqltypes.VarBinary, table.Pinned)}}
	rb.ERoute = route
	return rb, nil
}

// processJoin produces a builder subtree for the given Join.
// If the left and right nodes can be part of the same route,
// then it's a route. Otherwise, it's a join.
func processJoin(ajoin *sqlparser.JoinTableExpr, vschema VSchema) (builder, error) {
	switch ajoin.Join {
	case sqlparser.JoinStr, sqlparser.StraightJoinStr, sqlparser.LeftJoinStr:
	case sqlparser.RightJoinStr:
		convertToLeftJoin(ajoin)
	default:
		return nil, fmt.Errorf("unsupported: %s", ajoin.Join)
	}
	lplan, err := processTableExpr(ajoin.LeftExpr, vschema)
	if err != nil {
		return nil, err
	}
	rplan, err := processTableExpr(ajoin.RightExpr, vschema)
	if err != nil {
		return nil, err
	}
	return joinBuilders(lplan, rplan, ajoin)
}

// convertToLeftJoin converts a right join into a left join.
func convertToLeftJoin(ajoin *sqlparser.JoinTableExpr) {
	newRHS := ajoin.LeftExpr
	// If the LHS is a join, we have to parenthesize it.
	// Otherwise, it can be used as is.
	if _, ok := newRHS.(*sqlparser.JoinTableExpr); ok {
		newRHS = &sqlparser.ParenTableExpr{
			Exprs: sqlparser.TableExprs{newRHS},
		}
	}
	ajoin.LeftExpr, ajoin.RightExpr = ajoin.RightExpr, newRHS
	ajoin.Join = sqlparser.LeftJoinStr
}

func joinBuilders(left, right builder, ajoin *sqlparser.JoinTableExpr) (builder, error) {
	lRoute, leftIsRoute := left.(*route)
	rRoute, rightIsRoute := right.(*route)
	if leftIsRoute && rightIsRoute {
		// If both are routes, they have an opportunity
		// to merge into one. route.Join performs this work.
		return lRoute.Join(rRoute, ajoin)
	}

	// If any of them is not a route, we have to build
	// a new join primitve.
	return newJoin(left, right, ajoin)
}
