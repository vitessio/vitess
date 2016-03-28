// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// This file has functions to analyze the FROM clause
// for select statements.

// processTableExprs analyzes the FROM clause. It produces a planBuilder
// with all the routes identified.
func processTableExprs(tableExprs sqlparser.TableExprs, vschema *VSchema) (planBuilder, error) {
	if len(tableExprs) != 1 {
		return nil, errors.New("unsupported: ',' join operator")
	}
	return processTableExpr(tableExprs[0], vschema)
}

// processTableExpr produces a planBuilder subtree for the given TableExpr.
func processTableExpr(tableExpr sqlparser.TableExpr, vschema *VSchema) (planBuilder, error) {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		return processAliasedTable(tableExpr, vschema)
	case *sqlparser.ParenTableExpr:
		plan, err := processTableExprs(tableExpr.Exprs, vschema)
		// We want to point to the higher level parenthesis because
		// more routes can be merged with this one. If so, the order
		// should be maintained as dictated by the parenthesis.
		if route, ok := plan.(*routeBuilder); ok {
			route.Select.From = sqlparser.TableExprs{tableExpr}
		}
		return plan, err
	case *sqlparser.JoinTableExpr:
		return processJoin(tableExpr, vschema)
	}
	panic("unreachable")
}

// processAliasedTable produces a planBuilder subtree for the given AliasedTableExpr.
// If the expression is a subquery, then the the route built for it will contain
// the entire subquery tree in the from clause, as if it was a table.
// The symtab entry for the query will be a tableAlias where the columns
// will be built from the select expressions of the subquery.
// Since the table aliases only contain vindex columns, we'll follow
// the same rule: only columns from the subquery that are identified as
// vindex columns will be added to the tableAlias.
// The above statements imply that a subquery is allowed only if it's a route
// that can be treated like a normal table. If not, we return an error.
func processAliasedTable(tableExpr *sqlparser.AliasedTableExpr, vschema *VSchema) (planBuilder, error) {
	switch expr := tableExpr.Expr.(type) {
	case *sqlparser.TableName:
		route, table, err := getTablePlan(expr, vschema)
		if err != nil {
			return nil, err
		}
		alias := expr.Name
		if tableExpr.As != "" {
			alias = tableExpr.As
		}
		return newRouteBuilder(
			sqlparser.TableExprs([]sqlparser.TableExpr{tableExpr}),
			route,
			table,
			vschema,
			alias,
		), nil
	case *sqlparser.Subquery:
		sel, ok := expr.Select.(*sqlparser.Select)
		if !ok {
			return nil, errors.New("unsupported: union operator in subqueries")
		}
		subplan, err := processSelect(sel, vschema, nil)
		if err != nil {
			return nil, err
		}
		subroute, ok := subplan.(*routeBuilder)
		if !ok {
			return nil, errors.New("unsupported: complex join in subqueries")
		}
		table := &Table{
			Keyspace: subroute.Route.Keyspace,
		}
		for _, colsyms := range subroute.Colsyms {
			if colsyms.Vindex == nil {
				continue
			}
			table.ColVindexes = append(table.ColVindexes, &ColVindex{
				Col:    string(colsyms.Alias),
				Vindex: colsyms.Vindex,
			})
		}
		rtb := newRouteBuilder(
			sqlparser.TableExprs([]sqlparser.TableExpr{tableExpr}),
			subroute.Route,
			table,
			vschema,
			tableExpr.As,
		)
		subroute.Redirect = rtb
		return rtb, nil
	}
	panic("unreachable")
}

// getTablePlan produces the initial Route for the specified TableName.
// It also returns the associated vschema info (*Table) so that
// it can be used to create the symbol table entry.
func getTablePlan(tableName *sqlparser.TableName, vschema *VSchema) (*Route, *Table, error) {
	if tableName.Qualifier != "" {
		return nil, nil, errors.New("unsupported: keyspace name qualifier for tables")
	}
	table, err := vschema.FindTable(string(tableName.Name))
	if err != nil {
		return nil, nil, err
	}
	if table.Keyspace.Sharded {
		return &Route{
			Opcode:   SelectScatter,
			Keyspace: table.Keyspace,
			JoinVars: make(map[string]struct{}),
		}, table, nil
	}
	return &Route{
		Opcode:   SelectUnsharded,
		Keyspace: table.Keyspace,
		JoinVars: make(map[string]struct{}),
	}, table, nil
}

// processJoin produces a planBuilder subtree for the given Join.
// If the left and right nodes can be part of the same route,
// then it's a routeBuilder. Otherwise, it's a joinBuilder.
func processJoin(join *sqlparser.JoinTableExpr, vschema *VSchema) (planBuilder, error) {
	switch join.Join {
	case sqlparser.JoinStr, sqlparser.StraightJoinStr, sqlparser.LeftJoinStr:
	case sqlparser.RightJoinStr:
		convertToLeftJoin(join)
	default:
		return nil, fmt.Errorf("unsupported: %s", join.Join)
	}
	lplan, err := processTableExpr(join.LeftExpr, vschema)
	if err != nil {
		return nil, err
	}
	rplan, err := processTableExpr(join.RightExpr, vschema)
	if err != nil {
		return nil, err
	}
	return lplan.Join(rplan, join)
}

// convertToLeftJoin converts a right join into a left join.
func convertToLeftJoin(join *sqlparser.JoinTableExpr) {
	newRHS := join.LeftExpr
	// If the LHS is a join, we have to parenthesize it.
	// Otherwise, it can be used as is.
	if _, ok := newRHS.(*sqlparser.JoinTableExpr); ok {
		newRHS = &sqlparser.ParenTableExpr{
			Exprs: sqlparser.TableExprs{newRHS},
		}
	}
	join.LeftExpr, join.RightExpr = join.RightExpr, newRHS
	join.Join = sqlparser.LeftJoinStr
}
