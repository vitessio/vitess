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

// This file has functions to analyze the FROM clause.

var infoSchema = sqlparser.NewTableIdent("information_schema")

// processTableExprs analyzes the FROM clause. It produces a builder
// with all the routes identified.
func processTableExprs(tableExprs sqlparser.TableExprs, vschema VSchema) (builder, error) {
	if len(tableExprs) != 1 {
		lplan, err := processTableExpr(tableExprs[0], vschema)
		if err != nil {
			return nil, err
		}
		rplan, err := processTableExprs(tableExprs[1:], vschema)
		if err != nil {
			return nil, err
		}
		return lplan.Join(rplan, nil)
	}
	return processTableExpr(tableExprs[0], vschema)
}

// processTableExpr produces a builder subtree for the given TableExpr.
func processTableExpr(tableExpr sqlparser.TableExpr, vschema VSchema) (builder, error) {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		return processAliasedTable(tableExpr, vschema)
	case *sqlparser.ParenTableExpr:
		bldr, err := processTableExprs(tableExpr.Exprs, vschema)
		// We want to point to the higher level parenthesis because
		// more routes can be merged with this one. If so, the order
		// should be maintained as dictated by the parenthesis.
		if rb, ok := bldr.(*route); ok {
			// If a route is returned in this context, then we know that it only
			// contains the from clause of a brand new, partially built Select.
			// If there was a subquery, it would have been aliased to a name and the
			// entire thing would still be the from clause of a partially built Select.
			rb.Select.(*sqlparser.Select).From = sqlparser.TableExprs{tableExpr}
		}
		return bldr, err
	case *sqlparser.JoinTableExpr:
		return processJoin(tableExpr, vschema)
	}
	panic("unreachable")
}

// processAliasedTable produces a builder subtree for the given AliasedTableExpr.
// If the expression is a subquery, then the route built for it will contain
// the entire subquery tree in the from clause, as if it were a table.
// The symtab entry for the query will be a table where the columns
// will be built from the select expressions of the subquery.
// Since the table aliases only contain vindex columns, we'll follow
// the same rule: only columns from the subquery that are identified as
// vindex columns will be added to the table.
// A symtab symbol can only point to a route. This means that we cannot
// support complex joins in subqueries yet.
func processAliasedTable(tableExpr *sqlparser.AliasedTableExpr, vschema VSchema) (builder, error) {
	switch expr := tableExpr.Expr.(type) {
	case sqlparser.TableName:
		eroute, table, err := buildERoute(expr, vschema)
		if err != nil {
			return nil, err
		}
		rb := newRoute(
			&sqlparser.Select{From: sqlparser.TableExprs([]sqlparser.TableExpr{tableExpr})},
			eroute,
			vschema,
		)
		if table != nil {
			alias := expr
			if !tableExpr.As.IsEmpty() {
				alias = sqlparser.TableName{Name: tableExpr.As}
			}
			rb.symtab.InitWithAlias(alias, table, rb)
		}
		return rb, nil
	case *sqlparser.Subquery:
		var err error
		var subplan builder
		switch stmt := expr.Select.(type) {
		case *sqlparser.Select:
			subplan, err = processSelect(stmt, vschema, nil)
		case *sqlparser.Union:
			subplan, err = processUnion(stmt, vschema, nil)
		default:
			panic("unreachable")
		}
		if err != nil {
			return nil, err
		}
		subroute, ok := subplan.(*route)
		if !ok {
			return nil, errors.New("unsupported: complex join in subqueries")
		}
		table := &vindexes.Table{
			Keyspace: subroute.ERoute.Keyspace,
		}
		for _, rc := range subroute.ResultColumns {
			if rc.column.Vindex == nil {
				continue
			}
			// Check if a colvindex of the same name already exists.
			// Dups are not allowed in subqueries in this situation.
			for _, colVindex := range table.ColumnVindexes {
				if colVindex.Column.Equal(rc.alias) {
					return nil, fmt.Errorf("duplicate column aliases: %v", rc.alias)
				}
			}
			table.ColumnVindexes = append(table.ColumnVindexes, &vindexes.ColumnVindex{
				Column: rc.alias,
				Vindex: rc.column.Vindex,
			})
		}
		rb := newRoute(
			&sqlparser.Select{From: sqlparser.TableExprs([]sqlparser.TableExpr{tableExpr})},
			subroute.ERoute,
			vschema,
		)
		rb.symtab.InitWithAlias(sqlparser.TableName{Name: tableExpr.As}, table, rb)
		subroute.Redirect = rb
		return rb, nil
	}
	panic("unreachable")
}

// buildERoute produces the initial engine.Route for the specified TableName.
// It also returns the associated vschema info (*Table) so that
// it can be used to create the symbol table entry.
func buildERoute(tableName sqlparser.TableName, vschema VSchema) (*engine.Route, *vindexes.Table, error) {
	if tableName.Qualifier == infoSchema {
		ks, err := vschema.DefaultKeyspace()
		if err != nil {
			return nil, nil, err
		}
		return engine.NewRoute(engine.ExecDBA, ks), nil, nil
	}

	table, err := vschema.Find(tableName)
	if err != nil {
		return nil, nil, err
	}
	if table.Keyspace.Sharded {
		return engine.NewRoute(engine.SelectScatter, table.Keyspace), table, nil
	}
	return engine.NewRoute(engine.SelectUnsharded, table.Keyspace), table, nil
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
	return lplan.Join(rplan, ajoin)
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
