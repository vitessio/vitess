// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

func processWhere(where *sqlparser.Where, symbolTable *SymbolTable) error {
	if where == nil {
		return nil
	}
	filters := splitAndExpression(nil, where.Expr)
	for _, filter := range filters {
		err := processFilter(filter, symbolTable)
		if err != nil {
			return err
		}
	}
	return nil
}

func processFilter(filter sqlparser.BoolExpr, symbolTable *SymbolTable) error {
	routeBuilder, err := findRoute(filter, symbolTable)
	if err != nil {
		return err
	}
	pushFilter(filter, routeBuilder)
	if routeBuilder.IsRHS {
		// TODO(sougou): improve error.
		return errors.New("cannot push where clause into a LEFT JOIN route")
	}
	return nil
}

func findRoute(filter sqlparser.BoolExpr, symbolTable *SymbolTable) (routeBuilder *RouteBuilder, err error) {
	highestRoute := symbolTable.FirstRoute
	err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			tableAlias, _ := symbolTable.FindColumn(node, true)
			if tableAlias == nil {
				// Skip unresolved references.
				return true, nil
			}
			if tableAlias.Route.Order() > highestRoute.Order() {
				highestRoute = tableAlias.Route
			}
		case *sqlparser.Subquery:
			// TODO(sougou): implement.
			return false, errors.New("subqueries not supported yet")
		}
		return true, nil
	}, filter)
	if err != nil {
		return nil, err
	}
	return highestRoute, nil
}

func pushFilter(filter sqlparser.BoolExpr, routeBuilder *RouteBuilder) {
	routeBuilder.Select.AddWhere(filter)
}

// splitAndExpression breaks up the BoolExpr into AND-separated conditions
// and appends them to filters, which can be shuffled and recombined
// as needed.
func splitAndExpression(filters []sqlparser.BoolExpr, node sqlparser.BoolExpr) []sqlparser.BoolExpr {
	if node, ok := node.(*sqlparser.AndExpr); ok {
		filters = splitAndExpression(filters, node.Left)
		return splitAndExpression(filters, node.Right)
	}
	return append(filters, node)
}
