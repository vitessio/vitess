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
	return processBoolExpr(where.Expr, symbolTable)
}

func processBoolExpr(boolExpr sqlparser.BoolExpr, symbolTable *SymbolTable) error {
	filters := splitAndExpression(nil, boolExpr)
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
	if routeBuilder.IsRHS {
		// TODO(sougou): improve error.
		return errors.New("cannot push where clause into a LEFT JOIN route")
	}
	routeBuilder.Select.AddWhere(filter)
	updateRoute(routeBuilder, symbolTable, filter)
	return nil
}

func findRoute(filter sqlparser.BoolExpr, symbolTable *SymbolTable) (routeBuilder *RouteBuilder, err error) {
	highestRoute := symbolTable.FirstRoute
	err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			tableAlias, _ := symbolTable.FindColumn(node, nil, true)
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

func updateRoute(routeBuilder *RouteBuilder, symbolTable *SymbolTable, filter sqlparser.BoolExpr) {
	planID, vindex, values := computePlan(routeBuilder, symbolTable, filter)
	if planID == SelectScatter {
		return
	}
	switch routeBuilder.Route.PlanID {
	case SelectEqualUnique:
		if planID == SelectEqualUnique && vindex.Cost() < routeBuilder.Route.Vindex.Cost() {
			routeBuilder.Route.SetPlan(planID, vindex, values)
		}
	case SelectEqual:
		switch planID {
		case SelectEqualUnique:
			routeBuilder.Route.SetPlan(planID, vindex, values)
		case SelectEqual:
			if vindex.Cost() < routeBuilder.Route.Vindex.Cost() {
				routeBuilder.Route.SetPlan(planID, vindex, values)
			}
		}
	case SelectIN:
		switch planID {
		case SelectEqualUnique, SelectEqual:
			routeBuilder.Route.SetPlan(planID, vindex, values)
		case SelectIN:
			if vindex.Cost() < routeBuilder.Route.Vindex.Cost() {
				routeBuilder.Route.SetPlan(planID, vindex, values)
			}
		}
	case SelectScatter:
		switch planID {
		case SelectEqualUnique, SelectEqual, SelectIN:
			routeBuilder.Route.SetPlan(planID, vindex, values)
		}
	}
}

func computePlan(routeBuilder *RouteBuilder, symbolTable *SymbolTable, filter sqlparser.BoolExpr) (planID PlanID, vindex Vindex, values interface{}) {
	switch node := filter.(type) {
	case *sqlparser.ComparisonExpr:
		switch node.Operator {
		case sqlparser.EqualStr:
			return computeEqualPlan(routeBuilder, symbolTable, node)
		case sqlparser.InStr:
			return computeINPlan(routeBuilder, symbolTable, node)
		}
	}
	return SelectScatter, nil, nil
}

func computeEqualPlan(routeBuilder *RouteBuilder, symbolTable *SymbolTable, comparison *sqlparser.ComparisonExpr) (planID PlanID, vindex Vindex, values interface{}) {
	left := comparison.Left
	right := comparison.Right
	_, colVindex := symbolTable.FindColumn(left, routeBuilder, true)
	if colVindex == nil {
		left, right = right, left
		_, colVindex = symbolTable.FindColumn(left, routeBuilder, false)
		if colVindex == nil {
			return SelectScatter, nil, nil
		}
	}
	if !symbolTable.IsValue(right, routeBuilder) {
		return SelectScatter, nil, nil
	}
	if IsUnique(colVindex.Vindex) {
		return SelectEqualUnique, colVindex.Vindex, right
	}
	return SelectEqual, colVindex.Vindex, right
}

func computeINPlan(routeBuilder *RouteBuilder, symbolTable *SymbolTable, comparison *sqlparser.ComparisonExpr) (planID PlanID, vindex Vindex, values interface{}) {
	_, colVindex := symbolTable.FindColumn(comparison.Left, routeBuilder, true)
	if colVindex == nil {
		return SelectScatter, nil, nil
	}
	switch node := comparison.Right.(type) {
	case sqlparser.ValTuple:
		for _, n := range node {
			if !symbolTable.IsValue(n, routeBuilder) {
				return SelectScatter, nil, nil
			}
		}
		return SelectIN, colVindex.Vindex, comparison
	case sqlparser.ListArg:
		return SelectIN, colVindex.Vindex, comparison
	}
	return SelectScatter, nil, nil
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
