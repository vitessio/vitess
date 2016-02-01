// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

func processSelectExprs(sel *sqlparser.Select, planBuilder PlanBuilder, symbolTable *SymbolTable) error {
	allowAggregates := checkAllowAggregates(sel.SelectExprs, planBuilder, symbolTable)
	if sel.Distinct != "" {
		if !allowAggregates {
			return errors.New("query is too complex to allow aggregates")
		}
		// We know it's a RouteBuilder, but this may change
		// in the distant future.
		planBuilder.(*RouteBuilder).Select.Distinct = sel.Distinct
	}
	selectSymbols, err := findSelectRoutes(sel.SelectExprs, allowAggregates, symbolTable)
	if err != nil {
		return err
	}
	for i, selectExpr := range sel.SelectExprs {
		pushSelect(selectExpr, planBuilder, selectSymbols[i].Route.Order())
	}
	symbolTable.SelectSymbols = selectSymbols
	err = processGroupBy(sel.GroupBy, planBuilder, symbolTable)
	if err != nil {
		return err
	}
	return nil
}

func findSelectRoutes(selectExprs sqlparser.SelectExprs, allowAggregates bool, symbolTable *SymbolTable) ([]SelectSymbol, error) {
	selectSymbols := make([]SelectSymbol, len(selectExprs))
	for colnum, selectExpr := range selectExprs {
		err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.StarExpr:
				return false, errors.New("* expressions not allowed")
			case *sqlparser.Subquery:
				// TODO(sougou): implement.
				return false, errors.New("subqueries not supported yet")
			case *sqlparser.NonStarExpr:
				if node.As != "" {
					selectSymbols[colnum].Alias = node.As
				}
				col, ok := node.Expr.(*sqlparser.ColName)
				if ok {
					if selectSymbols[colnum].Alias == "" {
						selectSymbols[colnum].Alias = sqlparser.SQLName(sqlparser.String(col))
					}
					_, selectSymbols[colnum].Vindex = symbolTable.FindColumn(col, nil, true)
				}
			case *sqlparser.ColName:
				routeBuilder, _ := symbolTable.FindColumn(node, nil, true)
				if routeBuilder != nil {
					if selectSymbols[colnum].Route == nil {
						selectSymbols[colnum].Route = routeBuilder
					} else if selectSymbols[colnum].Route != routeBuilder {
						// TODO(sougou): better error.
						return false, errors.New("select expression is too complex")
					}
				}
			case *sqlparser.FuncExpr:
				if node.IsAggregate() {
					if !allowAggregates {
						// TODO(sougou): better error.
						return false, errors.New("query is too complex to allow aggregates")
					}
				}
			}
			return true, nil
		}, selectExpr)
		if err != nil {
			return nil, err
		}
		if selectSymbols[colnum].Route == nil {
			selectSymbols[colnum].Route = symbolTable.FirstRoute
		}
	}
	return selectSymbols, nil
}

func pushSelect(selectExpr sqlparser.SelectExpr, planBuilder PlanBuilder, routeNumber int) {
	switch planBuilder := planBuilder.(type) {
	case *JoinBuilder:
		if routeNumber <= planBuilder.LeftOrder {
			pushSelect(selectExpr, planBuilder.Left, routeNumber)
			planBuilder.Join.LeftCols = append(planBuilder.Join.LeftCols, planBuilder.Join.Len())
			return
		}
		pushSelect(selectExpr, planBuilder.Right, routeNumber)
		planBuilder.Join.RightCols = append(planBuilder.Join.RightCols, planBuilder.Join.Len())
	case *RouteBuilder:
		if routeNumber != planBuilder.Order() {
			// TODO(sougou): remove after testing
			panic("unexpcted values")
		}
		planBuilder.Select.SelectExprs = append(planBuilder.Select.SelectExprs, selectExpr)
	}
}

func checkAllowAggregates(selectExprs sqlparser.SelectExprs, planBuilder PlanBuilder, symbolTable *SymbolTable) bool {
	routeBuilder, ok := planBuilder.(*RouteBuilder)
	if !ok {
		return false
	}
	if routeBuilder.IsSingle() {
		return true
	}
	// It's a scatter route. We can allow aggregates if there is a unique
	// vindex in the select list.
	for _, selectExpr := range selectExprs {
		switch selectExpr := selectExpr.(type) {
		case *sqlparser.NonStarExpr:
			_, vindex := symbolTable.FindColumn(selectExpr.Expr, nil, true)
			if vindex != nil && IsUnique(vindex) {
				return true
			}
		}
	}
	return false
}

func processGroupBy(groupBy sqlparser.GroupBy, planBuilder PlanBuilder, symbolTable *SymbolTable) error {
	if groupBy == nil {
		return nil
	}
	routeBuilder, ok := planBuilder.(*RouteBuilder)
	if !ok {
		return errors.New("query is too complex to allow aggregates")
	}
	if hasSubqueries(groupBy) {
		return errors.New("subqueries not supported in group by")
	}
	if routeBuilder.IsSingle() {
		routeBuilder.Select.GroupBy = groupBy
		return nil
	}
	// It's a scatter route. We can allow group by if it references a
	// column with a unique vindex.
	for _, expr := range groupBy {
		_, vindex := symbolTable.FindColumn(expr, nil, true)
		if vindex != nil && IsUnique(vindex) {
			routeBuilder.Select.GroupBy = groupBy
			return nil
		}
	}
	return errors.New("query is too complex to allow aggregates")
}

func hasSubqueries(node sqlparser.SQLNode) bool {
	has := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node.(type) {
		case *sqlparser.Subquery:
			has = true
			// TODO(sougou): better error.
			return false, errors.New("dummy")
		}
		return true, nil
	}, node)
	return has
}
