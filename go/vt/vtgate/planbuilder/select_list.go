// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

func processSelectExprs(sel *sqlparser.Select, plan planBuilder, syms *symtab) error {
	allowAggregates := checkAllowAggregates(sel.SelectExprs, plan, syms)
	if sel.Distinct != "" {
		if !allowAggregates {
			return errors.New("query is too complex to allow aggregates")
		}
		// We know it's a routeBuilder, but this may change
		// in the distant future.
		plan.(*routeBuilder).Select.Distinct = sel.Distinct
	}
	colsyms, err := findSelectRoutes(sel.SelectExprs, allowAggregates, syms)
	if err != nil {
		return err
	}
	for i, selectExpr := range sel.SelectExprs {
		pushSelect(selectExpr, plan, colsyms[i])
	}
	syms.Colsyms = colsyms
	err = processGroupBy(sel.GroupBy, plan, syms)
	if err != nil {
		return err
	}
	return nil
}

func findSelectRoutes(selectExprs sqlparser.SelectExprs, allowAggregates bool, syms *symtab) ([]*colsym, error) {
	colsyms := make([]*colsym, len(selectExprs))
	for i, selectExpr := range selectExprs {
		colsyms[i] = &colsym{Route: syms.FirstRoute}
		err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.StarExpr:
				return false, errors.New("* expressions not allowed")
			case *sqlparser.Subquery:
				// TODO(sougou): implement.
				return false, errors.New("subqueries not supported yet")
			case *sqlparser.NonStarExpr:
				if node.As != "" {
					colsyms[i].Alias = node.As
				}
				col, ok := node.Expr.(*sqlparser.ColName)
				if !ok {
					break
				}
				if colsyms[i].Alias == "" {
					colsyms[i].Alias = sqlparser.SQLName(sqlparser.String(col))
				}
				var newRoute *routeBuilder
				newRoute, colsyms[i].Vindex = syms.Find(col, nil, true)
				if newRoute != nil {
					colsyms[i].Underlying = *col
					if newRoute.Order() > colsyms[i].Route.Order() {
						colsyms[i].Route = newRoute
					}
				}
				return false, nil
			case *sqlparser.ColName:
				newRoute, _ := syms.Find(node, nil, true)
				if newRoute != nil && newRoute.Order() > colsyms[i].Route.Order() {
					colsyms[i].Route = newRoute
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
		if colsyms[i].Route == nil {
			colsyms[i].Route = syms.FirstRoute
		}
	}
	return colsyms, nil
}

func pushSelect(selectExpr sqlparser.SelectExpr, plan planBuilder, colsym *colsym) {
	routeNumber := colsym.Route.Order()
	switch plan := plan.(type) {
	case *joinBuilder:
		if routeNumber <= plan.LeftOrder {
			pushSelect(selectExpr, plan.Left, colsym)
			plan.Join.LeftCols = append(plan.Join.LeftCols, plan.Join.Len())
			plan.LColsym = append(plan.LColsym, colsym)
			return
		}
		pushSelect(selectExpr, plan.Right, colsym)
		plan.Join.RightCols = append(plan.Join.RightCols, plan.Join.Len())
		plan.RColsym = append(plan.RColsym, colsym)
	case *routeBuilder:
		if routeNumber != plan.Order() {
			// TODO(sougou): remove after testing
			panic("unexpcted values")
		}
		plan.Select.SelectExprs = append(plan.Select.SelectExprs, selectExpr)
		plan.Colsym = append(plan.Colsym, colsym)
	}
}

func checkAllowAggregates(selectExprs sqlparser.SelectExprs, plan planBuilder, syms *symtab) bool {
	route, ok := plan.(*routeBuilder)
	if !ok {
		return false
	}
	if route.IsSingle() {
		return true
	}
	// It's a scatter route. We can allow aggregates if there is a unique
	// vindex in the select list.
	for _, selectExpr := range selectExprs {
		switch selectExpr := selectExpr.(type) {
		case *sqlparser.NonStarExpr:
			_, vindex := syms.Find(selectExpr.Expr, nil, true)
			if vindex != nil && IsUnique(vindex) {
				return true
			}
		}
	}
	return false
}

func processGroupBy(groupBy sqlparser.GroupBy, plan planBuilder, syms *symtab) error {
	if groupBy == nil {
		return nil
	}
	route, ok := plan.(*routeBuilder)
	if !ok {
		return errors.New("query is too complex to allow aggregates")
	}
	if hasSubqueries(groupBy) {
		return errors.New("subqueries not supported in group by")
	}
	if route.IsSingle() {
		route.Select.GroupBy = groupBy
		return nil
	}
	// It's a scatter route. We can allow group by if it references a
	// column with a unique vindex.
	for _, expr := range groupBy {
		_, vindex := syms.Find(expr, nil, true)
		if vindex != nil && IsUnique(vindex) {
			route.Select.GroupBy = groupBy
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
