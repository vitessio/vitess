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
	for i, node := range selectExprs {
		colsyms[i] = &colsym{Route: syms.FirstRoute}
		node, ok := node.(*sqlparser.NonStarExpr)
		if !ok {
			return nil, errors.New("* expressions not allowed")
		}
		if node.As != "" {
			colsyms[i].Alias = node.As
		}
		col, ok := node.Expr.(*sqlparser.ColName)
		if ok {
			if colsyms[i].Alias == "" {
				colsyms[i].Alias = sqlparser.SQLName(sqlparser.String(col))
			}
			var newRoute *routeBuilder
			var err error
			newRoute, err = syms.Find(col, true)
			if err != nil {
				return nil, err
			}
			if newRoute != nil {
				colsyms[i].Vindex = syms.Vindex(col, newRoute, true)
				colsyms[i].Underlying = *col
				if newRoute.Order() > colsyms[i].Route.Order() {
					colsyms[i].Route = newRoute
				}
			}
		}
		err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.ColName:
				newRoute, err := syms.Find(node, true)
				if err != nil {
					return false, err
				}
				if newRoute != nil && newRoute.Order() > colsyms[i].Route.Order() {
					colsyms[i].Route = newRoute
				}
			case *sqlparser.Subquery:
				// TODO(sougou): implement.
				return false, errors.New("subqueries not supported yet")
			case *sqlparser.FuncExpr:
				if node.IsAggregate() {
					if !allowAggregates {
						// TODO(sougou): better error.
						return false, errors.New("query is too complex to allow aggregates")
					}
				}
			}
			return true, nil
		}, node)
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
