// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

func processSelectExprs(sel *sqlparser.Select, plan planBuilder, syms *symtab) error {
	err := checkAggregates(sel, plan, syms)
	if err != nil {
		return err
	}
	if sel.Distinct != "" {
		// We know it's a routeBuilder, but this may change
		// in the distant future.
		plan.(*routeBuilder).Select.Distinct = sel.Distinct
	}
	colsyms, err := findSelectRoutes(sel.SelectExprs, syms, leftmost(plan))
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

func checkAggregates(sel *sqlparser.Select, plan planBuilder, syms *symtab) error {
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.FuncExpr:
			if node.IsAggregate() {
				// TODO(sougou): better error.
				return false, errors.New("query is too complex to allow aggregates")
			}
		}
		return true, nil
	}, sel.SelectExprs)
	if err == nil && sel.Distinct == "" {
		return nil
	}

	// Check if we can allow aggregates.
	route, ok := plan.(*routeBuilder)
	if !ok {
		return err
	}
	if route.IsSingle() {
		return nil
	}
	// It's a scatter route. We can allow aggregates if there is a unique
	// vindex in the select list.
	for _, selectExpr := range sel.SelectExprs {
		switch selectExpr := selectExpr.(type) {
		case *sqlparser.NonStarExpr:
			vindex := syms.Vindex(selectExpr.Expr, route, true)
			if vindex != nil && IsUnique(vindex) {
				return nil
			}
		}
	}
	return err
}

func findSelectRoutes(selectExprs sqlparser.SelectExprs, syms *symtab, startingRoute *routeBuilder) ([]*colsym, error) {
	colsyms := make([]*colsym, len(selectExprs))
	for i, node := range selectExprs {
		colsyms[i] = newColsym(syms)
		node, ok := node.(*sqlparser.NonStarExpr)
		if !ok {
			return nil, errors.New("* expressions not allowed")
		}
		var err error
		colsyms[i].Route, err = findRoute(node.Expr, syms, startingRoute)
		if err != nil {
			return nil, err
		}
		if node.As != "" {
			colsyms[i].Alias = node.As
		}
		col, ok := node.Expr.(*sqlparser.ColName)
		if ok {
			if colsyms[i].Alias == "" {
				colsyms[i].Alias = sqlparser.SQLName(sqlparser.String(col))
			}
			colsyms[i].Vindex = syms.Vindex(col, colsyms[i].Route, true)
			if _, isLocal, _ := syms.Find(col, true); isLocal {
				colsyms[i].Underlying = newColref(col)
			}
		}
	}
	return colsyms, nil
}

func pushSelect(selectExpr sqlparser.SelectExpr, plan planBuilder, colsym *colsym) int {
	routeNumber := colsym.Route.Order()
	switch plan := plan.(type) {
	case *joinBuilder:
		if routeNumber <= plan.LeftOrder {
			ret := pushSelect(selectExpr, plan.Left, colsym)
			plan.Join.Cols = append(plan.Join.Cols, -ret-1)
		} else {
			ret := pushSelect(selectExpr, plan.Right, colsym)
			plan.Join.Cols = append(plan.Join.Cols, ret+1)
		}
		return len(plan.Join.Cols) - 1
	case *routeBuilder:
		if routeNumber != plan.Order() {
			// TODO(sougou): remove after testing
			panic("unexpcted values")
		}
		plan.Select.SelectExprs = append(plan.Select.SelectExprs, selectExpr)
		plan.Colsyms = append(plan.Colsyms, colsym)
		return len(plan.Colsyms) - 1
	}
	panic("unexpected")
}
