// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// buildSelectPlan2 is the new function to build a Select plan.
func buildSelectPlan(sel *sqlparser.Select, vschema *VSchema) (plan interface{}, err error) {
	bindvars := getBindvars(sel)
	builder, err := processSelect(sel, vschema, nil)
	if err != nil {
		return nil, err
	}
	err = newGenerator(builder, bindvars).Generate()
	if err != nil {
		return nil, err
	}
	return getUnderlyingPlan(builder), nil
}

// getBindvars returns a map of the bind vars referenced in the statement.
func getBindvars(node sqlparser.SQLNode) map[string]struct{} {
	bindvars := make(map[string]struct{})
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case sqlparser.ValArg:
			bindvars[string(node[1:])] = struct{}{}
		case sqlparser.ListArg:
			bindvars[string(node[2:])] = struct{}{}
		}
		return true, nil
	}, node)
	return bindvars
}

// processSelect builds a plan for the given query or subquery.
func processSelect(sel *sqlparser.Select, vschema *VSchema, outer planBuilder) (planBuilder, error) {
	plan, err := processTableExprs(sel.From, vschema)
	if err != nil {
		return nil, err
	}
	if outer != nil {
		plan.Symtab().Outer = outer.Symtab()
	}
	if sel.Where != nil {
		err = processBoolExpr(sel.Where.Expr, plan, sqlparser.WhereStr)
		if err != nil {
			return nil, err
		}
	}
	err = processSelectExprs(sel, plan)
	if err != nil {
		return nil, err
	}
	if sel.Having != nil {
		err = processBoolExpr(sel.Having.Expr, plan, sqlparser.HavingStr)
		if err != nil {
			return nil, err
		}
	}
	err = processOrderBy(sel.OrderBy, plan)
	if err != nil {
		return nil, err
	}
	err = processLimit(sel.Limit, plan)
	if err != nil {
		return nil, err
	}
	processMisc(sel, plan)
	return plan, nil
}

func processBoolExpr(boolExpr sqlparser.BoolExpr, plan planBuilder, whereType string) error {
	filters := splitAndExpression(nil, boolExpr)
	for _, filter := range filters {
		route, err := findRoute(filter, plan)
		if err != nil {
			return err
		}
		err = route.PushFilter(filter, whereType)
		if err != nil {
			return err
		}
	}
	return nil
}

func processSelectExprs(sel *sqlparser.Select, plan planBuilder) error {
	err := checkAggregates(sel, plan)
	if err != nil {
		return err
	}
	if sel.Distinct != "" {
		// We know it's a routeBuilder, but this may change
		// in the distant future.
		plan.(*routeBuilder).MakeDistinct()
	}
	colsyms, err := pushSelectRoutes(sel.SelectExprs, plan)
	if err != nil {
		return err
	}
	plan.Symtab().Colsyms = colsyms
	err = processGroupBy(sel.GroupBy, plan)
	if err != nil {
		return err
	}
	return nil
}

func checkAggregates(sel *sqlparser.Select, plan planBuilder) error {
	hasAggregates := false
	if sel.Distinct != "" {
		hasAggregates = true
	} else {
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.FuncExpr:
				if node.IsAggregate() {
					hasAggregates = true
					return false, errors.New("dummy")
				}
			}
			return true, nil
		}, sel.SelectExprs)
	}
	if !hasAggregates {
		return nil
	}

	// Check if we can allow aggregates.
	route, ok := plan.(*routeBuilder)
	if !ok {
		return errors.New("unsupported: complex join with aggregates")
	}
	if route.IsSingle() {
		return nil
	}
	// It's a scatter route. We can allow aggregates if there is a unique
	// vindex in the select list.
	for _, selectExpr := range sel.SelectExprs {
		switch selectExpr := selectExpr.(type) {
		case *sqlparser.NonStarExpr:
			vindex := plan.Symtab().Vindex(selectExpr.Expr, route, true)
			if vindex != nil && IsUnique(vindex) {
				return nil
			}
		}
	}
	return errors.New("unsupported: scatter with aggregates")
}

func pushSelectRoutes(selectExprs sqlparser.SelectExprs, plan planBuilder) ([]*colsym, error) {
	colsyms := make([]*colsym, len(selectExprs))
	for i, node := range selectExprs {
		node, ok := node.(*sqlparser.NonStarExpr)
		if !ok {
			return nil, errors.New("unsupported: '*' expression in select")
		}
		route, err := findRoute(node.Expr, plan)
		if err != nil {
			return nil, err
		}
		colsyms[i], _, err = plan.PushSelect(node, route)
		if err != nil {
			return nil, err
		}
	}
	return colsyms, nil
}
