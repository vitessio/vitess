// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

func findRoute(expr sqlparser.Expr, plan planBuilder) (route *routeBuilder, err error) {
	highestRoute := leftmost(plan)
	var subroutes []*routeBuilder
	err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			newRoute, isLocal, err := plan.Symtab().Find(node, true)
			if err != nil {
				return false, err
			}
			if isLocal && newRoute.Order() > highestRoute.Order() {
				highestRoute = newRoute
			}
		case *sqlparser.Subquery:
			sel, ok := node.Select.(*sqlparser.Select)
			if !ok {
				return false, errors.New("complex selects not allowd in subqueries")
			}
			subplan, err := processSelect(sel, plan.Symtab().VSchema, plan)
			if err != nil {
				return false, err
			}
			subroute, ok := subplan.(*routeBuilder)
			if !ok {
				return false, errors.New("subquery is too complex")
			}
			for _, extern := range subroute.Symtab().Externs {
				newRoute, isLocal, err := plan.Symtab().Find(extern, false)
				if err != nil {
					return false, err
				}
				if isLocal && newRoute.Order() > highestRoute.Order() {
					highestRoute = newRoute
				}
			}
			subroutes = append(subroutes, subroute)
			return false, nil
		}
		return true, nil
	}, expr)
	if err != nil {
		return nil, err
	}
	for _, subroute := range subroutes {
		err = subqueryCanMerge(highestRoute, subroute, subroute.Symtab())
		if err != nil {
			return nil, err
		}
		// This should be moved out if we become capable of processing
		// subqueries without push-down.
		subroute.Redirect = highestRoute
	}
	return highestRoute, nil
}

func subqueryCanMerge(outer, inner *routeBuilder, outersyms *symtab) error {
	if outer.Route.Keyspace.Name != inner.Route.Keyspace.Name {
		return errors.New("subquery keyspaces don't match")
	}
	if !inner.IsSingle() {
		return errors.New("subquery is not a single route")
	}
	if inner.Route.PlanID == SelectUnsharded {
		return nil
	}
	// SelectEqualUnique
	// TODO(sougou): check other cases.
	switch vals := inner.Route.Values.(type) {
	case *sqlparser.ColName:
		mergeRoute, _, _ := outersyms.Find(vals, false)
		if mergeRoute != outer {
			return errors.New("subquery dependency doesn't match parent route")
		}
	}
	return nil
}

// colIsValue returns true if the expression can be treated as a value
// for the current route. External references are treated as value.
func exprIsValue(expr sqlparser.ValExpr, route *routeBuilder) bool {
	switch node := expr.(type) {
	case *sqlparser.ColName:
		switch meta := node.Metadata.(type) {
		case *colsym:
			return meta.Route() != route
		case *tableAlias:
			return meta.Route() != route
		}
		panic("unreachable")
	case sqlparser.ValArg, sqlparser.StrVal, sqlparser.NumVal:
		return true
	}
	return false
}

func leftmost(plan planBuilder) *routeBuilder {
	switch plan := plan.(type) {
	case *joinBuilder:
		return leftmost(plan.Left)
	case *routeBuilder:
		return plan
	}
	panic("unexpected")
}
