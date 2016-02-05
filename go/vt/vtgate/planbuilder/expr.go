// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

func findRoute(expr sqlparser.Expr, syms *symtab) (route *routeBuilder, err error) {
	highestRoute := syms.FirstRoute
	var subroutes []*routeBuilder
	var subsymslist []*symtab
	err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			newRoute, isLocal, err := syms.Find(node, true)
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
			subplan, subsyms, err := buildSelectPlan2(sel, syms.Schema, syms)
			if err != nil {
				return false, err
			}
			subroute, ok := subplan.(*routeBuilder)
			if !ok {
				return false, errors.New("subquery is too complex")
			}
			for _, extern := range subsyms.Externs {
				newRoute, isLocal, err := syms.Find(extern, false)
				if err != nil {
					return false, err
				}
				if isLocal && newRoute.Order() > highestRoute.Order() {
					highestRoute = newRoute
				}
			}
			subroutes = append(subroutes, subroute)
			subsymslist = append(subsymslist, subsyms)
		}
		return true, nil
	}, expr)
	if err != nil {
		return nil, err
	}
	for _, subroute := range subroutes {
		err = subqueryCanMerge(highestRoute, subroute, syms)
		if err != nil {
			return nil, err
		}
	}
	for i := range subsymslist {
		subsymslist[i].Reroute(subroutes[i], highestRoute)
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
