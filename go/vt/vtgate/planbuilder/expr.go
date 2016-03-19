// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"bytes"
	"errors"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// splitAndExpression breaks up the BoolExpr into AND-separated conditions
// and appends them to filters, which can be shuffled and recombined
// as needed.
func splitAndExpression(filters []sqlparser.BoolExpr, node sqlparser.BoolExpr) []sqlparser.BoolExpr {
	if node == nil {
		return filters
	}
	if node, ok := node.(*sqlparser.AndExpr); ok {
		filters = splitAndExpression(filters, node.Left)
		return splitAndExpression(filters, node.Right)
	}
	return append(filters, node)
}

// findRoute identifies the right-most route for expr. In situations where
// the expression addresses multiple routes, the expectation is that the
// executor will use the results of the previous routes to feed the necessary
// values for the external references.
// If the expression contains a subquery, the right-most route identification
// also follows the same rules of a normal expression. This is achieved by
// looking at the Externs field of its symbol table that contains the list of
// external references.
// Once the target route is identified, we have to verify that the subquery's
// route can be merged with it. If it cannot, we fail the query. This is because
// we don't have the ability to wire up subqueries through expression evaluation
// primitives. Consequently, if the plan for a subquery comes out as a Join,
// we can immediately error out.
// Since findRoute can itself be called from within a subquery, it has to assume
// that some of the external references may actually be pointing to an outer
// query. The isLocal response from the symtab is used to make sure that we
// only analyze symbols that point to the current symtab.
// If an expression has no references to the current query, then the left-most
// route is chosen as the default.
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
				return false, errors.New("unsupported: union operator in subqueries")
			}
			subplan, err := processSelect(sel, plan.Symtab().VSchema, plan)
			if err != nil {
				return false, err
			}
			subroute, ok := subplan.(*routeBuilder)
			if !ok {
				return false, errors.New("unsupported: complex join in subqueries")
			}
			for _, extern := range subroute.Symtab().Externs {
				// No error expected. These are resolved externs.
				newRoute, isLocal, _ := plan.Symtab().Find(extern, false)
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
		err = subqueryCanMerge(highestRoute, subroute)
		if err != nil {
			return nil, err
		}
		// This should be moved out if we become capable of processing
		// subqueries without push-down.
		subroute.Redirect = highestRoute
	}
	return highestRoute, nil
}

// subqueryCanMerge returns nil if the inner subquery
// can be merged with the specified outer route. If it
// cannot, then it returns an appropriate error.
func subqueryCanMerge(outer, inner *routeBuilder) error {
	if outer.Route.Keyspace.Name != inner.Route.Keyspace.Name {
		return errors.New("unsupported: subquery keyspace different from outer query")
	}
	if !inner.IsSingle() {
		return errors.New("unsupported: scatter subquery")
	}
	if inner.Route.Opcode == SelectUnsharded {
		return nil
	}
	// SelectEqualUnique
	switch vals := inner.Route.Values.(type) {
	case *sqlparser.ColName:
		outerVindex := outer.Symtab().Vindex(vals, outer, false)
		if outerVindex == inner.Route.Vindex {
			return nil
		}
	}
	if outer.Route.Opcode != SelectEqualUnique {
		return errors.New("unsupported: subquery does not depend on scatter outer query")
	}
	if !valEqual(outer.Route.Values, inner.Route.Values) {
		return errors.New("unsupported: subquery and parent route to different shards")
	}
	return nil
}

func hasSubquery(node sqlparser.SQLNode) bool {
	has := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		if _, ok := node.(*sqlparser.Subquery); ok {
			has = true
			return false, errors.New("dummy")
		}
		return true, nil
	}, node)
	return has
}

// exprIsValue returns true if the expression can be treated as a value
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

func valEqual(a, b interface{}) bool {
	switch a := a.(type) {
	case *sqlparser.ColName:
		if b, ok := b.(*sqlparser.ColName); ok {
			return newColref(a) == newColref(b)
		}
	case sqlparser.ValArg:
		if b, ok := b.(sqlparser.ValArg); ok {
			return bytes.Equal([]byte(a), []byte(b))
		}
	case sqlparser.StrVal:
		if b, ok := b.(sqlparser.StrVal); ok {
			return bytes.Equal([]byte(a), []byte(b))
		}
	case sqlparser.NumVal:
		if b, ok := b.(sqlparser.NumVal); ok {
			return bytes.Equal([]byte(a), []byte(b))
		}
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
	panic("unreachable")
}
