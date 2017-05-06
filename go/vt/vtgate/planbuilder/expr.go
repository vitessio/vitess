/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package planbuilder

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
)

// splitAndExpression breaks up the Expr into AND-separated conditions
// and appends them to filters, which can be shuffled and recombined
// as needed.
func splitAndExpression(filters []sqlparser.Expr, node sqlparser.Expr) []sqlparser.Expr {
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
func findRoute(expr sqlparser.Expr, bldr builder) (rb *route, err error) {
	highestRoute := bldr.Leftmost()
	var subroutes []*route
	err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			newRoute, isLocal, err := bldr.Symtab().Find(node, true)
			if err != nil {
				return false, err
			}
			if isLocal && newRoute.Order() > highestRoute.Order() {
				highestRoute = newRoute
			}
		case *sqlparser.Subquery:
			var subplan builder
			switch stmt := node.Select.(type) {
			case *sqlparser.Select:
				subplan, err = processSelect(stmt, bldr.Symtab().VSchema, bldr)
			case *sqlparser.Union:
				subplan, err = processUnion(stmt, bldr.Symtab().VSchema, bldr)
			default:
				panic("unreachable")
			}
			if err != nil {
				return false, err
			}
			subroute, ok := subplan.(*route)
			if !ok {
				return false, errors.New("unsupported: complex join in subqueries")
			}
			for _, extern := range subroute.Symtab().Externs {
				// No error expected. These are resolved externs.
				newRoute, isLocal, _ := bldr.Symtab().Find(extern, false)
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
func subqueryCanMerge(outer, inner *route) error {
	if outer.ERoute.Keyspace.Name != inner.ERoute.Keyspace.Name {
		return errors.New("unsupported: subquery keyspace different from outer query")
	}
	switch inner.ERoute.Opcode {
	case engine.SelectUnsharded:
		return nil
	case engine.SelectNext:
		return errors.New("unsupported: use of sequence in subquery")
	case engine.SelectEqualUnique:
		switch vals := inner.ERoute.Values.(type) {
		case *sqlparser.ColName:
			outerVindex := outer.Symtab().Vindex(vals, outer, false)
			if outerVindex == inner.ERoute.Vindex {
				return nil
			}
		}
	default:
		return errors.New("unsupported: scatter subquery")
	}
	// SelectEqualUnique
	if outer.ERoute.Opcode != engine.SelectEqualUnique {
		return errors.New("unsupported: subquery does not depend on scatter outer query")
	}
	if !valEqual(outer.ERoute.Values, inner.ERoute.Values) {
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
func exprIsValue(expr sqlparser.Expr, rb *route) bool {
	if node, ok := expr.(*sqlparser.ColName); ok {
		return node.Metadata.(sym).Route() != rb
	}
	return sqlparser.IsValue(expr)
}

func valEqual(a, b interface{}) bool {
	switch a := a.(type) {
	case *sqlparser.ColName:
		if b, ok := b.(*sqlparser.ColName); ok {
			return newColref(a) == newColref(b)
		}
	case *sqlparser.SQLVal:
		b, ok := b.(*sqlparser.SQLVal)
		if !ok {
			return false
		}
		switch a.Type {
		case sqlparser.ValArg:
			if b.Type == sqlparser.ValArg {
				return bytes.Equal([]byte(a.Val), []byte(b.Val))
			}
		case sqlparser.StrVal:
			switch b.Type {
			case sqlparser.StrVal:
				return bytes.Equal([]byte(a.Val), []byte(b.Val))
			case sqlparser.HexVal:
				return hexEqual(b, a)
			}
		case sqlparser.HexVal:
			return hexEqual(a, b)
		case sqlparser.IntVal:
			if b.Type == (sqlparser.IntVal) {
				return bytes.Equal([]byte(a.Val), []byte(b.Val))
			}
		}
	}
	return false
}

func hexEqual(a, b *sqlparser.SQLVal) bool {
	v, err := a.HexDecode()
	if err != nil {
		return false
	}
	switch b.Type {
	case sqlparser.StrVal:
		return bytes.Equal(v, b.Val)
	case sqlparser.HexVal:
		v2, err := b.HexDecode()
		if err != nil {
			return false
		}
		return bytes.Equal(v, v2)
	}
	return false
}

// valConvert converts an AST value to the Value field in the route.
func valConvert(node sqlparser.Expr) (interface{}, error) {
	switch node := node.(type) {
	case *sqlparser.SQLVal:
		switch node.Type {
		case sqlparser.ValArg:
			return string(node.Val), nil
		case sqlparser.StrVal:
			return []byte(node.Val), nil
		case sqlparser.HexVal:
			return node.HexDecode()
		case sqlparser.IntVal:
			val := string(node.Val)
			signed, err := strconv.ParseInt(val, 0, 64)
			if err == nil {
				return signed, nil
			}
			unsigned, err := strconv.ParseUint(val, 0, 64)
			if err == nil {
				return unsigned, nil
			}
			return nil, err
		}
	case *sqlparser.NullVal:
		return nil, nil
	}
	return nil, fmt.Errorf("%v is not a value", sqlparser.String(node))
}
