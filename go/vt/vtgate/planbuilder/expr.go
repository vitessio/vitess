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

// skipParenthesis skips the parenthesis (if any) of an expression and
// returns the innermost unparenthesized expression.
func skipParenthesis(node sqlparser.Expr) sqlparser.Expr {
	for {
		if node, ok := node.(*sqlparser.ParenExpr); ok {
			return skipParenthesis(node.Expr)
		}
		return node
	}
}

// findOrigin identifies the right-most origin referenced by expr. In situations where
// the expression references columns from multiple origins, the expression will be
// pushed to the right-most origin, and the executor will use the results of
// the previous origins to feed the necessary values to the primitives on the right.
//
// If the expression contains a subquery, the right-most origin identification
// also follows the same rules of a normal expression. This is achieved by
// looking at the Externs field of its symbol table that contains the list of
// external references.
//
// Once the target origin is identified, we have to verify that the subquery's
// route can be merged with it. If it cannot, we fail the query. This is because
// we don't have the ability to wire up subqueries through expression evaluation
// primitives. Consequently, if the plan for a subquery comes out as a Join,
// we can immediately error out.
//
// Since findOrigin can itself be called from within a subquery, it has to assume
// that some of the external references may actually be pointing to an outer
// query. The isLocal response from the symtab is used to make sure that we
// only analyze symbols that point to the current symtab.
//
// If an expression has no references to the current query, then the left-most
// origin is chosen as the default.
func findOrigin(expr sqlparser.Expr, bldr builder) (origin columnOriginator, err error) {
	highestOrigin := bldr.Leftmost()
	var subroutes []*route
	err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			newOrigin, isLocal, err := bldr.Symtab().Find(node)
			if err != nil {
				return false, err
			}
			if isLocal && newOrigin.Order() > highestOrigin.Order() {
				highestOrigin = newOrigin
			}
		case *sqlparser.Subquery:
			var subplan builder
			switch stmt := node.Select.(type) {
			case *sqlparser.Select:
				subplan, err = processSelect(stmt, bldr.Symtab().VSchema, bldr)
			case *sqlparser.Union:
				subplan, err = processUnion(stmt, bldr.Symtab().VSchema, bldr)
			default:
				panic(fmt.Sprintf("BUG: unexpected SELECT type: %T", node))
			}
			if err != nil {
				return false, err
			}
			subroute, isRoute := subplan.(*route)
			if !isRoute {
				return false, errors.New("unsupported: cross-shard query in subqueries")
			}
			for _, extern := range subroute.Symtab().Externs {
				// No error expected. These are resolved externs.
				newOrigin, isLocal, _ := bldr.Symtab().Find(extern)
				if isLocal && newOrigin.Order() > highestOrigin.Order() {
					highestOrigin = newOrigin
				}
			}
			subroutes = append(subroutes, subroute)
			return false, nil
		case *sqlparser.FuncExpr:
			// If it's last_insert_id, ensure it's a single unsharded route.
			if !node.Name.EqualString("last_insert_id") {
				return true, nil
			}
			if rb, isRoute := bldr.(*route); !isRoute || rb.ERoute.Keyspace.Sharded {
				return false, errors.New("unsupported: LAST_INSERT_ID is only allowed for unsharded keyspaces")
			}
		}
		return true, nil
	}, expr)
	if err != nil {
		return nil, err
	}
	highestRoute, isRoute := highestOrigin.(*route)
	if !isRoute && len(subroutes) > 0 {
		return nil, errors.New("unsupported: subquery cannot be merged with cross-shard subquery")
	}
	for _, subroute := range subroutes {
		if err := highestRoute.SubqueryCanMerge(subroute); err != nil {
			return nil, err
		}
		subroute.Redirect = highestRoute
	}
	return highestOrigin, nil
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

func validateSubquerySamePlan(outer *engine.Route, vschema VSchema, nodes ...sqlparser.SQLNode) bool {
	samePlan := true

	for _, node := range nodes {
		inSubQuery := false
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch nodeType := node.(type) {
			case *sqlparser.Subquery, *sqlparser.Insert:
				inSubQuery = true
				return true, nil
			case *sqlparser.Select:
				if !inSubQuery {
					return true, nil
				}
				bldr, err := processSelect(nodeType, vschema, nil)
				if err != nil {
					samePlan = false
					return false, err
				}
				innerRoute, ok := bldr.(*route)
				if !ok {
					samePlan = false
					return false, errors.New("dummy")
				}
				if innerRoute.ERoute.Keyspace.Name != outer.Keyspace.Name {
					samePlan = false
					return false, errors.New("dummy")
				}
			case *sqlparser.Union:
				if !inSubQuery {
					return true, nil
				}
				bldr, err := processUnion(nodeType, vschema, nil)
				if err != nil {
					samePlan = false
					return false, err
				}
				innerRoute, ok := bldr.(*route)
				if !ok {
					samePlan = false
					return false, errors.New("dummy")
				}
				if innerRoute.ERoute.Keyspace.Name != outer.Keyspace.Name {
					samePlan = false
					return false, errors.New("dummy")
				}
			}

			return true, nil
		}, node)
		if !samePlan {
			return false
		}
	}
	return true
}

func valEqual(a, b sqlparser.Expr) bool {
	switch a := a.(type) {
	case *sqlparser.ColName:
		if b, ok := b.(*sqlparser.ColName); ok {
			return a.Metadata == b.Metadata
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
