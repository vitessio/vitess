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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// splitAndExpression breaks up the Expr into AND-separated conditions
// and appends them to filters, which can be shuffled and recombined
// as needed.
func splitAndExpression(filters []sqlparser.Expr, node sqlparser.Expr) []sqlparser.Expr {
	if node == nil {
		return filters
	}
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		filters = splitAndExpression(filters, node.Left)
		return splitAndExpression(filters, node.Right)
	case *sqlparser.ParenExpr:
		// If the inner expression is AndExpr, then we can remove
		// the parenthesis because they are unnecessary.
		if node, ok := node.Expr.(*sqlparser.AndExpr); ok {
			return splitAndExpression(filters, node)
		}
	}
	return append(filters, node)
}

// skipParenthesis skips the parenthesis (if any) of an expression and
// returns the innermost unparenthesized expression.
func skipParenthesis(node sqlparser.Expr) sqlparser.Expr {
	if node, ok := node.(*sqlparser.ParenExpr); ok {
		return skipParenthesis(node.Expr)
	}
	return node
}

type subqueryInfo struct {
	ast    *sqlparser.Subquery
	bldr   builder
	origin builder
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
func (pb *primitiveBuilder) findOrigin(expr sqlparser.Expr) (pullouts []*pulloutSubquery, origin builder, pushExpr sqlparser.Expr, err error) {
	// highestOrigin tracks the highest origin referenced by the expression.
	// Default is the First.
	highestOrigin := pb.bldr.First()

	// subqueries tracks the list of subqueries encountered.
	var subqueries []subqueryInfo

	// constructsMap tracks the sub-construct in which a subquery
	// occurred. The construct type decides on how the query gets
	// pulled out.
	constructsMap := make(map[*sqlparser.Subquery]sqlparser.Expr)

	err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			newOrigin, isLocal, err := pb.st.Find(node)
			if err != nil {
				return false, err
			}
			if isLocal && newOrigin.Order() > highestOrigin.Order() {
				highestOrigin = newOrigin
			}
		case *sqlparser.ComparisonExpr:
			if node.Operator == sqlparser.InStr || node.Operator == sqlparser.NotInStr {
				if sq, ok := node.Right.(*sqlparser.Subquery); ok {
					constructsMap[sq] = node
				}
			}
		case *sqlparser.ExistsExpr:
			constructsMap[node.Subquery] = node
		case *sqlparser.Subquery:
			spb := newPrimitiveBuilder(pb.vschema, pb.jt)
			switch stmt := node.Select.(type) {
			case *sqlparser.Select:
				if err := spb.processSelect(stmt, pb.st); err != nil {
					return false, err
				}
			case *sqlparser.Union:
				if err := spb.processUnion(stmt, pb.st); err != nil {
					return false, err
				}
			default:
				panic(fmt.Sprintf("BUG: unexpected SELECT type: %T", node))
			}
			sqi := subqueryInfo{
				ast:  node,
				bldr: spb.bldr,
			}
			for _, extern := range spb.st.Externs {
				// No error expected. These are resolved externs.
				newOrigin, isLocal, _ := pb.st.Find(extern)
				if !isLocal {
					continue
				}
				if highestOrigin.Order() < newOrigin.Order() {
					highestOrigin = newOrigin
				}
				if sqi.origin == nil {
					sqi.origin = newOrigin
				} else if sqi.origin.Order() < newOrigin.Order() {
					sqi.origin = newOrigin
				}
			}
			subqueries = append(subqueries, sqi)
			return false, nil
		case *sqlparser.FuncExpr:
			switch {
			// If it's last_insert_id, ensure it's a single unsharded route.
			case node.Name.EqualString("last_insert_id"):
				if rb, isRoute := pb.bldr.(*route); !isRoute || rb.ERoute.Keyspace.Sharded {
					return false, errors.New("unsupported: LAST_INSERT_ID is only allowed for unsharded keyspaces")
				}
			}
			return true, nil
		}
		return true, nil
	}, expr)
	if err != nil {
		return nil, nil, nil, err
	}

	highestRoute, _ := highestOrigin.(*route)
	for _, sqi := range subqueries {
		subroute, _ := sqi.bldr.(*route)
		if highestRoute != nil && subroute != nil && highestRoute.SubqueryCanMerge(pb, subroute) {
			subroute.Redirect = highestRoute
			continue
		}
		if sqi.origin != nil {
			return nil, nil, nil, errors.New("unsupported: cross-shard correlated subquery")
		}

		sqName, hasValues := pb.jt.GenerateSubqueryVars()
		construct, ok := constructsMap[sqi.ast]
		if !ok {
			// (subquery) -> :_sq
			expr = sqlparser.ReplaceExpr(expr, sqi.ast, sqlparser.NewValArg([]byte(":"+sqName)))
			pullouts = append(pullouts, newPulloutSubquery(engine.PulloutValue, sqName, hasValues, sqi.bldr))
			continue
		}
		switch construct := construct.(type) {
		case *sqlparser.ComparisonExpr:
			if construct.Operator == sqlparser.InStr {
				// a in (subquery) -> (:__has_values = 1 and (a in ::__sq))
				newExpr := &sqlparser.ParenExpr{
					Expr: &sqlparser.AndExpr{
						Left: &sqlparser.ComparisonExpr{
							Left:     sqlparser.NewValArg([]byte(":" + hasValues)),
							Operator: sqlparser.EqualStr,
							Right:    sqlparser.NewIntVal([]byte("1")),
						},
						Right: &sqlparser.ParenExpr{
							Expr: sqlparser.ReplaceExpr(construct, sqi.ast, sqlparser.ListArg([]byte("::"+sqName))),
						},
					},
				}
				expr = sqlparser.ReplaceExpr(expr, construct, newExpr)
				pullouts = append(pullouts, newPulloutSubquery(engine.PulloutIn, sqName, hasValues, sqi.bldr))
			} else {
				// a not in (subquery) -> (:__has_values = 0 or (a not in ::__sq))
				newExpr := &sqlparser.ParenExpr{
					Expr: &sqlparser.OrExpr{
						Left: &sqlparser.ComparisonExpr{
							Left:     sqlparser.NewValArg([]byte(":" + hasValues)),
							Operator: sqlparser.EqualStr,
							Right:    sqlparser.NewIntVal([]byte("0")),
						},
						Right: &sqlparser.ParenExpr{
							Expr: sqlparser.ReplaceExpr(construct, sqi.ast, sqlparser.ListArg([]byte("::"+sqName))),
						},
					},
				}
				expr = sqlparser.ReplaceExpr(expr, construct, newExpr)
				pullouts = append(pullouts, newPulloutSubquery(engine.PulloutNotIn, sqName, hasValues, sqi.bldr))
			}
		case *sqlparser.ExistsExpr:
			// exists (subquery) -> :_has_values
			expr = sqlparser.ReplaceExpr(expr, construct, sqlparser.NewValArg([]byte(":"+hasValues)))
			pullouts = append(pullouts, newPulloutSubquery(engine.PulloutExists, sqName, hasValues, sqi.bldr))
		}
	}
	return pullouts, highestOrigin, expr, nil
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

func (pb *primitiveBuilder) validateSubquerySamePlan(nodes ...sqlparser.SQLNode) bool {
	var keyspace string
	if rb, ok := pb.bldr.(*route); ok {
		keyspace = rb.ERoute.Keyspace.Name
	}
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
				spb := newPrimitiveBuilder(pb.vschema, pb.jt)
				if err := spb.processSelect(nodeType, pb.st); err != nil {
					samePlan = false
					return false, err
				}
				innerRoute, ok := spb.bldr.(*route)
				if !ok {
					samePlan = false
					return false, errors.New("dummy")
				}
				if innerRoute.ERoute.Keyspace.Name != keyspace {
					samePlan = false
					return false, errors.New("dummy")
				}
			case *sqlparser.Union:
				if !inSubQuery {
					return true, nil
				}
				spb := newPrimitiveBuilder(pb.vschema, pb.jt)
				if err := spb.processUnion(nodeType, pb.st); err != nil {
					samePlan = false
					return false, err
				}
				innerRoute, ok := spb.bldr.(*route)
				if !ok {
					samePlan = false
					return false, errors.New("dummy")
				}
				if innerRoute.ERoute.Keyspace.Name != keyspace {
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
