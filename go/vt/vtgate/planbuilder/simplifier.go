/*
Copyright 2021 The Vitess Authors.

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
	"sync"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// simplifyStatement simplifies the AST of a query. It basically iteratevely prunes leaves of the AST, as long as the pruning
// continues to return true from the `test` function.
func simplifyStatement(
	in sqlparser.SelectStatement,
	currentDB string,
	si semantics.SchemaInformation,
	test func(sqlparser.SelectStatement) bool,
) sqlparser.SelectStatement {
	semTable, err := semantics.Analyze(in, currentDB, si)
	if err != nil {
		return nil
	}

	// we start by removing one table at a time, and see if we still have an interesting plan
	for idx := range semTable.Tables {
		clone := sqlparser.CloneSelectStatement(in)
		inner, err := semantics.Analyze(clone, currentDB, si)
		if err != nil {
			panic(err) // this should never happen
		}
		searchedTS := semantics.SingleTableSet(idx)
		simplified := removeTable(clone, searchedTS, inner)
		if simplified && test(clone) {
			name, _ := semTable.Tables[idx].Name()
			log.Errorf("removed table %s", name)
			return simplifyStatement(clone, currentDB, si, test)
		}
	}

	// if we get here, we couldn't find a simpler query by just removing one table,
	// we try to remove select expressions next
	ch := make(chan expressionCursor)
	findExpressions(in, ch)
	for cursor := range ch {
		s := &sqlparser.Shrinker{Orig: cursor.expr}
		newExpr := s.Next()
		for newExpr != nil {
			cursor.replace(newExpr)
			if test(in) {
				log.Errorf("simplified expression: %s -> %s", sqlparser.String(cursor.expr), sqlparser.String(newExpr))
				cursor.abort()
				return simplifyStatement(in, currentDB, si, test)
			}
			newExpr = s.Next()
		}
		// if we get here, we failed to simplify this expression,
		// so we put back in the original expression
		cursor.replace(cursor.expr)
		cursor.wg.Done()
	}

	return in
}

// removeTable removes the table with the given index from the select statement, which includes the FROM clause
// but also all expressions and predicates that depend on the table
func removeTable(clone sqlparser.SelectStatement, searchedTS semantics.TableSet, inner *semantics.SemTable) bool {
	simplified := false
	sqlparser.Rewrite(clone, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.JoinTableExpr:
			lft, ok := node.LeftExpr.(*sqlparser.AliasedTableExpr)
			if ok {
				ts := inner.TableSetFor(lft)
				if ts == searchedTS {
					cursor.Replace(node.RightExpr)
					simplified = true
				}
			}
			rgt, ok := node.RightExpr.(*sqlparser.AliasedTableExpr)
			if ok {
				ts := inner.TableSetFor(rgt)
				if ts == searchedTS {
					cursor.Replace(node.LeftExpr)
					simplified = true
				}
			}
		case *sqlparser.Select:
			if len(node.From) == 1 {
				return true
			}
			for i, tbl := range node.From {
				lft, ok := tbl.(*sqlparser.AliasedTableExpr)
				if ok {
					ts := inner.TableSetFor(lft)
					if ts == searchedTS {
						node.From = append(node.From[:i], node.From[i+1:]...)
						simplified = true
						return true
					}
				}
			}
		case *sqlparser.Where:
			exprs := sqlparser.SplitAndExpression(nil, node.Expr)
			var newPredicate sqlparser.Expr
			for _, expr := range exprs {
				if !inner.RecursiveDeps(expr).IsOverlapping(searchedTS) {
					newPredicate = sqlparser.AndExpressions(newPredicate, expr)
				}
			}
			node.Expr = newPredicate
		case sqlparser.SelectExprs:
			_, isSel := cursor.Parent().(*sqlparser.Select)
			if !isSel {
				return true
			}

			var newExprs sqlparser.SelectExprs
			for _, ae := range node {
				expr, ok := ae.(*sqlparser.AliasedExpr)
				if !ok {
					newExprs = append(newExprs, ae)
					continue
				}
				if !inner.RecursiveDeps(expr.Expr).IsOverlapping(searchedTS) || sqlparser.ContainsAggregation(expr.Expr) {
					newExprs = append(newExprs, ae)
				}
			}
			cursor.Replace(newExprs)
		case sqlparser.GroupBy:
			var newExprs sqlparser.GroupBy
			for _, expr := range node {
				if !inner.RecursiveDeps(expr).IsOverlapping(searchedTS) || sqlparser.ContainsAggregation(expr) {
					newExprs = append(newExprs, expr)
				}
			}
			cursor.Replace(newExprs)
		case sqlparser.OrderBy:
			var newExprs sqlparser.OrderBy
			for _, expr := range node {
				if !inner.RecursiveDeps(expr.Expr).IsOverlapping(searchedTS) || sqlparser.ContainsAggregation(expr.Expr) {
					newExprs = append(newExprs, expr)
				}
			}

			cursor.Replace(newExprs)
		}
		return true
	}, nil)
	return simplified
}

type expressionCursor struct {
	expr        sqlparser.Expr
	replace     func(replaceWith sqlparser.Expr)
	remove      func()
	restore     func()
	wg          *sync.WaitGroup
	abortMarker *sync2.AtomicBool
}

func (i expressionCursor) abort() {
	i.abortMarker.Set(true)
}

func newCursorItem(
	ch chan<- expressionCursor,
	expr sqlparser.Expr,
	abort *sync2.AtomicBool,
	replace func(replaceWith sqlparser.Expr),
	remove func(),
	restore func(),
) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	ch <- expressionCursor{
		expr:        expr,
		replace:     replace,
		remove:      remove,
		restore:     restore,
		wg:          wg,
		abortMarker: abort,
	}
	wg.Wait()
}

func findExpressions(clone sqlparser.SelectStatement, ch chan<- expressionCursor) {
	abort := &sync2.AtomicBool{}
	go func() {
		sqlparser.Rewrite(clone, func(cursor *sqlparser.Cursor) bool {
			switch node := cursor.Node().(type) {
			case sqlparser.SelectExprs:
				_, isSel := cursor.Parent().(*sqlparser.Select)
				if !isSel {
					return true
				}
				for idx := 0; idx < len(node); idx++ {
					ae := node[idx]
					expr, ok := ae.(*sqlparser.AliasedExpr)
					if !ok {
						continue
					}
					removed := false
					original := sqlparser.CloneExpr(expr.Expr)
					newCursorItem(
						ch, expr.Expr, abort,
						/*replace*/ func(replaceWith sqlparser.Expr) {
							if removed {
								panic("cant replace after remove without restore")
							}
							expr.Expr = replaceWith
						},
						/*remove*/ func() {
							if removed {
								panic("can't remove twice, silly")
							}
							withoutElement := append(node[:idx], node[idx+1:]...)
							cursor.Replace(withoutElement)
							node = withoutElement
							removed = true
						},
						/*restore*/ func() {
							if removed {
								front := make(sqlparser.SelectExprs, idx)
								copy(front, node[:idx])
								back := make(sqlparser.SelectExprs, len(node)-idx)
								copy(back, node[idx:])
								frontWithRestoredExpr := append(front, ae)
								node = append(frontWithRestoredExpr, back...)
								cursor.Replace(node)
								removed = false
								return
							}
							expr.Expr = original
						},
					)
					if abort.Get() {
						close(ch)
						return false
					}
				}
			case *sqlparser.Where:
				exprs := sqlparser.SplitAndExpression(nil, node.Expr)
				set := func(input []sqlparser.Expr) {
					node.Expr = sqlparser.AndExpressions(input...)
					exprs = input
				}
				for idx := 0; idx < len(exprs); idx++ {
					expr := exprs[idx]
					removed := false
					newCursorItem(ch, expr, abort,
						func(replaceWith sqlparser.Expr) {
							if removed {
								panic("cant replace after remove without restore")
							}
							exprs[idx] = replaceWith
							set(exprs)
						},
						/*remove*/ func() {
							if removed {
								panic("can't remove twice, silly")
							}
							set(append(exprs[:idx], exprs[idx+1:]...))
							removed = true
						},
						/*restore*/ func() {
							if removed {
								front := make([]sqlparser.Expr, idx)
								copy(front, exprs[:idx])
								back := make([]sqlparser.Expr, len(exprs)-idx)
								copy(back, exprs[idx:])
								frontWithRestoredExpr := append(front, expr)
								set(append(frontWithRestoredExpr, back...))
								removed = false
								return
							}
							exprs[idx] = expr
							set(exprs)
						})
					if abort.Get() {
						return false
					}
				}
			case *sqlparser.JoinCondition:

			}
			return true
		}, nil)
		close(ch)
	}()
}
