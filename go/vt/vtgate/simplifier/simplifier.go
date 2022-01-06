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

package simplifier

import (
	"sync"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// SimplifyStatement simplifies the AST of a query. It basically iteratevely prunes leaves of the AST, as long as the pruning
// continues to return true from the `test` function.
func SimplifyStatement(
	in sqlparser.SelectStatement,
	currentDB string,
	si semantics.SchemaInformation,
	testF func(sqlparser.SelectStatement) bool,
) sqlparser.SelectStatement {
	var semTable *semantics.SemTable
	var err error
	{
		// Since our semantic analysis changes the AST, we clone it first, so we have a pristine AST to play with
		clone := sqlparser.CloneSelectStatement(in)
		semTable, err = semantics.Analyze(clone, currentDB, si)
		if err != nil {
			return nil
		}
	}

	test := func(s sqlparser.SelectStatement) bool {
		// Since our semantic analysis changes the AST, we clone it first, so we have a pristine AST to play with
		return testF(sqlparser.CloneSelectStatement(s))
	}

	// we start by removing one table at a time, and see if we still have an interesting plan
	for idx := range semTable.Tables {
		clone := sqlparser.CloneSelectStatement(in)
		if err != nil {
			panic(err) // this should never happen
		}
		searchedTS := semantics.SingleTableSet(idx)
		simplified := removeTable(clone, searchedTS, semTable)
		if simplified && test(clone) {
			name, _ := semTable.Tables[idx].Name()
			log.Errorf("removed table %s", name)
			return SimplifyStatement(clone, currentDB, si, test)
		}
	}

	// now let's try to simplify * expressions
	if simplifyStarExpr(in, test) {
		return SimplifyStatement(in, currentDB, si, test)
	}

	// if we get here, we couldn't find a simpler query by just removing one table,
	// we try to remove select expressions next
	ch := make(chan expressionCursor)
	findExpressions(in, ch)
	for cursor := range ch {
		// first - let's try to remove the expression
		if cursor.remove() {
			if test(in) {
				log.Errorf("removed expression: %s", sqlparser.String(cursor.expr))
				cursor.abort()
				return SimplifyStatement(in, currentDB, si, test)
			}
			cursor.restore()
		}

		// ok, we seem to need this expression. let's see if we can find a simpler version
		s := &shrinker{orig: cursor.expr}
		newExpr := s.Next()
		for newExpr != nil {
			cursor.replace(newExpr)
			if test(in) {
				log.Errorf("simplified expression: %s -> %s", sqlparser.String(cursor.expr), sqlparser.String(newExpr))
				cursor.abort()
				return SimplifyStatement(in, currentDB, si, test)
			}
			newExpr = s.Next()
		}
		// if we get here, we failed to simplify this expression,
		// so we put back in the original expression
		cursor.restore()
		cursor.wg.Done()
	}

	return in
}

func simplifyStarExpr(in sqlparser.SelectStatement, test func(sqlparser.SelectStatement) bool) bool {
	simplified := false
	sqlparser.Rewrite(in, func(cursor *sqlparser.Cursor) bool {
		se, ok := cursor.Node().(*sqlparser.StarExpr)
		if !ok {
			return true
		}
		cursor.Replace(&sqlparser.AliasedExpr{
			Expr: sqlparser.NewIntLiteral("0"),
		})
		if test(in) {
			log.Errorf("replaced star with literal")
			simplified = true
			return false
		}
		cursor.Replace(se)

		return true
	}, nil)
	return simplified
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
	remove      func() bool
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
	remove func() bool,
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
						/*remove*/ func() bool {
							if removed {
								panic("can't remove twice, silly")
							}
							if len(node) == 1 {
								// can't remove the last expressions - we'd end up with an empty SELECT clause
								return false
							}
							withoutElement := append(node[:idx], node[idx+1:]...)
							cursor.Replace(withoutElement)
							node = withoutElement
							removed = true
							return true
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
				if !visitExpressions(exprs, set, ch, abort) {
					return false
				}
			case *sqlparser.JoinTableExpr:

			case *sqlparser.JoinCondition:
				join, ok := cursor.Parent().(*sqlparser.JoinTableExpr)
				if !ok {
					return true
				}
				if join.Join != sqlparser.NormalJoinType || node.Using != nil {
					return false
				}
				exprs := sqlparser.SplitAndExpression(nil, node.On)
				set := func(input []sqlparser.Expr) {
					node.On = sqlparser.AndExpressions(input...)
					exprs = input
				}
				if !visitExpressions(exprs, set, ch, abort) {
					return false
				}
			case sqlparser.GroupBy:
				set := func(input []sqlparser.Expr) {
					node = input
					cursor.Replace(node)
				}
				if !visitExpressions(node, set, ch, abort) {
					return false
				}
			case sqlparser.OrderBy:
				for idx := 0; idx < len(node); idx++ {
					order := node[idx]
					removed := false
					original := sqlparser.CloneExpr(order.Expr)
					newCursorItem(
						ch, order.Expr, abort,
						/*replace*/ func(replaceWith sqlparser.Expr) {
							if removed {
								panic("cant replace after remove without restore")
							}
							order.Expr = replaceWith
						},
						/*remove*/ func() bool {
							if removed {
								panic("can't remove twice, silly")
							}
							withoutElement := append(node[:idx], node[idx+1:]...)
							cursor.Replace(withoutElement)
							node = withoutElement
							removed = true
							return true
						},
						/*restore*/ func() {
							if removed {
								front := make(sqlparser.OrderBy, idx)
								copy(front, node[:idx])
								back := make(sqlparser.OrderBy, len(node)-idx)
								copy(back, node[idx:])
								frontWithRestoredExpr := append(front, order)
								node = append(frontWithRestoredExpr, back...)
								cursor.Replace(node)
								removed = false
								return
							}
							order.Expr = original
						},
					)
					if abort.Get() {
						close(ch)
						return false
					}
				}
			case *sqlparser.Limit:
				if node.Offset != nil {
					original := node.Offset
					newCursorItem(ch, node.Offset, abort,
						/*replace*/ func(replaceWith sqlparser.Expr) {
							node.Offset = replaceWith
						},
						/*remove*/ func() bool {
							node.Offset = nil
							return true
						},
						/*restore*/ func() {
							node.Offset = original
						})
				}
				if node.Rowcount != nil {
					original := node.Rowcount
					newCursorItem(ch, node.Rowcount, abort,
						/*replace*/ func(replaceWith sqlparser.Expr) {
							node.Rowcount = replaceWith
						},
						/*remove*/ func() bool {
							// removing Rowcount is an invalid op
							return false
						},
						/*restore*/ func() {
							node.Rowcount = original
						})
				}

			}
			return true
		}, nil)
		close(ch)
	}()
}

// visitExpressions allows the cursor to visit all expressions in a slice,
// and can replace or remove items and restore the slice.
func visitExpressions(
	exprs []sqlparser.Expr,
	set func(input []sqlparser.Expr),
	ch chan<- expressionCursor,
	abort *sync2.AtomicBool,
) bool {
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
			/*remove*/ func() bool {
				if removed {
					panic("can't remove twice, silly")
				}
				exprs = append(exprs[:idx], exprs[idx+1:]...)
				set(exprs)
				removed = true
				return true
			},
			/*restore*/ func() {
				if removed {
					front := make([]sqlparser.Expr, idx)
					copy(front, exprs[:idx])
					back := make([]sqlparser.Expr, len(exprs)-idx)
					copy(back, exprs[idx:])
					frontWithRestoredExpr := append(front, expr)
					exprs = append(frontWithRestoredExpr, back...)
					set(exprs)
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
	return true
}
