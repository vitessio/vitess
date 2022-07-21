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
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// SimplifyStatement simplifies the AST of a query. It basically iteratively prunes leaves of the AST, as long as the pruning
// continues to return true from the `test` function.
func SimplifyStatement(
	in sqlparser.SelectStatement,
	currentDB string,
	si semantics.SchemaInformation,
	testF func(sqlparser.SelectStatement) bool,
) sqlparser.SelectStatement {
	tables, err := getTables(in, currentDB, si)
	if err != nil {
		panic(err)
	}

	test := func(s sqlparser.SelectStatement) bool {
		// Since our semantic analysis changes the AST, we clone it first, so we have a pristine AST to play with
		return testF(sqlparser.CloneSelectStatement(s))
	}

	if success := trySimplifyUnions(sqlparser.CloneSelectStatement(in), test); success != nil {
		return SimplifyStatement(success, currentDB, si, testF)
	}

	// first we try to simplify the query by removing any table.
	// If we can remove a table and all uses of it, that's a good start
	if success := tryRemoveTable(tables, sqlparser.CloneSelectStatement(in), currentDB, si, testF); success != nil {
		return SimplifyStatement(success, currentDB, si, testF)
	}

	// now let's try to simplify * expressions
	if success := simplifyStarExpr(sqlparser.CloneSelectStatement(in), test); success != nil {
		return SimplifyStatement(success, currentDB, si, testF)
	}

	// we try to remove select expressions next
	if success := trySimplifyExpressions(sqlparser.CloneSelectStatement(in), test); success != nil {
		return SimplifyStatement(success, currentDB, si, testF)
	}
	return in
}

func trySimplifyExpressions(in sqlparser.SelectStatement, test func(sqlparser.SelectStatement) bool) sqlparser.SelectStatement {
	simplified := false
	visitAllExpressionsInAST(in, func(cursor expressionCursor) bool {
		// first - let's try to remove the expression
		if cursor.remove() {
			if test(in) {
				log.Errorf("removed expression: %s", sqlparser.String(cursor.expr))
				simplified = true
				return false
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
				simplified = true
				return false
			}
			newExpr = s.Next()
		}

		// if we get here, we failed to simplify this expression,
		// so we put back in the original expression
		cursor.restore()
		return true
	})

	if simplified {
		return in
	}

	return nil
}

func trySimplifyUnions(in sqlparser.SelectStatement, test func(sqlparser.SelectStatement) bool) (res sqlparser.SelectStatement) {

	if union, ok := in.(*sqlparser.Union); ok {
		// the root object is an UNION
		if test(sqlparser.CloneSelectStatement(union.Left)) {
			return union.Left
		}
		if test(sqlparser.CloneSelectStatement(union.Right)) {
			return union.Right
		}
	}

	abort := false

	sqlparser.Rewrite(in, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.Union:
			if _, ok := cursor.Parent().(*sqlparser.RootNode); ok {
				// we have already checked the root node
				return true
			}
			cursor.Replace(node.Left)
			clone := sqlparser.CloneSelectStatement(in)
			if test(clone) {
				log.Errorf("replaced UNION with one of its children")
				abort = true
				return true
			}
			cursor.Replace(node.Right)
			clone = sqlparser.CloneSelectStatement(in)
			if test(clone) {
				log.Errorf("replaced UNION with one of its children")
				abort = true
				return true
			}
			cursor.Replace(node)
		}
		return true
	}, func(*sqlparser.Cursor) bool {
		return !abort
	})

	if !abort {
		// we found no simplifications
		return nil
	}
	return in
}

func tryRemoveTable(tables []semantics.TableInfo, in sqlparser.SelectStatement, currentDB string, si semantics.SchemaInformation, test func(sqlparser.SelectStatement) bool) sqlparser.SelectStatement {
	// we start by removing one table at a time, and see if we still have an interesting plan
	for idx, tbl := range tables {
		clone := sqlparser.CloneSelectStatement(in)
		searchedTS := semantics.SingleTableSet(idx)
		simplified := removeTable(clone, searchedTS, currentDB, si)
		name, _ := tbl.Name()
		if simplified && test(clone) {
			log.Errorf("removed table %s", sqlparser.String(name))
			return clone
		}
	}

	return nil
}

func getTables(in sqlparser.SelectStatement, currentDB string, si semantics.SchemaInformation) ([]semantics.TableInfo, error) {
	// Since our semantic analysis changes the AST, we clone it first, so we have a pristine AST to play with
	clone := sqlparser.CloneSelectStatement(in)
	semTable, err := semantics.Analyze(clone, currentDB, si)
	if err != nil {
		return nil, err
	}
	return semTable.Tables, nil
}

func simplifyStarExpr(in sqlparser.SelectStatement, test func(sqlparser.SelectStatement) bool) sqlparser.SelectStatement {
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
	if simplified {
		return in
	}
	return nil
}

// removeTable removes the table with the given index from the select statement, which includes the FROM clause
// but also all expressions and predicates that depend on the table
func removeTable(clone sqlparser.SelectStatement, searchedTS semantics.TableSet, db string, si semantics.SchemaInformation) bool {
	semTable, err := semantics.Analyze(clone, db, si)
	if err != nil {
		panic(err)
	}

	simplified := true
	shouldKeepExpr := func(expr sqlparser.Expr) bool {
		return !semTable.RecursiveDeps(expr).IsOverlapping(searchedTS) || sqlparser.ContainsAggregation(expr)
	}
	sqlparser.Rewrite(clone, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.JoinTableExpr:
			lft, ok := node.LeftExpr.(*sqlparser.AliasedTableExpr)
			if ok {
				ts := semTable.TableSetFor(lft)
				if searchedTS.Equals(ts) {
					cursor.Replace(node.RightExpr)
				}
			}
			rgt, ok := node.RightExpr.(*sqlparser.AliasedTableExpr)
			if ok {
				ts := semTable.TableSetFor(rgt)
				if searchedTS.Equals(ts) {
					cursor.Replace(node.LeftExpr)
				}
			}
		case *sqlparser.Select:
			if len(node.From) == 1 {
				_, notJoin := node.From[0].(*sqlparser.AliasedTableExpr)
				if notJoin {
					simplified = false
					return false
				}
			}
			for i, tbl := range node.From {
				lft, ok := tbl.(*sqlparser.AliasedTableExpr)
				if ok {
					ts := semTable.TableSetFor(lft)
					if searchedTS.Equals(ts) {
						node.From = append(node.From[:i], node.From[i+1:]...)
						return true
					}
				}
			}
		case *sqlparser.Where:
			exprs := sqlparser.SplitAndExpression(nil, node.Expr)
			var newPredicate sqlparser.Expr
			for _, expr := range exprs {
				if !semTable.RecursiveDeps(expr).IsOverlapping(searchedTS) {
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
				if shouldKeepExpr(expr.Expr) {
					newExprs = append(newExprs, ae)
				}
			}
			cursor.Replace(newExprs)
		case sqlparser.GroupBy:
			var newExprs sqlparser.GroupBy
			for _, expr := range node {
				if shouldKeepExpr(expr) {
					newExprs = append(newExprs, expr)
				}
			}
			cursor.Replace(newExprs)
		case sqlparser.OrderBy:
			var newExprs sqlparser.OrderBy
			for _, expr := range node {
				if shouldKeepExpr(expr.Expr) {
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
	expr    sqlparser.Expr
	replace func(replaceWith sqlparser.Expr)
	remove  func() bool
	restore func()
}

func newExprCursor(expr sqlparser.Expr, replace func(replaceWith sqlparser.Expr), remove func() bool, restore func()) expressionCursor {
	return expressionCursor{
		expr:    expr,
		replace: replace,
		remove:  remove,
		restore: restore,
	}
}

// visitAllExpressionsInAST will walk the AST and visit all expressions
// This cursor has a few extra capabilities that the normal sqlparser.Rewrite does not have,
// such as visiting and being able to change individual expressions in a AND tree
func visitAllExpressionsInAST(clone sqlparser.SelectStatement, visit func(expressionCursor) bool) {
	abort := false
	post := func(*sqlparser.Cursor) bool {
		return !abort
	}
	pre := func(cursor *sqlparser.Cursor) bool {
		if abort {
			return true
		}
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
				item := newExprCursor(
					expr.Expr,
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
				abort = !visit(item)
			}
		case *sqlparser.Where:
			exprs := sqlparser.SplitAndExpression(nil, node.Expr)
			set := func(input []sqlparser.Expr) {
				node.Expr = sqlparser.AndExpressions(input...)
				exprs = input
			}
			abort = !visitExpressions(exprs, set, visit)
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
			abort = !visitExpressions(exprs, set, visit)
		case sqlparser.GroupBy:
			set := func(input []sqlparser.Expr) {
				node = input
				cursor.Replace(node)
			}
			abort = !visitExpressions(node, set, visit)
		case sqlparser.OrderBy:
			for idx := 0; idx < len(node); idx++ {
				order := node[idx]
				removed := false
				original := sqlparser.CloneExpr(order.Expr)
				item := newExprCursor(
					order.Expr,
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
						if len(withoutElement) == 0 {
							var nilVal sqlparser.OrderBy // this is used to create a typed nil value
							cursor.Replace(nilVal)
						} else {
							cursor.Replace(withoutElement)
						}
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
				abort = visit(item)
				if abort {
					break
				}
			}
		case *sqlparser.Limit:
			if node.Offset != nil {
				original := node.Offset
				cursor := newExprCursor(node.Offset,
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
				abort = visit(cursor)
			}
			if !abort && node.Rowcount != nil {
				original := node.Rowcount
				cursor := newExprCursor(node.Rowcount,
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
				abort = visit(cursor)
			}
		}
		return true
	}
	sqlparser.Rewrite(clone, pre, post)
}

// visitExpressions allows the cursor to visit all expressions in a slice,
// and can replace or remove items and restore the slice.
func visitExpressions(
	exprs []sqlparser.Expr,
	set func(input []sqlparser.Expr),
	visit func(expressionCursor) bool,
) bool {
	for idx := 0; idx < len(exprs); idx++ {
		expr := exprs[idx]
		removed := false
		item := newExprCursor(expr,
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
		if !visit(item) {
			return false
		}
	}
	return true
}
