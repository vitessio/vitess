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

	// first we try to simplify the query by removing any unions
	if success := trySimplifyUnions(sqlparser.CloneSelectStatement(in), test); success != nil {
		return SimplifyStatement(success, currentDB, si, testF)
	}

	// then we try to remove a table and all uses of it
	if success := tryRemoveTable(tables, sqlparser.CloneSelectStatement(in), currentDB, si, testF); success != nil {
		return SimplifyStatement(success, currentDB, si, testF)
	}

	// now let's try to simplify * expressions
	if success := simplifyStarExpr(sqlparser.CloneSelectStatement(in), test); success != nil {
		return SimplifyStatement(success, currentDB, si, testF)
	}

	// we try to remove/replace any expressions next
	if success := trySimplifyExpressions(sqlparser.CloneSelectStatement(in), test); success != nil {
		return SimplifyStatement(success, currentDB, si, testF)
	}

	// we try to remove distinct last
	if success := trySimplifyDistinct(sqlparser.CloneSelectStatement(in), test); success != nil {
		return SimplifyStatement(success, currentDB, si, testF)
	}

	return in
}

func trySimplifyDistinct(in sqlparser.SelectStatement, test func(statement sqlparser.SelectStatement) bool) sqlparser.SelectStatement {
	simplified := false
	alwaysVisitChildren := func(node, parent sqlparser.SQLNode) bool {
		return true
	}

	up := func(cursor *sqlparser.Cursor) bool {
		if sel, ok := cursor.Node().(*sqlparser.Select); ok {
			if sel.Distinct {
				sel.Distinct = false
				if test(sel) {
					log.Errorf("removed distinct to yield: %s", sqlparser.String(sel))
					simplified = true
				} else {
					sel.Distinct = true
				}
			}
		}

		return true
	}

	sqlparser.SafeRewrite(in, alwaysVisitChildren, up)

	if simplified {

		return in
	}
	// we found no simplifications
	return nil
}

func trySimplifyExpressions(in sqlparser.SelectStatement, test func(sqlparser.SelectStatement) bool) sqlparser.SelectStatement {
	simplified := false
	visit := func(cursor expressionCursor) bool {
		// first - let's try to remove the expression
		if cursor.remove() {
			if test(in) {
				log.Errorf("removed expression: %s", sqlparser.String(cursor.expr))
				simplified = true
				// initially return false, but that made the rewriter prematurely abort, if it was the last selectExpr
				return true
			}
			cursor.restore()
		}

		// ok, we seem to need this expression. let's see if we can find a simpler version
		newExpr := SimplifyExpr(cursor.expr, func(expr sqlparser.Expr) bool {
			cursor.replace(expr)
			if test(in) {
				log.Errorf("simplified expression: %s -> %s", sqlparser.String(cursor.expr), sqlparser.String(expr))
				cursor.restore()
				simplified = true
				return true
			}

			cursor.restore()
			return false
		})

		cursor.replace(newExpr)
		return true
	}

	visitAllExpressionsInAST(in, visit)

	if simplified {
		return in
	}
	// we found no simplifications
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

	simplified := false
	alwaysVisitChildren := func(node, parent sqlparser.SQLNode) bool {
		return true
	}

	up := func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.Union:
			if _, ok := cursor.Parent().(*sqlparser.RootNode); ok {
				// we have already checked the root node
				return true
			}
			cursor.Replace(node.Left)
			clone := sqlparser.CloneSelectStatement(in)
			if test(clone) {
				log.Errorf("replaced UNION with its left child: %s -> %s", sqlparser.String(node), sqlparser.String(node.Left))
				simplified = true
				return true
			}
			cursor.Replace(node.Right)
			clone = sqlparser.CloneSelectStatement(in)
			if test(clone) {
				log.Errorf("replaced UNION with its right child: %s -> %s", sqlparser.String(node), sqlparser.String(node.Right))
				simplified = true
				return true
			}
			cursor.Replace(node)
		}
		return true
	}

	sqlparser.SafeRewrite(in, alwaysVisitChildren, up)

	if simplified {

		return in
	}
	// we found no simplifications
	return nil
}

func tryRemoveTable(tables []semantics.TableInfo, in sqlparser.SelectStatement, currentDB string, si semantics.SchemaInformation, test func(sqlparser.SelectStatement) bool) sqlparser.SelectStatement {
	// we start by removing one table at a time, and see if we still have an interesting plan
	for idx, tbl := range tables {
		clone := sqlparser.CloneSelectStatement(in)
		searchedTS := semantics.SingleTableSet(idx)
		simplified := removeTable(clone, searchedTS, currentDB, si)
		name, _ := tbl.Name()
		if simplified && test(clone) {
			log.Errorf("removed table `%s`: \n%s\n%s", sqlparser.String(name), sqlparser.String(in), sqlparser.String(clone))
			return clone
		}
	}
	// we found no simplifications
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
	alwaysVisitChildren := func(node, parent sqlparser.SQLNode) bool {
		return true
	}

	up := func(cursor *sqlparser.Cursor) bool {
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
			return true
		}
		cursor.Replace(se)

		return true
	}

	sqlparser.SafeRewrite(in, alwaysVisitChildren, up)

	if simplified {
		return in
	}
	// we found no simplifications
	return nil
}

// removeTable removes the table with the given index from the select statement, which includes the FROM clause
// but also all expressions and predicates that depend on the table
func removeTable(clone sqlparser.SelectStatement, searchedTS semantics.TableSet, db string, si semantics.SchemaInformation) bool {
	semTable, err := semantics.Analyze(clone, db, si)
	if err != nil {
		panic(err)
	}

	simplified, kontinue := false, true
	shouldKeepExpr := func(expr sqlparser.Expr) bool {
		// why do we keep if the expr contains an aggregation?
		return !semTable.RecursiveDeps(expr).IsOverlapping(searchedTS) || sqlparser.ContainsAggregation(expr)
	}
	checkSelect := func(node, parent sqlparser.SQLNode) bool {
		if sel, ok := node.(*sqlparser.Select); ok {
			// remove the table from the from clause on the way down
			// so that it happens before removing it anywhere else
			kontinue, simplified = removeTableinSelect(sel, searchedTS, semTable, simplified)
		}

		return kontinue
	}

	up := func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.JoinTableExpr:
			simplified = removeTableinJoinTableExpr(node, searchedTS, semTable, cursor, simplified)
		case *sqlparser.Where:
			simplified = removeTableinWhere(node, shouldKeepExpr, simplified)
		case sqlparser.SelectExprs:
			simplified = removeTableinSelectExprs(node, cursor, shouldKeepExpr, simplified)
		case sqlparser.GroupBy:
			simplified = removeTableinGroupBy(node, cursor, shouldKeepExpr, simplified)
		case sqlparser.OrderBy:
			simplified = removeTableinOrderBy(node, cursor, shouldKeepExpr, simplified)
		}
		return true
	}

	sqlparser.SafeRewrite(clone, checkSelect, up)
	return simplified
}

func removeTableinJoinTableExpr(node *sqlparser.JoinTableExpr, searchedTS semantics.TableSet, semTable *semantics.SemTable, cursor *sqlparser.Cursor, simplified bool) bool {
	lft, ok := node.LeftExpr.(*sqlparser.AliasedTableExpr)
	if ok {
		ts := semTable.TableSetFor(lft)
		if searchedTS == ts {
			cursor.Replace(node.RightExpr)
			simplified = true
		}
	}
	rgt, ok := node.RightExpr.(*sqlparser.AliasedTableExpr)
	if ok {
		ts := semTable.TableSetFor(rgt)
		if searchedTS == ts {
			cursor.Replace(node.LeftExpr)
			simplified = true
		}
	}

	return simplified
}

func removeTableinSelect(node *sqlparser.Select, searchedTS semantics.TableSet, semTable *semantics.SemTable, simplified bool) (bool, bool) {
	if len(node.From) == 1 {
		_, notJoin := node.From[0].(*sqlparser.AliasedTableExpr)
		if notJoin {
			return false, simplified
		}
	}
	for i, tbl := range node.From {
		lft, ok := tbl.(*sqlparser.AliasedTableExpr)
		if ok {
			ts := semTable.TableSetFor(lft)
			if searchedTS == ts {
				node.From = append(node.From[:i], node.From[i+1:]...)
				simplified = true
			}
		}
	}

	return true, simplified
}

func removeTableinWhere(node *sqlparser.Where, shouldKeepExpr func(sqlparser.Expr) bool, simplified bool) bool {
	exprs := sqlparser.SplitAndExpression(nil, node.Expr)
	var newPredicate sqlparser.Expr
	for _, expr := range exprs {
		if shouldKeepExpr(expr) {
			newPredicate = sqlparser.AndExpressions(newPredicate, expr)
		} else {
			simplified = true
		}
	}
	node.Expr = newPredicate

	return simplified
}

func removeTableinSelectExprs(node sqlparser.SelectExprs, cursor *sqlparser.Cursor, shouldKeepExpr func(sqlparser.Expr) bool, simplified bool) bool {
	_, isSel := cursor.Parent().(*sqlparser.Select)
	if !isSel {
		return simplified
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
		} else {
			simplified = true
		}
	}
	cursor.Replace(newExprs)

	return simplified
}

func removeTableinGroupBy(node sqlparser.GroupBy, cursor *sqlparser.Cursor, shouldKeepExpr func(sqlparser.Expr) bool, simplified bool) bool {
	var newExprs sqlparser.GroupBy
	for _, expr := range node {
		if shouldKeepExpr(expr) {
			newExprs = append(newExprs, expr)
		} else {
			simplified = true
		}
	}
	cursor.Replace(newExprs)

	return simplified
}

func removeTableinOrderBy(node sqlparser.OrderBy, cursor *sqlparser.Cursor, shouldKeepExpr func(sqlparser.Expr) bool, simplified bool) bool {
	var newExprs sqlparser.OrderBy
	for _, expr := range node {
		if shouldKeepExpr(expr.Expr) {
			newExprs = append(newExprs, expr)
		} else {
			simplified = true
		}
	}

	cursor.Replace(newExprs)

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
// This cursor has a few extra capabilities that the normal sqlparser.SafeRewrite does not have,
// such as visiting and being able to change individual expressions in a AND tree
// if visit returns true, then traversal continues, otherwise traversal stops
func visitAllExpressionsInAST(clone sqlparser.SelectStatement, visit func(expressionCursor) bool) {
	alwaysVisitChildren := func(node, parent sqlparser.SQLNode) bool {
		return true
	}
	up := func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case sqlparser.SelectExprs:
			return visitSelectExprs(node, cursor, visit)
		case *sqlparser.Where:
			return visitWhere(node, visit)
		case *sqlparser.JoinCondition:
			return visitJoinCondition(node, cursor, visit)
		case sqlparser.GroupBy:
			return visitGroupBy(node, cursor, visit)
		case sqlparser.OrderBy:
			return visitOrderBy(node, cursor, visit)
		case *sqlparser.Limit:
			return visitLimit(node, cursor, visit)
		}
		return true
	}
	sqlparser.SafeRewrite(clone, alwaysVisitChildren, up)
}

func visitSelectExprs(node sqlparser.SelectExprs, cursor *sqlparser.Cursor, visit func(expressionCursor) bool) bool {
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
		if !visit(item) {
			return false
		}
	}

	return true
}

func visitWhere(node *sqlparser.Where, visit func(expressionCursor) bool) bool {
	exprs := sqlparser.SplitAndExpression(nil, node.Expr)
	set := func(input []sqlparser.Expr) {
		node.Expr = sqlparser.AndExpressions(input...)
		exprs = input
	}
	return visitExpressions(exprs, set, visit, 0)
}

func visitJoinCondition(node *sqlparser.JoinCondition, cursor *sqlparser.Cursor, visit func(expressionCursor) bool) bool {
	join, ok := cursor.Parent().(*sqlparser.JoinTableExpr)
	if !ok {
		return true
	}

	if node.Using != nil {
		return true
	}

	// for only left and right joins must the join condition be nonempty
	minExprs := 0
	if join.Join == sqlparser.LeftJoinType || join.Join == sqlparser.RightJoinType {
		minExprs = 1
	}

	exprs := sqlparser.SplitAndExpression(nil, node.On)
	set := func(input []sqlparser.Expr) {
		node.On = sqlparser.AndExpressions(input...)
		exprs = input
	}
	return visitExpressions(exprs, set, visit, minExprs)
}

func visitGroupBy(node sqlparser.GroupBy, cursor *sqlparser.Cursor, visit func(expressionCursor) bool) bool {
	set := func(input []sqlparser.Expr) {
		node = input
		cursor.Replace(node)
	}
	return visitExpressions(node, set, visit, 0)
}

func visitOrderBy(node sqlparser.OrderBy, cursor *sqlparser.Cursor, visit func(expressionCursor) bool) bool {
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
		if !visit(item) {
			return false
		}
	}

	return true
}

func visitLimit(node *sqlparser.Limit, cursor *sqlparser.Cursor, visit func(expressionCursor) bool) bool {
	if node.Offset != nil {
		original := node.Offset
		item := newExprCursor(node.Offset,
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
		if !visit(item) {
			return false
		}
	}
	if node.Rowcount != nil {
		original := node.Rowcount
		item := newExprCursor(node.Rowcount,
			/*replace*/ func(replaceWith sqlparser.Expr) {
				node.Rowcount = replaceWith
			},
			// this removes the whole limit clause
			/*remove*/
			func() bool {
				var nilVal *sqlparser.Limit // this is used to create a typed nil value
				cursor.Replace(nilVal)
				return true
			},
			/*restore*/ func() {
				node.Rowcount = original
			})
		if !visit(item) {
			return false
		}
	}

	return true
}

// visitExpressions allows the cursor to visit all expressions in a slice,
// and can replace or remove items and restore the slice.
func visitExpressions(
	exprs []sqlparser.Expr,
	set func(input []sqlparser.Expr),
	visit func(expressionCursor) bool,
	minExprs int,
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
				// need to keep at least minExprs
				if len(exprs) <= minExprs {
					return false
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
