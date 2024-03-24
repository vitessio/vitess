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

package semantics

import (
	"fmt"
	"strconv"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type earlyRewriter struct {
	binder          *binder
	scoper          *scoper
	clause          string
	warning         string
	expandedColumns map[sqlparser.TableName][]*sqlparser.ColName
	env             *vtenv.Environment
	aliasMapCache   map[*sqlparser.Select]map[string]exprContainer
	tables          *tableCollector

	// reAnalyze is used when we are running in the late stage, after the other parts of semantic analysis
	// have happened, and we are introducing or changing the AST. We invoke it so all parts of the query have been
	// typed, scoped and bound correctly
	reAnalyze func(n sqlparser.SQLNode) error
}

func (r *earlyRewriter) down(cursor *sqlparser.Cursor) error {
	switch node := cursor.Node().(type) {
	case sqlparser.SelectExprs:
		return r.handleSelectExprs(cursor, node)
	case *sqlparser.OrExpr:
		rewriteOrExpr(r.env, cursor, node)
	case *sqlparser.AndExpr:
		rewriteAndExpr(r.env, cursor, node)
	case *sqlparser.NotExpr:
		rewriteNotExpr(cursor, node)
	case *sqlparser.ComparisonExpr:
		return handleComparisonExpr(cursor, node)
	case *sqlparser.With:
		return r.handleWith(node)
	case *sqlparser.AliasedTableExpr:
		return r.handleAliasedTable(node)
	case *sqlparser.Delete:
		return handleDelete(node)
	}
	return nil
}

func (r *earlyRewriter) up(cursor *sqlparser.Cursor) error {
	switch node := cursor.Node().(type) {
	case *sqlparser.JoinTableExpr:
		return r.handleJoinTableExprUp(node)
	case *sqlparser.AliasedTableExpr:
		// this rewriting is done in the `up` phase, because we need the vindex hints to have been
		// processed while collecting the tables.
		return removeVindexHints(node)
	case sqlparser.GroupBy:
		r.clause = "group clause"
		iter := &exprIterator{
			node: node,
			idx:  -1,
		}
		return r.handleGroupBy(cursor.Parent(), iter)
	case sqlparser.OrderBy:
		r.clause = "order clause"
		iter := &orderByIterator{
			node: node,
			idx:  -1,
			r:    r,
		}
		return r.handleOrderBy(cursor.Parent(), iter)
	case *sqlparser.Where:
		if node.Type == sqlparser.HavingClause {
			return r.handleHavingClause(node, cursor.Parent())
		}

	}
	return nil
}

func handleDelete(del *sqlparser.Delete) error {
	// When we do not have any target, it is a single table delete.
	// In a single table delete, the table references is always a single aliased table expression.
	if len(del.Targets) != 0 {
		return nil
	}
	tblExpr, ok := del.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil
	}
	tblName, err := tblExpr.TableName()
	if err != nil {
		return err
	}
	del.Targets = append(del.Targets, tblName)
	return nil
}

func (r *earlyRewriter) handleAliasedTable(node *sqlparser.AliasedTableExpr) error {
	tbl, ok := node.Expr.(sqlparser.TableName)
	if !ok || tbl.Qualifier.NotEmpty() {
		return nil
	}
	scope := r.scoper.currentScope()
	cte := scope.findCTE(tbl.Name.String())
	if cte == nil {
		return nil
	}
	if node.As.IsEmpty() {
		node.As = tbl.Name
	}
	node.Expr = &sqlparser.DerivedTable{
		Select: cte.Subquery.Select,
	}
	if len(cte.Columns) > 0 {
		node.Columns = cte.Columns
	}
	return nil
}

func (r *earlyRewriter) handleWith(node *sqlparser.With) error {
	scope := r.scoper.currentScope()
	for _, cte := range node.CTEs {
		err := scope.addCTE(cte)
		if err != nil {
			return err
		}
	}
	node.CTEs = nil
	return nil
}

func rewriteNotExpr(cursor *sqlparser.Cursor, node *sqlparser.NotExpr) {
	cmp, ok := node.Expr.(*sqlparser.ComparisonExpr)
	if !ok {
		return
	}

	// There is no inverse operator for NullSafeEqualOp.
	// There doesn't exist a null safe non-equality.
	if cmp.Operator == sqlparser.NullSafeEqualOp {
		return
	}
	cmp.Operator = sqlparser.Inverse(cmp.Operator)
	cursor.Replace(cmp)
}

func (r *earlyRewriter) handleJoinTableExprUp(join *sqlparser.JoinTableExpr) error {
	// this rewriting is done in the `up` phase, because we need the scope to have been
	// filled in with the available tables
	if len(join.Condition.Using) == 0 {
		return nil
	}

	err := rewriteJoinUsing(r.binder, join)
	if err != nil {
		return err
	}

	// since the binder has already been over the join, we need to invoke it again, so it
	// can bind columns to the right tables

	return r.reAnalyze(join.Condition.On)
}

// removeVindexHints removes the vindex hints from the aliased table expression provided.
func removeVindexHints(node *sqlparser.AliasedTableExpr) error {
	if len(node.Hints) == 0 {
		return nil
	}
	var newHints sqlparser.IndexHints
	for _, hint := range node.Hints {
		if hint.Type.IsVindexHint() {
			continue
		}
		newHints = append(newHints, hint)
	}
	node.Hints = newHints
	return nil
}

// handleHavingClause processes the HAVING clause
func (r *earlyRewriter) handleHavingClause(node *sqlparser.Where, parent sqlparser.SQLNode) error {
	sel, ok := parent.(*sqlparser.Select)
	if !ok {
		return nil
	}
	expr, err := r.rewriteAliasesInHaving(node.Expr, sel)
	if err != nil {
		return err
	}
	node.Expr = expr
	return r.reAnalyze(expr)
}

// handleSelectExprs expands * in SELECT expressions.
func (r *earlyRewriter) handleSelectExprs(cursor *sqlparser.Cursor, node sqlparser.SelectExprs) error {
	_, isSel := cursor.Parent().(*sqlparser.Select)
	if !isSel {
		return nil
	}
	return r.expandStar(cursor, node)
}

type orderByIterator struct {
	node sqlparser.OrderBy
	idx  int
	r    *earlyRewriter
}

func (it *orderByIterator) next() sqlparser.Expr {
	it.idx++

	if it.idx >= len(it.node) {
		return nil
	}

	return it.node[it.idx].Expr
}

func (it *orderByIterator) replace(e sqlparser.Expr) (err error) {
	if it.idx >= len(it.node) {
		return vterrors.VT13001("went past the last item")
	}
	it.node[it.idx].Expr = e
	return nil
}

type exprIterator struct {
	node []sqlparser.Expr
	idx  int
}

func (it *exprIterator) next() sqlparser.Expr {
	it.idx++

	if it.idx >= len(it.node) {
		return nil
	}

	return it.node[it.idx]
}

func (it *exprIterator) replace(e sqlparser.Expr) error {
	if it.idx >= len(it.node) {
		return vterrors.VT13001("went past the last item")
	}
	it.node[it.idx] = e
	return nil
}

type iterator interface {
	next() sqlparser.Expr
	replace(e sqlparser.Expr) error
}

func (r *earlyRewriter) replaceLiteralsInOrderBy(e sqlparser.Expr, iter iterator) (bool, error) {
	lit := getIntLiteral(e)
	if lit == nil {
		return false, nil
	}

	newExpr, recheck, err := r.rewriteOrderByLiteral(lit)
	if err != nil {
		return false, err
	}

	if getIntLiteral(newExpr) == nil {
		coll, ok := e.(*sqlparser.CollateExpr)
		if ok {
			coll.Expr = newExpr
			newExpr = coll
		}
	} else {
		// the expression is still a literal int. that means that we don't really need to sort by it.
		// we'll just replace the number with a string instead, just like mysql would do in this situation
		// mysql> explain select 1 as foo from user group by 1;
		// <snip>
		// 	mysql> show warnings;
		// 	+-------+------+-----------------------------------------------------------------+
		// 	| Level | Code | Message                                                         |
		// 	+-------+------+-----------------------------------------------------------------+
		// 	| Note  | 1003 | /* select#1 */ select 1 AS `foo` from `test`.`user` group by '' |
		// 	+-------+------+-----------------------------------------------------------------+
		newExpr = sqlparser.NewStrLiteral("")
	}

	err = iter.replace(newExpr)
	if err != nil {
		return false, err
	}
	if recheck {
		err = r.reAnalyze(newExpr)
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *earlyRewriter) replaceLiteralsInGroupBy(e sqlparser.Expr) (sqlparser.Expr, error) {
	lit := getIntLiteral(e)
	if lit == nil {
		return nil, nil
	}

	newExpr, err := r.rewriteGroupByExpr(lit)
	if err != nil {
		return nil, err
	}

	if getIntLiteral(newExpr) == nil {
		coll, ok := e.(*sqlparser.CollateExpr)
		if ok {
			coll.Expr = newExpr
			newExpr = coll
		}
	} else {
		// the expression is still a literal int. that means that we don't really need to sort by it.
		// we'll just replace the number with a string instead, just like mysql would do in this situation
		// mysql> explain select 1 as foo from user group by 1;
		// <snip>
		// 	mysql> show warnings;
		// 	+-------+------+-----------------------------------------------------------------+
		// 	| Level | Code | Message                                                         |
		// 	+-------+------+-----------------------------------------------------------------+
		// 	| Note  | 1003 | /* select#1 */ select 1 AS `foo` from `test`.`user` group by '' |
		// 	+-------+------+-----------------------------------------------------------------+
		newExpr = sqlparser.NewStrLiteral("")
	}

	return newExpr, nil
}

func getIntLiteral(e sqlparser.Expr) *sqlparser.Literal {
	var lit *sqlparser.Literal
	switch node := e.(type) {
	case *sqlparser.Literal:
		lit = node
	case *sqlparser.CollateExpr:
		expr, ok := node.Expr.(*sqlparser.Literal)
		if !ok {
			return nil
		}
		lit = expr
	default:
		return nil
	}
	if lit.Type != sqlparser.IntVal {
		return nil
	}
	return lit
}

// handleOrderBy processes the ORDER BY clause.
func (r *earlyRewriter) handleOrderBy(parent sqlparser.SQLNode, iter iterator) error {
	stmt, ok := parent.(sqlparser.SelectStatement)
	if !ok {
		return nil
	}

	sel := sqlparser.GetFirstSelect(stmt)
	for e := iter.next(); e != nil; e = iter.next() {
		lit, err := r.replaceLiteralsInOrderBy(e, iter)
		if err != nil {
			return err
		}
		if lit {
			continue
		}

		expr, err := r.rewriteAliasesInOrderBy(e, sel)
		if err != nil {
			return err
		}

		if err = iter.replace(expr); err != nil {
			return err
		}

		if err = r.reAnalyze(expr); err != nil {
			return err
		}
	}

	return nil
}

// handleGroupBy processes the GROUP BY clause.
func (r *earlyRewriter) handleGroupBy(parent sqlparser.SQLNode, iter iterator) error {
	stmt, ok := parent.(sqlparser.SelectStatement)
	if !ok {
		return nil
	}

	sel := sqlparser.GetFirstSelect(stmt)
	for e := iter.next(); e != nil; e = iter.next() {
		expr, err := r.replaceLiteralsInGroupBy(e)
		if err != nil {
			return err
		}
		if expr == nil {
			expr, err = r.rewriteAliasesInGroupBy(e, sel)
			if err != nil {
				return err
			}

		}
		err = iter.replace(expr)
		if err != nil {
			return err
		}

		if err = r.reAnalyze(expr); err != nil {
			return err
		}
	}

	return nil
}

// rewriteHavingAndOrderBy rewrites columns in the ORDER BY and HAVING clauses to use aliases
// from the SELECT expressions when applicable, following MySQL scoping rules:
//   - A column identifier without a table qualifier that matches an alias introduced
//     in SELECT points to that expression, not any table column.
//   - However, if the aliased expression is an aggregation and the column identifier in
//     the HAVING/ORDER BY clause is inside an aggregation function, the rule does not apply.
func (r *earlyRewriter) rewriteAliasesInGroupBy(node sqlparser.Expr, sel *sqlparser.Select) (expr sqlparser.Expr, err error) {
	type ExprContainer struct {
		expr      sqlparser.Expr
		ambiguous bool
	}

	currentScope := r.scoper.currentScope()
	aliases := r.getAliasMap(sel)
	aggrTrack := &aggrTracker{}

	output := sqlparser.CopyOnRewrite(node, aggrTrack.down, func(cursor *sqlparser.CopyOnWriteCursor) {
		switch col := cursor.Node().(type) {
		case sqlparser.AggrFunc:
			aggrTrack.popAggr()
		case *sqlparser.ColName:
			if col.Qualifier.NonEmpty() {
				// we are only interested in columns not qualified by table names
				break
			}

			item, found := aliases[col.Name.Lowered()]
			if !found {
				break
			}

			isColumnOnTable, sure := r.isColumnOnTable(col, currentScope)
			if found && isColumnOnTable {
				r.warning = fmt.Sprintf("Column '%s' in group statement is ambiguous", sqlparser.String(col))
			}

			if isColumnOnTable && sure {
				break
			}

			if !sure {
				r.warning = "Missing table info, so not binding to anything on the FROM clause"
			}

			if item.ambiguous {
				err = newAmbiguousColumnError(col)
			} else if aggrTrack.insideAggr && sqlparser.ContainsAggregation(item.expr) {
				err = &InvalidUseOfGroupFunction{}
			}
			if err != nil {
				cursor.StopTreeWalk()
				return
			}

			cursor.Replace(sqlparser.CloneExpr(item.expr))
		}
	}, nil)

	expr = output.(sqlparser.Expr)
	return
}

func (r *earlyRewriter) rewriteAliasesInHaving(node sqlparser.Expr, sel *sqlparser.Select) (expr sqlparser.Expr, err error) {
	currentScope := r.scoper.currentScope()
	if currentScope.isUnion {
		// It is not safe to rewrite order by clauses in unions.
		return node, nil
	}

	aliases := r.getAliasMap(sel)
	aggrTrack := &aggrTracker{}
	output := sqlparser.CopyOnRewrite(node, aggrTrack.down, func(cursor *sqlparser.CopyOnWriteCursor) {
		var col *sqlparser.ColName

		switch node := cursor.Node().(type) {
		case sqlparser.AggrFunc:
			aggrTrack.popAggr()
			return
		case *sqlparser.ColName:
			col = node
		default:
			return
		}

		if col.Qualifier.NonEmpty() {
			// we are only interested in columns not qualified by table names
			return
		}

		item, found := aliases[col.Name.Lowered()]
		if aggrTrack.insideAggr {
			// inside aggregations, we want to first look for columns in the FROM clause
			isColumnOnTable, sure := r.isColumnOnTable(col, currentScope)
			if isColumnOnTable {
				if found && sure {
					r.warning = fmt.Sprintf("Column '%s' in having clause is ambiguous", sqlparser.String(col))
				}
				return
			}
		} else if !found {
			// if outside aggregations, we don't care about FROM columns
			// if there is no matching alias, there is no rewriting needed
			return
		}

		// If we get here, it means we have found an alias and want to use it
		if item.ambiguous {
			err = newAmbiguousColumnError(col)
		} else if aggrTrack.insideAggr && sqlparser.ContainsAggregation(item.expr) {
			err = &InvalidUseOfGroupFunction{}
		}
		if err != nil {
			cursor.StopTreeWalk()
			return
		}

		newColName := sqlparser.CopyOnRewrite(item.expr, nil, r.fillInQualifiers, nil)

		cursor.Replace(newColName)
	}, nil)

	expr = output.(sqlparser.Expr)
	return
}

type aggrTracker struct {
	insideAggr bool
}

func (at *aggrTracker) down(node, _ sqlparser.SQLNode) bool {
	switch node.(type) {
	case *sqlparser.Subquery:
		return false
	case sqlparser.AggrFunc:
		at.insideAggr = true
	}

	return true
}

func (at *aggrTracker) popAggr() {
	at.insideAggr = false
}

// rewriteAliasesInOrderBy rewrites columns in the ORDER BY to use aliases
// from the SELECT expressions when applicable, following MySQL scoping rules:
//   - A column identifier without a table qualifier that matches an alias introduced
//     in SELECT points to that expression, not any table column.
//   - However, if the aliased expression is an aggregation and the column identifier in
//     the HAVING/ORDER BY clause is inside an aggregation function, the rule does not apply.
func (r *earlyRewriter) rewriteAliasesInOrderBy(node sqlparser.Expr, sel *sqlparser.Select) (expr sqlparser.Expr, err error) {
	currentScope := r.scoper.currentScope()
	if currentScope.isUnion {
		// It is not safe to rewrite order by clauses in unions.
		return node, nil
	}

	aliases := r.getAliasMap(sel)
	aggrTrack := &aggrTracker{}
	output := sqlparser.CopyOnRewrite(node, aggrTrack.down, func(cursor *sqlparser.CopyOnWriteCursor) {
		var col *sqlparser.ColName

		switch node := cursor.Node().(type) {
		case sqlparser.AggrFunc:
			aggrTrack.popAggr()
			return
		case *sqlparser.ColName:
			col = node
		default:
			return
		}

		if col.Qualifier.NonEmpty() {
			// we are only interested in columns not qualified by table names
			return
		}

		var item exprContainer
		var found bool

		item, found = aliases[col.Name.Lowered()]
		if !found {
			// if there is no matching alias, there is no rewriting needed
			return
		}
		isColumnOnTable, sure := r.isColumnOnTable(col, currentScope)
		if found && isColumnOnTable && sure {
			r.warning = fmt.Sprintf("Column '%s' in order by statement is ambiguous", sqlparser.String(col))
		}

		topLevel := col == node
		if isColumnOnTable && sure && !topLevel {
			// we only want to replace columns that are not coming from the table
			return
		}

		if !sure {
			r.warning = "Missing table info, so not binding to anything on the FROM clause"
		}

		if item.ambiguous {
			err = newAmbiguousColumnError(col)
		} else if aggrTrack.insideAggr && sqlparser.ContainsAggregation(item.expr) {
			err = &InvalidUseOfGroupFunction{}
		}
		if err != nil {
			cursor.StopTreeWalk()
			return
		}

		newColName := sqlparser.CopyOnRewrite(item.expr, nil, r.fillInQualifiers, nil)

		cursor.Replace(newColName)
	}, nil)

	expr = output.(sqlparser.Expr)
	return
}

// fillInQualifiers adds qualifiers to any columns we have rewritten
func (r *earlyRewriter) fillInQualifiers(cursor *sqlparser.CopyOnWriteCursor) {
	col, ok := cursor.Node().(*sqlparser.ColName)
	if !ok || col.Qualifier.NonEmpty() {
		return
	}
	ts, found := r.binder.direct[col]
	if !found {
		panic("uh oh")
	}
	tbl := r.tables.Tables[ts.TableOffset()]
	tblName, err := tbl.Name()
	if err != nil {
		panic(err)
	}
	cursor.Replace(sqlparser.NewColNameWithQualifier(col.Name.String(), tblName))
}

func (r *earlyRewriter) isColumnOnTable(col *sqlparser.ColName, currentScope *scope) (isColumn bool, isCertain bool) {
	if !currentScope.stmtScope && currentScope.parent != nil {
		currentScope = currentScope.parent
	}
	deps, err := r.binder.resolveColumn(col, currentScope, false, false)
	if err != nil {
		return false, true
	}
	return true, deps.certain
}

func (r *earlyRewriter) getAliasMap(sel *sqlparser.Select) (aliases map[string]exprContainer) {
	var found bool
	aliases, found = r.aliasMapCache[sel]
	if found {
		return
	}
	aliases = map[string]exprContainer{}
	for _, e := range sel.SelectExprs {
		ae, ok := e.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}

		var alias string

		item := exprContainer{expr: ae.Expr}
		if ae.As.NotEmpty() {
			alias = ae.As.Lowered()
		} else if col, ok := ae.Expr.(*sqlparser.ColName); ok {
			alias = col.Name.Lowered()
		}

		if old, alreadyExists := aliases[alias]; alreadyExists && !sqlparser.Equals.Expr(old.expr, item.expr) {
			item.ambiguous = true
		}

		aliases[alias] = item
	}
	return aliases
}

type exprContainer struct {
	expr      sqlparser.Expr
	ambiguous bool
}

func (r *earlyRewriter) rewriteOrderByLiteral(node *sqlparser.Literal) (expr sqlparser.Expr, needReAnalysis bool, err error) {
	scope, found := r.scoper.specialExprScopes[node]
	if !found {
		return node, false, nil
	}
	num, err := strconv.Atoi(node.Val)
	if err != nil {
		return nil, false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "error parsing column number: %s", node.Val)
	}

	stmt, isSel := scope.stmt.(*sqlparser.Select)
	if !isSel {
		return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "error invalid statement type, expect Select, got: %T", scope.stmt)
	}

	if num < 1 || num > len(stmt.SelectExprs) {
		return nil, false, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadFieldError, "Unknown column '%d' in '%s'", num, r.clause)
	}

	// We loop like this instead of directly accessing the offset, to make sure there are no unexpanded `*` before
	for i := 0; i < num; i++ {
		if _, ok := stmt.SelectExprs[i].(*sqlparser.AliasedExpr); !ok {
			return nil, false, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "cannot use column offsets in %s when using `%s`", r.clause, sqlparser.String(stmt.SelectExprs[i]))
		}
	}

	colOffset := num - 1
	aliasedExpr, ok := stmt.SelectExprs[colOffset].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "don't know how to handle %s", sqlparser.String(node))
	}

	if scope.isUnion {
		colName := sqlparser.NewColName(aliasedExpr.ColumnName())
		vtabl, ok := scope.tables[0].(*vTableInfo)
		if !ok {
			panic("BUG: not expected")
		}

		// since column names can be ambiguous here, we want to do the binding by offset and not by column name
		allColExprs := vtabl.cols[colOffset]
		direct, recursive, typ := r.binder.org.depsForExpr(allColExprs)
		r.binder.direct[colName] = direct
		r.binder.recursive[colName] = recursive
		r.binder.typer.m[colName] = typ

		return colName, false, nil
	}

	return realCloneOfColNames(aliasedExpr.Expr, false), true, nil
}

func (r *earlyRewriter) rewriteGroupByExpr(node *sqlparser.Literal) (sqlparser.Expr, error) {
	scope, found := r.scoper.specialExprScopes[node]
	if !found {
		return node, nil
	}
	num, err := strconv.Atoi(node.Val)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "error parsing column number: %s", node.Val)
	}

	stmt, isSel := scope.stmt.(*sqlparser.Select)
	if !isSel {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "error invalid statement type, expect Select, got: %T", scope.stmt)
	}

	if num < 1 || num > len(stmt.SelectExprs) {
		return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadFieldError, "Unknown column '%d' in '%s'", num, r.clause)
	}

	// We loop like this instead of directly accessing the offset, to make sure there are no unexpanded `*` before
	for i := 0; i < num; i++ {
		if _, ok := stmt.SelectExprs[i].(*sqlparser.AliasedExpr); !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "cannot use column offsets in %s when using `%s`", r.clause, sqlparser.String(stmt.SelectExprs[i]))
		}
	}

	aliasedExpr, ok := stmt.SelectExprs[num-1].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "don't know how to handle %s", sqlparser.String(node))
	}

	if scope.isUnion {
		colName := sqlparser.NewColName(aliasedExpr.ColumnName())
		return colName, nil
	}

	return realCloneOfColNames(aliasedExpr.Expr, false), nil
}

// rewriteOrExpr rewrites OR expressions when the right side is FALSE.
func rewriteOrExpr(env *vtenv.Environment, cursor *sqlparser.Cursor, node *sqlparser.OrExpr) {
	newNode := rewriteOrFalse(env, *node)
	if newNode != nil {
		cursor.ReplaceAndRevisit(newNode)
	}
}

// rewriteAndExpr rewrites AND expressions when either side is TRUE.
func rewriteAndExpr(env *vtenv.Environment, cursor *sqlparser.Cursor, node *sqlparser.AndExpr) {
	newNode := rewriteAndTrue(env, *node)
	if newNode != nil {
		cursor.ReplaceAndRevisit(newNode)
	}
}

func rewriteAndTrue(env *vtenv.Environment, andExpr sqlparser.AndExpr) sqlparser.Expr {
	// we are looking for the pattern `WHERE c = 1 AND 1 = 1`
	isTrue := func(subExpr sqlparser.Expr) bool {
		coll := env.CollationEnv().DefaultConnectionCharset()
		evalEnginePred, err := evalengine.Translate(subExpr, &evalengine.Config{
			Environment: env,
			Collation:   coll,
		})
		if err != nil {
			return false
		}

		env := evalengine.EmptyExpressionEnv(env)
		res, err := env.Evaluate(evalEnginePred)
		if err != nil {
			return false
		}

		boolValue, err := res.Value(coll).ToBool()
		if err != nil {
			return false
		}

		return boolValue
	}

	if isTrue(andExpr.Left) {
		return andExpr.Right
	} else if isTrue(andExpr.Right) {
		return andExpr.Left
	}

	return nil
}

// handleComparisonExpr processes Comparison expressions, specifically for tuples with equal length and EqualOp operator.
func handleComparisonExpr(cursor *sqlparser.Cursor, node *sqlparser.ComparisonExpr) error {
	lft, lftOK := node.Left.(sqlparser.ValTuple)
	rgt, rgtOK := node.Right.(sqlparser.ValTuple)
	if !lftOK || !rgtOK || len(lft) != len(rgt) || node.Operator != sqlparser.EqualOp {
		return nil
	}
	var predicates []sqlparser.Expr
	for i, l := range lft {
		r := rgt[i]
		predicates = append(predicates, &sqlparser.ComparisonExpr{
			Operator: sqlparser.EqualOp,
			Left:     l,
			Right:    r,
			Escape:   node.Escape,
		})
	}
	cursor.Replace(sqlparser.AndExpressions(predicates...))
	return nil
}

func (r *earlyRewriter) expandStar(cursor *sqlparser.Cursor, node sqlparser.SelectExprs) error {
	currentScope := r.scoper.currentScope()
	var selExprs sqlparser.SelectExprs
	changed := false
	for _, selectExpr := range node {
		starExpr, isStarExpr := selectExpr.(*sqlparser.StarExpr)
		if !isStarExpr {
			selExprs = append(selExprs, selectExpr)
			continue
		}
		starExpanded, colNames, err := r.expandTableColumns(starExpr, currentScope.tables, r.binder.usingJoinInfo, r.scoper.org)
		if err != nil {
			return err
		}
		if !starExpanded || colNames == nil {
			selExprs = append(selExprs, selectExpr)
			continue
		}
		selExprs = append(selExprs, colNames...)
		changed = true
	}
	if changed {
		cursor.ReplaceAndRevisit(selExprs)
	}
	return nil
}

// realCloneOfColNames clones all the expressions including ColName.
// Since sqlparser.CloneRefOfColName does not clone col names, this method is needed.
func realCloneOfColNames(expr sqlparser.Expr, union bool) sqlparser.Expr {
	return sqlparser.CopyOnRewrite(expr, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
		exp, ok := cursor.Node().(*sqlparser.ColName)
		if !ok {
			return
		}

		newColName := *exp
		if union {
			newColName.Qualifier = sqlparser.TableName{}
		}
		cursor.Replace(&newColName)
	}, nil).(sqlparser.Expr)
}

func rewriteOrFalse(env *vtenv.Environment, orExpr sqlparser.OrExpr) sqlparser.Expr {
	// we are looking for the pattern `WHERE c = 1 OR 1 = 0`
	isFalse := func(subExpr sqlparser.Expr) bool {
		coll := env.CollationEnv().DefaultConnectionCharset()
		evalEnginePred, err := evalengine.Translate(subExpr, &evalengine.Config{
			Environment: env,
			Collation:   coll,
		})
		if err != nil {
			return false
		}

		env := evalengine.EmptyExpressionEnv(env)
		res, err := env.Evaluate(evalEnginePred)
		if err != nil {
			return false
		}

		boolValue, err := res.Value(coll).ToBool()
		if err != nil {
			return false
		}

		return !boolValue
	}

	if isFalse(orExpr.Left) {
		return orExpr.Right
	} else if isFalse(orExpr.Right) {
		return orExpr.Left
	}

	return nil
}

// rewriteJoinUsing rewrites SQL JOINs that use the USING clause to their equivalent
// JOINs with the ON condition. This function finds all the tables that have the
// specified columns in the USING clause, constructs an equality predicate for
// each pair of tables, and adds the resulting predicates to the WHERE clause
// of the outermost SELECT statement.
//
// For example, given the query:
//
//	SELECT * FROM t1 JOIN t2 USING (col1, col2)
//
// The rewriteJoinUsing function will rewrite the query to:
//
//	SELECT * FROM t1 JOIN t2 ON (t1.col1 = t2.col1 AND t1.col2 = t2.col2)
//
// This function returns an error if it encounters a non-authoritative table or
// if it cannot find a SELECT statement to add the WHERE predicate to.
func rewriteJoinUsing(b *binder, join *sqlparser.JoinTableExpr) error {
	predicates, err := buildJoinPredicates(b, join)
	if err != nil {
		return err
	}
	if len(predicates) > 0 {
		join.Condition.On = sqlparser.AndExpressions(predicates...)
		join.Condition.Using = nil
	}
	return nil
}

// buildJoinPredicates constructs the join predicates for a given set of USING columns.
// It returns a slice of sqlparser.Expr, each representing a join predicate for the given columns.
func buildJoinPredicates(b *binder, join *sqlparser.JoinTableExpr) ([]sqlparser.Expr, error) {
	var predicates []sqlparser.Expr

	for _, column := range join.Condition.Using {
		foundTables, err := findTablesWithColumn(b, join, column)
		if err != nil {
			return nil, err
		}

		predicates = append(predicates, createComparisonPredicates(column, foundTables)...)
	}

	return predicates, nil
}

func findOnlyOneTableInfoThatHasColumn(b *binder, tbl sqlparser.TableExpr, column sqlparser.IdentifierCI) ([]TableInfo, error) {
	switch tbl := tbl.(type) {
	case *sqlparser.AliasedTableExpr:
		ts := b.tc.tableSetFor(tbl)
		tblInfo := b.tc.Tables[ts.TableOffset()]
		for _, info := range tblInfo.getColumns() {
			if column.EqualString(info.Name) {
				return []TableInfo{tblInfo}, nil
			}
		}
		return nil, nil
	case *sqlparser.JoinTableExpr:
		tblInfoR, err := findOnlyOneTableInfoThatHasColumn(b, tbl.RightExpr, column)
		if err != nil {
			return nil, err
		}
		tblInfoL, err := findOnlyOneTableInfoThatHasColumn(b, tbl.LeftExpr, column)
		if err != nil {
			return nil, err
		}

		return append(tblInfoL, tblInfoR...), nil
	case *sqlparser.ParenTableExpr:
		var tblInfo []TableInfo
		for _, parenTable := range tbl.Exprs {
			newTblInfo, err := findOnlyOneTableInfoThatHasColumn(b, parenTable, column)
			if err != nil {
				return nil, err
			}
			if tblInfo != nil && newTblInfo != nil {
				return nil, vterrors.VT03021(column.String())
			}
			if newTblInfo != nil {
				tblInfo = newTblInfo
			}
		}
		return tblInfo, nil
	default:
		panic(fmt.Sprintf("unsupported TableExpr type in JOIN: %T", tbl))
	}
}

// findTablesWithColumn finds the tables with the specified column in the current scope.
func findTablesWithColumn(b *binder, join *sqlparser.JoinTableExpr, column sqlparser.IdentifierCI) ([]sqlparser.TableName, error) {
	leftTableInfo, err := findOnlyOneTableInfoThatHasColumn(b, join.LeftExpr, column)
	if err != nil {
		return nil, err
	}

	rightTableInfo, err := findOnlyOneTableInfoThatHasColumn(b, join.RightExpr, column)
	if err != nil {
		return nil, err
	}

	if leftTableInfo == nil || rightTableInfo == nil {
		return nil, ShardedError{Inner: vterrors.VT09015()}
	}
	var tableNames []sqlparser.TableName
	for _, info := range leftTableInfo {
		nm, err := info.Name()
		if err != nil {
			return nil, err
		}
		tableNames = append(tableNames, nm)
	}
	for _, info := range rightTableInfo {
		nm, err := info.Name()
		if err != nil {
			return nil, err
		}
		tableNames = append(tableNames, nm)
	}
	return tableNames, nil
}

// createComparisonPredicates creates a list of comparison predicates between the given column and foundTables.
func createComparisonPredicates(column sqlparser.IdentifierCI, foundTables []sqlparser.TableName) []sqlparser.Expr {
	var predicates []sqlparser.Expr
	for i, lft := range foundTables {
		for j := i + 1; j < len(foundTables); j++ {
			rgt := foundTables[j]
			predicates = append(predicates, createComparisonBetween(column, lft, rgt))
		}
	}
	return predicates
}

func createComparisonBetween(column sqlparser.IdentifierCI, lft, rgt sqlparser.TableName) *sqlparser.ComparisonExpr {
	return &sqlparser.ComparisonExpr{
		Operator: sqlparser.EqualOp,
		Left:     sqlparser.NewColNameWithQualifier(column.String(), lft),
		Right:    sqlparser.NewColNameWithQualifier(column.String(), rgt),
	}
}

func (r *earlyRewriter) expandTableColumns(
	starExpr *sqlparser.StarExpr,
	tables []TableInfo,
	joinUsing map[TableSet]map[string]TableSet,
	org originable,
) (bool, sqlparser.SelectExprs, error) {
	unknownTbl := true
	starExpanded := true
	state := &expanderState{
		colNames:        []sqlparser.SelectExpr{},
		needsQualifier:  len(tables) > 1,
		joinUsing:       joinUsing,
		org:             org,
		expandedColumns: map[sqlparser.TableName][]*sqlparser.ColName{},
	}

	for _, tbl := range tables {
		if !starExpr.TableName.IsEmpty() && !tbl.matches(starExpr.TableName) {
			continue
		}
		unknownTbl = false
		if !tbl.authoritative() {
			starExpanded = false
			break
		}
		err := state.processColumnsFor(tbl)
		if err != nil {
			return false, nil, err
		}
	}

	if unknownTbl {
		// This will only happen for case when starExpr has qualifier.
		return false, nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadDb, "Unknown table '%s'", sqlparser.String(starExpr.TableName))
	}

	if starExpanded {
		for k, v := range state.expandedColumns {
			r.expandedColumns[k] = v
		}
	}

	return starExpanded, state.colNames, nil
}

func (e *expanderState) processColumnsFor(tbl TableInfo) error {
	tblName, err := tbl.Name()
	if err != nil {
		return err
	}
	currTable := tbl.getTableSet(e.org)
	usingCols := e.joinUsing[currTable]
	if usingCols == nil {
		usingCols = map[string]TableSet{}
	}

	/*
		Redundant column elimination and column ordering occurs according to standard SQL, producing this display order:
		  *	First, coalesced common columns of the two joined tables, in the order in which they occur in the first table
		  *	Second, columns unique to the first table, in order in which they occur in that table
		  *	Third, columns unique to the second table, in order in which they occur in that table

		From: https://dev.mysql.com/doc/refman/8.0/en/join.html
	*/

outer:
	// in this first loop we just find columns used in any JOIN USING used on this table
	for _, col := range tbl.getColumns() {
		if col.Invisible {
			continue
		}
		ts, found := usingCols[col.Name]
		if found {
			for i, ts := range ts.Constituents() {
				if ts == currTable {
					if i == 0 {
						e.addColumn(col, tbl, tblName)
					} else {
						continue outer
					}
				}
			}
		}
	}

	// and this time around we are printing any columns not involved in any JOIN USING
	for _, col := range tbl.getColumns() {
		if col.Invisible {
			continue
		}

		if ts, found := usingCols[col.Name]; found && currTable.IsSolvedBy(ts) {
			continue
		}

		e.addColumn(col, tbl, tblName)
	}
	return nil
}

type expanderState struct {
	needsQualifier  bool
	colNames        sqlparser.SelectExprs
	joinUsing       map[TableSet]map[string]TableSet
	org             originable
	expandedColumns map[sqlparser.TableName][]*sqlparser.ColName
}

// addColumn adds columns to the expander state. If we have vschema info about the query,
// we also store which columns were expanded
func (e *expanderState) addColumn(col ColumnInfo, tbl TableInfo, tblName sqlparser.TableName) {
	var colName *sqlparser.ColName
	var alias sqlparser.IdentifierCI
	if e.needsQualifier {
		colName = sqlparser.NewColNameWithQualifier(col.Name, tblName)
	} else {
		colName = sqlparser.NewColName(col.Name)
	}
	e.colNames = append(e.colNames, &sqlparser.AliasedExpr{Expr: colName, As: alias})
	e.storeExpandInfo(tbl, tblName, colName)
}

func (e *expanderState) storeExpandInfo(tbl TableInfo, tblName sqlparser.TableName, colName *sqlparser.ColName) {
	vt := tbl.GetVindexTable()
	if vt == nil {
		return
	}
	keyspace := vt.Keyspace
	var ks sqlparser.IdentifierCS
	if keyspace != nil {
		ks = sqlparser.NewIdentifierCS(keyspace.Name)
	}
	tblName = sqlparser.TableName{
		Name:      tblName.Name,
		Qualifier: ks,
	}
	e.expandedColumns[tblName] = append(e.expandedColumns[tblName], colName)
}
