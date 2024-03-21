/*
Copyright 2022 The Vitess Authors.

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

package operators

import (
	"fmt"
	"slices"
	"sort"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	queryBuilder struct {
		ctx         *plancontext.PlanningContext
		stmt        sqlparser.Statement
		tableNames  []string
		dmlOperator Operator
	}
)

func (qb *queryBuilder) asSelectStatement() sqlparser.SelectStatement {
	return qb.stmt.(sqlparser.SelectStatement)
}
func (qb *queryBuilder) asOrderAndLimit() sqlparser.OrderAndLimit {
	return qb.stmt.(sqlparser.OrderAndLimit)
}

func ToSQL(ctx *plancontext.PlanningContext, op Operator) (_ sqlparser.Statement, _ Operator, err error) {
	defer PanicHandler(&err)

	q := &queryBuilder{ctx: ctx}
	buildQuery(op, q)
	if ctx.SemTable != nil {
		q.sortTables()
	}
	return q.stmt, q.dmlOperator, nil
}

func (qb *queryBuilder) addTable(db, tableName, alias string, tableID semantics.TableSet, hints sqlparser.IndexHints) {
	tableExpr := sqlparser.TableName{
		Name:      sqlparser.NewIdentifierCS(tableName),
		Qualifier: sqlparser.NewIdentifierCS(db),
	}
	qb.addTableExpr(tableName, alias, tableID, tableExpr, hints, nil)
}

func (qb *queryBuilder) addTableExpr(
	tableName, alias string,
	tableID semantics.TableSet,
	tblExpr sqlparser.SimpleTableExpr,
	hints sqlparser.IndexHints,
	columnAliases sqlparser.Columns,
) {
	if qb.stmt == nil {
		qb.stmt = &sqlparser.Select{}
	}
	tbl := &sqlparser.AliasedTableExpr{
		Expr:       tblExpr,
		Partitions: nil,
		As:         sqlparser.NewIdentifierCS(alias),
		Hints:      hints,
		Columns:    columnAliases,
	}
	qb.ctx.SemTable.ReplaceTableSetFor(tableID, tbl)
	qb.stmt.(FromStatement).SetFrom(append(qb.stmt.(FromStatement).GetFrom(), tbl))
	qb.tableNames = append(qb.tableNames, tableName)
}

func (qb *queryBuilder) addPredicate(expr sqlparser.Expr) {
	if qb.ctx.ShouldSkip(expr) {
		// This is a predicate that was added to the RHS of an ApplyJoin.
		// The original predicate will be added, so we don't have to add this here
		return
	}

	var addPred func(sqlparser.Expr)

	switch stmt := qb.stmt.(type) {
	case *sqlparser.Select:
		if containsAggr(expr) {
			addPred = stmt.AddHaving
		} else {
			addPred = stmt.AddWhere
		}
	case *sqlparser.Update:
		addPred = stmt.AddWhere
	case *sqlparser.Delete:
		addPred = stmt.AddWhere
	default:
		panic(fmt.Sprintf("cant add WHERE to %T", qb.stmt))
	}

	for _, exp := range sqlparser.SplitAndExpression(nil, expr) {
		addPred(exp)
	}
}

func (qb *queryBuilder) addGroupBy(original sqlparser.Expr) {
	sel := qb.stmt.(*sqlparser.Select)
	sel.GroupBy = append(sel.GroupBy, original)
}

func (qb *queryBuilder) addProjection(projection sqlparser.SelectExpr) {
	switch stmt := qb.stmt.(type) {
	case *sqlparser.Select:
		stmt.SelectExprs = append(stmt.SelectExprs, projection)
		return
	case *sqlparser.Union:
		if ae, ok := projection.(*sqlparser.AliasedExpr); ok {
			if col, ok := ae.Expr.(*sqlparser.ColName); ok {
				checkUnionColumnByName(col, stmt)
				return
			}
		}

		qb.pushUnionInsideDerived()
		qb.addProjection(projection)
		return
	}
	panic(vterrors.VT13001(fmt.Sprintf("unknown select statement type: %T", qb.stmt)))
}

func (qb *queryBuilder) pushUnionInsideDerived() {
	selStmt := qb.asSelectStatement()
	dt := &sqlparser.DerivedTable{
		Lateral: false,
		Select:  selStmt,
	}
	sel := &sqlparser.Select{
		From: []sqlparser.TableExpr{&sqlparser.AliasedTableExpr{
			Expr: dt,
			As:   sqlparser.NewIdentifierCS("dt"),
		}},
	}
	sel.SelectExprs = unionSelects(sqlparser.GetFirstSelect(selStmt).SelectExprs)
	qb.stmt = sel
}

func unionSelects(exprs sqlparser.SelectExprs) (selectExprs sqlparser.SelectExprs) {
	for _, col := range exprs {
		switch col := col.(type) {
		case *sqlparser.AliasedExpr:
			expr := sqlparser.NewColName(col.ColumnName())
			selectExprs = append(selectExprs, &sqlparser.AliasedExpr{Expr: expr})
		default:
			selectExprs = append(selectExprs, col)
		}
	}
	return
}

func checkUnionColumnByName(column *sqlparser.ColName, sel sqlparser.SelectStatement) {
	colName := column.Name.String()
	exprs := sqlparser.GetFirstSelect(sel).SelectExprs
	offset := slices.IndexFunc(exprs, func(expr sqlparser.SelectExpr) bool {
		switch ae := expr.(type) {
		case *sqlparser.StarExpr:
			return true
		case *sqlparser.AliasedExpr:
			// When accessing columns on top of a UNION, we fall back to this simple strategy of string comparisons
			return ae.ColumnName() == colName
		}
		return false
	})
	if offset == -1 {
		panic(vterrors.VT12001(fmt.Sprintf("did not find column [%s] on UNION", sqlparser.String(column))))
	}
}

func (qb *queryBuilder) clearProjections() {
	sel, isSel := qb.stmt.(*sqlparser.Select)
	if !isSel {
		return
	}
	sel.SelectExprs = nil
}

func (qb *queryBuilder) unionWith(other *queryBuilder, distinct bool) {
	qb.stmt = &sqlparser.Union{
		Left:     qb.asSelectStatement(),
		Right:    other.asSelectStatement(),
		Distinct: distinct,
	}
}

type FromStatement interface {
	GetFrom() []sqlparser.TableExpr
	SetFrom([]sqlparser.TableExpr)
	GetWherePredicate() sqlparser.Expr
	SetWherePredicate(sqlparser.Expr)
}

var _ FromStatement = (*sqlparser.Select)(nil)
var _ FromStatement = (*sqlparser.Update)(nil)
var _ FromStatement = (*sqlparser.Delete)(nil)

func (qb *queryBuilder) joinWith(other *queryBuilder, onCondition sqlparser.Expr, joinType sqlparser.JoinType) {
	stmt := qb.stmt.(FromStatement)
	otherStmt := other.stmt.(FromStatement)

	if sel, isSel := stmt.(*sqlparser.Select); isSel {
		otherSel := otherStmt.(*sqlparser.Select)
		sel.SelectExprs = append(sel.SelectExprs, otherSel.SelectExprs...)
	}

	qb.mergeWhereClauses(stmt, otherStmt)

	var newFromClause []sqlparser.TableExpr
	switch joinType {
	case sqlparser.NormalJoinType:
		newFromClause = append(stmt.GetFrom(), otherStmt.GetFrom()...)
		qb.addPredicate(onCondition)
	default:
		newFromClause = []sqlparser.TableExpr{buildJoin(stmt, otherStmt, onCondition, joinType)}
	}

	stmt.SetFrom(newFromClause)
}

func (qb *queryBuilder) mergeWhereClauses(stmt, otherStmt FromStatement) {
	predicate := stmt.GetWherePredicate()
	if otherPredicate := otherStmt.GetWherePredicate(); otherPredicate != nil {
		predExprs := sqlparser.SplitAndExpression(nil, predicate)
		otherExprs := sqlparser.SplitAndExpression(nil, otherPredicate)
		predicate = qb.ctx.SemTable.AndExpressions(append(predExprs, otherExprs...)...)
	}
	if predicate != nil {
		stmt.SetWherePredicate(predicate)
	}
}

func buildJoin(stmt FromStatement, otherStmt FromStatement, onCondition sqlparser.Expr, joinType sqlparser.JoinType) *sqlparser.JoinTableExpr {
	var lhs sqlparser.TableExpr
	fromClause := stmt.GetFrom()
	if len(fromClause) == 1 {
		lhs = fromClause[0]
	} else {
		lhs = &sqlparser.ParenTableExpr{Exprs: fromClause}
	}
	var rhs sqlparser.TableExpr
	otherFromClause := otherStmt.GetFrom()
	if len(otherFromClause) == 1 {
		rhs = otherFromClause[0]
	} else {
		rhs = &sqlparser.ParenTableExpr{Exprs: otherFromClause}
	}

	return &sqlparser.JoinTableExpr{
		LeftExpr:  lhs,
		RightExpr: rhs,
		Join:      joinType,
		Condition: &sqlparser.JoinCondition{
			On: onCondition,
		},
	}
}

func (qb *queryBuilder) sortTables() {
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		sel, isSel := node.(*sqlparser.Select)
		if !isSel {
			return true, nil
		}
		ts := &tableSorter{
			sel: sel,
			tbl: qb.ctx.SemTable,
		}
		sort.Sort(ts)
		return true, nil
	}, qb.stmt)

}

type tableSorter struct {
	sel *sqlparser.Select
	tbl *semantics.SemTable
}

// Len implements the Sort interface
func (ts *tableSorter) Len() int {
	return len(ts.sel.From)
}

// Less implements the Sort interface
func (ts *tableSorter) Less(i, j int) bool {
	lhs := ts.sel.From[i]
	rhs := ts.sel.From[j]
	left, ok := lhs.(*sqlparser.AliasedTableExpr)
	if !ok {
		return i < j
	}
	right, ok := rhs.(*sqlparser.AliasedTableExpr)
	if !ok {
		return i < j
	}

	return ts.tbl.TableSetFor(left).TableOffset() < ts.tbl.TableSetFor(right).TableOffset()
}

// Swap implements the Sort interface
func (ts *tableSorter) Swap(i, j int) {
	ts.sel.From[i], ts.sel.From[j] = ts.sel.From[j], ts.sel.From[i]
}

func removeKeyspaceFromSelectExpr(expr sqlparser.SelectExpr) {
	switch expr := expr.(type) {
	case *sqlparser.AliasedExpr:
		sqlparser.RemoveKeyspaceInCol(expr.Expr)
	case *sqlparser.StarExpr:
		expr.TableName.Qualifier = sqlparser.NewIdentifierCS("")
	}
}

func stripDownQuery(from, to sqlparser.SelectStatement) {
	switch node := from.(type) {
	case *sqlparser.Select:
		toNode, ok := to.(*sqlparser.Select)
		if !ok {
			panic(vterrors.VT13001("AST did not match"))
		}
		toNode.Distinct = node.Distinct
		toNode.GroupBy = node.GroupBy
		toNode.Having = node.Having
		toNode.OrderBy = node.OrderBy
		toNode.Comments = node.Comments
		toNode.Limit = node.Limit
		toNode.SelectExprs = node.SelectExprs
		for _, expr := range toNode.SelectExprs {
			removeKeyspaceFromSelectExpr(expr)
		}
	case *sqlparser.Union:
		toNode, ok := to.(*sqlparser.Union)
		if !ok {
			panic(vterrors.VT13001("AST did not match"))
		}
		stripDownQuery(node.Left, toNode.Left)
		stripDownQuery(node.Right, toNode.Right)
		toNode.OrderBy = node.OrderBy
	default:
		panic(vterrors.VT13001(fmt.Sprintf("this should not happen - we have covered all implementations of SelectStatement %T", from)))
	}
}

// buildQuery recursively builds the query into an AST, from an operator tree
func buildQuery(op Operator, qb *queryBuilder) {
	switch op := op.(type) {
	case *Table:
		buildTable(op, qb)
	case *Projection:
		buildProjection(op, qb)
	case *ApplyJoin:
		buildApplyJoin(op, qb)
	case *Filter:
		buildFilter(op, qb)
	case *Horizon:
		if op.TableId != nil {
			buildDerived(op, qb)
			return
		}
		buildHorizon(op, qb)
	case *Limit:
		buildLimit(op, qb)
	case *Ordering:
		buildOrdering(op, qb)
	case *Aggregator:
		buildAggregation(op, qb)
	case *Union:
		buildUnion(op, qb)
	case *Distinct:
		buildQuery(op.Source, qb)
		qb.asSelectStatement().MakeDistinct()
	case *Update:
		buildUpdate(op, qb)
	case *Delete:
		buildDelete(op, qb)
	case *Insert:
		buildDML(op, qb)
	default:
		panic(vterrors.VT13001(fmt.Sprintf("unknown operator to convert to SQL: %T", op)))
	}
}

func buildDelete(op *Delete, qb *queryBuilder) {
	qb.stmt = &sqlparser.Delete{
		Ignore:  op.Ignore,
		Targets: sqlparser.TableNames{op.Target.Name},
	}
	buildQuery(op.Source, qb)

	qb.dmlOperator = op
}

func buildUpdate(op *Update, qb *queryBuilder) {
	updExprs := getUpdateExprs(op)
	upd := &sqlparser.Update{
		Ignore: op.Ignore,
		Exprs:  updExprs,
	}
	qb.stmt = upd
	qb.dmlOperator = op
	buildQuery(op.Source, qb)
}

func getUpdateExprs(op *Update) sqlparser.UpdateExprs {
	updExprs := make(sqlparser.UpdateExprs, 0, len(op.Assignments))
	for _, se := range op.Assignments {
		updExprs = append(updExprs, &sqlparser.UpdateExpr{
			Name: se.Name,
			Expr: se.Expr.EvalExpr,
		})
	}
	return updExprs
}

type OpWithAST interface {
	Operator
	Statement() sqlparser.Statement
}

func buildDML(op OpWithAST, qb *queryBuilder) {
	qb.stmt = op.Statement()
	qb.dmlOperator = op
}

func buildAggregation(op *Aggregator, qb *queryBuilder) {
	buildQuery(op.Source, qb)

	qb.clearProjections()

	cols := op.GetColumns(qb.ctx)
	for _, column := range cols {
		qb.addProjection(column)
	}

	for _, by := range op.Grouping {
		qb.addGroupBy(by.Inner)
		simplified := by.Inner
		if by.WSOffset != -1 {
			qb.addGroupBy(weightStringFor(simplified))
		}
	}
}

func buildOrdering(op *Ordering, qb *queryBuilder) {
	buildQuery(op.Source, qb)

	for _, order := range op.Order {
		qb.asOrderAndLimit().AddOrder(order.Inner)
	}
}

func buildLimit(op *Limit, qb *queryBuilder) {
	buildQuery(op.Source, qb)
	qb.asOrderAndLimit().SetLimit(op.AST)
}

func buildTable(op *Table, qb *queryBuilder) {
	dbName := ""

	if op.QTable.IsInfSchema {
		dbName = op.QTable.Table.Qualifier.String()
	}
	qb.addTable(dbName, op.QTable.Table.Name.String(), op.QTable.Alias.As.String(), TableID(op), op.QTable.Alias.Hints)
	for _, pred := range op.QTable.Predicates {
		qb.addPredicate(pred)
	}
	for _, name := range op.Columns {
		qb.addProjection(&sqlparser.AliasedExpr{Expr: name})
	}
}

func buildProjection(op *Projection, qb *queryBuilder) {
	buildQuery(op.Source, qb)

	_, isSel := qb.stmt.(*sqlparser.Select)
	if isSel {
		qb.clearProjections()
		cols := op.GetSelectExprs(qb.ctx)
		for _, column := range cols {
			qb.addProjection(column)
		}
	}

	// if the projection is on derived table, we use the select we have
	// created above and transform it into a derived table
	if op.DT != nil {
		sel := qb.asSelectStatement()
		qb.stmt = nil
		qb.addTableExpr(op.DT.Alias, op.DT.Alias, TableID(op), &sqlparser.DerivedTable{
			Select: sel,
		}, nil, op.DT.Columns)
	}

	if !isSel {
		cols := op.GetSelectExprs(qb.ctx)
		for _, column := range cols {
			qb.addProjection(column)
		}
	}
}

func buildApplyJoin(op *ApplyJoin, qb *queryBuilder) {
	predicates := slice.Map(op.JoinPredicates.columns, func(jc applyJoinColumn) sqlparser.Expr {
		// since we are adding these join predicates, we need to mark to broken up version (RHSExpr) of it as done
		err := qb.ctx.SkipJoinPredicates(jc.Original)
		if err != nil {
			panic(err)
		}
		return jc.Original
	})
	pred := sqlparser.AndExpressions(predicates...)

	buildQuery(op.LHS, qb)

	qbR := &queryBuilder{ctx: qb.ctx}
	buildQuery(op.RHS, qbR)
	qb.joinWith(qbR, pred, op.JoinType)
}

func buildUnion(op *Union, qb *queryBuilder) {
	// the first input is built first
	buildQuery(op.Sources[0], qb)

	for i, src := range op.Sources {
		if i == 0 {
			continue
		}

		// now we can go over the remaining inputs and UNION them together
		qbOther := &queryBuilder{ctx: qb.ctx}
		buildQuery(src, qbOther)
		qb.unionWith(qbOther, op.distinct)
	}
}

func buildFilter(op *Filter, qb *queryBuilder) {
	buildQuery(op.Source, qb)

	for _, pred := range op.Predicates {
		qb.addPredicate(pred)
	}
}

func buildDerived(op *Horizon, qb *queryBuilder) {
	buildQuery(op.Source, qb)

	sqlparser.RemoveKeyspaceInCol(op.Query)

	stmt := qb.stmt
	qb.stmt = nil
	switch sel := stmt.(type) {
	case *sqlparser.Select:
		buildDerivedSelect(op, qb, sel)
		return
	case *sqlparser.Union:
		buildDerivedUnion(op, qb, sel)
		return
	}
	panic(fmt.Sprintf("unknown select statement type: %T", stmt))
}

func buildDerivedUnion(op *Horizon, qb *queryBuilder, union *sqlparser.Union) {
	opQuery, ok := op.Query.(*sqlparser.Union)
	if !ok {
		panic(vterrors.VT12001("Horizon contained SELECT but statement was UNION"))
	}

	union.Limit = opQuery.Limit
	union.OrderBy = opQuery.OrderBy
	union.Distinct = opQuery.Distinct

	qb.addTableExpr(op.Alias, op.Alias, TableID(op), &sqlparser.DerivedTable{
		Select: union,
	}, nil, op.ColumnAliases)
}

func buildDerivedSelect(op *Horizon, qb *queryBuilder, sel *sqlparser.Select) {
	opQuery, ok := op.Query.(*sqlparser.Select)
	if !ok {
		panic(vterrors.VT12001("Horizon contained UNION but statement was SELECT"))
	}
	sel.Limit = opQuery.Limit
	sel.OrderBy = opQuery.OrderBy
	sel.GroupBy = opQuery.GroupBy
	sel.Having = mergeHaving(sel.Having, opQuery.Having)
	sel.SelectExprs = opQuery.SelectExprs
	qb.addTableExpr(op.Alias, op.Alias, TableID(op), &sqlparser.DerivedTable{
		Select: sel,
	}, nil, op.ColumnAliases)
	for _, col := range op.Columns {
		qb.addProjection(&sqlparser.AliasedExpr{Expr: col})
	}
}

func buildHorizon(op *Horizon, qb *queryBuilder) {
	buildQuery(op.Source, qb)
	stripDownQuery(op.Query, qb.asSelectStatement())
	sqlparser.RemoveKeyspaceInCol(qb.stmt)
}

func mergeHaving(h1, h2 *sqlparser.Where) *sqlparser.Where {
	switch {
	case h1 == nil && h2 == nil:
		return nil
	case h1 == nil:
		return h2
	case h2 == nil:
		return h1
	default:
		h1.Expr = sqlparser.AndExpressions(h1.Expr, h2.Expr)
		return h1
	}
}
