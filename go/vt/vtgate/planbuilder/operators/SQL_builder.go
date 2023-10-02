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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	queryBuilder struct {
		ctx         *plancontext.PlanningContext
		stmt        sqlparser.Statement
		tableNames  []string
		dmlOperator ops.Operator
	}
)

func (qb *queryBuilder) asSelectStatement() sqlparser.SelectStatement {
	return qb.stmt.(sqlparser.SelectStatement)
}

func ToSQL(ctx *plancontext.PlanningContext, op ops.Operator) (sqlparser.Statement, ops.Operator, error) {
	q := &queryBuilder{ctx: ctx}
	err := buildQuery(op, q)
	if err != nil {
		return nil, nil, err
	}
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
	sel := qb.stmt.(*sqlparser.Select)
	elems := &sqlparser.AliasedTableExpr{
		Expr:       tblExpr,
		Partitions: nil,
		As:         sqlparser.NewIdentifierCS(alias),
		Hints:      hints,
		Columns:    columnAliases,
	}
	qb.ctx.SemTable.ReplaceTableSetFor(tableID, elems)
	sel.From = append(sel.From, elems)
	qb.stmt = sel
	qb.tableNames = append(qb.tableNames, tableName)
}

func (qb *queryBuilder) addPredicate(expr sqlparser.Expr) {
	if _, toBeSkipped := qb.ctx.SkipPredicates[expr]; toBeSkipped {
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

func (qb *queryBuilder) addProjection(projection sqlparser.SelectExpr) error {
	switch stmt := qb.stmt.(type) {
	case *sqlparser.Select:
		stmt.SelectExprs = append(stmt.SelectExprs, projection)
		return nil
	case *sqlparser.Union:
		if ae, ok := projection.(*sqlparser.AliasedExpr); ok {
			if col, ok := ae.Expr.(*sqlparser.ColName); ok {
				return checkUnionColumnByName(col, stmt)
			}
		}

		qb.pushUnionInsideDerived()
		return qb.addProjection(projection)
	}
	return vterrors.VT13001(fmt.Sprintf("unknown select statement type: %T", qb.stmt))
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

func checkUnionColumnByName(column *sqlparser.ColName, sel sqlparser.SelectStatement) error {
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
		return vterrors.VT12001(fmt.Sprintf("did not find column [%s] on UNION", sqlparser.String(column)))
	}
	return nil
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

func (qb *queryBuilder) joinInnerWith(other *queryBuilder, onCondition sqlparser.Expr) {
	sel := qb.stmt.(*sqlparser.Select)
	otherSel := other.stmt.(*sqlparser.Select)
	sel.From = append(sel.From, otherSel.From...)
	sel.SelectExprs = append(sel.SelectExprs, otherSel.SelectExprs...)

	var predicate sqlparser.Expr
	if sel.Where != nil {
		predicate = sel.Where.Expr
	}
	if otherSel.Where != nil {
		predExprs := sqlparser.SplitAndExpression(nil, predicate)
		otherExprs := sqlparser.SplitAndExpression(nil, otherSel.Where.Expr)
		predicate = qb.ctx.SemTable.AndExpressions(append(predExprs, otherExprs...)...)
	}
	if predicate != nil {
		sel.Where = &sqlparser.Where{Type: sqlparser.WhereClause, Expr: predicate}
	}

	qb.addPredicate(onCondition)
}

func (qb *queryBuilder) joinOuterWith(other *queryBuilder, onCondition sqlparser.Expr) {
	sel := qb.stmt.(*sqlparser.Select)
	otherSel := other.stmt.(*sqlparser.Select)
	var lhs sqlparser.TableExpr
	if len(sel.From) == 1 {
		lhs = sel.From[0]
	} else {
		lhs = &sqlparser.ParenTableExpr{Exprs: sel.From}
	}
	var rhs sqlparser.TableExpr
	if len(otherSel.From) == 1 {
		rhs = otherSel.From[0]
	} else {
		rhs = &sqlparser.ParenTableExpr{Exprs: otherSel.From}
	}
	sel.From = []sqlparser.TableExpr{&sqlparser.JoinTableExpr{
		LeftExpr:  lhs,
		RightExpr: rhs,
		Join:      sqlparser.LeftJoinType,
		Condition: &sqlparser.JoinCondition{
			On: onCondition,
		},
	}}

	sel.SelectExprs = append(sel.SelectExprs, otherSel.SelectExprs...)
	var predicate sqlparser.Expr
	if sel.Where != nil {
		predicate = sel.Where.Expr
	}
	if otherSel.Where != nil {
		predicate = qb.ctx.SemTable.AndExpressions(predicate, otherSel.Where.Expr)
	}
	if predicate != nil {
		sel.Where = &sqlparser.Where{Type: sqlparser.WhereClause, Expr: predicate}
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
		sqlparser.RemoveKeyspaceFromColName(expr.Expr)
	case *sqlparser.StarExpr:
		expr.TableName.Qualifier = sqlparser.NewIdentifierCS("")
	}
}

func stripDownQuery(from, to sqlparser.SelectStatement) error {
	var err error

	switch node := from.(type) {
	case *sqlparser.Select:
		toNode, ok := to.(*sqlparser.Select)
		if !ok {
			return vterrors.VT13001("AST did not match")
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
			return vterrors.VT13001("AST did not match")
		}
		err = stripDownQuery(node.Left, toNode.Left)
		if err != nil {
			return err
		}
		err = stripDownQuery(node.Right, toNode.Right)
		if err != nil {
			return err
		}
		toNode.OrderBy = node.OrderBy
	default:
		return vterrors.VT13001(fmt.Sprintf("this should not happen - we have covered all implementations of SelectStatement %T", from))
	}
	return nil
}

// buildQuery recursively builds the query into an AST, from an operator tree
func buildQuery(op ops.Operator, qb *queryBuilder) error {
	switch op := op.(type) {
	case *Table:
		buildTable(op, qb)
	case *Projection:
		return buildProjection(op, qb)
	case *ApplyJoin:
		return buildApplyJoin(op, qb)
	case *Filter:
		return buildFilter(op, qb)
	case *Horizon:
		if op.TableId != nil {
			return buildDerived(op, qb)
		}
		return buildHorizon(op, qb)
	case *Limit:
		return buildLimit(op, qb)
	case *Ordering:
		return buildOrdering(op, qb)
	case *Aggregator:
		return buildAggregation(op, qb)
	case *Union:
		return buildUnion(op, qb)
	case *Distinct:
		err := buildQuery(op.Source, qb)
		if err != nil {
			return err
		}
		qb.asSelectStatement().MakeDistinct()
	case *Update:
		buildUpdate(op, qb)
	case *Delete:
		buildDML(op, qb)
	case *Insert:
		buildDML(op, qb)
	default:
		return vterrors.VT13001(fmt.Sprintf("unknown operator to convert to SQL: %T", op))
	}
	return nil
}

func buildUpdate(op *Update, qb *queryBuilder) {
	tblName := sqlparser.NewTableName(op.QTable.Table.Name.String())
	aTblExpr := &sqlparser.AliasedTableExpr{
		Expr: tblName,
		As:   op.QTable.Alias.As,
	}
	updExprs := make(sqlparser.UpdateExprs, 0, len(op.Assignments))
	for _, se := range op.Assignments {
		updExprs = append(updExprs, &sqlparser.UpdateExpr{
			Name: se.Name,
			Expr: se.Expr.EvalExpr,
		})
	}

	qb.stmt = &sqlparser.Update{
		Ignore:     op.Ignore,
		TableExprs: sqlparser.TableExprs{aTblExpr},
		Exprs:      updExprs,
		OrderBy:    op.OrderBy,
		Limit:      op.Limit,
	}

	for _, pred := range op.QTable.Predicates {
		qb.addPredicate(pred)
	}

	qb.dmlOperator = op
}

type OpWithAST interface {
	ops.Operator
	Statement() sqlparser.Statement
}

func buildDML(op OpWithAST, qb *queryBuilder) {
	qb.stmt = op.Statement()
	qb.dmlOperator = op
}

func buildAggregation(op *Aggregator, qb *queryBuilder) error {
	err := buildQuery(op.Source, qb)
	if err != nil {
		return err
	}

	qb.clearProjections()

	cols, err := op.GetColumns(qb.ctx)
	if err != nil {
		return err
	}
	for _, column := range cols {
		err := qb.addProjection(column)
		if err != nil {
			return err
		}
	}

	for _, by := range op.Grouping {
		qb.addGroupBy(by.Inner)
		simplified := by.SimplifiedExpr
		if by.WSOffset != -1 {
			qb.addGroupBy(weightStringFor(simplified))
		}
	}

	return nil
}

func buildOrdering(op *Ordering, qb *queryBuilder) error {
	err := buildQuery(op.Source, qb)
	if err != nil {
		return err
	}

	for _, order := range op.Order {
		qb.asSelectStatement().AddOrder(order.Inner)
	}
	return nil
}

func buildLimit(op *Limit, qb *queryBuilder) error {
	err := buildQuery(op.Source, qb)
	if err != nil {
		return err
	}
	qb.asSelectStatement().SetLimit(op.AST)
	return nil
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
		err := qb.addProjection(&sqlparser.AliasedExpr{Expr: name})
		if err != nil {
			return
		}
	}
}

func buildProjection(op *Projection, qb *queryBuilder) error {
	err := buildQuery(op.Source, qb)
	if err != nil {
		return err
	}

	_, isSel := qb.stmt.(*sqlparser.Select)
	if isSel {
		qb.clearProjections()
		cols, err := op.GetSelectExprs(qb.ctx)
		if err != nil {
			return err
		}
		for _, column := range cols {
			err := qb.addProjection(column)
			if err != nil {
				return err
			}
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
		cols, err := op.GetSelectExprs(qb.ctx)
		if err != nil {
			return err
		}
		for _, column := range cols {
			err := qb.addProjection(column)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func buildApplyJoin(op *ApplyJoin, qb *queryBuilder) error {
	err := buildQuery(op.LHS, qb)
	if err != nil {
		return err
	}
	// If we are going to add the predicate used in join here
	// We should not add the predicate's copy of when it was split into
	// two parts. To avoid this, we use the SkipPredicates map.
	for _, expr := range qb.ctx.JoinPredicates[op.Predicate] {
		qb.ctx.SkipPredicates[expr] = nil
	}
	qbR := &queryBuilder{ctx: qb.ctx}
	err = buildQuery(op.RHS, qbR)
	if err != nil {
		return err
	}
	if op.LeftJoin {
		qb.joinOuterWith(qbR, op.Predicate)
	} else {
		qb.joinInnerWith(qbR, op.Predicate)
	}
	return nil
}

func buildUnion(op *Union, qb *queryBuilder) error {
	// the first input is built first
	err := buildQuery(op.Sources[0], qb)
	if err != nil {
		return err
	}

	for i, src := range op.Sources {
		if i == 0 {
			continue
		}

		// now we can go over the remaining inputs and UNION them together
		qbOther := &queryBuilder{ctx: qb.ctx}
		err = buildQuery(src, qbOther)
		if err != nil {
			return err
		}
		qb.unionWith(qbOther, op.distinct)
	}

	return nil
}

func buildFilter(op *Filter, qb *queryBuilder) error {
	err := buildQuery(op.Source, qb)
	if err != nil {
		return err
	}
	for _, pred := range op.Predicates {
		qb.addPredicate(pred)
	}
	return nil
}

func buildDerived(op *Horizon, qb *queryBuilder) error {
	err := buildQuery(op.Source, qb)
	if err != nil {
		return err
	}
	sqlparser.RemoveKeyspace(op.Query)

	stmt := qb.stmt
	qb.stmt = nil
	switch sel := stmt.(type) {
	case *sqlparser.Select:
		return buildDerivedSelect(op, qb, sel)
	case *sqlparser.Union:
		return buildDerivedUnion(op, qb, sel)
	}
	panic(fmt.Sprintf("unknown select statement type: %T", stmt))
}

func buildDerivedUnion(op *Horizon, qb *queryBuilder, union *sqlparser.Union) error {
	opQuery, ok := op.Query.(*sqlparser.Union)
	if !ok {
		return vterrors.VT12001("Horizon contained SELECT but statement was UNION")
	}

	union.Limit = opQuery.Limit
	union.OrderBy = opQuery.OrderBy
	union.Distinct = opQuery.Distinct

	qb.addTableExpr(op.Alias, op.Alias, TableID(op), &sqlparser.DerivedTable{
		Select: union,
	}, nil, op.ColumnAliases)

	return nil
}

func buildDerivedSelect(op *Horizon, qb *queryBuilder, sel *sqlparser.Select) error {
	opQuery, ok := op.Query.(*sqlparser.Select)
	if !ok {
		return vterrors.VT12001("Horizon contained UNION but statement was SELECT")
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
		err := qb.addProjection(&sqlparser.AliasedExpr{Expr: col})
		if err != nil {
			return err
		}
	}
	return nil

}

func buildHorizon(op *Horizon, qb *queryBuilder) error {
	err := buildQuery(op.Source, qb)
	if err != nil {
		return err
	}

	err = stripDownQuery(op.Query, qb.asSelectStatement())
	if err != nil {
		return err
	}
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		if aliasedExpr, ok := node.(sqlparser.SelectExpr); ok {
			removeKeyspaceFromSelectExpr(aliasedExpr)
		}
		return true, nil
	}, qb.stmt)
	return nil
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
