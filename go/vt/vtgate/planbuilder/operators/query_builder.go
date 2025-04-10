/*
Copyright 2025 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/predicates"
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

func (qb *queryBuilder) asSelectStatement() sqlparser.TableStatement {
	return qb.stmt.(sqlparser.TableStatement)

}
func (qb *queryBuilder) asOrderAndLimit() sqlparser.OrderAndLimit {
	return qb.stmt.(sqlparser.OrderAndLimit)
}

// includeTable will return false if the table is a CTE, and it is not merged
// it will return true if the table is not a CTE or if it is a CTE and it is merged
func (qb *queryBuilder) includeTable(op *Table) bool {
	if qb.ctx.SemTable == nil {
		return true
	}
	tbl, err := qb.ctx.SemTable.TableInfoFor(op.QTable.ID)
	if err != nil {
		panic(err)
	}
	cteTbl, isCTE := tbl.(*semantics.CTETable)
	if !isCTE {
		return true
	}

	return cteTbl.Merged
}

func (qb *queryBuilder) addTable(db, tableName, alias string, tableID semantics.TableSet, hints sqlparser.IndexHints) {
	if tableID.NumberOfTables() == 1 && qb.ctx.SemTable != nil {
		tblInfo, err := qb.ctx.SemTable.TableInfoFor(tableID)
		if err != nil {
			panic(err)
		}
		cte, isCTE := tblInfo.(*semantics.CTETable)
		if isCTE {
			tableName = cte.TableName
			db = ""
		}
	}
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
	if jp, ok := expr.(*predicates.JoinPredicate); ok {
		// we have to strip out the join predicate containers,
		// otherwise precedence calculations get messed up
		expr = jp.Current()
	}
	if expr == nil {
		return
	}

	var addPred func(sqlparser.Expr)

	switch stmt := qb.stmt.(type) {
	case *sqlparser.Select:
		if qb.ctx.ContainsAggr(expr) {
			addPred = stmt.AddHaving
		} else {
			addPred = stmt.AddWhere
		}
	case *sqlparser.Update:
		addPred = stmt.AddWhere
	case *sqlparser.Delete:
		addPred = stmt.AddWhere
	case nil:
		// this would happen if we are adding a predicate on a dual query.
		// we use this when building recursive CTE queries
		sel := &sqlparser.Select{}
		addPred = sel.AddWhere
		qb.stmt = sel
	default:
		panic(fmt.Sprintf("cant add WHERE to %T", qb.stmt))
	}

	for _, exp := range sqlparser.SplitAndExpression(nil, expr) {
		addPred(exp)
	}
}

func (qb *queryBuilder) addGroupBy(original sqlparser.Expr) {
	sel := qb.stmt.(*sqlparser.Select)
	sel.AddGroupBy(original)
}

func (qb *queryBuilder) setWithRollup() {
	sel := qb.stmt.(*sqlparser.Select)
	sel.GroupBy.WithRollup = true
}

func (qb *queryBuilder) addProjection(projection sqlparser.SelectExpr) {
	stripJoinPredicate(projection)
	switch stmt := qb.stmt.(type) {
	case *sqlparser.Select:
		stmt.AddSelectExpr(projection)
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

func stripJoinPredicate(projection sqlparser.SelectExpr) {
	ae, ok := projection.(*sqlparser.AliasedExpr)
	if ok {
		jp, ok := ae.Expr.(*predicates.JoinPredicate)
		if ok {
			ae.Expr = jp.Current()
		}
	}
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
	firstSelect := getFirstSelect(selStmt)
	sel.SetSelectExprs(unionSelects(firstSelect.GetColumns())...)
	qb.stmt = sel
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

func (qb *queryBuilder) recursiveCteWith(other *queryBuilder, name, alias string, distinct bool, columns sqlparser.Columns) {
	cteUnion := &sqlparser.Union{
		Left:     qb.stmt.(sqlparser.TableStatement),
		Right:    other.stmt.(sqlparser.TableStatement),
		Distinct: distinct,
	}

	qb.stmt = &sqlparser.Select{
		With: &sqlparser.With{
			Recursive: true,
			CTEs: []*sqlparser.CommonTableExpr{{
				ID:       sqlparser.NewIdentifierCS(name),
				Columns:  columns,
				Subquery: cteUnion,
			}},
		},
	}

	qb.addTable("", name, alias, "", nil)
}

func (qb *queryBuilder) joinWith(other *queryBuilder, onCondition sqlparser.Expr, joinType sqlparser.JoinType) {
	stmt := qb.stmt.(FromStatement)
	otherStmt := other.stmt.(FromStatement)

	if sel, isSel := stmt.(*sqlparser.Select); isSel {
		otherSel := otherStmt.(*sqlparser.Select)
		for _, expr := range otherSel.GetColumns() {
			sel.AddSelectExpr(expr)
		}
	}

	qb.mergeWhereClauses(stmt, otherStmt)

	var newFromClause []sqlparser.TableExpr
	switch joinType {
	case sqlparser.NormalJoinType:
		newFromClause = append(stmt.GetFrom(), otherStmt.GetFrom()...)
		for _, pred := range sqlparser.SplitAndExpression(nil, onCondition) {
			qb.addPredicate(pred)
		}
	default:
		newFromClause = []sqlparser.TableExpr{buildJoin(stmt, otherStmt, onCondition, joinType)}
	}

	stmt.SetFrom(newFromClause)
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

func unionSelects(exprs []sqlparser.SelectExpr) []sqlparser.SelectExpr {
	var selectExprs []sqlparser.SelectExpr
	for _, col := range exprs {
		switch col := col.(type) {
		case *sqlparser.AliasedExpr:
			expr := sqlparser.NewColName(col.ColumnName())
			selectExprs = append(selectExprs, &sqlparser.AliasedExpr{Expr: expr})
		default:
			selectExprs = append(selectExprs, col)
		}
	}
	return selectExprs
}

func checkUnionColumnByName(column *sqlparser.ColName, sel sqlparser.TableStatement) {
	colName := column.Name.String()
	firstSelect := getFirstSelect(sel)
	exprs := firstSelect.GetColumns()
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

type FromStatement interface {
	GetFrom() []sqlparser.TableExpr
	SetFrom([]sqlparser.TableExpr)
	GetWherePredicate() sqlparser.Expr
	SetWherePredicate(sqlparser.Expr)
}

var _ FromStatement = (*sqlparser.Select)(nil)
var _ FromStatement = (*sqlparser.Update)(nil)
var _ FromStatement = (*sqlparser.Delete)(nil)
