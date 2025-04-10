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

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func ToAST(ctx *plancontext.PlanningContext, op Operator) (_ sqlparser.Statement, _ Operator, err error) {
	defer PanicHandler(&err)

	q := &queryBuilder{ctx: ctx}
	buildAST(op, q)
	if ctx.SemTable != nil {
		q.sortTables()
	}
	return q.stmt, q.dmlOperator, nil
}

// buildAST recursively builds the query into an AST, from an operator tree
func buildAST(op Operator, qb *queryBuilder) {
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
		buildDistinct(op, qb)
	case *Update:
		buildUpdate(op, qb)
	case *Delete:
		buildDelete(op, qb)
	case *Insert:
		buildDML(op, qb)
	case *RecurseCTE:
		buildRecursiveCTE(op, qb)
	case *BlockBuild:
		buildBlockBuild(op, qb)
	case *BlockJoin:
		buildBlockJoin(op, qb)
	default:
		panic(vterrors.VT13001(fmt.Sprintf("unknown operator to convert to SQL: %T", op)))
	}
}

func buildDistinct(op *Distinct, qb *queryBuilder) {
	buildAST(op.Source, qb)
	statement := qb.asSelectStatement()
	d, ok := statement.(sqlparser.Distinctable)
	if !ok {
		panic(vterrors.VT13001("expected a select statement with distinct"))
	}
	d.MakeDistinct()
}

func buildBlockJoin(op *BlockJoin, qb *queryBuilder) {
	qb.ctx.SkipBlockJoinArgument(op.Destination)
	buildAST(op.LHS, qb)
	qbR := &queryBuilder{ctx: qb.ctx}
	buildAST(op.RHS, qbR)
	qb.joinWith(qbR, nil, sqlparser.NormalJoinType)
}

func buildBlockBuild(op *BlockBuild, qb *queryBuilder) {
	buildAST(op.Source, qb)
	if qb.ctx.IsBlockJoinArgumentSkipped(op.Name) {
		return
	}

	expr := &sqlparser.DerivedTable{
		Select: &sqlparser.ValuesStatement{
			ListArg: sqlparser.NewListArg(op.Name),
		},
	}

	deps := semantics.EmptyTableSet()
	for _, ae := range qb.ctx.GetBlockJoinColumns(op.Name) {
		deps = deps.Merge(qb.ctx.SemTable.RecursiveDeps(ae.Expr))
	}

	qb.addTableExpr(op.Name, op.Name, TableID(op), expr, nil, op.getColumnNamesFromCtx(qb.ctx))
}

func buildDelete(op *Delete, qb *queryBuilder) {
	qb.stmt = &sqlparser.Delete{
		Ignore:  op.Ignore,
		Targets: sqlparser.TableNames{op.Target.Name},
	}
	buildAST(op.Source, qb)

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
	buildAST(op.Source, qb)
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
	buildAST(op.Source, qb)

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
	if op.WithRollup {
		qb.setWithRollup()
	}

	if op.DT != nil {
		sel := qb.asSelectStatement()
		qb.stmt = nil
		qb.addTableExpr(op.DT.Alias, op.DT.Alias, TableID(op), &sqlparser.DerivedTable{
			Select: sel,
		}, nil, op.DT.Columns)
	}
}

func buildOrdering(op *Ordering, qb *queryBuilder) {
	buildAST(op.Source, qb)

	for _, order := range op.Order {
		qb.asOrderAndLimit().AddOrder(order.Inner)
	}
}

func buildLimit(op *Limit, qb *queryBuilder) {
	buildAST(op.Source, qb)
	qb.asOrderAndLimit().SetLimit(op.AST)
}

func buildTable(op *Table, qb *queryBuilder) {
	if !qb.includeTable(op) {
		return
	}

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
	buildAST(op.Source, qb)

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
		for _, column := range op.GetSelectExprs(qb.ctx) {
			qb.addProjection(column)
		}
	}
}

func buildApplyJoin(op *ApplyJoin, qb *queryBuilder) {
	preds := slice.Map(op.JoinPredicates.columns, func(jc applyJoinColumn) sqlparser.Expr {
		if jc.JoinPredicateID != nil {
			qb.ctx.PredTracker.Skip(*jc.JoinPredicateID)
		}
		return jc.Original
	})
	pred := sqlparser.AndExpressions(preds...)

	buildAST(op.LHS, qb)

	qbR := &queryBuilder{ctx: qb.ctx}
	buildAST(op.RHS, qbR)

	switch {
	// if we have a recursive cte, we might be missing a statement from one of the sides
	case qbR.stmt == nil:
		// do nothing
	case qb.stmt == nil:
		qb.stmt = qbR.stmt
	default:
		qb.joinWith(qbR, pred, op.JoinType)
	}
}

func buildUnion(op *Union, qb *queryBuilder) {
	// the first input is built first
	buildAST(op.Sources[0], qb)

	for i, src := range op.Sources {
		if i == 0 {
			continue
		}

		// now we can go over the remaining inputs and UNION them together
		qbOther := &queryBuilder{ctx: qb.ctx}
		buildAST(src, qbOther)
		qb.unionWith(qbOther, op.distinct)
	}
}

func buildFilter(op *Filter, qb *queryBuilder) {
	buildAST(op.Source, qb)

	for _, pred := range op.Predicates {
		qb.addPredicate(pred)
	}
}

func buildDerived(op *Horizon, qb *queryBuilder) {
	buildAST(op.Source, qb)

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
	sel.Distinct = opQuery.Distinct
	qb.addTableExpr(op.Alias, op.Alias, TableID(op), &sqlparser.DerivedTable{
		Select: sel,
	}, nil, op.ColumnAliases)
	for _, col := range op.Columns {
		qb.addProjection(&sqlparser.AliasedExpr{Expr: col})
	}
}

func buildHorizon(op *Horizon, qb *queryBuilder) {
	buildAST(op.Source, qb)
	stripDownQuery(op.Query, qb.asSelectStatement())
	sqlparser.RemoveKeyspaceInCol(qb.stmt)
}

func buildRecursiveCTE(op *RecurseCTE, qb *queryBuilder) {
	predicates := slice.Map(op.Predicates, func(jc *plancontext.RecurseExpression) sqlparser.Expr {
		// since we are adding these join predicates, we need to mark to broken up version (RHSExpr) of it as done
		if jc.JoinPredicateID != nil {
			qb.ctx.PredTracker.Skip(*jc.JoinPredicateID)
		}
		return jc.Original
	})
	pred := sqlparser.AndExpressions(predicates...)
	buildAST(op.Seed(), qb)
	qbR := &queryBuilder{ctx: qb.ctx}
	buildAST(op.Term(), qbR)
	qbR.addPredicate(pred)
	infoFor, err := qb.ctx.SemTable.TableInfoFor(op.OuterID)
	if err != nil {
		panic(err)
	}

	qb.recursiveCteWith(qbR, op.Def.Name, infoFor.GetAliasedTableExpr().As.String(), op.Distinct, op.Def.Columns)
}

func stripDownQuery(from, to sqlparser.TableStatement) {
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
		for _, expr := range toNode.SelectExprs.Exprs {
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

func removeKeyspaceFromSelectExpr(expr sqlparser.SelectExpr) {
	switch expr := expr.(type) {
	case *sqlparser.AliasedExpr:
		sqlparser.RemoveKeyspaceInCol(expr.Expr)
	case *sqlparser.StarExpr:
		expr.TableName.Qualifier = sqlparser.NewIdentifierCS("")
	}
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
