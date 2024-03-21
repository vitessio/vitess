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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

const foreignKeyConstraintValues = "fkc_vals"
const foreignKeyUpdateExpr = "fkc_upd"

// translateQueryToOp creates an operator tree that represents the input SELECT or UNION query
func translateQueryToOp(ctx *plancontext.PlanningContext, selStmt sqlparser.Statement) Operator {
	switch node := selStmt.(type) {
	case *sqlparser.Select:
		return createOperatorFromSelect(ctx, node)
	case *sqlparser.Union:
		return createOperatorFromUnion(ctx, node)
	case *sqlparser.Update:
		return createOperatorFromUpdate(ctx, node)
	case *sqlparser.Delete:
		return createOperatorFromDelete(ctx, node)
	case *sqlparser.Insert:
		return createOperatorFromInsert(ctx, node)
	default:
		panic(vterrors.VT12001(fmt.Sprintf("operator: %T", selStmt)))
	}
}

func createOperatorFromSelect(ctx *plancontext.PlanningContext, sel *sqlparser.Select) Operator {
	op := crossJoin(ctx, sel.From)

	if sel.Where != nil {
		op = addWherePredicates(ctx, sel.Where.Expr, op)
	}

	if sel.Comments != nil || sel.Lock != sqlparser.NoLock {
		op = &LockAndComment{
			Source:   op,
			Comments: sel.Comments,
			Lock:     sel.Lock,
		}
	}

	op = newHorizon(op, sel)

	return op
}

func addWherePredicates(ctx *plancontext.PlanningContext, expr sqlparser.Expr, op Operator) Operator {
	sqc := &SubQueryBuilder{}
	op = addWherePredsToSubQueryBuilder(ctx, expr, op, sqc)
	return sqc.getRootOperator(op, nil)
}

func addWherePredsToSubQueryBuilder(ctx *plancontext.PlanningContext, expr sqlparser.Expr, op Operator, sqc *SubQueryBuilder) Operator {
	outerID := TableID(op)
	exprs := sqlparser.SplitAndExpression(nil, expr)
	for _, expr := range exprs {
		sqlparser.RemoveKeyspaceInCol(expr)
		subq := sqc.handleSubquery(ctx, expr, outerID)
		if subq != nil {
			continue
		}
		op = op.AddPredicate(ctx, expr)
		addColumnEquality(ctx, expr)
	}
	return op
}

// cloneASTAndSemState clones the AST and the semantic state of the input node.
func cloneASTAndSemState[T sqlparser.SQLNode](ctx *plancontext.PlanningContext, original T) T {
	return sqlparser.CopyOnRewrite(original, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
		e, ok := cursor.Node().(sqlparser.Expr)
		if !ok {
			return
		}
		cursor.Replace(e) // We do this only to trigger the cloning of the AST
	}, ctx.SemTable.CopySemanticInfo).(T)
}

// findTablesContained returns the TableSet of all the contained
func findTablesContained(ctx *plancontext.PlanningContext, node sqlparser.SQLNode) (result semantics.TableSet) {
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		t, ok := node.(*sqlparser.AliasedTableExpr)
		if !ok {
			return true, nil
		}
		ts := ctx.SemTable.TableSetFor(t)
		result = result.Merge(ts)
		return true, nil
	}, node)
	return
}

// joinPredicateCollector is used to inspect the predicates inside the subquery, looking for any
// comparisons between the inner and the outer side.
// They can be used for merging the two parts of the query together
type joinPredicateCollector struct {
	predicates          sqlparser.Exprs
	remainingPredicates sqlparser.Exprs
	joinColumns         []applyJoinColumn

	totalID,
	subqID,
	outerID semantics.TableSet
}

func (jpc *joinPredicateCollector) inspectPredicate(
	ctx *plancontext.PlanningContext,
	predicate sqlparser.Expr,
) {
	pred := predicate
	deps := ctx.SemTable.RecursiveDeps(predicate)
	// if the subquery is not enough, but together we have all we need,
	// then we can use this predicate to connect the subquery to the outer query
	if !deps.IsSolvedBy(jpc.subqID) && deps.IsSolvedBy(jpc.totalID) {
		jpc.predicates = append(jpc.predicates, predicate)
		jc := breakExpressionInLHSandRHSForApplyJoin(ctx, predicate, jpc.outerID)
		jpc.joinColumns = append(jpc.joinColumns, jc)
		pred = jc.RHSExpr
	}
	jpc.remainingPredicates = append(jpc.remainingPredicates, pred)
}

func createOperatorFromUnion(ctx *plancontext.PlanningContext, node *sqlparser.Union) Operator {
	_, isRHSUnion := node.Right.(*sqlparser.Union)
	if isRHSUnion {
		panic(vterrors.VT12001("nesting of UNIONs on the right-hand side"))
	}
	opLHS := translateQueryToOp(ctx, node.Left)
	opRHS := translateQueryToOp(ctx, node.Right)
	lexprs := ctx.SemTable.SelectExprs(node.Left)
	rexprs := ctx.SemTable.SelectExprs(node.Right)

	unionCols := ctx.SemTable.SelectExprs(node)
	union := newUnion([]Operator{opLHS, opRHS}, []sqlparser.SelectExprs{lexprs, rexprs}, unionCols, node.Distinct)
	return newHorizon(union, node)
}

// createOpFromStmt creates an operator from the given statement. It takes in two additional arguments—
//  1. verifyAllFKs: For this given statement, do we need to verify validity of all the foreign keys on the vtgate level.
//  2. fkToIgnore: The foreign key constraint to specifically ignore while planning the statement. This field is used in UPDATE CASCADE planning, wherein while planning the child update
//     query, we need to ignore the parent foreign key constraint that caused the cascade in question.
func createOpFromStmt(inCtx *plancontext.PlanningContext, stmt sqlparser.Statement, verifyAllFKs bool, fkToIgnore string) Operator {
	ctx, err := plancontext.CreatePlanningContext(stmt, inCtx.ReservedVars, inCtx.VSchema, inCtx.PlannerVersion)
	if err != nil {
		panic(err)
	}

	// TODO (@GuptaManan100, @harshit-gangal): When we add cross-shard foreign keys support,
	// we should augment the semantic analysis to also tell us whether the given query has any cross shard parent foreign keys to validate.
	// If there are, then we have to run the query with FOREIGN_KEY_CHECKS off because we can't be sure if the DML will succeed on MySQL with the checks on.
	// So, we should set VerifyAllFKs to true. i.e. we should add `|| ctx.SemTable.RequireForeignKeyChecksOff()` to the below condition.
	if verifyAllFKs {
		// If ctx.VerifyAllFKs is already true we don't want to turn it off.
		ctx.VerifyAllFKs = verifyAllFKs
	}

	// From all the parent foreign keys involved, we should remove the one that we need to ignore.
	err = ctx.SemTable.RemoveParentForeignKey(fkToIgnore)
	if err != nil {
		panic(err)
	}

	// Now, we can filter the foreign keys further based on the planning context, specifically whether we are running
	// this query with FOREIGN_KEY_CHECKS off or not. If the foreign key checks are enabled, then we don't need to verify
	// the validity of shard-scoped RESTRICT foreign keys, since MySQL will do that for us. Similarly, we don't need to verify
	// if the shard-scoped parent foreign key constraints are valid.
	switch stmt.(type) {
	case *sqlparser.Update, *sqlparser.Insert:
		err = ctx.SemTable.RemoveNonRequiredForeignKeys(ctx.VerifyAllFKs, vindexes.UpdateAction)
	case *sqlparser.Delete:
		err = ctx.SemTable.RemoveNonRequiredForeignKeys(ctx.VerifyAllFKs, vindexes.DeleteAction)
	}
	if err != nil {
		panic(err)
	}

	op, err := PlanQuery(ctx, stmt)
	if err != nil {
		panic(err)
	}

	// need to remember which predicates have been broken up during join planning
	inCtx.KeepPredicateInfo(ctx)

	return op
}

func getOperatorFromTableExpr(ctx *plancontext.PlanningContext, tableExpr sqlparser.TableExpr, onlyTable bool) Operator {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		return getOperatorFromAliasedTableExpr(ctx, tableExpr, onlyTable)
	case *sqlparser.JoinTableExpr:
		return getOperatorFromJoinTableExpr(ctx, tableExpr)
	case *sqlparser.ParenTableExpr:
		return crossJoin(ctx, tableExpr.Exprs)
	default:
		panic(vterrors.VT13001(fmt.Sprintf("unable to use: %T table type", tableExpr)))
	}
}

func getOperatorFromJoinTableExpr(ctx *plancontext.PlanningContext, tableExpr *sqlparser.JoinTableExpr) Operator {
	lhs := getOperatorFromTableExpr(ctx, tableExpr.LeftExpr, false)
	rhs := getOperatorFromTableExpr(ctx, tableExpr.RightExpr, false)

	switch tableExpr.Join {
	case sqlparser.NormalJoinType:
		return createInnerJoin(ctx, tableExpr, lhs, rhs)
	case sqlparser.LeftJoinType, sqlparser.RightJoinType:
		return createLeftOuterJoin(ctx, tableExpr, lhs, rhs)
	case sqlparser.StraightJoinType:
		return createStraightJoin(ctx, tableExpr, lhs, rhs)
	default:
		panic(vterrors.VT13001("unsupported: %s", tableExpr.Join.ToString()))
	}
}

func getOperatorFromAliasedTableExpr(ctx *plancontext.PlanningContext, tableExpr *sqlparser.AliasedTableExpr, onlyTable bool) Operator {
	tableID := ctx.SemTable.TableSetFor(tableExpr)
	switch tbl := tableExpr.Expr.(type) {
	case sqlparser.TableName:
		tableInfo, err := ctx.SemTable.TableInfoFor(tableID)
		if err != nil {
			panic(err)
		}

		if vt, isVindex := tableInfo.(*semantics.VindexTable); isVindex {
			solves := tableID
			return &Vindex{
				Table: VindexTable{
					TableID: tableID,
					Alias:   tableExpr,
					Table:   tbl,
					VTable:  vt.Table.GetVindexTable(),
				},
				Vindex: vt.Vindex,
				Solved: solves,
			}
		}
		qg := newQueryGraph()
		isInfSchema := tableInfo.IsInfSchema()
		qt := &QueryTable{Alias: tableExpr, Table: tbl, ID: tableID, IsInfSchema: isInfSchema}
		qg.Tables = append(qg.Tables, qt)
		return qg
	case *sqlparser.DerivedTable:
		if onlyTable && tbl.Select.GetLimit() == nil {
			tbl.Select.SetOrderBy(nil)
		}

		inner := translateQueryToOp(ctx, tbl.Select)
		if horizon, ok := inner.(*Horizon); ok {
			horizon.TableId = &tableID
			horizon.Alias = tableExpr.As.String()
			horizon.ColumnAliases = tableExpr.Columns
			qp := CreateQPFromSelectStatement(ctx, tbl.Select)
			horizon.QP = qp
		}

		return inner
	default:
		panic(vterrors.VT13001(fmt.Sprintf("unable to use: %T", tbl)))
	}
}

func crossJoin(ctx *plancontext.PlanningContext, exprs sqlparser.TableExprs) Operator {
	var output Operator
	for _, tableExpr := range exprs {
		op := getOperatorFromTableExpr(ctx, tableExpr, len(exprs) == 1)
		if output == nil {
			output = op
		} else {
			output = createJoin(ctx, output, op)
		}
	}
	return output
}

func createQueryTableForDML(
	ctx *plancontext.PlanningContext,
	tableExpr sqlparser.TableExpr,
	whereClause *sqlparser.Where,
) (semantics.TableInfo, *QueryTable) {
	alTbl, ok := tableExpr.(*sqlparser.AliasedTableExpr)
	if !ok {
		panic(vterrors.VT13001("expected AliasedTableExpr"))
	}
	tblName, ok := alTbl.Expr.(sqlparser.TableName)
	if !ok {
		panic(vterrors.VT13001("expected TableName"))
	}

	tableID := ctx.SemTable.TableSetFor(alTbl)
	tableInfo, err := ctx.SemTable.TableInfoFor(tableID)
	if err != nil {
		panic(err)
	}

	if tableInfo.IsInfSchema() {
		panic(vterrors.VT12001("update information schema tables"))
	}

	var predicates []sqlparser.Expr
	if whereClause != nil {
		predicates = sqlparser.SplitAndExpression(nil, whereClause.Expr)
	}
	qt := &QueryTable{
		ID:         tableID,
		Alias:      alTbl,
		Table:      tblName,
		Predicates: predicates,
	}
	return tableInfo, qt
}

func addColumnEquality(ctx *plancontext.PlanningContext, expr sqlparser.Expr) {
	switch expr := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if expr.Operator != sqlparser.EqualOp {
			return
		}

		if left, isCol := expr.Left.(*sqlparser.ColName); isCol {
			ctx.SemTable.AddColumnEquality(left, expr.Right)
		}
		if right, isCol := expr.Right.(*sqlparser.ColName); isCol {
			ctx.SemTable.AddColumnEquality(right, expr.Left)
		}
	}
}

// createSelectionOp creates the selection operator to select the parent columns for the foreign key constraints.
// The Select statement looks something like this - `SELECT <parent_columns_in_fk for all the foreign key constraints> FROM <parent_table> WHERE <where_clause_of_update>`
// TODO (@Harshit, @GuptaManan100): Compress the columns in the SELECT statement, if there are multiple foreign key constraints using the same columns.
func createSelectionOp(
	ctx *plancontext.PlanningContext,
	selectExprs []sqlparser.SelectExpr,
	tableExprs sqlparser.TableExprs,
	where *sqlparser.Where,
	orderBy sqlparser.OrderBy,
	limit *sqlparser.Limit,
	lock sqlparser.Lock,
) Operator {
	selectionStmt := &sqlparser.Select{
		SelectExprs: selectExprs,
		From:        tableExprs,
		Where:       where,
		OrderBy:     orderBy,
		Limit:       limit,
		Lock:        lock,
	}
	// There are no foreign keys to check for a select query, so we can pass anything for verifyAllFKs and fkToIgnore.
	return createOpFromStmt(ctx, selectionStmt, false /* verifyAllFKs */, "" /* fkToIgnore */)
}
