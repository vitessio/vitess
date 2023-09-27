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
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

const foreignKeyConstraintValues = "fkc_vals"

// translateQueryToOp creates an operator tree that represents the input SELECT or UNION query
func translateQueryToOp(ctx *plancontext.PlanningContext, selStmt sqlparser.Statement) (op ops.Operator, err error) {
	switch node := selStmt.(type) {
	case *sqlparser.Select:
		op, err = createOperatorFromSelect(ctx, node)
	case *sqlparser.Union:
		op, err = createOperatorFromUnion(ctx, node)
	case *sqlparser.Update:
		op, err = createOperatorFromUpdate(ctx, node)
	case *sqlparser.Delete:
		op, err = createOperatorFromDelete(ctx, node)
	case *sqlparser.Insert:
		op, err = createOperatorFromInsert(ctx, node)
	default:
		err = vterrors.VT12001(fmt.Sprintf("operator: %T", selStmt))
	}
	if err != nil {
		return nil, err
	}

	return op, nil
}

func createOperatorFromSelect(ctx *plancontext.PlanningContext, sel *sqlparser.Select) (ops.Operator, error) {
	op, err := crossJoin(ctx, sel.From)
	if err != nil {
		return nil, err
	}

	if sel.Where != nil {
		op, err = addWherePredicates(ctx, sel.Where.Expr, op)
		if err != nil {
			return nil, err
		}
	}

	if sel.Comments != nil || sel.Lock != sqlparser.NoLock {
		op = &LockAndComment{
			Source:   op,
			Comments: sel.Comments,
			Lock:     sel.Lock,
		}
	}

	op = newHorizon(op, sel)

	return op, nil
}

func addWherePredicates(ctx *plancontext.PlanningContext, expr sqlparser.Expr, op ops.Operator) (ops.Operator, error) {
	sqc := &SubQueryContainer{}
	outerID := TableID(op)
	exprs := sqlparser.SplitAndExpression(nil, expr)
	for _, expr := range exprs {
		sqlparser.RemoveKeyspaceFromColName(expr)
		subq, err := sqc.handleSubquery(ctx, expr, outerID)
		if err != nil {
			return nil, err
		}
		if subq != nil {
			continue
		}
		op, err = op.AddPredicate(ctx, expr)
		if err != nil {
			return nil, err
		}
		addColumnEquality(ctx, expr)
	}
	return sqc.getRootOperator(op), nil
}

func (sqc *SubQueryContainer) handleSubquery(
	ctx *plancontext.PlanningContext,
	expr sqlparser.Expr,
	outerID semantics.TableSet,
) (*SubQuery, error) {
	subq, parentExpr := getSubQuery(expr)
	if subq == nil {
		return nil, nil
	}
	argName := ctx.GetReservedArgumentFor(subq)
	sqInner, err := createSubqueryOp(ctx, parentExpr, expr, subq, outerID, argName)
	if err != nil {
		return nil, err
	}
	sqc.Inner = append(sqc.Inner, sqInner)

	return sqInner, nil
}

func (sqc *SubQueryContainer) getRootOperator(op ops.Operator) ops.Operator {
	if len(sqc.Inner) == 0 {
		return op
	}

	sqc.Outer = op
	return sqc
}

func getSubQuery(expr sqlparser.Expr) (subqueryExprExists *sqlparser.Subquery, parentExpr sqlparser.Expr) {
	flipped := false
	_ = sqlparser.Rewrite(expr, func(cursor *sqlparser.Cursor) bool {
		if subq, ok := cursor.Node().(*sqlparser.Subquery); ok {
			subqueryExprExists = subq
			parentExpr = subq
			if expr, ok := cursor.Parent().(sqlparser.Expr); ok {
				parentExpr = expr
			}
			flipped = true
			return false
		}
		return true
	}, func(cursor *sqlparser.Cursor) bool {
		if !flipped {
			return true
		}
		if not, isNot := cursor.Parent().(*sqlparser.NotExpr); isNot {
			parentExpr = not
		}
		return false
	})
	return
}

func createSubqueryOp(
	ctx *plancontext.PlanningContext,
	parent, original sqlparser.Expr,
	subq *sqlparser.Subquery,
	outerID semantics.TableSet,
	name string,
) (*SubQuery, error) {
	switch parent := parent.(type) {
	case *sqlparser.NotExpr:
		switch parent.Expr.(type) {
		case *sqlparser.ExistsExpr:
			return createSubquery(ctx, original, subq, outerID, parent, name, opcode.PulloutNotExists, false)
		case *sqlparser.ComparisonExpr:
			panic("should have been rewritten")
		}
	case *sqlparser.ExistsExpr:
		return createSubquery(ctx, original, subq, outerID, parent, name, opcode.PulloutExists, false)
	case *sqlparser.ComparisonExpr:
		return createComparisonSubQuery(ctx, parent, original, subq, outerID, name)
	}
	return createSubquery(ctx, original, subq, outerID, parent, name, opcode.PulloutValue, false)
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
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
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

// inspectSelect goes through all the predicates contained in the SELECT query
// and extracts subqueries into operators, and rewrites the original query to use
// arguments instead of subqueries.
func (sqc *SubQueryContainer) inspectSelect(
	ctx *plancontext.PlanningContext,
	sel *sqlparser.Select,
) (sqlparser.Exprs, []JoinColumn, error) {
	// first we need to go through all the places where one can find predicates
	// and search for subqueries
	newWhere, wherePreds, whereJoinCols, err := sqc.inspectWhere(ctx, sel.Where)
	if err != nil {
		return nil, nil, err
	}
	newHaving, havingPreds, havingJoinCols, err := sqc.inspectWhere(ctx, sel.Having)
	if err != nil {
		return nil, nil, err
	}

	newFrom, onPreds, onJoinCols, err := sqc.inspectOnExpr(ctx, sel.From)
	if err != nil {
		return nil, nil, err
	}

	// then we use the updated AST structs to build the operator
	// these AST elements have any subqueries replace by arguments
	sel.Where = newWhere
	sel.Having = newHaving
	sel.From = newFrom

	return append(append(wherePreds, havingPreds...), onPreds...),
		append(append(whereJoinCols, havingJoinCols...), onJoinCols...),
		nil
}

// inspectStatement goes through all the predicates contained in the AST
// and extracts subqueries into operators
func (sqc *SubQueryContainer) inspectStatement(ctx *plancontext.PlanningContext,
	stmt sqlparser.SelectStatement,
) (sqlparser.Exprs, []JoinColumn, error) {
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		return sqc.inspectSelect(ctx, stmt)
	case *sqlparser.Union:
		exprs1, cols1, err := sqc.inspectStatement(ctx, stmt.Left)
		if err != nil {
			return nil, nil, err
		}
		exprs2, cols2, err := sqc.inspectStatement(ctx, stmt.Right)
		if err != nil {
			return nil, nil, err
		}
		return append(exprs1, exprs2...), append(cols1, cols2...), nil
	}
	panic("unknown type")
}

func createSubquery(
	ctx *plancontext.PlanningContext,
	original sqlparser.Expr,
	subq *sqlparser.Subquery,
	outerID semantics.TableSet,
	parent sqlparser.Expr,
	argName string,
	filterType opcode.PulloutOpcode,
	isProjection bool,
) (*SubQuery, error) {
	topLevel := ctx.SemTable.EqualsExpr(original, parent)
	original = cloneASTAndSemState(ctx, original)
	originalSq := cloneASTAndSemState(ctx, subq)
	subqID := findTablesContained(ctx, subq.Select)
	totalID := subqID.Merge(outerID)
	sqc := &SubQueryContainer{totalID: totalID, subqID: subqID, outerID: outerID}

	predicates, joinCols, err := sqc.inspectStatement(ctx, subq.Select)
	if err != nil {
		return nil, err
	}

	stmt := rewriteRemainingColumns(ctx, subq.Select, subqID)

	// TODO: this should not be needed. We are using CopyOnRewrite above, but somehow this is not getting copied
	ctx.SemTable.CopySemanticInfo(subq.Select, stmt)

	opInner, err := translateQueryToOp(ctx, stmt)
	if err != nil {
		return nil, err
	}

	opInner = sqc.getRootOperator(opInner)
	return &SubQuery{
		FilterType:       filterType,
		Subquery:         opInner,
		Predicates:       predicates,
		Original:         original,
		ArgName:          argName,
		originalSubquery: originalSq,
		IsProjection:     isProjection,
		TopLevel:         topLevel,
		JoinColumns:      joinCols,
	}, nil
}

func rewriteRemainingColumns(
	ctx *plancontext.PlanningContext,
	stmt sqlparser.SelectStatement,
	subqID semantics.TableSet,
) sqlparser.SelectStatement {
	return sqlparser.CopyOnRewrite(stmt, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
		colname, isColname := cursor.Node().(*sqlparser.ColName)
		if !isColname {
			return
		}
		deps := ctx.SemTable.RecursiveDeps(colname)
		if deps.IsSolvedBy(subqID) {
			return
		}
		rsv := ctx.GetReservedArgumentFor(colname)
		cursor.Replace(sqlparser.NewArgument(rsv))
	}, nil).(sqlparser.SelectStatement)
}

func (sqc *SubQueryContainer) inspectWhere(
	ctx *plancontext.PlanningContext,
	in *sqlparser.Where,
) (*sqlparser.Where, sqlparser.Exprs, []JoinColumn, error) {
	if in == nil {
		return nil, nil, nil, nil
	}
	jpc := &joinPredicateCollector{
		totalID: sqc.totalID,
		subqID:  sqc.subqID,
		outerID: sqc.outerID,
	}
	for _, predicate := range sqlparser.SplitAndExpression(nil, in.Expr) {
		sqlparser.RemoveKeyspaceFromColName(predicate)
		subq, err := sqc.handleSubquery(ctx, predicate, sqc.totalID)
		if err != nil {
			return nil, nil, nil, err
		}
		if subq != nil {
			continue
		}
		if err = jpc.inspectPredicate(ctx, predicate); err != nil {
			return nil, nil, nil, err
		}
	}

	if len(jpc.remainingPredicates) == 0 {
		in = nil
	} else {
		in.Expr = sqlparser.AndExpressions(jpc.remainingPredicates...)
	}

	return in, jpc.predicates, jpc.joinColumns, nil
}

func (sqc *SubQueryContainer) inspectOnExpr(
	ctx *plancontext.PlanningContext,
	from []sqlparser.TableExpr,
) (newFrom []sqlparser.TableExpr, onPreds sqlparser.Exprs, onJoinCols []JoinColumn, err error) {
	for _, tbl := range from {
		tbl := sqlparser.CopyOnRewrite(tbl, dontEnterSubqueries, func(cursor *sqlparser.CopyOnWriteCursor) {
			cond, ok := cursor.Node().(*sqlparser.JoinCondition)
			if !ok || cond.On == nil {
				return
			}

			jpc := &joinPredicateCollector{
				totalID: sqc.totalID,
				subqID:  sqc.subqID,
				outerID: sqc.outerID,
			}

			for _, pred := range sqlparser.SplitAndExpression(nil, cond.On) {
				subq, innerErr := sqc.handleSubquery(ctx, pred, sqc.totalID)
				if err != nil {
					err = innerErr
					cursor.StopTreeWalk()
					return
				}
				if subq != nil {
					continue
				}
				if err = jpc.inspectPredicate(ctx, pred); err != nil {
					err = innerErr
					cursor.StopTreeWalk()
					return
				}
			}
			if len(jpc.remainingPredicates) == 0 {
				cond.On = nil
			} else {
				cond.On = sqlparser.AndExpressions(jpc.remainingPredicates...)
			}
			onPreds = append(onPreds, jpc.predicates...)
			onJoinCols = append(onJoinCols, jpc.joinColumns...)
		}, ctx.SemTable.CopySemanticInfo)
		if err != nil {
			return
		}
		newFrom = append(newFrom, tbl.(sqlparser.TableExpr))
	}
	return
}

func createComparisonSubQuery(
	ctx *plancontext.PlanningContext,
	parent *sqlparser.ComparisonExpr,
	original sqlparser.Expr,
	subFromOutside *sqlparser.Subquery,
	outerID semantics.TableSet,
	name string,
) (*SubQuery, error) {
	subq, outside := semantics.GetSubqueryAndOtherSide(parent)
	if outside == nil || subq != subFromOutside {
		panic("uh oh")
	}

	filterType := opcode.PulloutValue
	switch parent.Operator {
	case sqlparser.InOp:
		filterType = opcode.PulloutIn
	case sqlparser.NotInOp:
		filterType = opcode.PulloutNotIn
	}

	subquery, err := createSubquery(ctx, original, subq, outerID, parent, name, filterType, false)
	if err != nil {
		return nil, err
	}

	// if we are comparing with a column from the inner subquery,
	// we add this extra predicate to check if the two sides are mergable or not
	if ae, ok := subq.Select.GetColumns()[0].(*sqlparser.AliasedExpr); ok {
		subquery.OuterPredicate = &sqlparser.ComparisonExpr{
			Operator: sqlparser.EqualOp,
			Left:     outside,
			Right:    ae.Expr,
		}
	}

	return subquery, err
}

// joinPredicateCollector is used to inspect the predicates inside the subquery, looking for any
// comparisons between the inner and the outer side.
// They can be used for merging the two parts of the query together
type joinPredicateCollector struct {
	predicates          sqlparser.Exprs
	remainingPredicates sqlparser.Exprs
	joinColumns         []JoinColumn

	totalID,
	subqID,
	outerID semantics.TableSet
}

func (jpc *joinPredicateCollector) inspectPredicate(
	ctx *plancontext.PlanningContext,
	predicate sqlparser.Expr,
) error {
	pred := predicate
	deps := ctx.SemTable.RecursiveDeps(predicate)
	// if the subquery is not enough, but together we have all we need,
	// then we can use this predicate to connect the subquery to the outer query
	if !deps.IsSolvedBy(jpc.subqID) && deps.IsSolvedBy(jpc.totalID) {
		jpc.predicates = append(jpc.predicates, predicate)
		jc, err := BreakExpressionInLHSandRHS(ctx, predicate, jpc.outerID)
		if err != nil {
			return err
		}
		jpc.joinColumns = append(jpc.joinColumns, jc)
		pred = jc.RHSExpr
	}
	jpc.remainingPredicates = append(jpc.remainingPredicates, pred)
	return nil
}

func createOperatorFromUnion(ctx *plancontext.PlanningContext, node *sqlparser.Union) (ops.Operator, error) {
	opLHS, err := translateQueryToOp(ctx, node.Left)
	if err != nil {
		return nil, err
	}

	_, isRHSUnion := node.Right.(*sqlparser.Union)
	if isRHSUnion {
		return nil, vterrors.VT12001("nesting of UNIONs on the right-hand side")
	}
	opRHS, err := translateQueryToOp(ctx, node.Right)
	if err != nil {
		return nil, err
	}

	lexprs := ctx.SemTable.SelectExprs(node.Left)
	rexprs := ctx.SemTable.SelectExprs(node.Right)

	unionCols := ctx.SemTable.SelectExprs(node)
	union := newUnion([]ops.Operator{opLHS, opRHS}, []sqlparser.SelectExprs{lexprs, rexprs}, unionCols, node.Distinct)
	return newHorizon(union, node), nil
}

// createOpFromStmt creates an operator from the given statement. It takes in two additional arguments—
// 1. verifyAllFKs: For this given statement, do we need to verify validity of all the foreign keys on the vtgate level.
// 2. fkToIgnore: The foreign key constraint to specifically ignore while planning the statement.
func createOpFromStmt(ctx *plancontext.PlanningContext, stmt sqlparser.Statement, verifyAllFKs bool, fkToIgnore string) (ops.Operator, error) {
	newCtx, err := plancontext.CreatePlanningContext(stmt, ctx.ReservedVars, ctx.VSchema, ctx.PlannerVersion)
	if err != nil {
		return nil, err
	}

	newCtx.VerifyAllFKs = verifyAllFKs
	newCtx.ParentFKToIgnore = fkToIgnore

	return PlanQuery(newCtx, stmt)
}

func getOperatorFromTableExpr(ctx *plancontext.PlanningContext, tableExpr sqlparser.TableExpr, onlyTable bool) (ops.Operator, error) {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		return getOperatorFromAliasedTableExpr(ctx, tableExpr, onlyTable)
	case *sqlparser.JoinTableExpr:
		return getOperatorFromJoinTableExpr(ctx, tableExpr)
	case *sqlparser.ParenTableExpr:
		return crossJoin(ctx, tableExpr.Exprs)
	default:
		return nil, vterrors.VT13001(fmt.Sprintf("unable to use: %T table type", tableExpr))
	}
}

func getOperatorFromJoinTableExpr(ctx *plancontext.PlanningContext, tableExpr *sqlparser.JoinTableExpr) (ops.Operator, error) {
	lhs, err := getOperatorFromTableExpr(ctx, tableExpr.LeftExpr, false)
	if err != nil {
		return nil, err
	}
	rhs, err := getOperatorFromTableExpr(ctx, tableExpr.RightExpr, false)
	if err != nil {
		return nil, err
	}

	switch tableExpr.Join {
	case sqlparser.NormalJoinType:
		return createInnerJoin(ctx, tableExpr, lhs, rhs)
	case sqlparser.LeftJoinType, sqlparser.RightJoinType:
		return createOuterJoin(tableExpr, lhs, rhs)
	default:
		return nil, vterrors.VT13001("unsupported: %s", tableExpr.Join.ToString())
	}
}

func getOperatorFromAliasedTableExpr(ctx *plancontext.PlanningContext, tableExpr *sqlparser.AliasedTableExpr, onlyTable bool) (ops.Operator, error) {
	tableID := ctx.SemTable.TableSetFor(tableExpr)
	switch tbl := tableExpr.Expr.(type) {
	case sqlparser.TableName:
		tableInfo, err := ctx.SemTable.TableInfoFor(tableID)
		if err != nil {
			return nil, err
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
			}, nil
		}
		qg := newQueryGraph()
		isInfSchema := tableInfo.IsInfSchema()
		qt := &QueryTable{Alias: tableExpr, Table: tbl, ID: tableID, IsInfSchema: isInfSchema}
		qg.Tables = append(qg.Tables, qt)
		return qg, nil
	case *sqlparser.DerivedTable:
		if onlyTable && tbl.Select.GetLimit() == nil {
			tbl.Select.SetOrderBy(nil)
		}

		inner, err := translateQueryToOp(ctx, tbl.Select)
		if err != nil {
			return nil, err
		}
		if horizon, ok := inner.(*Horizon); ok {
			horizon.TableId = &tableID
			horizon.Alias = tableExpr.As.String()
			horizon.ColumnAliases = tableExpr.Columns
			qp, err := CreateQPFromSelectStatement(ctx, tbl.Select)
			if err != nil {
				return nil, err
			}
			horizon.QP = qp
		}

		return inner, nil
	default:
		return nil, vterrors.VT13001(fmt.Sprintf("unable to use: %T", tbl))
	}
}

func crossJoin(ctx *plancontext.PlanningContext, exprs sqlparser.TableExprs) (ops.Operator, error) {
	var output ops.Operator
	for _, tableExpr := range exprs {
		op, err := getOperatorFromTableExpr(ctx, tableExpr, len(exprs) == 1)
		if err != nil {
			return nil, err
		}
		if output == nil {
			output = op
		} else {
			output = createJoin(ctx, output, op)
		}
	}
	return output, nil
}

func createQueryTableForDML(ctx *plancontext.PlanningContext, tableExpr sqlparser.TableExpr, whereClause *sqlparser.Where) (semantics.TableInfo, *QueryTable, error) {
	alTbl, ok := tableExpr.(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, nil, vterrors.VT13001("expected AliasedTableExpr")
	}
	tblName, ok := alTbl.Expr.(sqlparser.TableName)
	if !ok {
		return nil, nil, vterrors.VT13001("expected TableName")
	}

	tableID := ctx.SemTable.TableSetFor(alTbl)
	tableInfo, err := ctx.SemTable.TableInfoFor(tableID)
	if err != nil {
		return nil, nil, err
	}

	if tableInfo.IsInfSchema() {
		return nil, nil, vterrors.VT12001("update information schema tables")
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
	return tableInfo, qt, nil
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
	limit *sqlparser.Limit,
	lock sqlparser.Lock,
) (ops.Operator, error) {
	selectionStmt := &sqlparser.Select{
		SelectExprs: selectExprs,
		From:        tableExprs,
		Where:       where,
		Limit:       limit,
		Lock:        lock,
	}
	// There are no foreign keys to check for a select query, so we can pass anything for verifyAllFKs and fkToIgnore.
	return createOpFromStmt(ctx, selectionStmt, false /* verifyAllFKs */, "" /* fkToIgnore */)
}

func selectParentColumns(fk vindexes.ChildFKInfo, lastOffset int) ([]int, []sqlparser.SelectExpr) {
	var cols []int
	var exprs []sqlparser.SelectExpr
	for _, column := range fk.ParentColumns {
		cols = append(cols, lastOffset)
		exprs = append(exprs, aeWrap(sqlparser.NewColName(column.String())))
		lastOffset++
	}
	return cols, exprs
}
