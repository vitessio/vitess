/*
Copyright 2019 The Vitess Authors.

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

package planbuilder

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/physical"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/vt/vtgate/semantics"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

type horizonPlanning struct {
	sel            *sqlparser.Select
	qp             *abstract.QueryProjection
	vtgateGrouping bool
}

func (hp *horizonPlanning) planHorizon(ctx *plancontext.PlanningContext, plan logicalPlan) (logicalPlan, error) {
	rb, isRoute := plan.(*routeGen4)
	if !isRoute && ctx.SemTable.NotSingleRouteErr != nil {
		// If we got here, we don't have a single shard plan
		return nil, ctx.SemTable.NotSingleRouteErr
	}

	if isRoute && rb.isSingleShard() {
		err := planSingleShardRoutePlan(hp.sel, rb)
		if err != nil {
			return nil, err
		}
		return plan, nil
	}

	qp, err := abstract.CreateQPFromSelect(hp.sel, ctx.SemTable)
	if err != nil {
		return nil, err
	}

	hp.qp = qp

	needAggrOrHaving := hp.qp.NeedsAggregation() || hp.sel.Having != nil
	canShortcut := isRoute && !needAggrOrHaving && len(hp.qp.OrderExprs) == 0

	if needAggrOrHaving {
		plan, err = hp.planAggregations(ctx, plan)
		if err != nil {
			return nil, err
		}
	} else {
		_, isOA := plan.(*orderedAggregate)
		if isOA {
			plan = &simpleProjection{
				logicalPlanCommon: newBuilderCommon(plan),
				eSimpleProj:       &engine.SimpleProjection{},
			}
		}

		if canShortcut {
			err = planSingleShardRoutePlan(hp.sel, rb)
			if err != nil {
				return nil, err
			}
		} else {
			err = pushProjections(ctx, plan, hp.qp.SelectExprs)
			if err != nil {
				return nil, err
			}
		}
	}

	// If we have done the shortcut that means we already planned order by
	// and group by, thus we don't need to do it again.
	if !canShortcut {
		if len(hp.qp.OrderExprs) > 0 {
			plan, err = hp.planOrderBy(ctx, hp.qp.OrderExprs, plan)
			if err != nil {
				return nil, err
			}
		}

		if hp.qp.CanPushDownSorting && hp.vtgateGrouping {
			plan, err = hp.planGroupByUsingOrderBy(ctx, plan)
			if err != nil {
				return nil, err
			}
		}
	}

	plan, err = hp.planDistinct(ctx, plan)
	if err != nil {
		return nil, err
	}

	plan, err = hp.truncateColumnsIfNeeded(ctx, plan)
	if err != nil {
		return nil, err
	}

	return plan, nil
}

func pushProjections(ctx *plancontext.PlanningContext, plan logicalPlan, selectExprs []abstract.SelectExpr) error {
	for _, e := range selectExprs {
		aliasExpr, err := e.GetAliasedExpr()
		if err != nil {
			return err
		}
		if _, _, err := pushProjection(ctx, aliasExpr, plan, true, false, false); err != nil {
			return err
		}
	}
	return nil
}

func (hp *horizonPlanning) truncateColumnsIfNeeded(ctx *plancontext.PlanningContext, plan logicalPlan) (logicalPlan, error) {
	if len(plan.OutputColumns()) == hp.sel.GetColumnCount() {
		return plan, nil
	}
	switch p := plan.(type) {
	case *routeGen4:
		p.eroute.SetTruncateColumnCount(hp.sel.GetColumnCount())
	case *joinGen4, *semiJoin, *hashJoin:
		// since this is a join, we can safely add extra columns and not need to truncate them
	case *orderedAggregate:
		p.truncateColumnCount = hp.sel.GetColumnCount()
	case *memorySort:
		p.truncater.SetTruncateColumnCount(hp.sel.GetColumnCount())
	case *pulloutSubquery:
		newUnderlyingPlan, err := hp.truncateColumnsIfNeeded(ctx, p.underlying)
		if err != nil {
			return nil, err
		}
		p.underlying = newUnderlyingPlan
	default:
		plan = &simpleProjection{
			logicalPlanCommon: newBuilderCommon(plan),
			eSimpleProj:       &engine.SimpleProjection{},
		}

		err := pushProjections(ctx, plan, hp.qp.SelectExprs)
		if err != nil {
			return nil, err
		}
	}
	return plan, nil
}

// pushProjection pushes a projection to the plan.
func pushProjection(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, plan logicalPlan, inner, reuseCol, hasAggregation bool) (offset int, added bool, err error) {
	switch node := plan.(type) {
	case *routeGen4:
		_, isColName := expr.Expr.(*sqlparser.ColName)
		if !isColName {
			_, err := evalengine.Translate(expr.Expr, ctx.SemTable)
			if err != nil {
				if vterrors.Code(err) != vtrpcpb.Code_UNIMPLEMENTED {
					return 0, false, err
				} else if !inner {
					return 0, false, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: cross-shard left join and column expressions")
				}
			}
		}
		if reuseCol {
			if i := checkIfAlreadyExists(expr, node.Select, ctx.SemTable); i != -1 {
				return i, false, nil
			}
		}
		expr.Expr = sqlparser.RemoveKeyspaceFromColName(expr.Expr)
		sel, isSel := node.Select.(*sqlparser.Select)
		if !isSel {
			return 0, false, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadFieldError, "unsupported: pushing projection '%s' on %T", sqlparser.String(expr), node.Select)
		}

		// if we are trying to push a projection that belongs to a DerivedTable
		// we rewrite that expression, so it matches the column name used inside
		// that derived table.
		err = rewriteProjectionOfDerivedTable(expr, ctx.SemTable)
		if err != nil {
			return 0, false, err
		}

		offset := len(sel.SelectExprs)
		sel.SelectExprs = append(sel.SelectExprs, expr)
		return offset, true, nil
	case *hashJoin:
		lhsSolves := node.Left.ContainsTables()
		rhsSolves := node.Right.ContainsTables()
		deps := ctx.SemTable.RecursiveDeps(expr.Expr)
		var column int
		var appended bool
		passDownReuseCol := reuseCol
		if !reuseCol {
			passDownReuseCol = expr.As.IsEmpty()
		}
		switch {
		case deps.IsSolvedBy(lhsSolves):
			offset, added, err := pushProjection(ctx, expr, node.Left, inner, passDownReuseCol, hasAggregation)
			if err != nil {
				return 0, false, err
			}
			column = -(offset + 1)
			appended = added
		case deps.IsSolvedBy(rhsSolves):
			offset, added, err := pushProjection(ctx, expr, node.Right, inner && node.Opcode != engine.LeftJoin, passDownReuseCol, hasAggregation)
			if err != nil {
				return 0, false, err
			}
			column = offset + 1
			appended = added
		default:
			// if an expression has aggregation, then it should not be split up and pushed to both sides,
			// for example an expression like count(*) will have dependencies on both sides, but we should not push it
			// instead we should return an error
			if hasAggregation {
				return 0, false, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: cross-shard query with aggregates")
			}
			return 0, false, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: hash join with projection from both sides of the join")
		}
		if reuseCol && !appended {
			for idx, col := range node.Cols {
				if column == col {
					return idx, false, nil
				}
			}
			// the column was not appended to either child, but we could not find it in out cols list,
			// so we'll still add it
		}
		node.Cols = append(node.Cols, column)
		return len(node.Cols) - 1, true, nil
	case *joinGen4:
		lhsSolves := node.Left.ContainsTables()
		rhsSolves := node.Right.ContainsTables()
		deps := ctx.SemTable.RecursiveDeps(expr.Expr)
		var column int
		var appended bool
		passDownReuseCol := reuseCol
		if !reuseCol {
			passDownReuseCol = expr.As.IsEmpty()
		}
		switch {
		case deps.IsSolvedBy(lhsSolves):
			offset, added, err := pushProjection(ctx, expr, node.Left, inner, passDownReuseCol, hasAggregation)
			if err != nil {
				return 0, false, err
			}
			column = -(offset + 1)
			appended = added
		case deps.IsSolvedBy(rhsSolves):
			offset, added, err := pushProjection(ctx, expr, node.Right, inner && node.Opcode != engine.LeftJoin, passDownReuseCol, hasAggregation)
			if err != nil {
				return 0, false, err
			}
			column = offset + 1
			appended = added
		default:
			// if an expression has aggregation, then it should not be split up and pushed to both sides,
			// for example an expression like count(*) will have dependencies on both sides, but we should not push it
			// instead we should return an error
			if hasAggregation {
				return 0, false, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: cross-shard query with aggregates")
			}
			// now we break the expression into left and right side dependencies and rewrite the left ones to bind variables
			bvName, cols, rewrittenExpr, err := physical.BreakExpressionInLHSandRHS(ctx, expr.Expr, lhsSolves)
			if err != nil {
				return 0, false, err
			}
			// go over all the columns coming from the left side of the tree and push them down. While at it, also update the bind variable map.
			// It is okay to reuse the columns on the left side since
			// the final expression which will be selected will be pushed into the right side.
			for i, col := range cols {
				colOffset, _, err := pushProjection(ctx, &sqlparser.AliasedExpr{Expr: col}, node.Left, inner, true, false)
				if err != nil {
					return 0, false, err
				}
				node.Vars[bvName[i]] = colOffset
			}
			// push the rewritten expression on the right side of the tree. Here we should take care whether we want to reuse the expression or not.
			expr.Expr = rewrittenExpr
			offset, added, err := pushProjection(ctx, expr, node.Right, inner && node.Opcode != engine.LeftJoin, passDownReuseCol, false)
			if err != nil {
				return 0, false, err
			}
			column = offset + 1
			appended = added
		}
		if reuseCol && !appended {
			for idx, col := range node.Cols {
				if column == col {
					return idx, false, nil
				}
			}
			// the column was not appended to either child, but we could not find it in out cols list,
			// so we'll still add it
		}
		node.Cols = append(node.Cols, column)
		return len(node.Cols) - 1, true, nil
	case *pulloutSubquery:
		// push projection to the outer query
		return pushProjection(ctx, expr, node.underlying, inner, reuseCol, hasAggregation)
	case *simpleProjection:
		offset, _, err := pushProjection(ctx, expr, node.input, inner, true, hasAggregation)
		if err != nil {
			return 0, false, err
		}
		for i, value := range node.eSimpleProj.Cols {
			// we return early if we already have the column in the simple projection's
			// output list so we do not add it again.
			if reuseCol && value == offset {
				return i, false, nil
			}
		}
		node.eSimpleProj.Cols = append(node.eSimpleProj.Cols, offset)
		return len(node.eSimpleProj.Cols) - 1, true, nil
	case *orderedAggregate:
		colName, isColName := expr.Expr.(*sqlparser.ColName)
		for _, aggregate := range node.aggregates {
			if sqlparser.EqualsExpr(aggregate.Expr, expr.Expr) {
				return aggregate.Col, false, nil
			}
			if isColName && colName.Name.EqualString(aggregate.Alias) {
				return aggregate.Col, false, nil
			}
		}
		for _, key := range node.groupByKeys {
			if sqlparser.EqualsExpr(key.Expr, expr.Expr) {
				return key.KeyCol, false, nil
			}
		}
		return 0, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "cannot push projections in ordered aggregates")
	case *vindexFunc:
		colsBefore := len(node.eVindexFunc.Cols)
		i, err := node.SupplyProjection(expr, reuseCol)
		if err != nil {
			return 0, false, err
		}
		return i /* col added */, len(node.eVindexFunc.Cols) > colsBefore, nil
	case *limit:
		return pushProjection(ctx, expr, node.input, inner, reuseCol, hasAggregation)
	case *distinct:
		return pushProjection(ctx, expr, node.input, inner, reuseCol, hasAggregation)
	case *filter:
		return pushProjection(ctx, expr, node.input, inner, reuseCol, hasAggregation)
	case *semiJoin:
		passDownReuseCol := reuseCol
		if !reuseCol {
			passDownReuseCol = expr.As.IsEmpty()
		}
		offset, added, err := pushProjection(ctx, expr, node.lhs, inner, passDownReuseCol, hasAggregation)
		if err != nil {
			return 0, false, err
		}
		column := -(offset + 1)
		if reuseCol && !added {
			for idx, col := range node.cols {
				if column == col {
					return idx, false, nil
				}
			}
		}
		node.cols = append(node.cols, column)
		return len(node.cols) - 1, true, nil
	case *concatenateGen4:
		if hasAggregation {
			return 0, false, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: aggregation on unions")
		}
		offset, added, err := pushProjection(ctx, expr, node.sources[0], inner, reuseCol, hasAggregation)
		if err != nil {
			return 0, false, err
		}
		if added && ctx.SemTable.DirectDeps(expr.Expr).NumberOfTables() > 0 {
			return 0, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "pushing projection %v on concatenate should reference an existing column", sqlparser.String(expr))
		}
		if added {
			for _, source := range node.sources[1:] {
				_, _, err := pushProjection(ctx, expr, source, inner, reuseCol, hasAggregation)
				if err != nil {
					return 0, false, err
				}
			}
		}
		return offset, added, nil
	default:
		return 0, false, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "[BUG] push projection does not yet support: %T", node)
	}
}

func rewriteProjectionOfDerivedTable(expr *sqlparser.AliasedExpr, semTable *semantics.SemTable) error {
	ti, err := semTable.TableInfoForExpr(expr.Expr)
	if err != nil && err != semantics.ErrMultipleTables {
		return err
	}
	_, isDerivedTable := ti.(*semantics.DerivedTable)
	if isDerivedTable {
		expr.Expr, err = semantics.RewriteDerivedExpression(expr.Expr, ti)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkIfAlreadyExists(expr *sqlparser.AliasedExpr, node sqlparser.SelectStatement, semTable *semantics.SemTable) int {
	exprDep := semTable.RecursiveDeps(expr.Expr)
	// Here to find if the expr already exists in the SelectStatement, we have 3 cases
	// input is a Select -> In this case we want to search in the select
	// input is a Union -> In this case we want to search in the First Select of the Union
	// input is a Parenthesised Select -> In this case we want to search in the select
	// all these three cases are handled by the call to GetFirstSelect.
	sel := sqlparser.GetFirstSelect(node)

	for i, selectExpr := range sel.SelectExprs {
		selectExpr, ok := selectExpr.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}

		selectExprCol, isSelectExprCol := selectExpr.Expr.(*sqlparser.ColName)
		exprCol, isExprCol := expr.Expr.(*sqlparser.ColName)
		selectExprDep := semTable.RecursiveDeps(selectExpr.Expr)

		// Check that the two expressions have the same dependencies
		if selectExprDep != exprDep {
			continue
		}

		if selectExpr.As.IsEmpty() {
			// we don't have an alias

			if isSelectExprCol && isExprCol && exprCol.Name.Equal(selectExprCol.Name) {
				// the expressions are ColName, we compare their name
				return i
			} else if sqlparser.EqualsExpr(selectExpr.Expr, expr.Expr) {
				// the expressions are not ColName, so we just compare the expressions
				return i
			}
		} else if isExprCol && selectExpr.As.Equal(exprCol.Name) {
			// we have an aliased column, checking if the expression is matching the alias
			return i
		}
	}
	return -1
}

func (hp *horizonPlanning) planAggregations(ctx *plancontext.PlanningContext, plan logicalPlan) (out logicalPlan, err error) {
	if !hp.qp.HasAggr {
		return plan, nil
	}

	isPushable := !isJoin(plan)
	vindexOverlapWithGrouping := hasUniqueVindex(ctx.VSchema, ctx.SemTable, hp.qp.GroupByExprs)
	var oa *orderedAggregate
	if isPushable && vindexOverlapWithGrouping {
		// If we have a plan that we can push the group by and aggregation through, we don't need to do aggregation
		// at the vtgate level at all
		panic("oh noes")
	} else {
		oa = &orderedAggregate{}
		out = oa
	}

	// we know we will need these expressions, so we just push them all the way to where they come from
	for _, expr := range hp.qp.GroupByExprs {
		if oa != nil {
			gb := &engine.GroupByParams{
				Expr:        expr.Inner,
				FromGroupBy: true,
				CollationID: ctx.SemTable.CollationForExpr(expr.Inner),
			}
			oa.groupByKeys = append(oa.groupByKeys, gb)
		}
	}

	aggregationExprs, err := hp.qp.AggregationExpressions()
	if err != nil {
		return nil, err
	}

	aggPlan, groupings, aggrParams, err := hp.pushAggregation(ctx, plan, hp.qp.GroupByExprs, aggregationExprs)
	if err != nil {
		return nil, err
	}

	if oa != nil {
		oa.aggregates = aggrParams
		for i, grouping := range groupings {
			oa.groupByKeys[i].KeyCol = grouping.col
			oa.groupByKeys[i].WeightStringCol = grouping.wsCol
		}
		oa.resultsBuilder = resultsBuilder{
			logicalPlanCommon: newBuilderCommon(aggPlan),
			weightStrings:     make(map[*resultColumn]int),
		}
	}

	return out, nil
}

type offsets struct {
	col, wsCol int
}

// pushAggregation pushes grouping and aggregation as far down in the tree as possible
func (hp *horizonPlanning) pushAggregation(
	ctx *plancontext.PlanningContext,
	plan logicalPlan,
	grouping []abstract.GroupBy,
	aggregations []abstract.Aggr,
) (newPlan logicalPlan, groupingOffsets []offsets, outputAggrs []*engine.AggregateParams, err error) {
	switch plan := plan.(type) {
	case *routeGen4:
		groupingOffsets, outputAggrs, err = pushAggrOnRoute(ctx, plan, aggregations, grouping)
		if err != nil {
			return nil, nil, nil, err
		}
		newPlan = plan

		return
	case *joinGen4:
		return hp.pushAggrOnJoin(ctx, grouping, aggregations, plan)

	default:
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "pushAggregation %T", plan)
	}
}

func (hp *horizonPlanning) pushAggrOnJoin(
	ctx *plancontext.PlanningContext,
	grouping []abstract.GroupBy,
	aggregations []abstract.Aggr,
	join *joinGen4,
) (logicalPlan, []offsets, []*engine.AggregateParams, error) {
	var lhsAggrs, rhsAggrs []abstract.Aggr
	for _, aggr := range aggregations {
		if isCountStar(aggr.Func) {
			lhsAggrs = append(lhsAggrs, aggr)
			rhsAggrs = append(rhsAggrs, aggr)
		} else {
			// deps := ctx.SemTable.RecursiveDeps(aggr.Func)
			// switch {
			// case deps.IsSolvedBy(join.Left.ContainsTables()):
			//
			// case deps.IsSolvedBy(join.Right.ContainsTables()):
			// default:
			return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "aggregation on columns from different sources not supported yet")
			// }
		}
	}

	// We need to group by the columns used in the join condition.
	// If we don't, the LHS will not be able to return the column, and it can't be used to send down to the RHS
	lhsCols, err := hp.createGroupingsForColumns(ctx, join.LHSColumns)
	if err != nil {
		return nil, nil, nil, err
	}

	// Here we split the grouping depending on if they should with the LHS or RHS of the query
	// This is done by using the semantic table and checking dependencies
	lhsGrouping, rhsGrouping, groupingOffsets, err := splitGroupingsToLeftAndRight(ctx, join, grouping, lhsCols)
	if err != nil {
		return nil, nil, nil, err
	}

	// Next we push the aggregations to both sides
	lhsPlan, lhsOffsets, lhsAggregations, err := hp.pushAggregation(ctx, join.Left, lhsGrouping, lhsAggrs)
	if err != nil {
		return nil, nil, nil, err
	}
	rhsPlan, rhsOffsets, rhsAggregations, err := hp.pushAggregation(ctx, join.Right, rhsGrouping, rhsAggrs)
	if err != nil {
		return nil, nil, nil, err
	}
	join.Left, join.Right = lhsPlan, rhsPlan

	// Next, we have to pass through the grouping values through the join and the projection we add on top
	// We added new groupings to the LHS because of the join condition, so we don't want to pass through everything,
	// just the groupings that are used by operators on top of this current one
	outputGroupings := make([]offsets, 0, len(groupingOffsets))
	proj := &projection{source: join}
	for _, groupBy := range groupingOffsets {
		var offset offsets
		var f func(i int) int
		if groupBy < 0 {
			offset = lhsOffsets[-groupBy-1]
			f = func(i int) int { return -(i + 1) }
		} else {
			offset = rhsOffsets[groupBy-1]
			f = func(i int) int { return i + 1 }
		}
		outputGroupings = append(outputGroupings, passGroupingsThroughJoinAndProj(proj, join, offset, f))
	}

	// Here we produce information about the grouping columns projected through
	outputAggrs := produceOutputAggregations(aggregations, lhsAggregations, rhsAggregations, join, proj)

	return proj, outputGroupings, outputAggrs, nil
}

func produceOutputAggregations(
	aggregations []abstract.Aggr,
	lhsAggregations, rhsAggregations []*engine.AggregateParams,
	join *joinGen4,
	proj *projection,
) []*engine.AggregateParams {
	var outputAggrs []*engine.AggregateParams
	for idx, aggr := range aggregations {
		// create the projection expression that will multiply the incoming aggregations
		l, r := lhsAggregations[idx], rhsAggregations[idx]
		offset := len(join.Cols)
		expr := &sqlparser.BinaryExpr{
			Operator: sqlparser.MultOp,
			Left:     sqlparser.Offset(offset),
			Right:    sqlparser.Offset(offset + 1),
		}
		output := len(proj.columns)
		proj.columns = append(proj.columns, expr)
		proj.columnNames = append(proj.columnNames, aggr.Alias)

		// pass through the output columns from the join
		join.Cols = append(join.Cols, -(l.Col + 1))
		join.Cols = append(join.Cols, r.Col+1)

		outputAggrs = append(outputAggrs, &engine.AggregateParams{
			Opcode: engine.AggregateSum,
			Col:    output,
			Alias:  aggr.Alias,
			Expr:   aggr.Func,
		})
	}
	return outputAggrs
}

func splitGroupingsToLeftAndRight(
	ctx *plancontext.PlanningContext,
	join *joinGen4,
	grouping, lhsGrouping []abstract.GroupBy,
) ([]abstract.GroupBy, []abstract.GroupBy, []int, error) {
	var rhsGrouping []abstract.GroupBy

	lhsTS := join.Left.ContainsTables()
	rhsTS := join.Right.ContainsTables()
	// here we store information about which side the grouping value is coming from.
	// Negative values from the left operator and positive values are offsets into the RHS
	var groupingOffsets []int
	for _, groupBy := range grouping {
		deps := ctx.SemTable.RecursiveDeps(groupBy.Inner)
		switch {
		case deps.IsSolvedBy(lhsTS):
			groupingOffsets = append(groupingOffsets, -(len(lhsGrouping) + 1))
			lhsGrouping = append(lhsGrouping, groupBy)
		case deps.IsSolvedBy(rhsTS):
			groupingOffsets = append(groupingOffsets, len(rhsGrouping)+1)
			rhsGrouping = append(rhsGrouping, groupBy)
		default:
			return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "grouping on columns from different sources not supported yet")
		}
	}
	return lhsGrouping, rhsGrouping, groupingOffsets, nil
}
func passGroupingsThroughJoinAndProj(proj *projection, join *joinGen4, offset offsets, f func(int) int) (output offsets) {
	output.col = len(proj.columns)
	proj.columns = append(proj.columns, sqlparser.Offset(len(join.Cols)))
	proj.columnNames = append(proj.columnNames, "")
	join.Cols = append(join.Cols, f(offset.col))
	if offset.wsCol > -1 {
		output.wsCol = len(proj.columns)
		proj.columns = append(proj.columns, sqlparser.Offset(len(join.Cols)))
		proj.columnNames = append(proj.columnNames, "")
		join.Cols = append(join.Cols, f(offset.wsCol))
	}
	return
}

func (hp *horizonPlanning) createGroupingsForColumns(
	ctx *plancontext.PlanningContext,
	columns []*sqlparser.ColName,
) ([]abstract.GroupBy, error) {
	var lhsGrouping []abstract.GroupBy
	for _, lhsColumn := range columns {
		expr, wsExpr, err := hp.qp.GetSimplifiedExpr(lhsColumn, ctx.SemTable)
		if err != nil {
			return nil, err
		}

		lhsGrouping = append(lhsGrouping, abstract.GroupBy{
			Inner:         expr,
			WeightStrExpr: wsExpr,
		})
	}
	return lhsGrouping, nil
}

func isCountStar(f *sqlparser.FuncExpr) bool {
	_, isStar := f.Exprs[0].(*sqlparser.StarExpr)
	return isStar
}

func pushAggrOnRoute(
	ctx *plancontext.PlanningContext,
	plan *routeGen4,
	aggregations []abstract.Aggr,
	grouping []abstract.GroupBy,
) ([]offsets, []*engine.AggregateParams, error) {
	sel, isSel := plan.Select.(*sqlparser.Select)
	if !isSel {
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't plan aggregation on union")
	}

	vtgateAggregation := make([]*engine.AggregateParams, 0, len(aggregations))
	for _, aggregation := range aggregations {
		sel.SelectExprs = append(sel.SelectExprs, aggregation.Original)
		param := &engine.AggregateParams{
			Opcode: engine.AggregateSum,
			Col:    len(sel.SelectExprs) - 1,
			Alias:  aggregation.Alias,
		}
		vtgateAggregation = append(vtgateAggregation, param)
	}

	groupingOffsets := make([]offsets, 0, len(grouping))
	sel.GroupBy = make([]sqlparser.Expr, 0, len(grouping))
	for _, expr := range grouping {
		sel.GroupBy = append(sel.GroupBy, expr.Inner)
		col, wsCol, err := wrapAndPushExpr(ctx, expr.Inner, expr.WeightStrExpr, plan)
		if err != nil {
			return nil, nil, err
		}
		groupingOffsets = append(groupingOffsets, offsets{
			col:   col,
			wsCol: wsCol,
		})
	}

	return groupingOffsets, vtgateAggregation, nil
}

// createPushExprAndAlias creates the expression that should be pushed down to the leaves,
// and changes the opcode, so it is a distinct one if needed
func (hp *horizonPlanning) createPushExprAndAlias(
	ctx *plancontext.PlanningContext,
	expr abstract.SelectExpr,
	handleDistinct bool,
	innerAliased *sqlparser.AliasedExpr,
	opcode engine.AggregateOpcode,
	oa *orderedAggregate,
) (*sqlparser.AliasedExpr, *engine.AggregateParams) {
	aliasExpr, isAlias := expr.Col.(*sqlparser.AliasedExpr)
	if !isAlias {
		return nil, nil
	}
	var alias string
	if aliasExpr.As.IsEmpty() {
		alias = sqlparser.String(aliasExpr.Expr)
	} else {
		alias = aliasExpr.As.String()
	}
	if handleDistinct {
		aliasExpr = innerAliased

		switch opcode {
		case engine.AggregateCount:
			opcode = engine.AggregateCountDistinct
		case engine.AggregateSum:
			opcode = engine.AggregateSumDistinct
		}

		oa.preProcess = true
		by := abstract.GroupBy{
			Inner:             innerAliased.Expr,
			WeightStrExpr:     innerAliased.Expr,
			DistinctAggrIndex: len(oa.aggregates) + 1,
		}
		hp.qp.GroupByExprs = append(hp.qp.GroupByExprs, by)
	}
	collID := collations.Unknown
	if innerAliased != nil {
		collID = ctx.SemTable.CollationForExpr(innerAliased.Expr)
	}

	param := &engine.AggregateParams{
		Opcode:      opcode,
		Alias:       alias,
		CollationID: collID,
	}
	return aliasExpr, param
}

func hasUniqueVindex(vschema plancontext.VSchema, semTable *semantics.SemTable, groupByExprs []abstract.GroupBy) bool {
	for _, groupByExpr := range groupByExprs {
		if exprHasUniqueVindex(vschema, semTable, groupByExpr.WeightStrExpr) {
			return true
		}
	}
	return false
}

func planGroupByGen4(ctx *plancontext.PlanningContext, groupExpr abstract.GroupBy, plan logicalPlan, wsAdded bool) error {
	switch node := plan.(type) {
	case *routeGen4:
		sel := node.Select.(*sqlparser.Select)
		sel.GroupBy = append(sel.GroupBy, groupExpr.Inner)
		// If a weight_string function is added to the select list,
		// then we need to add that to the group by clause otherwise the query will fail on mysql with full_group_by error
		// as the weight_string function might not be functionally dependent on the group by.
		if wsAdded {
			sel.GroupBy = append(sel.GroupBy, weightStringFor(groupExpr.WeightStrExpr))
		}
		return nil
	case *joinGen4, *hashJoin:
		_, _, err := wrapAndPushExpr(ctx, groupExpr.Inner, groupExpr.WeightStrExpr, node)
		return err
	case *orderedAggregate:
		keyCol, wsOffset, err := wrapAndPushExpr(ctx, groupExpr.Inner, groupExpr.WeightStrExpr, node.input)
		if err != nil {
			return err
		}
		if groupExpr.DistinctAggrIndex == 0 {
			node.groupByKeys = append(node.groupByKeys, &engine.GroupByParams{KeyCol: keyCol, WeightStringCol: wsOffset, Expr: groupExpr.WeightStrExpr, CollationID: ctx.SemTable.CollationForExpr(groupExpr.Inner)})
		} else {
			if wsOffset != -1 {
				node.aggregates[groupExpr.DistinctAggrIndex-1].WAssigned = true
				node.aggregates[groupExpr.DistinctAggrIndex-1].WCol = wsOffset
			}
		}
		err = planGroupByGen4(ctx, groupExpr, node.input, wsOffset != -1)
		if err != nil {
			return err
		}
		return nil
	case *pulloutSubquery:
		return planGroupByGen4(ctx, groupExpr, node.underlying, wsAdded)
	case *semiJoin:
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: group by in a query having a correlated subquery")
	default:
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: group by on: %T", plan)
	}
}

func (hp *horizonPlanning) planGroupByUsingOrderBy(ctx *plancontext.PlanningContext, plan logicalPlan) (logicalPlan, error) {
	var orderExprs []abstract.OrderBy
	for _, groupExpr := range hp.qp.GroupByExprs {
		addExpr := true
		for _, orderExpr := range hp.qp.OrderExprs {
			if sqlparser.EqualsExpr(groupExpr.Inner, orderExpr.Inner.Expr) {
				addExpr = false
				break
			}
		}
		if addExpr {
			orderExprs = append(orderExprs, abstract.OrderBy{
				Inner:         &sqlparser.Order{Expr: groupExpr.Inner},
				WeightStrExpr: groupExpr.WeightStrExpr},
			)
		}
	}
	if len(orderExprs) > 0 {
		return hp.planOrderBy(ctx, orderExprs, plan)
	}
	return plan, nil
}

func (hp *horizonPlanning) planOrderBy(ctx *plancontext.PlanningContext, orderExprs []abstract.OrderBy, plan logicalPlan) (logicalPlan, error) {
	switch plan := plan.(type) {
	case *routeGen4:
		newPlan, err := planOrderByForRoute(ctx, orderExprs, plan, hp.qp.HasStar)
		if err != nil {
			return nil, err
		}
		return newPlan, nil
	case *joinGen4:
		newPlan, err := hp.planOrderByForJoin(ctx, orderExprs, plan)
		if err != nil {
			return nil, err
		}

		return newPlan, nil
	case *hashJoin:
		newPlan, err := hp.planOrderByForHashJoin(ctx, orderExprs, plan)
		if err != nil {
			return nil, err
		}

		return newPlan, nil
	case *orderedAggregate:
		// remove ORDER BY NULL from the list of order by expressions since we will be doing the ordering on vtgate level so NULL is not useful
		var orderExprsWithoutNils []abstract.OrderBy
		for _, expr := range orderExprs {
			if sqlparser.IsNull(expr.Inner.Expr) {
				continue
			}
			orderExprsWithoutNils = append(orderExprsWithoutNils, expr)
		}
		orderExprs = orderExprsWithoutNils

		for _, order := range orderExprs {
			if sqlparser.ContainsAggregation(order.WeightStrExpr) {
				ms, err := createMemorySortPlanOnAggregation(plan, orderExprs)
				if err != nil {
					return nil, err
				}
				return ms, nil
			}
		}
		newInput, err := hp.planOrderBy(ctx, orderExprs, plan.input)
		if err != nil {
			return nil, err
		}
		plan.input = newInput
		return plan, nil
	case *memorySort:
		return plan, nil
	case *simpleProjection:
		return hp.createMemorySortPlan(ctx, plan, orderExprs, true)
	case *vindexFunc:
		// This is evaluated at VTGate only, so weight_string function cannot be used.
		return hp.createMemorySortPlan(ctx, plan, orderExprs /* useWeightStr */, false)
	case *limit, *semiJoin, *filter, *pulloutSubquery:
		inputs := plan.Inputs()
		if len(inputs) == 0 {
			break
		}
		newFirstInput, err := hp.planOrderBy(ctx, orderExprs, inputs[0])
		if err != nil {
			return nil, err
		}
		inputs[0] = newFirstInput
		err = plan.Rewrite(inputs...)
		if err != nil {
			return nil, err
		}
		return plan, nil
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "ordering on complex query %T", plan)
}

func isSpecialOrderBy(o abstract.OrderBy) bool {
	if sqlparser.IsNull(o.Inner.Expr) {
		return true
	}
	f, isFunction := o.Inner.Expr.(*sqlparser.FuncExpr)
	return isFunction && f.Name.Lowered() == "rand"
}

func planOrderByForRoute(ctx *plancontext.PlanningContext, orderExprs []abstract.OrderBy, plan *routeGen4, hasStar bool) (logicalPlan, error) {
	for _, order := range orderExprs {
		err := checkOrderExprCanBePlannedInScatter(plan, order, hasStar)
		if err != nil {
			return nil, err
		}
		plan.Select.AddOrder(order.Inner)
		if isSpecialOrderBy(order) {
			continue
		}
		offset, weightStringOffset, err := wrapAndPushExpr(ctx, order.Inner.Expr, order.WeightStrExpr, plan)
		if err != nil {
			return nil, err
		}
		plan.eroute.OrderBy = append(plan.eroute.OrderBy, engine.OrderByParams{
			Col:             offset,
			WeightStringCol: weightStringOffset,
			Desc:            order.Inner.Direction == sqlparser.DescOrder,
			CollationID:     ctx.SemTable.CollationForExpr(order.Inner.Expr),
		})
	}
	return plan, nil
}

// checkOrderExprCanBePlannedInScatter verifies that the given order by expression can be planned.
// It checks if the expression exists in the plan's select list when the query is a scatter.
func checkOrderExprCanBePlannedInScatter(plan *routeGen4, order abstract.OrderBy, hasStar bool) error {
	if !hasStar {
		return nil
	}
	sel := sqlparser.GetFirstSelect(plan.Select)
	found := false
	for _, expr := range sel.SelectExprs {
		aliasedExpr, isAliasedExpr := expr.(*sqlparser.AliasedExpr)
		if isAliasedExpr && sqlparser.EqualsExpr(aliasedExpr.Expr, order.Inner.Expr) {
			found = true
			break
		}
	}
	if !found {
		return vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: in scatter query: order by must reference a column in the select list: "+sqlparser.String(order.Inner))
	}
	return nil
}

// wrapAndPushExpr pushes the expression and weighted_string function to the plan using semantics.SemTable
// It returns (expr offset, weight_string offset, error)
func wrapAndPushExpr(ctx *plancontext.PlanningContext, expr sqlparser.Expr, weightStrExpr sqlparser.Expr, plan logicalPlan) (int, int, error) {
	offset, _, err := pushProjection(ctx, &sqlparser.AliasedExpr{Expr: expr}, plan, true, true, false)
	if err != nil {
		return 0, 0, err
	}
	if weightStrExpr == nil {
		return offset, -1, nil
	}
	if !sqlparser.IsColName(expr) {
		unary, ok := expr.(*sqlparser.ConvertExpr)
		if ok && sqlparser.IsColName(unary.Expr) {
			expr = unary.Expr
		} else {
			return 0, 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: in scatter query: complex order by expression: %s", sqlparser.String(expr))
		}
	}
	qt := ctx.SemTable.TypeFor(expr)
	wsNeeded := true
	if qt != nil && sqltypes.IsNumber(*qt) {
		wsNeeded = false
	}

	weightStringOffset := -1
	if wsNeeded {
		weightStringOffset, _, err = pushProjection(ctx, &sqlparser.AliasedExpr{Expr: weightStringFor(weightStrExpr)}, plan, true, true, false)
		if err != nil {
			return 0, 0, err
		}
	}
	return offset, weightStringOffset, nil
}

func weightStringFor(expr sqlparser.Expr) sqlparser.Expr {
	return &sqlparser.FuncExpr{
		Name: sqlparser.NewColIdent("weight_string"),
		Exprs: []sqlparser.SelectExpr{
			&sqlparser.AliasedExpr{
				Expr: expr,
			},
		},
	}

}

func (hp *horizonPlanning) planOrderByForHashJoin(ctx *plancontext.PlanningContext, orderExprs []abstract.OrderBy, plan *hashJoin) (logicalPlan, error) {
	if len(orderExprs) == 1 && isSpecialOrderBy(orderExprs[0]) {
		rhs, err := hp.planOrderBy(ctx, orderExprs, plan.Right)
		if err != nil {
			return nil, err
		}
		plan.Right = rhs
		return plan, nil
	}
	if orderExprsDependsOnTableSet(orderExprs, ctx.SemTable, plan.Right.ContainsTables()) {
		newRight, err := hp.planOrderBy(ctx, orderExprs, plan.Right)
		if err != nil {
			return nil, err
		}
		plan.Right = newRight
		return plan, nil
	}
	sortPlan, err := hp.createMemorySortPlan(ctx, plan, orderExprs, true)
	if err != nil {
		return nil, err
	}
	return sortPlan, nil
}

func (hp *horizonPlanning) planOrderByForJoin(ctx *plancontext.PlanningContext, orderExprs []abstract.OrderBy, plan *joinGen4) (logicalPlan, error) {
	if len(orderExprs) == 1 && isSpecialOrderBy(orderExprs[0]) {
		lhs, err := hp.planOrderBy(ctx, orderExprs, plan.Left)
		if err != nil {
			return nil, err
		}
		rhs, err := hp.planOrderBy(ctx, orderExprs, plan.Right)
		if err != nil {
			return nil, err
		}
		plan.Left = lhs
		plan.Right = rhs
		return plan, nil
	}
	if orderExprsDependsOnTableSet(orderExprs, ctx.SemTable, plan.Left.ContainsTables()) {
		newLeft, err := hp.planOrderBy(ctx, orderExprs, plan.Left)
		if err != nil {
			return nil, err
		}
		plan.Left = newLeft
		return plan, nil
	}
	sortPlan, err := hp.createMemorySortPlan(ctx, plan, orderExprs, true)
	if err != nil {
		return nil, err
	}
	return sortPlan, nil
}

func createMemorySortPlanOnAggregation(plan *orderedAggregate, orderExprs []abstract.OrderBy) (logicalPlan, error) {
	primitive := &engine.MemorySort{}
	ms := &memorySort{
		resultsBuilder: resultsBuilder{
			logicalPlanCommon: newBuilderCommon(plan),
			weightStrings:     make(map[*resultColumn]int),
			truncater:         primitive,
		},
		eMemorySort: primitive,
	}

	for _, order := range orderExprs {
		offset, woffset, idx, found := findExprInOrderedAggr(plan, order)
		if !found {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "expected to find the order by expression (%s) in orderedAggregate", sqlparser.String(order.Inner))
		}

		collationID := collations.Unknown
		if woffset != -1 {
			collationID = plan.groupByKeys[idx].CollationID
		}
		ms.eMemorySort.OrderBy = append(ms.eMemorySort.OrderBy, engine.OrderByParams{
			Col:               offset,
			WeightStringCol:   woffset,
			Desc:              order.Inner.Direction == sqlparser.DescOrder,
			StarColFixedIndex: offset,
			CollationID:       collationID,
		})
	}
	return ms, nil
}

func findExprInOrderedAggr(plan *orderedAggregate, order abstract.OrderBy) (keyCol int, weightStringCol int, index int, found bool) {
	for idx, key := range plan.groupByKeys {
		if sqlparser.EqualsExpr(order.WeightStrExpr, key.Expr) {
			return key.KeyCol, key.WeightStringCol, idx, true
		}
	}
	for idx, aggregate := range plan.aggregates {
		if sqlparser.EqualsExpr(order.WeightStrExpr, aggregate.Expr) {
			return aggregate.Col, -1, idx, true
		}
	}
	return 0, 0, 0, false
}

func (hp *horizonPlanning) createMemorySortPlan(ctx *plancontext.PlanningContext, plan logicalPlan, orderExprs []abstract.OrderBy, useWeightStr bool) (logicalPlan, error) {
	primitive := &engine.MemorySort{}
	ms := &memorySort{
		resultsBuilder: resultsBuilder{
			logicalPlanCommon: newBuilderCommon(plan),
			weightStrings:     make(map[*resultColumn]int),
			truncater:         primitive,
		},
		eMemorySort: primitive,
	}

	for _, order := range orderExprs {
		wsExpr := order.WeightStrExpr
		if !useWeightStr {
			wsExpr = nil
		}
		offset, weightStringOffset, err := wrapAndPushExpr(ctx, order.Inner.Expr, wsExpr, plan)
		if err != nil {
			return nil, err
		}
		ms.eMemorySort.OrderBy = append(ms.eMemorySort.OrderBy, engine.OrderByParams{
			Col:               offset,
			WeightStringCol:   weightStringOffset,
			Desc:              order.Inner.Direction == sqlparser.DescOrder,
			StarColFixedIndex: offset,
			CollationID:       ctx.SemTable.CollationForExpr(order.Inner.Expr),
		})
	}
	return ms, nil
}

func orderExprsDependsOnTableSet(orderExprs []abstract.OrderBy, semTable *semantics.SemTable, ts semantics.TableSet) bool {
	for _, expr := range orderExprs {
		exprDependencies := semTable.RecursiveDeps(expr.Inner.Expr)
		if !exprDependencies.IsSolvedBy(ts) {
			return false
		}
	}
	return true
}

func (hp *horizonPlanning) planDistinct(ctx *plancontext.PlanningContext, plan logicalPlan) (logicalPlan, error) {
	if !hp.qp.NeedsDistinct() {
		return plan, nil
	}
	switch p := plan.(type) {
	case *routeGen4:
		// we always make the underlying query distinct,
		// and then we might also add a distinct operator on top if it is needed
		p.Select.MakeDistinct()
		if p.isSingleShard() || selectHasUniqueVindex(ctx.VSchema, ctx.SemTable, hp.qp.SelectExprs) {
			return plan, nil
		}

		return hp.addDistinct(ctx, plan)
	case *joinGen4, *pulloutSubquery:
		return hp.addDistinct(ctx, plan)
	case *orderedAggregate:
		return hp.planDistinctOA(ctx.SemTable, p)
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unknown plan type for DISTINCT %T", plan)
	}
}

func (hp *horizonPlanning) planDistinctOA(semTable *semantics.SemTable, currPlan *orderedAggregate) (logicalPlan, error) {
	oa := &orderedAggregate{
		resultsBuilder: resultsBuilder{
			logicalPlanCommon: newBuilderCommon(currPlan),
			weightStrings:     make(map[*resultColumn]int),
		},
	}
	for _, sExpr := range hp.qp.SelectExprs {
		expr, err := sExpr.GetExpr()
		if err != nil {
			return nil, err
		}
		found := false
		for _, grpParam := range currPlan.groupByKeys {
			if sqlparser.EqualsExpr(expr, grpParam.Expr) {
				found = true
				oa.groupByKeys = append(oa.groupByKeys, grpParam)
				break
			}
		}
		if found {
			continue
		}
		for _, aggrParam := range currPlan.aggregates {
			if sqlparser.EqualsExpr(expr, aggrParam.Expr) {
				found = true
				oa.groupByKeys = append(oa.groupByKeys, &engine.GroupByParams{KeyCol: aggrParam.Col, WeightStringCol: -1, CollationID: semTable.CollationForExpr(expr)})
				break
			}
		}
		if !found {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unable to plan distinct query as the column is not projected: %s", sqlparser.String(sExpr.Col))
		}
	}
	return oa, nil
}

func (hp *horizonPlanning) addDistinct(ctx *plancontext.PlanningContext, plan logicalPlan) (logicalPlan, error) {
	var orderExprs []abstract.OrderBy
	var groupByKeys []*engine.GroupByParams
	for index, sExpr := range hp.qp.SelectExprs {
		aliasExpr, err := sExpr.GetAliasedExpr()
		if err != nil {
			return nil, err
		}
		if isAmbiguousOrderBy(index, aliasExpr.As, hp.qp.SelectExprs) {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "generating order by clause: ambiguous symbol reference: %s", sqlparser.String(aliasExpr.As))
		}
		var inner sqlparser.Expr
		if aliasExpr.As.IsEmpty() {
			inner = aliasExpr.Expr
		} else {
			// If we have an alias, we need to use the alias and not the original expression
			// to make sure dependencies work correctly,
			// we simply copy the dependencies of the original expression here
			inner = sqlparser.NewColName(aliasExpr.As.String())
			ctx.SemTable.CopyDependencies(aliasExpr.Expr, inner)
		}
		grpParam := &engine.GroupByParams{KeyCol: index, WeightStringCol: -1, CollationID: ctx.SemTable.CollationForExpr(inner)}
		_, wOffset, err := wrapAndPushExpr(ctx, aliasExpr.Expr, aliasExpr.Expr, plan)
		if err != nil {
			return nil, err
		}
		grpParam.WeightStringCol = wOffset
		groupByKeys = append(groupByKeys, grpParam)

		orderExprs = append(orderExprs, abstract.OrderBy{
			Inner:         &sqlparser.Order{Expr: inner},
			WeightStrExpr: aliasExpr.Expr},
		)
	}
	innerPlan, err := hp.planOrderBy(ctx, orderExprs, plan)
	if err != nil {
		return nil, err
	}
	oa := &orderedAggregate{
		resultsBuilder: resultsBuilder{
			logicalPlanCommon: newBuilderCommon(innerPlan),
			weightStrings:     make(map[*resultColumn]int),
		},
		groupByKeys: groupByKeys,
	}
	return oa, nil
}

func isAmbiguousOrderBy(index int, col sqlparser.ColIdent, exprs []abstract.SelectExpr) bool {
	if col.String() == "" {
		return false
	}
	for i, expr := range exprs {
		if i == index {
			continue
		}
		aliasExpr, isAlias := expr.Col.(*sqlparser.AliasedExpr)
		if !isAlias {
			// TODO: handle star expression error
			return true
		}
		alias := aliasExpr.As
		if alias.IsEmpty() {
			if col, ok := aliasExpr.Expr.(*sqlparser.ColName); ok {
				alias = col.Name
			}
		}
		if col.Equal(alias) {
			return true
		}
	}
	return false
}

func selectHasUniqueVindex(vschema plancontext.VSchema, semTable *semantics.SemTable, sel []abstract.SelectExpr) bool {
	for _, expr := range sel {
		exp, err := expr.GetExpr()
		if err != nil {
			// TODO: handle star expression error
			return false
		}
		if exprHasUniqueVindex(vschema, semTable, exp) {
			return true
		}
	}
	return false
}

// needDistinctHandling returns true if oa needs to handle the distinct clause.
// If true, it will also return the aliased expression that needs to be pushed
// down into the underlying route.
func (hp *horizonPlanning) needDistinctHandling(
	ctx *plancontext.PlanningContext,
	funcExpr *sqlparser.FuncExpr,
	opcode engine.AggregateOpcode,
	input logicalPlan,
) (bool, *sqlparser.AliasedExpr, error) {
	if !funcExpr.Distinct {
		return false, nil, nil
	}
	if opcode != engine.AggregateCount && opcode != engine.AggregateSum {
		return false, nil, nil
	}
	innerAliased, ok := funcExpr.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return false, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "syntax error: %s", sqlparser.String(funcExpr))
	}
	_, ok = input.(*routeGen4)
	if !ok {
		// Unreachable
		return true, innerAliased, nil
	}
	if exprHasUniqueVindex(ctx.VSchema, ctx.SemTable, innerAliased.Expr) {
		// if we can see a unique vindex on this table/column,
		// we know the results will be unique, and we don't need to DISTINCTify them
		return false, nil, nil
	}
	return true, innerAliased, nil
}

func (hp *horizonPlanning) planHaving(ctx *plancontext.PlanningContext, plan logicalPlan) (logicalPlan, error) {
	if hp.sel.Having == nil {
		return plan, nil
	}
	return pushHaving(ctx, hp.sel.Having.Expr, plan)
}

func pushHaving(ctx *plancontext.PlanningContext, expr sqlparser.Expr, plan logicalPlan) (logicalPlan, error) {
	switch node := plan.(type) {
	case *routeGen4:
		sel := sqlparser.GetFirstSelect(node.Select)
		sel.AddHaving(expr)
		return plan, nil
	case *pulloutSubquery:
		return pushHaving(ctx, expr, node.underlying)
	case *simpleProjection:
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: filtering on results of cross-shard derived table")
	case *orderedAggregate:
		return newFilter(ctx, plan, expr)
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unreachable %T.filtering", plan)
}

func isJoin(plan logicalPlan) bool {
	switch plan.(type) {
	case *joinGen4, *hashJoin:
		return true
	default:
		return false
	}
}

func exprHasUniqueVindex(vschema plancontext.VSchema, semTable *semantics.SemTable, expr sqlparser.Expr) bool {
	col, isCol := expr.(*sqlparser.ColName)
	if !isCol {
		return false
	}
	ts := semTable.RecursiveDeps(expr)
	tableInfo, err := semTable.TableInfoFor(ts)
	if err != nil {
		return false
	}
	tableName, err := tableInfo.Name()
	if err != nil {
		return false
	}
	vschemaTable, _, _, _, _, err := vschema.FindTableOrVindex(tableName)
	if err != nil {
		return false
	}
	for _, vindex := range vschemaTable.ColumnVindexes {
		if len(vindex.Columns) > 1 || !vindex.IsUnique() {
			return false
		}
		if col.Name.Equal(vindex.Columns[0]) {
			return true
		}
	}
	return false
}

func planSingleShardRoutePlan(sel sqlparser.SelectStatement, rb *routeGen4) error {
	err := stripDownQuery(sel, rb.Select)
	if err != nil {
		return err
	}
	sqlparser.Rewrite(rb.Select, func(cursor *sqlparser.Cursor) bool {
		if aliasedExpr, ok := cursor.Node().(sqlparser.SelectExpr); ok {
			removeKeyspaceFromSelectExpr(aliasedExpr)
		}
		return true
	}, nil)
	return nil
}

func removeKeyspaceFromSelectExpr(expr sqlparser.SelectExpr) {
	switch expr := expr.(type) {
	case *sqlparser.AliasedExpr:
		sqlparser.RemoveKeyspaceFromColName(expr.Expr)
	case *sqlparser.StarExpr:
		expr.TableName.Qualifier = sqlparser.NewTableIdent("")
	}
}

func stripDownQuery(from, to sqlparser.SelectStatement) error {
	var err error

	switch node := from.(type) {
	case *sqlparser.Select:
		toNode, ok := to.(*sqlparser.Select)
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "AST did not match")
		}
		toNode.Distinct = node.Distinct
		toNode.GroupBy = node.GroupBy
		toNode.Having = node.Having
		toNode.OrderBy = node.OrderBy
		toNode.Comments = node.Comments
		toNode.SelectExprs = node.SelectExprs
		for _, expr := range toNode.SelectExprs {
			removeKeyspaceFromSelectExpr(expr)
		}
	case *sqlparser.Union:
		toNode, ok := to.(*sqlparser.Union)
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "AST did not match")
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
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: this should not happen - we have covered all implementations of SelectStatement %T", from)
	}
	return nil
}
