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
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/vt/vtgate/semantics"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

type horizonPlanning struct {
	sel *sqlparser.Select
	qp  *operators.QueryProjection
}

func (hp *horizonPlanning) planHorizon(ctx *plancontext.PlanningContext, plan logicalPlan, truncateColumns bool) (logicalPlan, error) {
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

	// If the current plan is a simpleProjection, we want to rewrite derived expression.
	// In transformDerivedPlan (operator_transformers.go), derived tables that are not
	// a simple route are put behind a simpleProjection. In this simple projection,
	// every Route will represent the original derived table. Thus, pushing new expressions
	// to those Routes require us to rewrite them.
	// On the other hand, when a derived table is a simple Route, we do not put it under
	// a simpleProjection. We create a new Route that contains the derived table in the
	// FROM clause. Meaning that, when we push expressions to the select list of this
	// new Route, we do not want them to rewrite them.
	if _, isSimpleProj := plan.(*simpleProjection); isSimpleProj {
		oldRewriteDerivedExpr := ctx.RewriteDerivedExpr
		defer func() {
			ctx.RewriteDerivedExpr = oldRewriteDerivedExpr
		}()
		ctx.RewriteDerivedExpr = true
	}

	var err error
	hp.qp, err = operators.CreateQPFromSelect(hp.sel)
	if err != nil {
		return nil, err
	}

	needsOrdering := len(hp.qp.OrderExprs) > 0
	canShortcut := isRoute && hp.sel.Having == nil && !needsOrdering

	// If we still have a HAVING clause, it's because it could not be pushed to the WHERE,
	// so it probably has aggregations
	switch {
	case hp.qp.NeedsAggregation() || hp.sel.Having != nil:
		plan, err = hp.planAggregations(ctx, plan)
		if err != nil {
			return nil, err
		}
		// if we already did sorting, we don't need to do it again
		needsOrdering = needsOrdering && !hp.qp.CanPushDownSorting
	case canShortcut:
		err = planSingleShardRoutePlan(hp.sel, rb)
		if err != nil {
			return nil, err
		}
	default:
		err = pushProjections(ctx, plan, hp.qp.SelectExprs)
		if err != nil {
			return nil, err
		}
	}

	// If we didn't already take care of ORDER BY during aggregation planning, we need to handle it now
	if needsOrdering {
		plan, err = hp.planOrderBy(ctx, hp.qp.OrderExprs, plan)
		if err != nil {
			return nil, err
		}
	}

	plan, err = hp.planDistinct(ctx, plan)
	if err != nil {
		return nil, err
	}

	if !truncateColumns {
		return plan, nil
	}

	plan, err = hp.truncateColumnsIfNeeded(ctx, plan)
	if err != nil {
		return nil, err
	}

	return plan, nil
}

func pushProjections(ctx *plancontext.PlanningContext, plan logicalPlan, selectExprs []operators.SelectExpr) error {
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
	if len(plan.OutputColumns()) == hp.qp.GetColumnCount() {
		return plan, nil
	}
	switch p := plan.(type) {
	case *routeGen4:
		p.eroute.SetTruncateColumnCount(hp.qp.GetColumnCount())
	case *joinGen4, *semiJoin, *hashJoin:
		// since this is a join, we can safely add extra columns and not need to truncate them
	case *orderedAggregate:
		p.truncateColumnCount = hp.qp.GetColumnCount()
	case *memorySort:
		p.truncater.SetTruncateColumnCount(hp.qp.GetColumnCount())
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

		exprs := hp.qp.SelectExprs[0:hp.qp.GetColumnCount()]
		err := pushProjections(ctx, plan, exprs)
		if err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func checkIfAlreadyExists(expr *sqlparser.AliasedExpr, node sqlparser.SelectStatement, semTable *semantics.SemTable) int {
	// Here to find if the expr already exists in the SelectStatement, we have 3 cases
	// input is a Select -> In this case we want to search in the select
	// input is a Union -> In this case we want to search in the First Select of the Union
	// input is a Parenthesised Select -> In this case we want to search in the select
	// all these three cases are handled by the call to GetFirstSelect.
	sel := sqlparser.GetFirstSelect(node)

	exprCol, isExprCol := expr.Expr.(*sqlparser.ColName)

	// first pass - search for aliased expressions
	for i, selectExpr := range sel.SelectExprs {
		if !isExprCol {
			break
		}

		selectExpr, ok := selectExpr.(*sqlparser.AliasedExpr)
		if ok && selectExpr.As.Equal(exprCol.Name) {
			return i
		}
	}

	// next pass - we are searching the actual expressions and not the aliases
	for i, selectExpr := range sel.SelectExprs {
		selectExpr, ok := selectExpr.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}

		if semTable.EqualsExpr(expr.Expr, selectExpr.Expr) {
			return i
		}
	}
	return -1
}

func (hp *horizonPlanning) planAggregations(ctx *plancontext.PlanningContext, plan logicalPlan) (logicalPlan, error) {
	isPushable := !isJoin(plan)
	grouping := hp.qp.GetGrouping()
	vindexOverlapWithGrouping := hasUniqueVindex(ctx.SemTable, grouping)
	if isPushable && vindexOverlapWithGrouping {
		// If we have a plan that we can push the group by and aggregation through, we don't need to do aggregation
		// at the vtgate level at all
		err := hp.planAggregationWithoutOA(ctx, plan)
		if err != nil {
			return nil, err
		}
		resultPlan, err := hp.planOrderBy(ctx, hp.qp.OrderExprs, plan)
		if err != nil {
			return nil, err
		}

		newPlan, err := hp.planHaving(ctx, resultPlan)
		if err != nil {
			return nil, err
		}

		return newPlan, nil
	}

	return hp.planAggrUsingOA(ctx, plan, grouping)
}

func (hp *horizonPlanning) planAggrUsingOA(
	ctx *plancontext.PlanningContext,
	plan logicalPlan,
	grouping []operators.GroupBy,
) (logicalPlan, error) {
	oa := &orderedAggregate{
		groupByKeys: make([]*engine.GroupByParams, 0, len(grouping)),
	}

	var order []operators.OrderBy
	if hp.qp.CanPushDownSorting {
		hp.qp.AlignGroupByAndOrderBy()
		// the grouping order might have changed, so we reload the grouping expressions
		grouping = hp.qp.GetGrouping()
		order = hp.qp.OrderExprs
	} else {
		for _, expr := range grouping {
			order = append(order, expr.AsOrderBy())
		}
	}

	// here we are building up the grouping keys for the OA,
	// but they are lacking the input offsets because we have yet to push the columns down
	for _, expr := range grouping {
		oa.groupByKeys = append(oa.groupByKeys, &engine.GroupByParams{
			Expr:        expr.Inner,
			FromGroupBy: true,
			CollationID: ctx.SemTable.CollationForExpr(expr.Inner),
		})
	}

	if hp.sel.Having != nil {
		rewriter := hp.qp.AggrRewriter()
		sqlparser.Rewrite(hp.sel.Having.Expr, rewriter.Rewrite(), nil)
		if rewriter.Err != nil {
			return nil, rewriter.Err
		}
	}

	aggregationExprs, err := hp.qp.AggregationExpressions()
	if err != nil {
		return nil, err
	}

	// If we have a distinct aggregating expression,
	// we handle it by pushing it down to the underlying input as a grouping column
	distinctGroupBy, distinctOffsets, aggrs, err := hp.handleDistinctAggr(ctx, aggregationExprs)
	if err != nil {
		return nil, err
	}

	if len(distinctGroupBy) > 0 {
		grouping = append(grouping, distinctGroupBy...)
		// all the distinct grouping aggregates use the same expression, so it should be OK to just add it once
		order = append(order, distinctGroupBy[0].AsOrderBy())
		oa.preProcess = true
	}

	newPlan, groupingOffsets, aggrParamOffsets, pushed, err := hp.pushAggregation(ctx, plan, grouping, aggrs, false)
	if err != nil {
		return nil, err
	}
	if !pushed {
		oa.preProcess = true
		oa.aggrOnEngine = true
	}

	plan = newPlan

	_, isRoute := plan.(*routeGen4)
	needsProj := !isRoute
	var aggPlan = plan
	var proj *projection
	if needsProj {
		length := getLengthOfProjection(groupingOffsets, aggrs)
		proj = &projection{
			source:      plan,
			columns:     make([]sqlparser.Expr, length),
			columnNames: make([]string, length),
		}
		aggPlan = proj
	}

	aggrParams, err := generateAggregateParams(aggrs, aggrParamOffsets, proj, pushed)
	if err != nil {
		return nil, err
	}

	if proj != nil {
		groupingOffsets, err = passGroupingColumns(proj, groupingOffsets, grouping)
		if err != nil {
			return nil, err
		}
	}

	// Next we add the aggregation expressions and grouping offsets to the OA
	addColumnsToOA(ctx, oa, distinctGroupBy, aggrParams, distinctOffsets, groupingOffsets, aggregationExprs)

	aggPlan, err = hp.planOrderBy(ctx, order, aggPlan)
	if err != nil {
		return nil, err
	}

	oa.resultsBuilder = resultsBuilder{
		logicalPlanCommon: newBuilderCommon(aggPlan),
		weightStrings:     make(map[*resultColumn]int),
	}

	return hp.planHaving(ctx, oa)
}

func passGroupingColumns(proj *projection, groupings []offsets, grouping []operators.GroupBy) (projGrpOffsets []offsets, err error) {
	for idx, grp := range groupings {
		origGrp := grouping[idx]
		var offs offsets
		expr := origGrp.AsAliasedExpr()
		offs.col, err = proj.addColumn(origGrp.InnerIndex, sqlparser.NewOffset(grp.col, expr.Expr), expr.ColumnName())
		if err != nil {
			return nil, err
		}
		if grp.wsCol != -1 {
			offs.wsCol, err = proj.addColumn(nil, sqlparser.NewOffset(grp.wsCol, weightStringFor(expr.Expr)), "")
			if err != nil {
				return nil, err
			}
		}
		projGrpOffsets = append(projGrpOffsets, offs)
	}
	return projGrpOffsets, nil
}

func generateAggregateParams(aggrs []operators.Aggr, aggrParamOffsets [][]offsets, proj *projection, pushed bool) ([]*engine.AggregateParams, error) {
	aggrParams := make([]*engine.AggregateParams, len(aggrs))
	for idx, paramOffset := range aggrParamOffsets {
		aggr := aggrs[idx]
		incomingOffset := paramOffset[0].col
		var offset int
		if proj != nil {
			var aggrExpr sqlparser.Expr
			for _, ofs := range paramOffset {
				curr := &sqlparser.Offset{V: ofs.col}
				if aggrExpr == nil {
					aggrExpr = curr
				} else {
					aggrExpr = &sqlparser.BinaryExpr{
						Operator: sqlparser.MultOp,
						Left:     aggrExpr,
						Right:    curr,
					}
				}
			}

			pos, err := proj.addColumn(aggr.Index, aggrExpr, aggr.Alias)
			if err != nil {
				return nil, err
			}
			offset = pos
		} else {
			offset = incomingOffset
		}

		opcode := engine.AggregateSum
		switch aggr.OpCode {
		case engine.AggregateMin, engine.AggregateMax, engine.AggregateRandom:
			opcode = aggr.OpCode
		case engine.AggregateCount, engine.AggregateCountStar, engine.AggregateCountDistinct, engine.AggregateSumDistinct:
			if !pushed {
				opcode = aggr.OpCode
			}
		}

		aggrParams[idx] = &engine.AggregateParams{
			Opcode:     opcode,
			Col:        offset,
			Alias:      aggr.Alias,
			Expr:       aggr.Original.Expr,
			Original:   aggr.Original,
			OrigOpcode: aggr.OpCode,
		}
	}
	return aggrParams, nil
}

func addColumnsToOA(
	ctx *plancontext.PlanningContext,
	oa *orderedAggregate,
	// these are the group by expressions that where added because we have unique aggregations
	distinctGroupBy []operators.GroupBy,
	// these are the aggregate params we already have for non-distinct aggregations
	aggrParams []*engine.AggregateParams,
	// distinctOffsets mark out where we need to use the distinctGroupBy offsets
	// to create *engine.AggregateParams for the distinct aggregations
	distinctOffsets []int,
	// these are the offsets for the group by params
	groupings []offsets,
	// aggregationExprs are all the original aggregation expressions the query requested
	aggregationExprs []operators.Aggr,
) {
	if len(distinctGroupBy) == 0 {
		// no distinct aggregations
		oa.aggregates = aggrParams
	} else {
		count := len(groupings) - len(distinctOffsets)
		addDistinctAggr := func(offset int) {
			// the last grouping we pushed is the one we added for the distinct aggregation
			o := groupings[count]
			count++
			a := aggregationExprs[offset]
			collID := ctx.SemTable.CollationForExpr(a.Func.GetArg())
			oa.aggregates = append(oa.aggregates, &engine.AggregateParams{
				Opcode:      a.OpCode,
				Col:         o.col,
				KeyCol:      o.col,
				WAssigned:   o.wsCol >= 0,
				WCol:        o.wsCol,
				Alias:       a.Alias,
				Original:    a.Original,
				CollationID: collID,
			})
		}
		lastOffset := distinctOffsets[len(distinctOffsets)-1]
		distinctIdx := 0
		for i := 0; i <= lastOffset || i <= len(aggrParams); i++ {
			for distinctIdx < len(distinctOffsets) && i == distinctOffsets[distinctIdx] {
				// we loop here since we could be dealing with multiple distinct aggregations after each other
				addDistinctAggr(i)
				distinctIdx++
			}
			if i < len(aggrParams) {
				oa.aggregates = append(oa.aggregates, aggrParams[i])
			}
		}

		// we have to remove the tail of the grouping offsets, so we only have the offsets for the GROUP BY in the query
		groupings = groupings[:len(groupings)-len(distinctOffsets)]
	}

	for i, grouping := range groupings {
		oa.groupByKeys[i].KeyCol = grouping.col
		oa.groupByKeys[i].WeightStringCol = grouping.wsCol
	}
}

// handleDistinctAggr takes in a slice of aggregations and returns GroupBy elements that replace
// the distinct aggregations in the input, along with a slice of offsets and the non-distinct aggregations left,
// so we can later reify the original aggregations
func (hp *horizonPlanning) handleDistinctAggr(ctx *plancontext.PlanningContext, exprs []operators.Aggr) (
	distincts []operators.GroupBy, offsets []int, aggrs []operators.Aggr, err error) {
	var distinctExpr sqlparser.Expr
	for i, expr := range exprs {
		if !expr.Distinct {
			aggrs = append(aggrs, expr)
			continue
		}

		inner, innerWS, err := hp.qp.GetSimplifiedExpr(expr.Func.GetArg())
		if err != nil {
			return nil, nil, nil, err
		}
		if exprHasVindex(ctx.SemTable, innerWS, false) {
			aggrs = append(aggrs, expr)
			continue
		}
		if distinctExpr == nil {
			distinctExpr = innerWS
		} else {
			if !ctx.SemTable.EqualsExpr(distinctExpr, innerWS) {
				err = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: only one distinct aggregation allowed in a select: %s", sqlparser.String(expr.Original))
				return nil, nil, nil, err
			}
		}
		distincts = append(distincts, operators.GroupBy{
			Inner:         inner,
			WeightStrExpr: innerWS,
			InnerIndex:    expr.Index,
		})
		offsets = append(offsets, i)
	}
	return
}

func (hp *horizonPlanning) planAggregationWithoutOA(ctx *plancontext.PlanningContext, plan logicalPlan) error {
	for _, expr := range hp.qp.SelectExprs {
		aliasedExpr, err := expr.GetAliasedExpr()
		if err != nil {
			return err
		}
		_, _, err = pushProjection(ctx, aliasedExpr, plan, true, false, false)
		if err != nil {
			return err
		}
	}
	for _, expr := range hp.qp.GetGrouping() {
		// since all the grouping will be done at the mysql level,
		// we know that we won't need any weight_string() calls
		err := planGroupByGen4(ctx, expr, plan /*weighString*/, false)
		if err != nil {
			return err
		}
	}
	return nil
}

type offsets struct {
	col, wsCol int
}

func newOffset(col int) offsets {
	return offsets{col: col, wsCol: -1}
}

func (hp *horizonPlanning) createGroupingsForColumns(columns []*sqlparser.ColName) ([]operators.GroupBy, error) {
	var lhsGrouping []operators.GroupBy
	for _, lhsColumn := range columns {
		expr, wsExpr, err := hp.qp.GetSimplifiedExpr(lhsColumn)
		if err != nil {
			return nil, err
		}

		lhsGrouping = append(lhsGrouping, operators.GroupBy{
			Inner:         expr,
			WeightStrExpr: wsExpr,
		})
	}
	return lhsGrouping, nil
}

func hasUniqueVindex(semTable *semantics.SemTable, groupByExprs []operators.GroupBy) bool {
	for _, groupByExpr := range groupByExprs {
		if exprHasUniqueVindex(semTable, groupByExpr.WeightStrExpr) {
			return true
		}
	}
	return false
}

func (hp *horizonPlanning) planOrderBy(ctx *plancontext.PlanningContext, orderExprs []operators.OrderBy, plan logicalPlan) (logicalPlan, error) {
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
		var orderExprsWithoutNils []operators.OrderBy
		for _, expr := range orderExprs {
			if sqlparser.IsNull(expr.Inner.Expr) {
				continue
			}
			orderExprsWithoutNils = append(orderExprsWithoutNils, expr)
		}
		orderExprs = orderExprsWithoutNils

		for _, order := range orderExprs {
			if sqlparser.ContainsAggregation(order.WeightStrExpr) {
				ms, err := createMemorySortPlanOnAggregation(ctx, plan, orderExprs)
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
	case *limit, *semiJoin, *filter, *pulloutSubquery, *projection:
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

func isSpecialOrderBy(o operators.OrderBy) bool {
	if sqlparser.IsNull(o.Inner.Expr) {
		return true
	}
	f, isFunction := o.Inner.Expr.(*sqlparser.FuncExpr)
	return isFunction && f.Name.Lowered() == "rand"
}

func planOrderByForRoute(ctx *plancontext.PlanningContext, orderExprs []operators.OrderBy, plan *routeGen4, hasStar bool) (logicalPlan, error) {
	for _, order := range orderExprs {
		err := checkOrderExprCanBePlannedInScatter(plan, order, hasStar)
		if err != nil {
			return nil, err
		}
		plan.Select.AddOrder(order.Inner)
		if isSpecialOrderBy(order) {
			continue
		}
		var wsExpr sqlparser.Expr
		if ctx.SemTable.NeedsWeightString(order.Inner.Expr) {
			wsExpr = order.WeightStrExpr
		}

		offset, weightStringOffset, err := wrapAndPushExpr(ctx, order.Inner.Expr, wsExpr, plan)
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
func checkOrderExprCanBePlannedInScatter(plan *routeGen4, order operators.OrderBy, hasStar bool) error {
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
		switch unary := expr.(type) {
		case *sqlparser.CastExpr:
			expr = unary.Expr
		case *sqlparser.ConvertExpr:
			expr = unary.Expr
		}
		if !sqlparser.IsColName(expr) {
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
		aliasedExpr := &sqlparser.AliasedExpr{Expr: weightStringFor(weightStrExpr)}
		weightStringOffset, _, err = pushProjection(ctx, aliasedExpr, plan, true, true, false)
		if err != nil {
			return 0, 0, err
		}
	}
	return offset, weightStringOffset, nil
}

func weightStringFor(expr sqlparser.Expr) sqlparser.Expr {
	return &sqlparser.WeightStringFuncExpr{Expr: expr}
}

func (hp *horizonPlanning) planOrderByForHashJoin(ctx *plancontext.PlanningContext, orderExprs []operators.OrderBy, plan *hashJoin) (logicalPlan, error) {
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

func (hp *horizonPlanning) planOrderByForJoin(ctx *plancontext.PlanningContext, orderExprs []operators.OrderBy, plan *joinGen4) (logicalPlan, error) {
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
	// We can only push down sorting on the LHS of the join.
	// If the order is on the RHS, we need to do the sorting on the vtgate
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

func createMemorySortPlanOnAggregation(ctx *plancontext.PlanningContext, plan *orderedAggregate, orderExprs []operators.OrderBy) (logicalPlan, error) {
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
		offset, woffset, found := findExprInOrderedAggr(plan, order)
		if !found {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "expected to find the order by expression (%s) in orderedAggregate", sqlparser.String(order.Inner))
		}

		collationID := ctx.SemTable.CollationForExpr(order.WeightStrExpr)
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

func findExprInOrderedAggr(plan *orderedAggregate, order operators.OrderBy) (keyCol int, weightStringCol int, found bool) {
	for _, key := range plan.groupByKeys {
		if sqlparser.EqualsExpr(order.WeightStrExpr, key.Expr) ||
			sqlparser.EqualsExpr(order.Inner.Expr, key.Expr) {
			return key.KeyCol, key.WeightStringCol, true
		}
	}
	for _, aggregate := range plan.aggregates {
		if sqlparser.EqualsExpr(order.WeightStrExpr, aggregate.Original.Expr) ||
			sqlparser.EqualsExpr(order.Inner.Expr, aggregate.Original.Expr) {
			return aggregate.Col, -1, true
		}
	}
	return 0, 0, false
}

func (hp *horizonPlanning) createMemorySortPlan(ctx *plancontext.PlanningContext, plan logicalPlan, orderExprs []operators.OrderBy, useWeightStr bool) (logicalPlan, error) {
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

func orderExprsDependsOnTableSet(orderExprs []operators.OrderBy, semTable *semantics.SemTable, ts semantics.TableSet) bool {
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
		if p.isSingleShard() || selectHasUniqueVindex(ctx.SemTable, hp.qp.SelectExprs) {
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
			if semTable.EqualsExpr(expr, grpParam.Expr) {
				found = true
				oa.groupByKeys = append(oa.groupByKeys, grpParam)
				break
			}
		}
		if found {
			continue
		}
		for _, aggrParam := range currPlan.aggregates {
			if semTable.EqualsExpr(expr, aggrParam.Expr) {
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
	var orderExprs []operators.OrderBy
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
		grpParam := &engine.GroupByParams{KeyCol: index, WeightStringCol: -1, CollationID: ctx.SemTable.CollationForExpr(inner), Expr: inner}
		_, wOffset, err := wrapAndPushExpr(ctx, aliasExpr.Expr, aliasExpr.Expr, plan)
		if err != nil {
			return nil, err
		}
		grpParam.WeightStringCol = wOffset
		groupByKeys = append(groupByKeys, grpParam)

		orderExprs = append(orderExprs, operators.OrderBy{
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

func isAmbiguousOrderBy(index int, col sqlparser.IdentifierCI, exprs []operators.SelectExpr) bool {
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

func selectHasUniqueVindex(semTable *semantics.SemTable, sel []operators.SelectExpr) bool {
	for _, expr := range sel {
		exp, err := expr.GetExpr()
		if err != nil {
			// TODO: handle star expression error
			return false
		}
		if exprHasUniqueVindex(semTable, exp) {
			return true
		}
	}
	return false
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

func exprHasUniqueVindex(semTable *semantics.SemTable, expr sqlparser.Expr) bool {
	return exprHasVindex(semTable, expr, true)
}

func exprHasVindex(semTable *semantics.SemTable, expr sqlparser.Expr, hasToBeUnique bool) bool {
	col, isCol := expr.(*sqlparser.ColName)
	if !isCol {
		return false
	}
	ts := semTable.RecursiveDeps(expr)
	tableInfo, err := semTable.TableInfoFor(ts)
	if err != nil {
		return false
	}
	vschemaTable := tableInfo.GetVindexTable()
	for _, vindex := range vschemaTable.ColumnVindexes {
		if len(vindex.Columns) > 1 || hasToBeUnique && !vindex.IsUnique() {
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
		expr.TableName.Qualifier = sqlparser.NewIdentifierCS("")
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

func planGroupByGen4(ctx *plancontext.PlanningContext, groupExpr operators.GroupBy, plan logicalPlan, wsAdded bool) error {
	switch node := plan.(type) {
	case *routeGen4:
		sel := node.Select.(*sqlparser.Select)
		sel.AddGroupBy(groupExpr.Inner)
		// If a weight_string function is added to the select list,
		// then we need to add that to the group by clause otherwise the query will fail on mysql with full_group_by error
		// as the weight_string function might not be functionally dependent on the group by.
		if wsAdded {
			sel.AddGroupBy(weightStringFor(groupExpr.WeightStrExpr))
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

func getLengthOfProjection(groupingOffsets []offsets, aggregations []operators.Aggr) int {
	length := 0
	for _, groupBy := range groupingOffsets {
		if groupBy.wsCol != -1 {
			length++
		}
		length++
	}
	length += len(aggregations)
	return length
}
