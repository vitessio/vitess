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

package planbuilder

import (
	"strconv"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// pushAggregation pushes grouping and aggregation as far down in the tree as possible
// the output `outputAggrsOffset` needs a little explaining: this is the offsets for aggregation - remember
// that aggregation can be broken down into multiple expressions that are later combined.
// this is why this output is a slice of slices
func (hp *horizonPlanning) pushAggregation(
	ctx *plancontext.PlanningContext,
	plan logicalPlan,
	grouping []operators.GroupBy,
	aggregations []operators.Aggr,
	ignoreOutputOrder bool,
) (output logicalPlan,
	groupingOffsets []offsets,
	outputAggrsOffset [][]offsets,
	pushed bool,
	err error) {
	pushed = true
	switch plan := plan.(type) {
	case *routeGen4:
		output = plan
		groupingOffsets, outputAggrsOffset, _, err = pushAggrOnRoute(ctx, plan, aggregations, grouping, ignoreOutputOrder)
		return

	case *joinGen4:
		output = plan
		groupingOffsets, outputAggrsOffset, err = hp.pushAggrOnJoin(ctx, plan, grouping, aggregations)
		return

	case *semiJoin:
		output = plan
		groupingOffsets, outputAggrsOffset, pushed, err = hp.pushAggrOnSemiJoin(ctx, plan, grouping, aggregations, ignoreOutputOrder)
		return

	case *simpleProjection:
		// we just remove the simpleProjection. We are doing an OA on top anyway, so no need to clean up the output columns
		return hp.pushAggregation(ctx, plan.input, grouping, aggregations, ignoreOutputOrder)

	case *limit:
		// if we are seeing a limit, it's because we are building on top of a derived table.
		output = plan
		pushed = false

		for _, grp := range grouping {
			offset, wOffset, err := wrapAndPushExpr(ctx, grp.Inner, grp.WeightStrExpr, plan.input)
			if err != nil {
				return nil, nil, nil, false, err
			}
			groupingOffsets = append(groupingOffsets, offsets{
				col:   offset,
				wsCol: wOffset,
			})
		}

		for _, aggr := range aggregations {
			var offset int
			aggrExpr, ok := aggr.Original.Expr.(sqlparser.AggrFunc)
			if !ok {
				return nil, nil, nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG]: unexpected expression: %v", aggr.Original)
			}

			switch aggrExpr.(type) {
			case *sqlparser.CountStar:
				offset = 0
			default:
				if len(aggrExpr.GetArgs()) != 1 {
					return nil, nil, nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG]: unexpected expression: %v", aggrExpr)
				}
				offset, _, err = pushProjection(ctx, &sqlparser.AliasedExpr{Expr: aggrExpr.GetArg() /*As: expr.As*/}, plan.input, true, true, false)
			}

			if err != nil {
				return nil, nil, nil, false, err
			}

			outputAggrsOffset = append(outputAggrsOffset, []offsets{newOffset(offset)})
		}

		return
	default:
		err = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "using aggregation on top of a %T plan is not yet supported", plan)
		return
	}
}

func pushAggrOnRoute(
	ctx *plancontext.PlanningContext,
	plan *routeGen4,
	aggregations []operators.Aggr,
	grouping []operators.GroupBy,
	ignoreOutputOrder bool,
) (
	groupingOffsets []offsets,
	vtgateAggregation [][]offsets,
	nonAggrOffsets []offsets,
	err error,
) {
	columnOrderMatters := !ignoreOutputOrder
	sel, isSel := plan.Select.(*sqlparser.Select)
	if !isSel {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't plan aggregation on union")
	}

	var groupingCols []int
	var reorg = passThrough

	if columnOrderMatters {
		// During this first run, we push the projections for the normal columns (not the weigh_string ones, that is)
		// in the order that the user asked for it
		// sortOffsets also returns a reorgFunc,
		// that can be used to rearrange the produced outputs to the original order
		var it *sortedIterator
		var err error
		grouping, reorg, it = sortOffsets(grouping, aggregations)
		vtgateAggregation, groupingCols, err = pushAggrsAndGroupingInOrder(ctx, plan, it, sel, vtgateAggregation, groupingCols)
		if err != nil {
			return nil, nil, nil, err
		}
	} else {
		// if we haven't already pushed the aggregations, now is the time
		for _, aggregation := range aggregations {
			param := addAggregationToSelect(sel, aggregation)
			vtgateAggregation = append(vtgateAggregation, []offsets{param})
		}
	}

	groupingOffsets = make([]offsets, 0, len(grouping))
	for idx, expr := range grouping {
		sel.AddGroupBy(expr.Inner)
		var pos offsets
		if ignoreOutputOrder {
			// we have not yet pushed anything, so we need to push the expression first
			col, _, err := addExpressionToRoute(ctx, plan, &sqlparser.AliasedExpr{Expr: expr.Inner}, true)
			if err != nil {
				return nil, nil, nil, err
			}
			pos = newOffset(col)
		} else {
			pos = newOffset(groupingCols[idx])
		}

		if expr.WeightStrExpr != nil && ctx.SemTable.NeedsWeightString(expr.Inner) {
			wsExpr := weightStringFor(expr.WeightStrExpr)
			wsCol, _, err := addExpressionToRoute(ctx, plan, &sqlparser.AliasedExpr{Expr: wsExpr}, true)
			if err != nil {
				return nil, nil, nil, err
			}
			pos.wsCol = wsCol
			sel.AddGroupBy(wsExpr)
		}
		groupingOffsets = append(groupingOffsets, pos)
	}

	groupingOffsets, vtgateAggregation = reorg(groupingOffsets, vtgateAggregation)
	return groupingOffsets, vtgateAggregation, nil, nil
}

func pushAggrsAndGroupingInOrder(
	ctx *plancontext.PlanningContext,
	plan *routeGen4,
	it *sortedIterator,
	sel *sqlparser.Select,
	vtgateAggregation [][]offsets,
	groupingCols []int,
) ([][]offsets, []int, error) {
	for it.next() {
		groupBy, aggregation := it.current()
		if aggregation != nil {
			param := addAggregationToSelect(sel, *aggregation)
			vtgateAggregation = append(vtgateAggregation, []offsets{param})
			continue
		}
		if groupBy != nil {
			reuseCol := groupBy.InnerIndex == nil
			col, _, err := addExpressionToRoute(ctx, plan, groupBy.AsAliasedExpr(), reuseCol)
			groupingCols = append(groupingCols, col)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	return vtgateAggregation, groupingCols, nil
}

// addAggregationToSelect adds the aggregation to the SELECT statement and returns the AggregateParams to be used outside
func addAggregationToSelect(sel *sqlparser.Select, aggregation operators.Aggr) offsets {
	// TODO: removing duplicated aggregation expression should also be done at the join level
	for i, expr := range sel.SelectExprs {
		aliasedExpr, isAliasedExpr := expr.(*sqlparser.AliasedExpr)
		if !isAliasedExpr {
			continue
		}
		if sqlparser.EqualsExpr(aliasedExpr.Expr, aggregation.Original.Expr) {
			return newOffset(i)
		}
	}

	sel.SelectExprs = append(sel.SelectExprs, aggregation.Original)
	return newOffset(len(sel.SelectExprs) - 1)
}

func countStarAggr() *operators.Aggr {
	f := &sqlparser.CountStar{}

	return &operators.Aggr{
		Original: &sqlparser.AliasedExpr{Expr: f},
		OpCode:   engine.AggregateCountStar,
		Alias:    "count(*)",
	}
}

/*
We push down aggregations using the logic from the paper Orthogonal Optimization of Subqueries and Aggregation, by
Cesar A. Galindo-Legaria and Milind M. Joshi from Microsoft Corp.

It explains how one can split an aggregation into local aggregates that depend on only one side of the join.
The local aggregates can then be gathered together to produce the global
group by/aggregate query that the user asked for.

In Vitess, this is particularly useful because it allows us to push aggregation down to the routes, even when
we have to join the results at the vtgate level. Instead of doing all the grouping and aggregation at the
vtgate level, we can offload most of the work to MySQL, and at the vtgate just summarize the results.
*/
func (hp *horizonPlanning) pushAggrOnJoin(
	ctx *plancontext.PlanningContext,
	join *joinGen4,
	grouping []operators.GroupBy,
	aggregations []operators.Aggr,
) ([]offsets, [][]offsets, error) {
	// First we separate aggregations according to which side the dependencies are coming from
	lhsAggrs, rhsAggrs, err := splitAggregationsToLeftAndRight(ctx, aggregations, join)
	if err != nil {
		return nil, nil, err
	}

	// We need to group by the columns used in the join condition.
	// If we don't, the LHS will not be able to return the column, and it can't be used to send down to the RHS
	lhsCols, err := hp.createGroupingsForColumns(join.LHSColumns)
	if err != nil {
		return nil, nil, err
	}

	// Here we split the grouping depending on if they should with the LHS or RHS of the query
	// This is done by using the semantic table and checking dependencies
	lhsGrouping, rhsGrouping, groupingOffsets, err := splitGroupingsToLeftAndRight(ctx, join, grouping, lhsCols)
	if err != nil {
		return nil, nil, err
	}

	// If the rhs has no grouping column then a count(*) will return 0 from the query and will get mapped to the record from left hand side.
	// This is an incorrect behaviour as the join condition has not matched, so we add a literal 1 to the select query and also group by on it.
	// So that only if join condition matches the records will be mapped and returned.
	if len(rhsGrouping) == 0 && len(rhsAggrs) != 0 {
		l := sqlparser.NewIntLiteral("1")
		aExpr := &sqlparser.AliasedExpr{
			Expr: l,
		}
		offset, _, err := pushProjection(ctx, aExpr, join.Right, true, true, false)
		if err != nil {
			return nil, nil, err
		}
		l = sqlparser.NewIntLiteral(strconv.Itoa(offset + 1))
		rhsGrouping = append(rhsGrouping, operators.GroupBy{Inner: l})
	}

	// Next we push the aggregations to both sides
	newLHS, lhsOffsets, lhsAggrOffsets, _, err := hp.filteredPushAggregation(ctx, join.Left, lhsGrouping, lhsAggrs, true)
	if err != nil {
		return nil, nil, err
	}

	newRHS, rhsOffsets, rhsAggrOffsets, _, err := hp.filteredPushAggregation(ctx, join.Right, rhsGrouping, rhsAggrs, true)
	if err != nil {
		return nil, nil, err
	}
	join.Left, join.Right = newLHS, newRHS

	// Next, we have to pass through the grouping values through the join and the projection we add on top
	// We added new groupings to the LHS because of the join condition, so we don't want to pass through everything,
	// just the groupings that are used by operators on top of this current one
	wsOutputGrpOffset := len(groupingOffsets) + len(join.Cols)
	outputGroupings := make([]offsets, 0, len(groupingOffsets))
	var wsOffsets []int
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
		outputGrouping := newOffset(len(join.Cols))
		join.Cols = append(join.Cols, f(offset.col))
		if offset.wsCol > -1 {
			// we add the weight_string calls at the end of the join columns
			outputGrouping.wsCol = wsOutputGrpOffset + len(wsOffsets)
			wsOffsets = append(wsOffsets, f(offset.wsCol))
		}
		outputGroupings = append(outputGroupings, outputGrouping)
	}
	join.Cols = append(join.Cols, wsOffsets...)

	outputAggrOffsets := make([][]offsets, 0, len(aggregations))
	for idx := range aggregations {
		l, r := lhsAggrOffsets[idx], rhsAggrOffsets[idx]
		var offSlice []offsets
		for _, off := range l {
			offSlice = append(offSlice, newOffset(len(join.Cols)))
			join.Cols = append(join.Cols, -(off.col + 1))
		}
		for _, off := range r {
			offSlice = append(offSlice, newOffset(len(join.Cols)))
			join.Cols = append(join.Cols, off.col+1)
		}
		outputAggrOffsets = append(outputAggrOffsets, offSlice)
	}
	return outputGroupings, outputAggrOffsets, err
}

/*
pushAggrOnSemiJoin works similarly to pushAggrOnJoin, but it's simpler, because we don't get any inputs from the RHS,
so there are no aggregations or groupings that have to be sent to the RHS

We do however need to add the columns used in the subquery coming from the LHS to the grouping.
That way we get the aggregation grouped by the column we need to use to decide if the row should
*/
func (hp *horizonPlanning) pushAggrOnSemiJoin(
	ctx *plancontext.PlanningContext,
	join *semiJoin,
	grouping []operators.GroupBy,
	aggregations []operators.Aggr,
	ignoreOutputOrder bool,
) ([]offsets, [][]offsets, bool, error) {
	// We need to group by the columns used in the join condition.
	// If we don't, the LHS will not be able to return the column, and it can't be used to send down to the RHS
	lhsCols, err := hp.createGroupingsForColumns(join.LHSColumns)
	if err != nil {
		return nil, nil, false, err
	}

	totalGrouping := append(grouping, lhsCols...)
	newLeft, groupingOffsets, aggrParams, pushed, err := hp.pushAggregation(ctx, join.lhs, totalGrouping, aggregations, ignoreOutputOrder)
	if err != nil {
		return nil, nil, false, err
	}
	join.lhs = newLeft

	outputGroupings := make([]offsets, 0, len(grouping))
	for idx := range grouping {
		outputGroupings = append(outputGroupings, groupingOffsets[idx])
	}

	return outputGroupings, aggrParams, pushed, nil
}

// this method takes a slice of aggregations that can have missing spots in the form of `nil`,
// and pushes the non-empty values down.
// during aggregation planning, it's important to know which of
// the incoming aggregations correspond to what is sent to the LHS and RHS.
// Some aggregations only need to be sent to one of the sides of the join, and in that case,
// the other side will have a nil in this offset of the aggregations
func (hp *horizonPlanning) filteredPushAggregation(
	ctx *plancontext.PlanningContext,
	plan logicalPlan,
	grouping []operators.GroupBy,
	aggregations []*operators.Aggr,
	ignoreOutputOrder bool,
) (out logicalPlan, groupingOffsets []offsets, outputAggrs [][]offsets, pushed bool, err error) {
	used := make([]bool, len(aggregations))
	var aggrs []operators.Aggr

	for idx, aggr := range aggregations {
		if aggr != nil {
			used[idx] = true
			aggrs = append(aggrs, *aggr)
		}
	}
	newplan, groupingOffsets, pushedAggrs, pushed, err := hp.pushAggregation(ctx, plan, grouping, aggrs, ignoreOutputOrder)
	if err != nil {
		return nil, nil, nil, pushed, err
	}
	idx := 0
	for _, b := range used {
		if !b {
			outputAggrs = append(outputAggrs, nil)
			continue
		}
		outputAggrs = append(outputAggrs, pushedAggrs[idx])
		idx++
	}
	return newplan, groupingOffsets, outputAggrs, pushed, nil
}

func isMinOrMax(in engine.AggregateOpcode) bool {
	switch in {
	case engine.AggregateMin, engine.AggregateMax:
		return true
	default:
		return false
	}
}

func splitAggregationsToLeftAndRight(
	ctx *plancontext.PlanningContext,
	aggregations []operators.Aggr,
	join *joinGen4,
) ([]*operators.Aggr, []*operators.Aggr, error) {
	var lhsAggrs, rhsAggrs []*operators.Aggr
	for _, aggr := range aggregations {
		newAggr := aggr
		if _, ok := aggr.Original.Expr.(*sqlparser.CountStar); ok {
			lhsAggrs = append(lhsAggrs, &newAggr)
			rhsAggrs = append(rhsAggrs, &newAggr)
		} else {
			deps := ctx.SemTable.RecursiveDeps(aggr.Original.Expr)
			var other *operators.Aggr
			// if we are sending down min/max, we don't have to multiply the results with anything
			if !isMinOrMax(aggr.OpCode) {
				other = countStarAggr()
			}
			switch {
			case deps.IsSolvedBy(join.Left.ContainsTables()):
				lhsAggrs = append(lhsAggrs, &newAggr)
				rhsAggrs = append(rhsAggrs, other)
			case deps.IsSolvedBy(join.Right.ContainsTables()):
				rhsAggrs = append(rhsAggrs, &newAggr)
				lhsAggrs = append(lhsAggrs, other)
			default:
				return nil, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "aggregation on columns from different sources not supported yet")
			}
		}
	}
	return lhsAggrs, rhsAggrs, nil
}

func splitGroupingsToLeftAndRight(
	ctx *plancontext.PlanningContext,
	join *joinGen4,
	grouping, lhsGrouping []operators.GroupBy,
) ([]operators.GroupBy, []operators.GroupBy, []int, error) {
	var rhsGrouping []operators.GroupBy

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

type (
	reorgFunc      = func(groupByOffsets []offsets, aggrOffsets [][]offsets) ([]offsets, [][]offsets)
	sortedIterator struct {
		grouping     []operators.GroupBy
		aggregations []operators.Aggr
		valueGB      *operators.GroupBy
		valueA       *operators.Aggr
		groupbyIdx   int
		aggrIdx      int
	}
)

func (it *sortedIterator) current() (*operators.GroupBy, *operators.Aggr) {
	return it.valueGB, it.valueA
}

func (it *sortedIterator) next() bool {
	if it.aggrIdx < len(it.aggregations) && it.groupbyIdx < len(it.grouping) {
		aggregation := it.aggregations[it.aggrIdx]
		groupBy := it.grouping[it.groupbyIdx]
		if operators.CompareRefInt(aggregation.Index, groupBy.InnerIndex) {
			it.aggrIdx++
			it.valueA, it.valueGB = &aggregation, nil
			return true
		}
		it.groupbyIdx++
		it.valueA, it.valueGB = nil, &groupBy
		return true
	}

	if it.groupbyIdx < len(it.grouping) {
		groupBy := it.grouping[it.groupbyIdx]
		it.groupbyIdx++
		it.valueA, it.valueGB = nil, &groupBy
		return true
	}
	if it.aggrIdx < len(it.aggregations) {
		aggregation := it.aggregations[it.aggrIdx]
		it.aggrIdx++
		it.valueA, it.valueGB = &aggregation, nil
		return true
	}
	return false
}

func passThrough(groupByOffsets []offsets, aggrOffsets [][]offsets) ([]offsets, [][]offsets) {
	return groupByOffsets, aggrOffsets
}

func sortOffsets(grouping []operators.GroupBy, aggregations []operators.Aggr) ([]operators.GroupBy, reorgFunc, *sortedIterator) {
	originalGrouping := make([]operators.GroupBy, len(grouping))
	originalAggr := make([]operators.Aggr, len(aggregations))
	copy(originalAggr, aggregations)
	copy(originalGrouping, grouping)
	operators.SortAggregations(aggregations)
	operators.SortGrouping(grouping)

	reorg := func(groupByOffsets []offsets, aggrOffsets [][]offsets) ([]offsets, [][]offsets) {
		orderedGroupingOffsets := make([]offsets, 0, len(originalGrouping))
		for _, og := range originalGrouping {
			for i, g := range grouping {
				if og.Inner == g.Inner {
					orderedGroupingOffsets = append(orderedGroupingOffsets, groupByOffsets[i])
					break
				}
			}
		}

		orderedAggrs := make([][]offsets, 0, len(originalAggr))
		for _, og := range originalAggr {
			for i, g := range aggregations {
				if og.Original.Expr == g.Original.Expr {
					orderedAggrs = append(orderedAggrs, aggrOffsets[i])
					break
				}
			}
		}

		return orderedGroupingOffsets, orderedAggrs
	}

	return grouping, reorg, &sortedIterator{
		grouping:     grouping,
		aggregations: aggregations,
	}
}
