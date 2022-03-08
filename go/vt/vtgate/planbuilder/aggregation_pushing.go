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
	"sort"

	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/mysql/collations"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// pushAggregation pushes grouping and aggregation as far down in the tree as possible
func (hp *horizonPlanning) pushAggregation(
	ctx *plancontext.PlanningContext,
	plan logicalPlan,
	grouping []abstract.GroupBy,
	aggregations []abstract.Aggr,
	ignoreOutputOrder bool,
) (groupingOffsets []offsets, outputAggrsOffset [][]offsets, err error) {
	switch plan := plan.(type) {
	case *routeGen4:
		groupingOffsets, outputAggrsOffset, err = pushAggrOnRoute(ctx, plan, aggregations, grouping, ignoreOutputOrder)
		if err != nil {
			return nil, nil, err
		}
		return

	case *joinGen4:
		return hp.pushAggrOnJoin(ctx, grouping, aggregations, plan)

	case *semiJoin:
		return hp.pushAggrOnSemiJoin(ctx, grouping, aggregations, plan, ignoreOutputOrder)

	default:
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "using aggregation on top of a %T plan is not yet supported", plan)
	}
}

func pushAggrOnRoute(
	ctx *plancontext.PlanningContext,
	plan *routeGen4,
	aggregations []abstract.Aggr,
	grouping []abstract.GroupBy,
	ignoreOutputOrder bool,
) ([]offsets, [][]offsets, error) {
	sel, isSel := plan.Select.(*sqlparser.Select)
	if !isSel {
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't plan aggregation on union")
	}

	var originalGrouping []abstract.GroupBy
	var vtgateAggregation [][]offsets
	var groupingCols []int

	// aggIdx keeps track of the index of the aggregations we have processed so far. Starts with 0, goes all the way to len(aggregations)
	aggrIdx := 0
	if !ignoreOutputOrder {
		// creating a copy of the original grouping list, so we can reorder them
		// we do the actual copying inside the if statement below
		originalGrouping = make([]abstract.GroupBy, len(grouping))
		vtgateAggregation = make([][]offsets, 0, len(aggregations))
		copy(originalGrouping, grouping)

		// If we care for the output order, we first sort the aggregations and the groupBys independently
		// Then we run a merge sort on the two lists. This ensures that we start pushing the aggregations and groupBys in the ascending order
		// So their ordering in the SELECT statement of the route matches what we intended
		groupbyIdx := 0
		sort.Sort(abstract.Aggrs(aggregations))
		sort.Sort(abstract.GroupBys(grouping))
		for aggrIdx < len(aggregations) && groupbyIdx < len(grouping) {
			aggregation := aggregations[aggrIdx]
			groupBy := grouping[groupbyIdx]
			// Compare the reference of integers, treating nils as columns that go to the end
			// since they don't have any explicit ordering specified
			if abstract.CompareRefInt(aggregation.Index, groupBy.InnerIndex) {
				aggrIdx++
				param := addAggregationToSelect(sel, aggregation)
				vtgateAggregation = append(vtgateAggregation, []offsets{param})
			} else {
				// Here we only push the groupBy column and not the weight string expr even if it is required.
				// We rely on the later for-loop to push the weight strings. This is required since we care about the order
				// and don't want to insert weight_strings in the beginning. We don't keep track of the offsets of where these
				// are pushed since the later coll will reuse them and get the offset for us.
				groupbyIdx++
				reuseCol := groupBy.InnerIndex == nil
				col, _, err := addExpressionToRoute(ctx, plan, groupBy.AsAliasedExpr(), reuseCol)
				groupingCols = append(groupingCols, col)
				if err != nil {
					return nil, nil, err
				}
			}
		}
		// Go over the remaining grouping values that we should push.
		for groupbyIdx < len(grouping) {
			groupBy := grouping[groupbyIdx]
			groupbyIdx++
			reuseCol := groupBy.InnerIndex == nil
			col, _, err := addExpressionToRoute(ctx, plan, groupBy.AsAliasedExpr(), reuseCol)
			groupingCols = append(groupingCols, col)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	// Go over the remaining aggregations and add them to the SELECT.
	for aggrIdx < len(aggregations) {
		aggregation := aggregations[aggrIdx]
		aggrIdx++
		param := addAggregationToSelect(sel, aggregation)
		vtgateAggregation = append(vtgateAggregation, []offsets{param})
	}

	groupingOffsets := make([]offsets, 0, len(grouping))
	for idx, expr := range grouping {
		sel.AddGroupBy(expr.Inner)
		collID := ctx.SemTable.CollationForExpr(expr.Inner)
		qt := ctx.SemTable.TypeFor(expr.Inner)
		wsExpr := expr.WeightStrExpr
		if collID != collations.Unknown || (qt != nil && sqltypes.IsNumber(*qt)) {
			wsExpr = nil
		}
		var col int
		var err error
		if ignoreOutputOrder {
			col, _, err = addExpressionToRoute(ctx, plan, &sqlparser.AliasedExpr{Expr: expr.Inner}, true)
			if err != nil {
				return nil, nil, err
			}
		} else {
			col = groupingCols[idx]
		}
		wsCol := -1
		if wsExpr != nil {
			ctx.SemTable.CollationForExpr(expr.Inner)
			wsExpr = weightStringFor(expr.WeightStrExpr)
			wsCol, _, err = addExpressionToRoute(ctx, plan, &sqlparser.AliasedExpr{Expr: wsExpr}, true)
			if err != nil {
				return nil, nil, err
			}
			if wsCol >= 0 {
				sel.AddGroupBy(wsExpr)
			}
		}
		groupingOffsets = append(groupingOffsets, offsets{
			col:   col,
			wsCol: wsCol,
		})
	}

	if ignoreOutputOrder {
		return groupingOffsets, vtgateAggregation, nil
	}

	// re-ordering the output slice to match the input order
	orderedGroupingOffsets := make([]offsets, 0, len(grouping))
	for _, og := range originalGrouping {
		for i, g := range grouping {
			if og.Inner == g.Inner {
				orderedGroupingOffsets = append(orderedGroupingOffsets, groupingOffsets[i])
				break
			}
		}
	}
	return orderedGroupingOffsets, vtgateAggregation, nil
}

// addAggregationToSelect adds the aggregation to the SELECT statement and returns the AggregateParams to be used outside
func addAggregationToSelect(sel *sqlparser.Select, aggregation abstract.Aggr) offsets {
	// TODO: removing duplicated aggregation expression should also be done at the join level
	offset := -1
	for i, expr := range sel.SelectExprs {
		aliasedExpr, isAliasedExpr := expr.(*sqlparser.AliasedExpr)
		if !isAliasedExpr {
			continue
		}
		if sqlparser.EqualsExpr(aliasedExpr.Expr, aggregation.Func) {
			offset = i
			break
		}
	}
	if offset == -1 {
		sel.SelectExprs = append(sel.SelectExprs, aggregation.Original)
		offset = len(sel.SelectExprs) - 1
	}

	return offsets{col: offset}
}

func countStarAggr() *abstract.Aggr {
	f := &sqlparser.FuncExpr{
		Name:     sqlparser.NewColIdent("count"),
		Distinct: false,
		Exprs:    []sqlparser.SelectExpr{&sqlparser.StarExpr{}},
	}

	return &abstract.Aggr{
		Original: &sqlparser.AliasedExpr{Expr: f},
		Func:     f,
		OpCode:   engine.AggregateCount,
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
	grouping []abstract.GroupBy,
	aggregations []abstract.Aggr,
	join *joinGen4,
) ([]offsets, [][]offsets, error) {
	// First we separate aggregations according to which side the dependencies are coming from
	lhsAggrs, rhsAggrs, err := splitAggregationsToLeftAndRight(ctx, aggregations, join)
	if err != nil {
		return nil, nil, err
	}

	// We need to group by the columns used in the join condition.
	// If we don't, the LHS will not be able to return the column, and it can't be used to send down to the RHS
	lhsCols, err := hp.createGroupingsForColumns(ctx, join.LHSColumns)
	if err != nil {
		return nil, nil, err
	}

	// Here we split the grouping depending on if they should with the LHS or RHS of the query
	// This is done by using the semantic table and checking dependencies
	lhsGrouping, rhsGrouping, groupingOffsets, err := splitGroupingsToLeftAndRight(ctx, join, grouping, lhsCols)
	if err != nil {
		return nil, nil, err
	}

	// Next we push the aggregations to both sides
	lhsOffsets, lhsAggrOffsets, err := hp.filteredPushAggregation(ctx, join.Left, lhsGrouping, lhsAggrs, true)
	if err != nil {
		return nil, nil, err
	}

	rhsOffsets, rhsAggrOffsets, err := hp.filteredPushAggregation(ctx, join.Right, rhsGrouping, rhsAggrs, true)
	if err != nil {
		return nil, nil, err
	}

	// Next, we have to pass through the grouping values through the join and the projection we add on top
	// We added new groupings to the LHS because of the join condition, so we don't want to pass through everything,
	// just the groupings that are used by operators on top of this current one
	outputGroupings := make([]offsets, 0, len(groupingOffsets))
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
		var outputGrouping offsets
		outputGrouping.col = len(join.Cols)
		join.Cols = append(join.Cols, f(offset.col))
		if offset.wsCol > -1 {
			outputGrouping.wsCol = len(join.Cols)
			join.Cols = append(join.Cols, f(offset.wsCol))
		}
		outputGroupings = append(outputGroupings, outputGrouping)
	}

	outputAggrOffsets := make([][]offsets, 0, len(aggregations))
	for idx := range aggregations {
		l, r := lhsAggrOffsets[idx], rhsAggrOffsets[idx]
		var offSlice []offsets
		for _, off := range l {
			offSlice = append(offSlice, offsets{col: len(join.Cols)})
			join.Cols = append(join.Cols, -(off.col + 1))
		}
		for _, off := range r {
			offSlice = append(offSlice, offsets{col: len(join.Cols)})
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
	grouping []abstract.GroupBy,
	aggregations []abstract.Aggr,
	join *semiJoin,
	ignoreOutputOrder bool,
) ([]offsets, [][]offsets, error) {
	// We need to group by the columns used in the join condition.
	// If we don't, the LHS will not be able to return the column, and it can't be used to send down to the RHS
	lhsCols, err := hp.createGroupingsForColumns(ctx, join.LHSColumns)
	if err != nil {
		return nil, nil, err
	}

	totalGrouping := append(grouping, lhsCols...)
	groupingOffsets, aggrParams, err := hp.pushAggregation(ctx, join.lhs, totalGrouping, aggregations, ignoreOutputOrder)
	if err != nil {
		return nil, nil, err
	}

	outputGroupings := make([]offsets, 0, len(grouping))
	for idx := range grouping {
		outputGroupings = append(outputGroupings, groupingOffsets[idx])
	}

	return outputGroupings, aggrParams, nil
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
	grouping []abstract.GroupBy,
	aggregations []*abstract.Aggr,
	ignoreOutputOrder bool,
) (groupingOffsets []offsets, outputAggrs [][]offsets, err error) {
	used := make([]bool, len(aggregations))
	var aggrs []abstract.Aggr

	for idx, aggr := range aggregations {
		if aggr != nil {
			used[idx] = true
			aggrs = append(aggrs, *aggr)
		}
	}
	groupingOffsets, pushedAggrs, err := hp.pushAggregation(ctx, plan, grouping, aggrs, ignoreOutputOrder)
	if err != nil {
		return nil, nil, err
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
	return groupingOffsets, outputAggrs, nil
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
	aggregations []abstract.Aggr,
	join *joinGen4,
) ([]*abstract.Aggr, []*abstract.Aggr, error) {
	var lhsAggrs, rhsAggrs []*abstract.Aggr
	for _, aggr := range aggregations {
		newAggr := aggr
		if isCountStar(aggr.Func) {
			lhsAggrs = append(lhsAggrs, &newAggr)
			rhsAggrs = append(rhsAggrs, &newAggr)
		} else {
			deps := ctx.SemTable.RecursiveDeps(aggr.Func)
			var other *abstract.Aggr
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
