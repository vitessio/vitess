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
) (newPlan logicalPlan, groupingOffsets []offsets, outputAggrs []*engine.AggregateParams, err error) {
	switch plan := plan.(type) {
	case *routeGen4:
		groupingOffsets, outputAggrs, err = pushAggrOnRoute(ctx, plan, aggregations, grouping, ignoreOutputOrder)
		if err != nil {
			return nil, nil, nil, err
		}
		newPlan = plan

		return
	case *joinGen4:
		return hp.pushAggrOnJoin(ctx, grouping, aggregations, plan, ignoreOutputOrder)

	default:
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "pushAggregation %T", plan)
	}
}

func pushAggrOnRoute(
	ctx *plancontext.PlanningContext,
	plan *routeGen4,
	aggregations []abstract.Aggr,
	grouping []abstract.GroupBy,
	ignoreOutputOrder bool,
) ([]offsets, []*engine.AggregateParams, error) {
	sel, isSel := plan.Select.(*sqlparser.Select)
	if !isSel {
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't plan aggregation on union")
	}

	// creating a copy of the original grouping list, so we can reorder it later
	originalGrouping := make([]abstract.GroupBy, len(grouping))
	vtgateAggregation := make([]*engine.AggregateParams, 0, len(aggregations))
	// aggIdx keeps track of the index of the aggregations we have processed so far. Starts with 0, goes all the way to len(aggregations)
	aggrIdx := 0
	if !ignoreOutputOrder {
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
				vtgateAggregation = append(vtgateAggregation, param)
			} else {
				// Here we only push the groupBy column and not the weight string expr even if it is required.
				// We rely on the later for-loop to push the weight strings. This is required since we care about the order
				// and don't want to insert weight_strings in the beginning. We don't keep track of the offsets of where these
				// are pushed since the later coll will reuse them and get the offset for us.
				groupbyIdx++
				_, _, err := pushProjection(ctx, &sqlparser.AliasedExpr{Expr: groupBy.Inner}, plan, true, true, false)
				if err != nil {
					return nil, nil, err
				}
			}
		}
		// Go over the remaining grouping values that we should push.
		for groupbyIdx < len(grouping) {
			groupBy := grouping[groupbyIdx]
			groupbyIdx++
			_, _, err := pushProjection(ctx, &sqlparser.AliasedExpr{Expr: groupBy.Inner}, plan, true, true, false)
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
		vtgateAggregation = append(vtgateAggregation, param)
	}

	groupingOffsets := make([]offsets, 0, len(grouping))
	sel.GroupBy = make([]sqlparser.Expr, 0, len(grouping))
	for _, expr := range grouping {
		sel.GroupBy = append(sel.GroupBy, expr.Inner)
		collID := ctx.SemTable.CollationForExpr(expr.Inner)
		wsExpr := expr.WeightStrExpr
		if collID != collations.Unknown {
			wsExpr = nil
		}
		col, wsCol, err := wrapAndPushExpr(ctx, expr.Inner, wsExpr, plan)
		if err != nil {
			return nil, nil, err
		}
		if wsCol >= 0 {
			sel.GroupBy = append(sel.GroupBy, weightStringFor(expr.WeightStrExpr))
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
func addAggregationToSelect(sel *sqlparser.Select, aggregation abstract.Aggr) *engine.AggregateParams {
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

	opcode := engine.AggregateSum
	switch aggregation.OpCode {
	case engine.AggregateMin, engine.AggregateMax:
		opcode = aggregation.OpCode
	}

	param := &engine.AggregateParams{
		Opcode: opcode,
		Col:    offset,
		Alias:  aggregation.Alias,
		Expr:   aggregation.Func,
	}
	return param
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
	ignoreOutputOrder bool,
) (logicalPlan, []offsets, []*engine.AggregateParams, error) {
	// First we separate aggregations according to which side the dependencies are coming from
	lhsAggrs, rhsAggrs, err := splitAggregationsToLeftAndRight(ctx, aggregations, join)
	if err != nil {
		return nil, nil, nil, err
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
	lhsPlan, lhsOffsets, lhsAggregations, err := hp.filteredPushAggregation(ctx, join.Left, lhsGrouping, lhsAggrs, true)
	if err != nil {
		return nil, nil, nil, err
	}

	rhsPlan, rhsOffsets, rhsAggregations, err := hp.filteredPushAggregation(ctx, join.Right, rhsGrouping, rhsAggrs, true)
	if err != nil {
		return nil, nil, nil, err
	}
	join.Left, join.Right = lhsPlan, rhsPlan

	// depending on whether we need to multiply anything before it is aggregated with the OA,
	// we might or might not need a projection between the join and the outside
	// the following method abstracts this away so the rest of this method doesn't
	// have to consider whether projection is needed or not
	pusher := getAggrPusher(aggregations, groupingOffsets, lhsOffsets, rhsOffsets, join)

	// Next, we have to pass through the grouping values through the join and the projection we add on top
	// We added new groupings to the LHS because of the join condition, so we don't want to pass through everything,
	// just the groupings that are used by operators on top of this current one
	outputGroupings := make([]offsets, 0, len(groupingOffsets))
	for idx, groupBy := range groupingOffsets {
		var offset offsets
		var f func(i int) int
		if groupBy < 0 {
			offset = lhsOffsets[-groupBy-1]
			f = func(i int) int { return -(i + 1) }
		} else {
			offset = rhsOffsets[groupBy-1]
			f = func(i int) int { return i + 1 }
		}
		outputColIdx := grouping[idx].InnerIndex
		if ignoreOutputOrder {
			outputColIdx = nil
		}
		outputGrouping, err := pusher.pushGrouping(offset, f, outputColIdx)
		if err != nil {
			return nil, nil, nil, err
		}
		outputGroupings = append(outputGroupings, outputGrouping)
	}

	// Here we produce information about the grouping columns projected through
	outputAggrs, err := pusher.pushAggregation(aggregations, lhsAggregations, rhsAggregations, ignoreOutputOrder)
	if err != nil {
		return nil, nil, nil, err
	}
	return pusher.resultPlan(), outputGroupings, outputAggrs, err
}

func (hp *horizonPlanning) filteredPushAggregation(
	ctx *plancontext.PlanningContext,
	plan logicalPlan,
	grouping []abstract.GroupBy,
	aggregations []*abstract.Aggr,
	ignoreOutputOrder bool,
) (newPlan logicalPlan, groupingOffsets []offsets, outputAggrs []*engine.AggregateParams, err error) {
	used := make([]bool, len(aggregations))
	var aggrs []abstract.Aggr

	for idx, aggr := range aggregations {
		if aggr != nil {
			used[idx] = true
			aggrs = append(aggrs, *aggr)
		}
	}
	newPlan, groupingOffsets, pushedAggrs, err := hp.pushAggregation(ctx, plan, grouping, aggrs, ignoreOutputOrder)
	if err != nil {
		return nil, nil, nil, err
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
	return newPlan, groupingOffsets, outputAggrs, nil
}

func getAggrPusher(aggregations []abstract.Aggr, groupingOffsets []int, lhsOffsets []offsets, rhsOffsets []offsets, join *joinGen4) aggrPusher {
	needsProjection := false
	for _, aggregation := range aggregations {
		switch aggregation.OpCode {
		case engine.AggregateSum, engine.AggregateCount, engine.AggregateSumDistinct, engine.AggregateCountDistinct:
			needsProjection = true
		}
		if needsProjection {
			break
		}
	}
	var pusher aggrPusher
	if needsProjection {
		length := getLengthOfProjection(groupingOffsets, lhsOffsets, rhsOffsets, aggregations)
		pusher = projAggrPusher{join: join, proj: &projection{
			source:      join,
			columns:     make([]sqlparser.Expr, length),
			columnNames: make([]string, length),
		}}
	} else {
		pusher = joinAggrPusher{join: join}
	}
	return pusher
}

func getLengthOfProjection(groupingOffsets []int, lhsOffsets []offsets, rhsOffsets []offsets, aggregations []abstract.Aggr) int {
	length := 0
	var offset offsets
	for _, groupBy := range groupingOffsets {
		if groupBy < 0 {
			offset = lhsOffsets[-groupBy-1]
		} else {
			offset = rhsOffsets[groupBy-1]
		}
		if offset.wsCol != -1 {
			length++
		}
		length++
	}
	length += len(aggregations)
	return length
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

type aggrPusher interface {
	pushGrouping(offset offsets, f func(int) int, outputColIdx *int) (output offsets, err error)
	pushAggregation(aggregations []abstract.Aggr, lhs, rhs []*engine.AggregateParams, ignoreOutputOrder bool) ([]*engine.AggregateParams, error)
	resultPlan() logicalPlan
}

type projAggrPusher struct {
	proj *projection
	join *joinGen4
}

func (pap projAggrPusher) pushGrouping(offset offsets, f func(int) int, outputColIdx *int) (output offsets, err error) {
	output.col, err = pap.proj.addColumn(outputColIdx, sqlparser.Offset(len(pap.join.Cols)), "")
	if err != nil {
		return
	}
	pap.join.Cols = append(pap.join.Cols, f(offset.col))
	if offset.wsCol > -1 {
		output.wsCol, err = pap.proj.addColumn(nil, sqlparser.Offset(len(pap.join.Cols)), "")
		if err != nil {
			return
		}
		pap.join.Cols = append(pap.join.Cols, f(offset.wsCol))
	}
	return
}

func (pap projAggrPusher) pushAggregation(aggregations []abstract.Aggr, lhsAggregations, rhsAggregations []*engine.AggregateParams, ignoreOutputOrder bool) ([]*engine.AggregateParams, error) {
	var outputAggrs []*engine.AggregateParams
	for idx, aggr := range aggregations {
		// create the projection expression that will multiply the incoming aggregations
		l, r := lhsAggregations[idx], rhsAggregations[idx]
		switch {
		case l != nil && r != nil:
			// If we have aggregation coming in from both sides, we need to multiply the two incoming values with each other
			offset := len(pap.join.Cols)
			expr := &sqlparser.BinaryExpr{
				Operator: sqlparser.MultOp,
				Left:     sqlparser.Offset(offset),
				Right:    sqlparser.Offset(offset + 1),
			}
			outputIdx := aggr.Index
			if ignoreOutputOrder {
				outputIdx = nil
			}
			output, err := pap.proj.addColumn(outputIdx, expr, aggr.Alias)
			if err != nil {
				return nil, err
			}

			// pass through the output columns from the join
			pap.join.Cols = append(pap.join.Cols, -(l.Col + 1))
			pap.join.Cols = append(pap.join.Cols, r.Col+1)

			outputAggrs = append(outputAggrs, &engine.AggregateParams{
				Opcode: engine.AggregateSum,
				Col:    output,
				Alias:  aggr.Alias,
				Expr:   aggr.Func,
			})
		default:
			// It's an aggregation that can be safely pushed down to one side only
			offset := len(pap.join.Cols)
			outputIdx := aggr.Index
			if ignoreOutputOrder {
				outputIdx = nil
			}
			pap.join.Cols = append(pap.join.Cols, getJoinColOffset(l, r))
			output, err := pap.proj.addColumn(outputIdx, sqlparser.Offset(offset), aggr.Alias)
			if err != nil {
				return nil, err
			}

			outputAggrs = append(outputAggrs, &engine.AggregateParams{
				Opcode: aggr.OpCode,
				Col:    output,
				Alias:  aggr.Alias,
				Expr:   aggr.Func,
			})
		}
	}
	return outputAggrs, nil
}

func (pap projAggrPusher) resultPlan() logicalPlan {
	return pap.proj
}

type joinAggrPusher struct {
	join *joinGen4
}

func (jap joinAggrPusher) pushGrouping(offset offsets, f func(int) int, outputColIdx *int) (output offsets, err error) {
	output.col = len(jap.join.Cols)
	jap.join.Cols = append(jap.join.Cols, f(offset.col))
	if offset.wsCol > -1 {
		output.wsCol = len(jap.join.Cols)
		jap.join.Cols = append(jap.join.Cols, f(offset.wsCol))
	}
	return
}

func (jap joinAggrPusher) pushAggregation(aggregations []abstract.Aggr, lhsAggregations, rhsAggregations []*engine.AggregateParams, ignoreOutputOrder bool) ([]*engine.AggregateParams, error) {
	var outputAggrs []*engine.AggregateParams
	for idx, aggr := range aggregations {
		l, r := lhsAggregations[idx], rhsAggregations[idx]
		offset := len(jap.join.Cols)
		jap.join.Cols = append(jap.join.Cols, getJoinColOffset(l, r))

		outputAggrs = append(outputAggrs, &engine.AggregateParams{
			Opcode: aggr.OpCode,
			Col:    offset,
			Alias:  aggr.Alias,
			Expr:   aggr.Func,
		})
	}

	return outputAggrs, nil
}

func getJoinColOffset(l *engine.AggregateParams, r *engine.AggregateParams) int {
	if l != nil {
		return -(l.Col + 1)
	}
	return r.Col + 1
}

func (jap joinAggrPusher) resultPlan() logicalPlan {
	return jap.join
}
