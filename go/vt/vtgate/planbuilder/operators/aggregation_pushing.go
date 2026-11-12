/*
Copyright 2023 The Vitess Authors.

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
	"errors"
	"fmt"
	"slices"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func errDistinctAggrWithMultiExpr(f sqlparser.AggrFunc) {
	if f == nil {
		panic(vterrors.VT12001("distinct aggregation function with multiple expressions"))
	}
	panic(vterrors.VT12001(fmt.Sprintf("distinct aggregation function with multiple expressions '%s'", sqlparser.String(f))))
}

func tryPushAggregator(ctx *plancontext.PlanningContext, aggregator *Aggregator) (output Operator, applyResult *ApplyResult) {
	if aggregator.Pushed {
		return aggregator, NoRewrite
	}

	// this rewrite is always valid, and we should do it whenever possible
	if route, ok := aggregator.Source.(*Route); ok && (route.IsSingleShard() || overlappingUniqueVindex(ctx, aggregator.Grouping)) {
		return Swap(aggregator, route, "push down aggregation under route - remove original")
	}

	// other rewrites require us to have reached this phase before we can consider them
	if !reachedPhase(ctx, delegateAggregation) {
		return aggregator, NoRewrite
	}

	// if we have not yet been able to push this aggregation down,
	// we need to turn AVG into SUM/COUNT to support this over a sharded keyspace
	if needAvgBreaking(aggregator.Aggregations) {
		return splitAvgAggregations(ctx, aggregator)
	}

	switch src := aggregator.Source.(type) {
	case *Route:
		// if we have a single sharded route, we can push it down
		output, applyResult = pushAggregationThroughRoute(ctx, aggregator, src)
	case *ApplyJoin:
		output, applyResult = pushAggregationThroughApplyJoin(ctx, aggregator, src)
	case *HashJoin:
		output, applyResult = pushAggregationThroughHashJoin(ctx, aggregator, src)
	case *Filter:
		output, applyResult = pushAggregationThroughFilter(ctx, aggregator, src)
	case *SubQueryContainer:
		output, applyResult = pushAggregationThroughSubquery(ctx, aggregator, src)
	default:
		return aggregator, NoRewrite
	}

	if output == nil {
		return aggregator, NoRewrite
	}

	aggregator.Pushed = true

	return
}

func reachedPhase(ctx *plancontext.PlanningContext, p Phase) bool {
	b := ctx.CurrentPhase >= int(p)
	return b
}

// pushAggregationThroughSubquery pushes an aggregation under a subquery.
// Any columns that are needed to evaluate the subquery needs to be added as
// grouping columns to the aggregation being pushed down, and then after the
// subquery evaluation we are free to reassemble the total aggregation values.
// This is very similar to how we push aggregation through an apply-join.
func pushAggregationThroughSubquery(
	ctx *plancontext.PlanningContext,
	rootAggr *Aggregator,
	src *SubQueryContainer,
) (Operator, *ApplyResult) {
	// A subquery that doesn't merge becomes a filter above the aggregation pushed under it, which reads these columns
	// when a join is below it. Over a route the filter goes into the route's query.
	underJoin := joinsOutsideRoutes(src.Outer)
	filterColumns := func(subQuery *SubQuery) (cols []sqlparser.Expr) {
		if underJoin && !subQuery.IsArgument {
			for _, col := range outsideSubqueryColumns(subQuery.Original) {
				cols = append(cols, col)
			}
		}
		return cols
	}
	var cols []sqlparser.Expr
	for _, subQuery := range src.Inner {
		for _, col := range subQuery.OuterExpressionsNeeded(ctx, src.Outer) {
			cols = append(cols, col)
		}
		cols = append(cols, filterColumns(subQuery)...)
	}
	if keepsDistinctAbove(ctx, rootAggr, src.Outer, cols) {
		return nil, nil
	}
	_, overJoin := src.Outer.(*ApplyJoin)

	pushedAggr := rootAggr.SplitAggregatorBelowOperators(ctx, []Operator{src.Outer})
	addGrouping := func(col sqlparser.Expr) {
		if slices.IndexFunc(pushedAggr.Columns, func(ae *sqlparser.AliasedExpr) bool {
			return ctx.SemTable.EqualsExpr(ae.Expr, col)
		}) >= 0 {
			// we already have the column, no need to push it again
			return
		}
		pushedAggr.addColumnWithoutPushing(ctx, aeWrap(col), true)
	}
	for _, subQuery := range src.Inner {
		for _, colName := range subQuery.OuterExpressionsNeeded(ctx, src.Outer) {
			addGrouping(colName)
		}
		// Above a join, the filter reads them from the sides below it.
		if cols := filterColumns(subQuery); overJoin && len(cols) > 0 {
			before := len(pushedAggr.Columns)
			for _, col := range cols {
				addGrouping(col)
			}
			if len(pushedAggr.Columns) > before && rootAggr.ResultColumns == 0 {
				rootAggr.ResultColumns = len(rootAggr.Columns)
			}
		}
	}

	src.Outer = pushedAggr

	for _, aggr := range pushedAggr.Aggregations {
		// we rewrite columns in the aggregation to use the argument form of the subquery
		aggr.Original.Expr = rewriteColNameToArgument(ctx, aggr.Original.Expr, aggr.SubQueryExpression, src.Inner...)
		pushedAggr.Columns[aggr.ColOffset].Expr = rewriteColNameToArgument(ctx, pushedAggr.Columns[aggr.ColOffset].Expr, aggr.SubQueryExpression, src.Inner...)
	}

	if !rootAggr.Original {
		return src, Rewrote("push Aggregation under subquery - keep original")
	}

	rootAggr.aggregateTheAggregates()

	return rootAggr, Rewrote("push Aggregation under subquery")
}

// keepsDistinctAbove says if a distinct aggregation of a SELECT with a subquery has to stay above the subqueries, counted
// in VTGate, because pushing it under them would group by some of cols, which the query doesn't group by. The partial
// results are summed per group, which counts a value once for every group it is in, unless the column is an argument of
// every distinct aggregation and the aggregations reach the shards as they are: through a unique vindex, or on a route
// to one shard. Otherwise the aggregation below groups by its argument, and summing those groups adds up its values.
// Without a subquery the aggregation is pushed as on main.
func keepsDistinctAbove(ctx *plancontext.PlanningContext, aggregator *Aggregator, below Operator, cols []sqlparser.Expr) bool {
	distinct := distinctAggregationsNextToSubquery(aggregator)
	pushable, _ := checkIfWeCanPush(ctx, aggregator)
	if route, isRoute := below.(*Route); isRoute && route.IsSingleShard() {
		pushable = true
	}
	if len(distinct) == 0 || !slices.ContainsFunc(cols, func(col sqlparser.Expr) bool {
		grouped := slices.ContainsFunc(aggregator.Grouping, func(gb GroupBy) bool { return ctx.SemTable.EqualsExpr(gb.Inner, col) })
		counted := !slices.ContainsFunc(distinct, func(aggr sqlparser.AggrFunc) bool {
			return !slices.ContainsFunc(aggr.GetArgs(), func(arg sqlparser.Expr) bool { return ctx.SemTable.EqualsExpr(arg, col) })
		})
		return !grouped && !(counted && pushable)
	}) {
		return false
	}
	keepDistinctArgument(ctx, aggregator)
	return true
}

// keepsDistinctAboveJoin says if a distinct aggregation of a SELECT with a subquery has to stay above a join, counted in
// VTGate. Without a subquery the aggregation is pushed as on main.
func keepsDistinctAboveJoin(ctx *plancontext.PlanningContext, aggregator *Aggregator) bool {
	if len(distinctAggregationsNextToSubquery(aggregator)) == 0 {
		return false
	}
	keepDistinctArgument(ctx, aggregator)
	return true
}

func distinctAggregationsNextToSubquery(aggregator *Aggregator) []sqlparser.AggrFunc {
	if aggregator.QP == nil || !hasSubquery(aggregator.QP.Select) {
		return nil
	}
	return distinctAggregations(aggregator.QP.Select)
}

// keepDistinctArgument keeps the argument of the distinct aggregations of an aggregator that stays above an operator,
// for the ordering that VTGate counts them over, as for a distinct aggregation that can't be pushed through a join.
// VTGate orders the rows by one argument, so distinct aggregations of different arguments can't stay above.
func keepDistinctArgument(ctx *plancontext.PlanningContext, aggregator *Aggregator) {
	var distinctExprs []sqlparser.Expr
	for _, aggr := range aggregator.Aggregations {
		if !aggr.Distinct {
			continue
		}
		args := aggr.Func.GetArgs()
		if len(args) != 1 {
			errDistinctAggrWithMultiExpr(aggr.Func)
		}
		if len(distinctExprs) > 0 && !ctx.SemTable.EqualsExpr(distinctExprs[0], args[0]) {
			panic(vterrors.VT12001("only one DISTINCT aggregation is allowed in a SELECT: " + sqlparser.String(aggr.Original)))
		}
		distinctExprs = append(distinctExprs, args[0])
	}
	if len(distinctExprs) > 0 {
		aggregator.DistinctExpr = distinctExprs[0]
	}
}

// hasSubquery says if root has a subquery in an expression, also inside its derived tables and CTEs.
func hasSubquery(root sqlparser.SQLNode) (found bool) {
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if _, isSubquery := node.(*sqlparser.Subquery); isSubquery {
			found = true
		}
		return !found, nil
	}, root)
	return found
}

// joinsOutsideRoutes says if op has a join that VTGate runs.
func joinsOutsideRoutes(op Operator) bool {
	switch op.(type) {
	case *ApplyJoin, *HashJoin:
		return true
	case *Route:
		return false
	}
	return slices.ContainsFunc(op.Inputs(), joinsOutsideRoutes)
}

func (a *Aggregator) aggregateTheAggregates() {
	for i := range a.Aggregations {
		aggregateTheAggregate(a, i)
	}
}

func aggregateTheAggregate(a *Aggregator, i int) {
	aggr := a.Aggregations[i]
	switch aggr.OpCode {
	case opcode.AggregateCount, opcode.AggregateCountStar, opcode.AggregateCountDistinct, opcode.AggregateSumDistinct:
		// All count variations turn into SUM above the Route. This is also applied for Sum distinct when it is pushed down.
		// Think of it as we are SUMming together a bunch of distributed COUNTs.
		aggr.OriginalOpCode, aggr.OpCode = aggr.OpCode, opcode.AggregateSum
		a.Aggregations[i] = aggr
	}
}

func pushAggregationThroughRoute(
	ctx *plancontext.PlanningContext,
	aggregator *Aggregator,
	route *Route,
) (Operator, *ApplyResult) {
	// Create a new aggregator to be placed below the route.
	aggrBelowRoute := aggregator.SplitAggregatorBelowOperators(ctx, route.Inputs())
	aggrBelowRoute.Aggregations = nil

	pushAggregations(ctx, aggregator, aggrBelowRoute)

	// Set the source of the route to the new aggregator placed below the route.
	route.Source = aggrBelowRoute

	if !aggregator.Original {
		// we only keep the root aggregation, if this aggregator was created
		// by splitting one and pushing under a join, we can get rid of this one
		return aggregator.Source, Rewrote("push aggregation under route - remove original")
	}

	return aggregator, Rewrote("push aggregation under route - keep original")
}

// pushAggregations splits aggregations between the original aggregator and the one we are pushing down
func pushAggregations(ctx *plancontext.PlanningContext, aggregator *Aggregator, aggrBelowRoute *Aggregator) {
	canPushDistinctAggr, distinctExprs := checkIfWeCanPush(ctx, aggregator)

	distinctAggrGroupByAdded := false

	for i, aggr := range aggregator.Aggregations {
		if !aggr.Distinct || canPushDistinctAggr {
			aggrBelowRoute.Aggregations = append(aggrBelowRoute.Aggregations, aggr)
			aggregateTheAggregate(aggregator, i)
			aggregator.Aggregations[i].PushedDown = true
			continue
		}

		if len(distinctExprs) != 1 {
			errDistinctAggrWithMultiExpr(aggr.Func)
		}

		// We handle a distinct aggregation by turning it into a group by and
		// doing the aggregating on the vtgate level instead
		aeDistinctExpr := aeWrap(distinctExprs[0])
		aggrBelowRoute.Columns[aggr.ColOffset] = aeDistinctExpr

		// We handle a distinct aggregation by turning it into a group by and
		// doing the aggregating on the vtgate level instead
		// Adding to group by can be done only once even though there are multiple distinct aggregation with same expression.
		if !distinctAggrGroupByAdded {
			groupBy := NewGroupBy(distinctExprs[0])
			groupBy.ColOffset = aggr.ColOffset
			aggrBelowRoute.Grouping = append(aggrBelowRoute.Grouping, groupBy)
			distinctAggrGroupByAdded = true
		}
	}

	if !canPushDistinctAggr {
		aggregator.DistinctExpr = distinctExprs[0]
	}
}

func checkIfWeCanPush(ctx *plancontext.PlanningContext, aggregator *Aggregator) (bool, []sqlparser.Expr) {
	canPush := true
	var distinctExprs []sqlparser.Expr
	var differentExpr *sqlparser.AliasedExpr

	for _, aggr := range aggregator.Aggregations {
		if !aggr.Distinct {
			continue
		}

		args := aggr.Func.GetArgs()
		hasUniqVindex := false
		for _, arg := range args {
			if exprHasUniqueVindex(ctx, arg) {
				hasUniqVindex = true
				break
			}
		}
		if !hasUniqVindex {
			canPush = false
		}
		if len(distinctExprs) == 0 {
			distinctExprs = args
		}
		for idx, expr := range distinctExprs {
			if !ctx.SemTable.EqualsExpr(expr, args[idx]) {
				differentExpr = aggr.Original
				break
			}
		}
	}

	if !canPush && differentExpr != nil {
		panic(vterrors.VT12001("only one DISTINCT aggregation is allowed in a SELECT: " + sqlparser.String(differentExpr)))
	}

	return canPush, distinctExprs
}

func pushAggregationThroughFilter(
	ctx *plancontext.PlanningContext,
	aggregator *Aggregator,
	filter *Filter,
) (Operator, *ApplyResult) {
	columnsNeeded := collectColNamesNeeded(ctx, filter)
	pushedAggr := aggregator.SplitAggregatorBelowOperators(ctx, []Operator{filter.Source})
withNextColumn:
	for _, col := range columnsNeeded {
		for _, gb := range pushedAggr.Grouping {
			if ctx.SemTable.EqualsExpr(col, gb.Inner) {
				continue withNextColumn
			}
		}
		pushedAggr.addColumnWithoutPushing(ctx, aeWrap(col), true)
	}

	// Set the source of the filter to the new aggregator placed below the route.
	filter.Source = pushedAggr

	if !aggregator.Original {
		// we only keep the root aggregation, if this aggregator was created
		// by splitting one and pushing under a join, we can get rid of this one
		return aggregator.Source, Rewrote("push aggregation under filter - remove original")
	}
	aggregator.aggregateTheAggregates()
	return aggregator, Rewrote("push aggregation under filter - keep original")
}

func collectColNamesNeeded(ctx *plancontext.PlanningContext, f *Filter) (columnsNeeded []*sqlparser.ColName) {
	for _, p := range f.Predicates {
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			col, ok := node.(*sqlparser.ColName)
			if !ok {
				return true, nil
			}
			for _, existing := range columnsNeeded {
				if ctx.SemTable.EqualsExpr(col, existing) {
					return true, nil
				}
			}
			columnsNeeded = append(columnsNeeded, col)
			return true, nil
		}, p)
	}
	return
}

func overlappingUniqueVindex(ctx *plancontext.PlanningContext, groupByExprs []GroupBy) bool {
	for _, groupByExpr := range groupByExprs {
		if exprHasUniqueVindex(ctx, groupByExpr.Inner) {
			return true
		}
	}
	return false
}

func exprHasUniqueVindex(ctx *plancontext.PlanningContext, expr sqlparser.Expr) bool {
	return exprHasVindex(ctx, expr, true)
}

func exprHasVindex(ctx *plancontext.PlanningContext, expr sqlparser.Expr, hasToBeUnique bool) bool {
	col, isCol := expr.(*sqlparser.ColName)
	if !isCol {
		return false
	}
	ts := ctx.SemTable.RecursiveDeps(expr)
	tableInfo, err := ctx.SemTable.TableInfoFor(ts)
	if err != nil {
		return false
	}
	vschemaTable := tableInfo.GetVindexTable()
	for _, vindex := range vschemaTable.ColumnVindexes {
		// TODO: Support composite vindexes (multicol, etc).
		if len(vindex.Columns) > 1 || hasToBeUnique && !vindex.IsUnique() {
			return false
		}
		if col.Name.Equal(vindex.Columns[0]) {
			return true
		}
	}
	return false
}

/*
Using techniques from "Orthogonal Optimization of Subqueries and Aggregation" by Cesar A. Galindo-Legaria
and Milind M. Joshi (Microsoft Corp), we push down aggregations. It splits an aggregation
into local aggregates depending on one side of a join, and pushes these into the inputs of the join.
These then combine to form the final group by/aggregate query.

In Vitess, this technique is extremely useful. It enables pushing aggregations to routes,
even with joins at the vtgate level. Thus, rather than handling all grouping and
aggregation at vtgate, most work is offloaded to MySQL, with vtgate summarizing results.

# For a query like:

select count(*) from L JOIN R on L.id = R.id

Original:

		 Aggr         <- Original grouping, doing count(*)
		  |
		Join
		/  \
	  L     R

Transformed:

		  rootAggr  <- New grouping SUMs distributed `count(*)`
			  |
			Proj    <- Projection multiplying `count(*)` from each side of the join
			  |
			Join
		   /    \
	   lhsAggr rhsAggr <- `count(*)` aggregation can now be pushed under a route
		/         \
	   L           R
*/
func pushAggregationThroughApplyJoin(ctx *plancontext.PlanningContext, rootAggr *Aggregator, join *ApplyJoin) (Operator, *ApplyResult) {
	// A join returns the partial result of one side once for every row of the other side that matches it, which a
	// distinct aggregation can't add up.
	if keepsDistinctAboveJoin(ctx, rootAggr) {
		return nil, nil
	}

	lhs := createJoinPusher(rootAggr, join.LHS)
	rhs := createJoinPusher(rootAggr, join.RHS)

	columns := &applyJoinColumns{}
	output, err := splitAggrColumnsToLeftAndRight(ctx, rootAggr, join, !join.JoinType.IsInner(), columns, lhs, rhs)
	if err != nil {
		// if we get this error, we just abort the splitting and fall back on simpler ways of solving the same query
		if errors.Is(err, errAbortAggrPushing) {
			return nil, nil
		}
		panic(err)
	}
	join.JoinColumns = columns

	splitGroupingToLeftAndRight(ctx, rootAggr, lhs, rhs, join.JoinColumns)

	// We need to add any columns coming from the lhs of the join, in its predicates or fed to the rhs, to the group by on that side
	// If we don't, the LHS will not be able to return the column, and it can't be used to send down to the RHS
	addColumnsFromLHSInJoinPredicates(ctx, join, lhs)

	join.LHS, join.RHS = lhs.pushed, rhs.pushed

	if !rootAggr.Original {
		// we only keep the root aggregation, if this aggregator was created
		// by splitting one and pushing under a join, we can get rid of this one
		return output, Rewrote("push Aggregation under join - keep original")
	}

	rootAggr.aggregateTheAggregates()
	rootAggr.Source = output
	return rootAggr, Rewrote("push Aggregation under join")
}

// pushAggregationThroughHashJoin pushes aggregation through a hash-join in a similar way to pushAggregationThroughApplyJoin
func pushAggregationThroughHashJoin(ctx *plancontext.PlanningContext, rootAggr *Aggregator, join *HashJoin) (Operator, *ApplyResult) {
	lhs := createJoinPusher(rootAggr, join.LHS)
	rhs := createJoinPusher(rootAggr, join.RHS)

	columns := &hashJoinColumns{}
	output, err := splitAggrColumnsToLeftAndRight(ctx, rootAggr, join, join.LeftJoin, columns, lhs, rhs)
	if err != nil {
		// if we get this error, we just abort the splitting and fall back on simpler ways of solving the same query
		if errors.Is(err, errAbortAggrPushing) {
			return nil, nil
		}
		panic(err)
	}

	// The two sides of the hash comparisons are added as grouping expressions
	for _, cmp := range join.JoinComparisons {
		lhs.addGrouping(ctx, NewGroupBy(cmp.LHS))
		columns.addLeft(cmp.LHS)

		rhs.addGrouping(ctx, NewGroupBy(cmp.RHS))
		columns.addRight(cmp.RHS)
	}

	// The grouping columns need to be pushed down as grouping columns on the respective sides
	for _, groupBy := range rootAggr.Grouping {
		deps := ctx.SemTable.RecursiveDeps(groupBy.Inner)
		switch {
		case deps.IsSolvedBy(lhs.tableID):
			lhs.addGrouping(ctx, groupBy)
			columns.addLeft(groupBy.Inner)
		case deps.IsSolvedBy(rhs.tableID):
			rhs.addGrouping(ctx, groupBy)
			columns.addRight(groupBy.Inner)
		case deps.IsSolvedBy(lhs.tableID.Merge(rhs.tableID)):
			// TODO: Support this as well
			return nil, nil
		default:
			panic(vterrors.VT13001(fmt.Sprintf("grouping with bad dependencies %s", groupBy.Inner)))
		}
	}

	join.LHS, join.RHS = lhs.pushed, rhs.pushed
	join.columns = columns

	if !rootAggr.Original {
		// we only keep the root aggregation, if this aggregator was created
		// by splitting one and pushing under a join, we can get rid of this one
		return output, Rewrote("push Aggregation under hash join - keep original")
	}

	rootAggr.aggregateTheAggregates()
	rootAggr.Source = output
	return rootAggr, Rewrote("push Aggregation under hash join")
}

var errAbortAggrPushing = errors.New("abort aggregation pushing")

func createJoinPusher(rootAggr *Aggregator, operator Operator) *joinPusher {
	return &joinPusher{
		orig: rootAggr,
		pushed: &Aggregator{
			unaryOperator: newUnaryOp(operator),
			QP:            rootAggr.QP,
		},
		columns: initColReUse(len(rootAggr.Columns)),
		tableID: TableID(operator),
	}
}

func addColumnsFromLHSInJoinPredicates(ctx *plancontext.PlanningContext, join *ApplyJoin, lhs *joinPusher) {
	var bves []BindVarExpr
	for _, pred := range join.JoinPredicates.columns {
		bves = append(bves, pred.LHSExprs...)
	}
	bves = append(bves, join.ExtraLHSVars...)
	for _, bve := range bves {
		idx, found := canReuseColumn(ctx, lhs.pushed.Columns, bve.Expr, extractExpr)
		if !found {
			idx = len(lhs.pushed.Columns)
			lhs.pushed.Columns = append(lhs.pushed.Columns, aeWrap(bve.Expr))
		}
		_, found = canReuseColumn(ctx, lhs.pushed.Grouping, bve.Expr, func(by GroupBy) sqlparser.Expr {
			return by.Inner
		})

		if found {
			continue
		}

		lhs.pushed.Grouping = append(lhs.pushed.Grouping, GroupBy{
			Inner:     bve.Expr,
			ColOffset: idx,
			WSOffset:  -1,
		})
	}
}

// distinctAggregations returns the distinct aggregations of root itself, also in HAVING or on the other side of a join,
// where a pushed aggregator doesn't see them. The aggregations of the SELECTs inside root belong to those.
func distinctAggregations(root sqlparser.SQLNode) (aggrs []sqlparser.AggrFunc) {
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if _, isStmt := node.(sqlparser.TableStatement); isStmt && node != root {
			return false, nil
		}
		if aggr, isAggr := node.(sqlparser.AggrFunc); isAggr && sqlparser.IsDistinct(aggr) {
			aggrs = append(aggrs, aggr)
		}
		return true, nil
	}, root)
	return aggrs
}

func splitGroupingToLeftAndRight(
	ctx *plancontext.PlanningContext,
	rootAggr *Aggregator,
	lhs, rhs *joinPusher,
	columns joinColumns,
) {
	for _, groupBy := range rootAggr.Grouping {
		deps := ctx.SemTable.RecursiveDeps(groupBy.Inner)
		switch {
		case deps.IsSolvedBy(lhs.tableID):
			lhs.addGrouping(ctx, groupBy)
			columns.addLeft(groupBy.Inner)
		case deps.IsSolvedBy(rhs.tableID):
			rhs.addGrouping(ctx, groupBy)
			columns.addRight(groupBy.Inner)
		case deps.IsSolvedBy(lhs.tableID.Merge(rhs.tableID)):
			jc := breakExpressionInLHSandRHS(ctx, groupBy.Inner, lhs.tableID)
			for _, lhsExpr := range jc.LHSExprs {
				e := lhsExpr.Expr
				lhs.addGrouping(ctx, NewGroupBy(e))
			}
			rhs.addGrouping(ctx, NewGroupBy(jc.RHSExpr))
		default:
			panic(vterrors.VT13001(fmt.Sprintf("grouping with bad dependencies %s", groupBy.Inner)))
		}
	}
}

// splitAggrColumnsToLeftAndRight pushes all aggregations on the aggregator above a join and
// pushes them to one or both sides of the join, and also provides the projections needed to re-assemble the
// aggregations that have been spread across the join
func splitAggrColumnsToLeftAndRight(
	ctx *plancontext.PlanningContext,
	aggregator *Aggregator,
	join Operator,
	leftJoin bool,
	columns joinColumns,
	lhs, rhs *joinPusher,
) (Operator, error) {
	proj := newAliasedProjection(join)
	proj.FromAggr = true
	builder := &aggBuilder{
		lhs:         lhs,
		rhs:         rhs,
		joinColumns: columns,
		proj:        proj,
		outerJoin:   leftJoin,
	}

	canPushDistinctAggr, distinctExprs := checkIfWeCanPush(ctx, aggregator)

	// Distinctable aggregation cannot be pushed down in the join.
	// We keep node of the distinct aggregation expression to be used later for ordering.
	if !canPushDistinctAggr {
		if len(distinctExprs) != 1 {
			errDistinctAggrWithMultiExpr(nil)
		}
		aggregator.DistinctExpr = distinctExprs[0]
		return nil, errAbortAggrPushing
	}

outer:
	// we prefer adding the aggregations in the same order as the columns are declared
	for colIdx, col := range aggregator.Columns {
		for _, aggr := range aggregator.Aggregations {
			if aggr.ColOffset == colIdx {
				err := builder.handleAggr(ctx, aggr)
				if err != nil {
					return nil, err
				}
				continue outer
			}
		}
		builder.proj.addUnexploredExpr(col, col.Expr)
	}

	return builder.proj, nil
}

func coalesceFunc(e sqlparser.Expr) sqlparser.Expr {
	// `coalesce(e,1)` will return `e` if `e` is not `NULL`, otherwise it will return `1`
	return sqlparser.NewFuncExpr("coalesce", e, sqlparser.NewIntLiteral("1"))
}

func initColReUse(size int) []int {
	cols := make([]int, size)
	for i := range size {
		cols[i] = -1
	}
	return cols
}

func extractExpr(expr *sqlparser.AliasedExpr) sqlparser.Expr { return expr.Expr }

func needAvgBreaking(aggrs []Aggr) bool {
	for _, aggr := range aggrs {
		if aggr.OpCode == opcode.AggregateAvg {
			return true
		}
	}
	return false
}

// splitAvgAggregations takes an aggregator that has AVG aggregations in it and splits
// these into sum/count expressions that can be spread out to shards
func splitAvgAggregations(ctx *plancontext.PlanningContext, aggr *Aggregator) (Operator, *ApplyResult) {
	proj := newAliasedProjection(aggr)

	var columns []*sqlparser.AliasedExpr
	var aggregations []Aggr

	for offset, col := range aggr.Columns {
		avg, ok := col.Expr.(*sqlparser.Avg)
		if !ok {
			proj.addColumnWithoutPushing(ctx, col, false /* addToGroupBy */)
			continue
		}

		if avg.Distinct {
			panic(vterrors.VT12001("AVG(distinct <>)"))
		}

		// We have an AVG that we need to split
		sumExpr := &sqlparser.Sum{Arg: avg.Arg}
		countExpr := &sqlparser.Count{Args: []sqlparser.Expr{avg.Arg}}
		calcExpr := &sqlparser.BinaryExpr{
			Operator: sqlparser.DivOp,
			Left:     sumExpr,
			Right:    countExpr,
		}

		outputColumn := aeWrap(col.Expr)
		outputColumn.As = sqlparser.NewIdentifierCI(col.ColumnName())
		proj.addUnexploredExpr(sqlparser.Clone(col), calcExpr)
		col.Expr = sumExpr
		found := false
		for aggrOffset, aggregation := range aggr.Aggregations {
			if offset == aggregation.ColOffset {
				// We have found the AVG column. We'll change it to SUM, and then we add a COUNT as well
				aggr.Aggregations[aggrOffset].OpCode = opcode.AggregateSum

				countExprAlias := aeWrap(countExpr)
				countAggr := NewAggr(opcode.AggregateCount, countExpr, countExprAlias, sqlparser.String(countExpr))
				countAggr.ColOffset = len(aggr.Columns) + len(columns)
				aggregations = append(aggregations, countAggr)
				columns = append(columns, countExprAlias)
				found = true
				break // no need to search the remaining aggregations
			}
		}
		if !found {
			// if we get here, it's because we didn't find the aggregation. Something is wrong
			panic(vterrors.VT13001("no aggregation pointing to this column was found"))
		}
	}

	aggr.Columns = append(aggr.Columns, columns...)
	aggr.Aggregations = append(aggr.Aggregations, aggregations...)

	return proj, Rewrote("split avg aggregation")
}
