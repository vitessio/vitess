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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/predicates"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
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
	pushedAggr := rootAggr.SplitAggregatorBelowOperators(ctx, []Operator{src.Outer})
	addGrouping := func(col *sqlparser.ColName, checkDistinct bool) {
		if slices.IndexFunc(pushedAggr.Columns, func(ae *sqlparser.AliasedExpr) bool {
			return ctx.SemTable.EqualsExpr(ae.Expr, col)
		}) >= 0 {
			// we already have the column, no need to push it again
			return
		}
		if checkDistinct {
			checkGroupingKeepsDistinct(ctx, rootAggr, col)
			if join, isJoin := src.Outer.(*ApplyJoin); isJoin {
				checkJoinKeepsDistinct(ctx, rootAggr, join)
			}
		}
		pushedAggr.addColumnWithoutPushing(ctx, aeWrap(col), true)
	}
	for _, subQuery := range src.Inner {
		for _, colName := range subQuery.OuterExpressionsNeeded(ctx, src.Outer) {
			addGrouping(colName, false)
		}
		// A filter that doesn't merge ends up above the join, and then reads these columns from the sides below it.
		if _, isJoin := src.Outer.(*ApplyJoin); isJoin && !subQuery.IsArgument {
			before := len(pushedAggr.Columns)
			for _, col := range outsideSubqueryColumns(subQuery.Original) {
				addGrouping(col, true)
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

// checkGroupingKeepsDistinct refuses to group by col when the aggregator has a distinct aggregation. Partial results are
// summed per group, which is only right when no value of the distinct argument is in two groups. That holds when the
// groupings and the argument determine col.
func checkGroupingKeepsDistinct(ctx *plancontext.PlanningContext, aggregator *Aggregator, col sqlparser.Expr) {
	for _, aggr := range aggregator.Aggregations {
		if !aggr.Distinct {
			continue
		}
		args := aggr.Func.GetArgs()
		if !isDetermined(ctx, aggregator.QP.Select, append(groupingExprs(aggregator.Grouping), args...), args, col) {
			panic(vterrors.VT12001("distinct aggregation over a subquery that needs more grouping columns"))
		}
	}
}

// checkJoinKeepsDistinct refuses a distinct aggregation that is pushed to one side of the join, when the join repeats
// its partial results: the other side returns a group from several shards, or the join columns that the left side
// groups by split the values of the argument between groups. A distinct aggregation that can't be pushed stays above
// the join.
func checkJoinKeepsDistinct(ctx *plancontext.PlanningContext, aggregator *Aggregator, join *ApplyJoin) {
	if canPush, _ := checkIfWeCanPush(ctx, aggregator); !canPush {
		return
	}
	var lhsKeys, rhsKeys []sqlparser.Expr
	for _, pred := range join.JoinPredicates.columns {
		for _, bve := range pred.LHSExprs {
			lhsKeys = append(lhsKeys, bve.Expr)
		}
		for _, col := range outsideSubqueryColumns(pred.Original) {
			if ctx.SemTable.RecursiveDeps(col).IsSolvedBy(TableID(join.RHS)) {
				rhsKeys = append(rhsKeys, col)
			}
		}
	}
	for _, aggr := range aggregator.Aggregations {
		if !aggr.Distinct {
			continue
		}
		args := aggr.Func.GetArgs()
		deps := ctx.SemTable.RecursiveDeps(args[0])
		grouping := groupingExprs(aggregator.Grouping)
		var other Operator
		var otherKeys []sqlparser.Expr
		splits := false
		switch {
		case deps.IsSolvedBy(TableID(join.RHS)):
			other, otherKeys = join.LHS, lhsKeys
		case deps.IsSolvedBy(TableID(join.LHS)):
			other, otherKeys = join.RHS, rhsKeys
			seeds := append(slices.Clone(grouping), args...)
			splits = slices.ContainsFunc(lhsKeys, func(key sqlparser.Expr) bool {
				return !isDetermined(ctx, aggregator.QP.Select, seeds, args, key)
			})
		default:
			continue
		}
		if splits || !returnsEachGroupOnce(ctx, aggregator.QP.Select, other, append(grouping, otherKeys...)) {
			panic(vterrors.VT12001("distinct aggregation over a subquery that needs more grouping columns"))
		}
	}
}

// returnsEachGroupOnce says if no group of op, where it groups by the exprs, comes from several shards. That holds when
// every route that isn't a single shard has the value of a unique vindex column fixed by the exprs.
func returnsEachGroupOnce(ctx *plancontext.PlanningContext, sel *sqlparser.Select, op Operator, exprs []sqlparser.Expr) bool {
	var candidates []sqlparser.Expr
	for _, eq := range joinEqualities(ctx, sel, nil) {
		candidates = append(candidates, eq.cmp.Left, eq.cmp.Right)
	}
	candidates = append(candidates, exprs...)
	once := true
	_ = Visit(op, func(op Operator) error {
		route, isRoute := op.(*Route)
		if !isRoute || route.IsSingleShard() {
			return nil
		}
		routeID := TableID(route)
		if !slices.ContainsFunc(candidates, func(expr sqlparser.Expr) bool {
			return ctx.SemTable.RecursiveDeps(expr).IsSolvedBy(routeID) && exprHasUniqueVindex(ctx, expr) &&
				isDetermined(ctx, sel, exprs, nil, expr)
		}) {
			once = false
		}
		return nil
	})
	return once
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
	// main selects the columns fed to the right side by a subquery without grouping on them, which MySQL rejects with
	// ERROR 1055 unless the grouping determines them. Grouping on them, or leaving out those of a failed merge, would
	// instead count a distinct aggregation anywhere in the query once for every row that a join repeats it for.
	sel := lhs.orig.QP.Select
	distinct := hasDistinctAggregation(sel)
	fixed := columnsFixedBelow(ctx, join.LHS)
	refuseUngrouped := func(expr sqlparser.Expr) {
		seeds := append(groupingExprs(lhs.pushed.Grouping), fixed...)
		if distinct && !isDetermined(ctx, sel, seeds, nil, expr) {
			panic(vterrors.VT12001("distinct aggregation over a subquery that needs more grouping columns"))
		}
	}
	var bves []BindVarExpr
	for _, pred := range join.JoinPredicates.columns {
		bves = append(bves, pred.LHSExprs...)
	}
	extraFrom := len(bves)
	bves = append(bves, join.ExtraLHSVars...)
	for i, bve := range bves {
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
		if i >= extraFrom {
			refuseUngrouped(bve.Expr)
			checkJoinKeepsDistinct(ctx, lhs.orig, join)
		}

		lhs.pushed.Grouping = append(lhs.pushed.Grouping, GroupBy{
			Inner:     bve.Expr,
			ColOffset: idx,
			WSOffset:  -1,
		})
	}
	for _, expr := range join.UnmergedLHSExprs {
		refuseUngrouped(expr)
	}
}

// isDetermined says if the values of the seeds fix the value of expr in every row of the SELECT where no column of
// nonNull is NULL. A row is fixed by a unique key, or by all the columns of a composite one, and the equalities of the
// query pass fixed values on to other columns.
func isDetermined(ctx *plancontext.PlanningContext, sel *sqlparser.Select, seeds, nonNull []sqlparser.Expr, expr sqlparser.Expr) bool {
	var cols []*sqlparser.ColName
	var rows semantics.TableSet
	isKnown := func(col *sqlparser.ColName) bool {
		deps := ctx.SemTable.RecursiveDeps(col)
		return deps.NotEmpty() && deps.IsSolvedBy(rows) ||
			slices.ContainsFunc(cols, func(known *sqlparser.ColName) bool { return ctx.SemTable.EqualsExpr(known, col) })
	}
	learn := func(col *sqlparser.ColName) bool {
		if isKnown(col) {
			return false
		}
		cols = append(cols, col)
		if exprIsUniqueKey(ctx, col) {
			rows = rows.Merge(ctx.SemTable.RecursiveDeps(col))
		}
		return true
	}
	areKnown := func(e sqlparser.Expr) bool {
		exprCols := outsideSubqueryColumns(e)
		return len(exprCols) > 0 && !slices.ContainsFunc(exprCols, func(col *sqlparser.ColName) bool { return !isKnown(col) })
	}
	for _, seed := range seeds {
		if col, isCol := seed.(*sqlparser.ColName); isCol {
			learn(col)
		}
	}
	equalities := joinEqualities(ctx, sel, nonNull)
	for grown := true; grown; {
		grown = false
		for _, eq := range equalities {
			if slices.ContainsFunc(eq.needs, func(col *sqlparser.ColName) bool { return !isKnown(col) }) {
				continue
			}
			cmp := eq.cmp
			for _, pair := range [][2]sqlparser.Expr{{cmp.Left, cmp.Right}, {cmp.Right, cmp.Left}} {
				if to, isCol := pair[1].(*sqlparser.ColName); isCol && areKnown(pair[0]) && learn(to) {
					grown = true
				}
			}
		}
		for _, col := range cols {
			if deps := ctx.SemTable.RecursiveDeps(col); !deps.IsSolvedBy(rows) && compositeKeyIsKnown(ctx, col, cols) {
				rows, grown = rows.Merge(deps), true
			}
		}
	}
	return areKnown(expr)
}

// columnsFixedBelow returns the columns that have one value for every row of op: the ones that a filter compares to an
// argument, and the ones that the subqueries on op, or the joins in it, group by when the aggregation is pushed under
// them. A join inside a route is one query, so it groups by nothing.
func columnsFixedBelow(ctx *plancontext.PlanningContext, op Operator) (cols []sqlparser.Expr) {
	if container, isContainer := op.(*SubQueryContainer); isContainer {
		for _, subQuery := range container.Inner {
			for _, col := range subQuery.OuterExpressionsNeeded(ctx, container.Outer) {
				cols = append(cols, col)
			}
		}
	}
	var collect func(op Operator, inRoute bool)
	collect = func(op Operator, inRoute bool) {
		switch op := op.(type) {
		case *ApplyJoin:
			if !inRoute {
				for _, pred := range op.JoinPredicates.columns {
					for _, bve := range pred.LHSExprs {
						cols = append(cols, bve.Expr)
					}
				}
				for _, bve := range op.ExtraLHSVars {
					cols = append(cols, bve.Expr)
				}
			}
		case *Route:
			inRoute = true
		case *Filter:
			for _, pred := range op.Predicates {
				if jp, isJoinPredicate := pred.(*predicates.JoinPredicate); isJoinPredicate {
					pred = jp.Current()
				}
				cmp, isCmp := pred.(*sqlparser.ComparisonExpr)
				if !isCmp || cmp.Operator != sqlparser.EqualOp {
					continue
				}
				for _, pair := range [][2]sqlparser.Expr{{cmp.Left, cmp.Right}, {cmp.Right, cmp.Left}} {
					if _, isArg := pair[1].(*sqlparser.Argument); isArg {
						cols = append(cols, pair[0])
					}
				}
			}
		}
		for _, input := range op.Inputs() {
			collect(input, inRoute)
		}
	}
	collect(op, false)
	return cols
}

func groupingExprs(grouping []GroupBy) []sqlparser.Expr {
	exprs := make([]sqlparser.Expr, 0, len(grouping))
	for _, gb := range grouping {
		exprs = append(exprs, gb.Inner)
	}
	return exprs
}

// compositeKeyIsKnown says if cols has every column of a key of the table of col, and the key is unique across shards.
func compositeKeyIsKnown(ctx *plancontext.PlanningContext, col *sqlparser.ColName, cols []*sqlparser.ColName) bool {
	table := ctx.SemTable.RecursiveDeps(col)
	vt := realVindexTable(ctx, col)
	if vt == nil {
		return false
	}
	for _, key := range uniqueKeys(vt) {
		var keyCols []sqlparser.Expr
		for _, name := range key {
			i := slices.IndexFunc(cols, func(known *sqlparser.ColName) bool {
				return known.Name.Equal(name) && ctx.SemTable.RecursiveDeps(known) == table
			})
			if i < 0 {
				break
			}
			keyCols = append(keyCols, cols[i])
		}
		if len(keyCols) == len(key) && keyIsUniqueAcrossShards(ctx, vt, keyCols) {
			return true
		}
	}
	return false
}

// equality is an equality of the query, which passes values on only once the columns in needs are known.
type equality struct {
	cmp   *sqlparser.ComparisonExpr
	needs []*sqlparser.ColName
}

// joinEqualities returns the equalities that every row of the query's FROM and WHERE clauses where no column of nonNull
// is NULL fulfils: the conjuncts of the WHERE clause and of the ON clauses of the SELECT. The ON clause of a
// left join holds only in the rows where its right side matched, which a column on that side that can't be NULL shows.
// Without one, whether a row matched depends only on the left side's columns in the ON clause, so the ON clause fixes
// the right side once those columns are known.
func joinEqualities(ctx *plancontext.PlanningContext, sel *sqlparser.Select, nonNull []sqlparser.Expr) (equalities []equality) {
	if sel == nil {
		return nil
	}
	nonNull = append(slices.Clone(nonNull), columnsNeverNull(sel)...)
	add := func(cond sqlparser.Expr, needs []*sqlparser.ColName) {
		for _, cmp := range equalitiesIn(cond) {
			equalities = append(equalities, equality{cmp: cmp, needs: needs})
		}
	}
	if sel.Where != nil {
		add(sel.Where.Expr, nil)
	}
	var collect func(sqlparser.TableExpr)
	collect = func(te sqlparser.TableExpr) {
		switch te := te.(type) {
		case *sqlparser.JoinTableExpr:
			collect(te.LeftExpr)
			collect(te.RightExpr)
			if te.Condition == nil || te.Condition.On == nil {
				return
			}
			switch {
			case te.Join.IsInner():
				add(te.Condition.On, nil)
			case te.Join == sqlparser.LeftJoinType || te.Join == sqlparser.RightJoinType:
				preserved, nullSupplying := outerJoinSides(te)
				right := tablesIn(ctx, nullSupplying)
				if slices.ContainsFunc(nonNull, func(expr sqlparser.Expr) bool { return isColumnOf(ctx, expr, right) }) {
					add(te.Condition.On, nil)
					return
				}
				left := tablesIn(ctx, preserved)
				var leftCols []*sqlparser.ColName
				for _, col := range outsideSubqueryColumns(te.Condition.On) {
					if isColumnOf(ctx, col, left) {
						leftCols = append(leftCols, col)
					}
				}
				add(te.Condition.On, leftCols)
			}
		case *sqlparser.ParenTableExpr:
			for _, inner := range te.Exprs {
				collect(inner)
			}
		}
	}
	for _, te := range sel.From {
		collect(te)
	}
	return equalities
}

// columnsNeverNull returns the columns of the equalities that every row of the query fulfils: those of the WHERE clause
// and of the ON clauses of the inner joins that no outer join can fill with NULLs.
func columnsNeverNull(sel *sqlparser.Select) (cols []sqlparser.Expr) {
	add := func(cond sqlparser.Expr) {
		for _, cmp := range equalitiesIn(cond) {
			for _, side := range []sqlparser.Expr{cmp.Left, cmp.Right} {
				if col, isCol := side.(*sqlparser.ColName); isCol {
					cols = append(cols, col)
				}
			}
		}
	}
	if sel.Where != nil {
		add(sel.Where.Expr)
	}
	var collect func(sqlparser.TableExpr)
	collect = func(te sqlparser.TableExpr) {
		switch te := te.(type) {
		case *sqlparser.JoinTableExpr:
			if !te.Join.IsInner() {
				preserved, _ := outerJoinSides(te)
				collect(preserved)
				return
			}
			collect(te.LeftExpr)
			collect(te.RightExpr)
			if te.Condition != nil && te.Condition.On != nil {
				add(te.Condition.On)
			}
		case *sqlparser.ParenTableExpr:
			for _, inner := range te.Exprs {
				collect(inner)
			}
		}
	}
	for _, te := range sel.From {
		collect(te)
	}
	return cols
}

// outerJoinSides returns the side of an outer join whose rows all show up in the result, and the one that is filled
// with NULLs where nothing matches.
func outerJoinSides(join *sqlparser.JoinTableExpr) (preserved, nullSupplying sqlparser.TableExpr) {
	if join.Join == sqlparser.RightJoinType || join.Join == sqlparser.NaturalRightJoinType {
		return join.RightExpr, join.LeftExpr
	}
	return join.LeftExpr, join.RightExpr
}

func equalitiesIn(cond sqlparser.Expr) (equalities []*sqlparser.ComparisonExpr) {
	for _, conjunct := range sqlparser.SplitAndExpression(nil, cond) {
		if cmp, isCmp := conjunct.(*sqlparser.ComparisonExpr); isCmp && cmp.Operator == sqlparser.EqualOp {
			equalities = append(equalities, cmp)
		}
	}
	return equalities
}

func tablesIn(ctx *plancontext.PlanningContext, te sqlparser.TableExpr) (tables semantics.TableSet) {
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if ate, isAliased := node.(*sqlparser.AliasedTableExpr); isAliased {
			tables = tables.Merge(ctx.SemTable.TableSetFor(ate))
		}
		return true, nil
	}, te)
	return tables
}

func isColumnOf(ctx *plancontext.PlanningContext, expr sqlparser.Expr, tables semantics.TableSet) bool {
	_, isCol := expr.(*sqlparser.ColName)
	deps := ctx.SemTable.RecursiveDeps(expr)
	return isCol && deps.NotEmpty() && deps.IsSolvedBy(tables)
}

// hasDistinctAggregation says if root has a distinct aggregation of its own, also in HAVING or on the other side of a
// join, where a pushed aggregator doesn't see it. The aggregations of the SELECTs inside root belong to those.
func hasDistinctAggregation(root sqlparser.SQLNode) (found bool) {
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if _, isStmt := node.(sqlparser.TableStatement); isStmt && node != root {
			return false, nil
		}
		if aggr, isAggr := node.(sqlparser.AggrFunc); isAggr && sqlparser.IsDistinct(aggr) {
			found = true
		}
		return !found, nil
	}, root)
	return found
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

func exprIsUniqueKey(ctx *plancontext.PlanningContext, expr sqlparser.Expr) bool {
	col, isCol := expr.(*sqlparser.ColName)
	if !isCol {
		return false
	}
	vt := realVindexTable(ctx, expr)
	if vt == nil {
		return false
	}
	return slices.ContainsFunc(uniqueKeys(vt), func(key []sqlparser.IdentifierCI) bool {
		return len(key) == 1 && key[0].Equal(col.Name)
	}) && keyIsUniqueAcrossShards(ctx, vt, []sqlparser.Expr{expr})
}

// realVindexTable returns the vindex table of the real table that expr is a column of.
func realVindexTable(ctx *plancontext.PlanningContext, expr sqlparser.Expr) *vindexes.BaseTable {
	ti, err := ctx.SemTable.TableInfoFor(ctx.SemTable.DirectDeps(expr))
	if err != nil {
		return nil
	}
	if _, isReal := ti.(*semantics.RealTable); !isReal {
		return nil
	}
	return ti.GetVindexTable()
}

// uniqueKeys returns the columns of the primary key and of each unique key of the table. MySQL allows many rows with
// NULL in a unique key, so a unique key counts only when all its columns are known to be NOT NULL.
func uniqueKeys(vt *vindexes.BaseTable) (keys [][]sqlparser.IdentifierCI) {
	if len(vt.PrimaryKey) > 0 {
		keys = append(keys, vt.PrimaryKey)
	}
	for _, uk := range vt.UniqueKeys {
		var key []sqlparser.IdentifierCI
		for _, expr := range uk {
			if c, isCol := expr.(*sqlparser.ColName); isCol {
				key = append(key, c.Name)
			}
		}
		if len(key) == len(uk) && !slices.ContainsFunc(key, func(name sqlparser.IdentifierCI) bool { return mayBeNull(vt, name) }) {
			keys = append(keys, key)
		}
	}
	return keys
}

func mayBeNull(vt *vindexes.BaseTable, name sqlparser.IdentifierCI) bool {
	i := slices.IndexFunc(vt.Columns, func(col vindexes.Column) bool { return col.Name.Equal(name) })
	return i < 0 || vt.Columns[i].Nullable
}

// keyIsUniqueAcrossShards says if a key made of cols has one row in the whole keyspace. MySQL enforces a primary or
// unique key per shard, so the same value can be in two shards unless a column has a unique vindex.
func keyIsUniqueAcrossShards(ctx *plancontext.PlanningContext, vt *vindexes.BaseTable, cols []sqlparser.Expr) bool {
	if vt.Keyspace == nil || !vt.Keyspace.Sharded || vt.Pinned != nil || vt.Type == vindexes.TypeReference {
		return true
	}
	return slices.ContainsFunc(cols, func(col sqlparser.Expr) bool { return exprHasUniqueVindex(ctx, col) })
}
