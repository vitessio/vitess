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
	"io"
	"strconv"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func planQuery(ctx *plancontext.PlanningContext, root Operator) Operator {
	var selExpr sqlparser.SelectExprs
	if horizon, isHorizon := root.(*Horizon); isHorizon {
		sel := sqlparser.GetFirstSelect(horizon.Query)
		selExpr = sqlparser.CloneSelectExprs(sel.SelectExprs)
	}

	output := runPhases(ctx, root)
	output = planOffsets(ctx, output)

	if DebugOperatorTree {
		fmt.Println("After offset planning:")
		fmt.Println(ToTree(output))
	}

	output = compact(ctx, output)

	return addTruncationOrProjectionToReturnOutput(ctx, selExpr, output)
}

// runPhases is the process of figuring out how to perform the operations in the Horizon
// If we can push it under a route - done.
// If we can't, we will instead expand the Horizon into
// smaller operators and try to push these down as far as possible
func runPhases(ctx *plancontext.PlanningContext, root Operator) Operator {
	op := root

	p := phaser{}
	for phase := p.next(ctx); phase != DONE; phase = p.next(ctx) {
		ctx.CurrentPhase = int(phase)
		if DebugOperatorTree {
			fmt.Printf("PHASE: %s\n", phase.String())
		}

		op = phase.act(ctx, op)
		op = runRewriters(ctx, op)
		op = compact(ctx, op)
	}

	return addGroupByOnRHSOfJoin(op)
}

func runRewriters(ctx *plancontext.PlanningContext, root Operator) Operator {
	visitor := func(in Operator, _ semantics.TableSet, isRoot bool) (Operator, *ApplyResult) {
		switch in := in.(type) {
		case *Horizon:
			return pushOrExpandHorizon(ctx, in)
		case *Join:
			return optimizeJoin(ctx, in)
		case *Projection:
			return tryPushProjection(ctx, in)
		case *Limit:
			return tryPushLimit(in)
		case *Ordering:
			return tryPushOrdering(ctx, in)
		case *Aggregator:
			return tryPushAggregator(ctx, in)
		case *Filter:
			return tryPushFilter(ctx, in)
		case *Distinct:
			return tryPushDistinct(in)
		case *Union:
			return tryPushUnion(ctx, in)
		case *SubQueryContainer:
			return pushOrMergeSubQueryContainer(ctx, in)
		case *QueryGraph:
			return optimizeQueryGraph(ctx, in)
		case *LockAndComment:
			return pushLockAndComment(in)
		case *Delete:
			return tryPushDelete(in)
		default:
			return in, NoRewrite
		}
	}

	return FixedPointBottomUp(root, TableID, visitor, stopAtRoute)
}

func tryPushDelete(in *Delete) (Operator, *ApplyResult) {
	if src, ok := in.Source.(*Route); ok {
		return pushDeleteUnderRoute(in, src)
	}
	return in, NoRewrite
}

func pushDeleteUnderRoute(in *Delete, src *Route) (Operator, *ApplyResult) {
	switch r := src.Routing.(type) {
	case *SequenceRouting:
		// Sequences are just unsharded routes
		src.Routing = &AnyShardRouting{
			keyspace: r.keyspace,
		}
	case *AnyShardRouting:
		// References would have an unsharded source
		// Alternates are not required.
		r.Alternates = nil
	}
	return Swap(in, src, "pushed delete under route")
}

func createDeleteWithInput(ctx *plancontext.PlanningContext, in *Delete, src Operator) (Operator, *ApplyResult) {
	if len(in.Target.VTable.PrimaryKey) == 0 {
		panic(vterrors.VT09015())
	}
	dm := &DeleteWithInput{}
	var leftComp sqlparser.ValTuple
	proj := newAliasedProjection(src)
	for _, col := range in.Target.VTable.PrimaryKey {
		colName := sqlparser.NewColNameWithQualifier(col.String(), in.Target.Name)
		proj.AddColumn(ctx, true, false, aeWrap(colName))
		dm.cols = append(dm.cols, colName)
		leftComp = append(leftComp, colName)
		ctx.SemTable.Recursive[colName] = in.Target.ID
	}

	dm.Source = proj

	var targetTable *Table
	_ = Visit(src, func(operator Operator) error {
		if tbl, ok := operator.(*Table); ok && tbl.QTable.ID == in.Target.ID {
			targetTable = tbl
			return io.EOF
		}
		return nil
	})
	if targetTable == nil {
		panic(vterrors.VT13001("target DELETE table not found"))
	}

	// optimize for case when there is only single column on left hand side.
	var lhs sqlparser.Expr = leftComp
	if len(leftComp) == 1 {
		lhs = leftComp[0]
	}
	compExpr := sqlparser.NewComparisonExpr(sqlparser.InOp, lhs, sqlparser.ListArg(engine.DmVals), nil)
	targetQT := targetTable.QTable
	qt := &QueryTable{
		ID:         targetQT.ID,
		Alias:      sqlparser.CloneRefOfAliasedTableExpr(targetQT.Alias),
		Table:      sqlparser.CloneTableName(targetQT.Table),
		Predicates: []sqlparser.Expr{compExpr},
	}

	qg := &QueryGraph{Tables: []*QueryTable{qt}}
	in.Source = qg

	if in.OwnedVindexQuery != nil {
		in.OwnedVindexQuery.From = sqlparser.TableExprs{targetQT.Alias}
		in.OwnedVindexQuery.Where = sqlparser.NewWhere(sqlparser.WhereClause, compExpr)
	}
	dm.Delete = in

	return dm, Rewrote("changed Delete to DeleteWithInput")
}

func pushLockAndComment(l *LockAndComment) (Operator, *ApplyResult) {
	switch src := l.Source.(type) {
	case *Horizon, *QueryGraph:
		// we want to wait until the horizons have been pushed under a route or expanded
		// that way we know that we've replaced the QueryGraphs with Routes
		return l, NoRewrite
	case *Route:
		src.Comments = l.Comments
		src.Lock = l.Lock
		return src, Rewrote("put lock and comment into route")
	default:
		inputs := src.Inputs()
		for i, op := range inputs {
			inputs[i] = &LockAndComment{
				Source:   op,
				Comments: l.Comments,
				Lock:     l.Lock,
			}
		}
		src.SetInputs(inputs)
		return src, Rewrote("pushed down lock and comments")
	}
}

func pushOrExpandHorizon(ctx *plancontext.PlanningContext, in *Horizon) (Operator, *ApplyResult) {
	if in.IsDerived() {
		newOp, result := pushDerived(ctx, in)
		if result != NoRewrite {
			return newOp, result
		}
	}

	if !reachedPhase(ctx, initialPlanning) {
		return in, NoRewrite
	}

	if ctx.SemTable.QuerySignature.SubQueries {
		return expandHorizon(ctx, in)
	}

	rb, isRoute := in.src().(*Route)
	if isRoute && rb.IsSingleShard() {
		return Swap(in, rb, "push horizon into route")
	}

	sel, isSel := in.selectStatement().(*sqlparser.Select)

	qp := in.getQP(ctx)

	needsOrdering := len(qp.OrderExprs) > 0
	hasHaving := isSel && sel.Having != nil

	canPush := isRoute &&
		!hasHaving &&
		!needsOrdering &&
		!qp.NeedsAggregation() &&
		!in.selectStatement().IsDistinct() &&
		in.selectStatement().GetLimit() == nil

	if canPush {
		return Swap(in, rb, "push horizon into route")
	}

	return expandHorizon(ctx, in)
}

func tryPushLimit(in *Limit) (Operator, *ApplyResult) {
	switch src := in.Source.(type) {
	case *Route:
		return tryPushingDownLimitInRoute(in, src)
	case *Aggregator:
		return in, NoRewrite
	case *Limit:
		combinedLimit := mergeLimits(in.AST, src.AST)
		if combinedLimit == nil {
			break
		}
		// we can remove the other LIMIT
		in.AST = combinedLimit
		in.Source = src.Source
		return in, Rewrote("merged two limits")
	}
	return setUpperLimit(in)
}

func mergeLimits(l1, l2 *sqlparser.Limit) *sqlparser.Limit {
	// To merge two relational LIMIT operators with LIMIT and OFFSET, we need to combine their
	// LIMIT and OFFSET values appropriately.
	// Let's denote the first LIMIT operator as LIMIT_1 with LIMIT_1 and OFFSET_1,
	// and the second LIMIT operator as LIMIT_2 with LIMIT_2 and OFFSET_2.
	// The second LIMIT operator receives the output of the first LIMIT operator, meaning the first LIMIT and
	// OFFSET are applied first, and then the second LIMIT and OFFSET are applied to the resulting subset.
	//
	// The goal is to determine the effective combined LIMIT and OFFSET values when applying these two operators sequentially.
	//
	// Combined Offset:
	// The combined offset (OFFSET_combined) is the sum of the two offsets because you need to skip OFFSET_1 rows first,
	// and then apply the second offset OFFSET_2 to the result.
	// OFFSET_combined = OFFSET_1 + OFFSET_2

	// Combined Limit:
	// The combined limit (LIMIT_combined) needs to account for both limits. The effective limit should not exceed the rows returned by the first limit,
	// so it is the minimum of the remaining rows after the first offset and the second limit.
	// LIMIT_combined = min(LIMIT_2, LIMIT_1 - OFFSET_2)

	// Note: If LIMIT_1 - OFFSET_2 is negative or zero, it means there are no rows left to limit, so LIMIT_combined should be zero.

	// Example:
	// First LIMIT operator: LIMIT 10 OFFSET 5 (LIMIT_1 = 10, OFFSET_1 = 5)
	// Second LIMIT operator: LIMIT 7 OFFSET 3 (LIMIT_2 = 7, OFFSET_2 = 3)

	// Calculations:
	// Combined OFFSET:
	// OFFSET_combined = 5 + 3 = 8

	// Combined LIMIT:
	// remaining rows after OFFSET_2 = 10 - 3 = 7
	// LIMIT_combined = min(7, 7) = 7

	// So, the combined result would be:
	// LIMIT 7 OFFSET 8

	// This method ensures that the final combined LIMIT and OFFSET correctly reflect the sequential application of the two original operators.
	combinedLimit, failed := mergeLimitExpressions(l1.Rowcount, l2.Rowcount, l2.Offset)
	if failed {
		return nil
	}
	combinedOffset, failed := mergeOffsetExpressions(l1.Offset, l2.Offset)
	if failed {
		return nil
	}

	return &sqlparser.Limit{
		Offset:   combinedOffset,
		Rowcount: combinedLimit,
	}
}

func mergeOffsetExpressions(e1, e2 sqlparser.Expr) (expr sqlparser.Expr, failed bool) {
	switch {
	case e1 == nil && e2 == nil:
		return nil, false
	case e1 == nil:
		return e2, false
	case e2 == nil:
		return e1, false
	default:
		v1str, ok := e1.(*sqlparser.Literal)
		if !ok {
			return nil, true
		}
		v2str, ok := e2.(*sqlparser.Literal)
		if !ok {
			return nil, true
		}
		v1, _ := strconv.Atoi(v1str.Val)
		v2, _ := strconv.Atoi(v2str.Val)
		return sqlparser.NewIntLiteral(strconv.Itoa(v1 + v2)), false
	}
}

// mergeLimitExpressions merges two LIMIT expressions with an OFFSET expression.
// l1: First LIMIT expression.
// l2: Second LIMIT expression.
// off2: Second OFFSET expression.
// Returns the merged LIMIT expression and a boolean indicating if the merge failed.
func mergeLimitExpressions(l1, l2, off2 sqlparser.Expr) (expr sqlparser.Expr, failed bool) {
	switch {
	// If both limits are nil, there's nothing to merge, return nil without failure.
	case l1 == nil && l2 == nil:
		return nil, false

	// If the first limit is nil, the second limit determines the final limit.
	case l1 == nil:
		return l2, false

	// If the second limit is nil, calculate the remaining limit after applying the offset to the first limit.
	case l2 == nil:
		if off2 == nil {
			// No offset, so the first limit is used directly.
			return l1, false
		}
		off2, ok := off2.(*sqlparser.Literal)
		if !ok {
			// If the offset is not a literal, fail the merge.
			return nil, true
		}
		lim1str, ok := l1.(*sqlparser.Literal)
		if !ok {
			// If the first limit is not a literal, return the first limit without failing.
			return nil, false
		}
		// Calculate the remaining limit after the offset.
		off2int, _ := strconv.Atoi(off2.Val)
		l1int, _ := strconv.Atoi(lim1str.Val)
		lim := l1int - off2int
		if lim < 0 {
			lim = 0
		}
		return sqlparser.NewIntLiteral(strconv.Itoa(lim)), false

	default:
		v1str, ok1 := l1.(*sqlparser.Literal)
		if ok1 && v1str.Val == "1" {
			// If the first limit is "1", it dominates, so return it.
			return l1, false
		}
		v2str, ok2 := l2.(*sqlparser.Literal)
		if ok2 && v2str.Val == "1" {
			// If the second limit is "1", it dominates, so return it.
			return l2, false
		}
		if !ok1 || !ok2 {
			// If either limit is not a literal, fail the merge.
			return nil, true
		}

		var off2int int
		if off2 != nil {
			off2, ok := off2.(*sqlparser.Literal)
			if !ok {
				// If the offset is not a literal, fail the merge.
				return nil, true
			}
			off2int, _ = strconv.Atoi(off2.Val)
		}

		v1, _ := strconv.Atoi(v1str.Val)
		v2, _ := strconv.Atoi(v2str.Val)
		lim := min(v2, v1-off2int)
		if lim < 0 {
			// If the combined limit is negative, set it to zero.
			lim = 0
		}
		return sqlparser.NewIntLiteral(strconv.Itoa(lim)), false
	}
}

func tryPushingDownLimitInRoute(in *Limit, src *Route) (Operator, *ApplyResult) {
	if src.IsSingleShardOrByDestination() {
		return Swap(in, src, "push limit under route")
	}

	return setUpperLimit(in)
}

func setUpperLimit(in *Limit) (Operator, *ApplyResult) {
	if in.Pushed {
		return in, NoRewrite
	}
	in.Pushed = true
	visitor := func(op Operator, _ semantics.TableSet, _ bool) (Operator, *ApplyResult) {
		return op, NoRewrite
	}
	var result *ApplyResult
	shouldVisit := func(op Operator) VisitRule {
		switch op := op.(type) {
		case *Join, *ApplyJoin, *SubQueryContainer, *SubQuery:
			// we can't push limits down on either side
			return SkipChildren
		case *Aggregator:
			if len(op.Grouping) > 0 {
				// we can't push limits down if we have a group by
				return SkipChildren
			}
		case *Route:
			newSrc := &Limit{
				Source: op.Source,
				AST:    &sqlparser.Limit{Rowcount: sqlparser.NewArgument("__upper_limit")},
				Pushed: false,
			}
			op.Source = newSrc
			result = result.Merge(Rewrote("push limit under route"))
			return SkipChildren
		}
		return VisitChildren
	}

	TopDown(in.Source, TableID, visitor, shouldVisit)

	return in, result
}

func tryPushOrdering(ctx *plancontext.PlanningContext, in *Ordering) (Operator, *ApplyResult) {
	switch src := in.Source.(type) {
	case *Route:
		return Swap(in, src, "push ordering under route")
	case *Filter:
		return Swap(in, src, "push ordering under filter")
	case *ApplyJoin:
		if canPushLeft(ctx, src, in.Order) {
			return pushOrderLeftOfJoin(src, in)
		}
	case *Ordering:
		// we'll just remove the order underneath. The top order replaces whatever was incoming
		in.Source = src.Source
		return in, Rewrote("remove double ordering")
	case *Projection:
		return pushOrderingUnderProjection(ctx, in, src)
	case *Aggregator:
		if !src.QP.AlignGroupByAndOrderBy(ctx) && !overlaps(ctx, in.Order, src.Grouping) {
			return in, NoRewrite
		}

		return pushOrderingUnderAggr(ctx, in, src)
	case *SubQueryContainer:
		return pushOrderingToOuterOfSubqueryContainer(ctx, in, src)
	case *SubQuery:
		return pushOrderingToOuterOfSubquery(ctx, in, src)
	}
	return in, NoRewrite
}

func pushOrderingToOuterOfSubquery(ctx *plancontext.PlanningContext, in *Ordering, sq *SubQuery) (Operator, *ApplyResult) {
	outerTableID := TableID(sq.Outer)
	for idx, order := range in.Order {
		deps := ctx.SemTable.RecursiveDeps(order.Inner.Expr)
		if !deps.IsSolvedBy(outerTableID) {
			return in, NoRewrite
		}
		in.Order[idx].SimplifiedExpr = sq.rewriteColNameToArgument(order.SimplifiedExpr)
		in.Order[idx].Inner.Expr = sq.rewriteColNameToArgument(order.Inner.Expr)
	}
	sq.Outer, in.Source = in, sq.Outer
	return sq, Rewrote("push ordering into outer side of subquery")
}

func pushOrderingToOuterOfSubqueryContainer(ctx *plancontext.PlanningContext, in *Ordering, subq *SubQueryContainer) (Operator, *ApplyResult) {
	outerTableID := TableID(subq.Outer)
	for _, order := range in.Order {
		deps := ctx.SemTable.RecursiveDeps(order.Inner.Expr)
		if !deps.IsSolvedBy(outerTableID) {
			return in, NoRewrite
		}
	}
	subq.Outer, in.Source = in, subq.Outer
	return subq, Rewrote("push ordering into outer side of subquery")
}

func pushOrderingUnderProjection(ctx *plancontext.PlanningContext, in *Ordering, proj *Projection) (Operator, *ApplyResult) {
	// we can move ordering under a projection if it's not introducing a column we're sorting by
	for _, by := range in.Order {
		if !mustFetchFromInput(by.SimplifiedExpr) {
			return in, NoRewrite
		}
	}
	ap, ok := proj.Columns.(AliasedProjections)
	if !ok {
		return in, NoRewrite
	}
	for _, projExpr := range ap {
		if projExpr.Info != nil {
			return in, NoRewrite
		}
	}
	return Swap(in, proj, "push ordering under projection")
}

func pushOrderLeftOfJoin(src *ApplyJoin, in *Ordering) (Operator, *ApplyResult) {
	// ApplyJoin is stable in regard to the columns coming from the LHS,
	// so if all the ordering columns come from the LHS, we can push down the Ordering there
	src.LHS, in.Source = in, src.LHS
	return src, Rewrote("push down ordering on the LHS of a join")
}

func overlaps(ctx *plancontext.PlanningContext, order []OrderBy, grouping []GroupBy) bool {
ordering:
	for _, orderBy := range order {
		for _, groupBy := range grouping {
			if ctx.SemTable.EqualsExprWithDeps(orderBy.SimplifiedExpr, groupBy.Inner) {
				continue ordering
			}
		}
		return false
	}

	return true
}

func pushOrderingUnderAggr(ctx *plancontext.PlanningContext, order *Ordering, aggregator *Aggregator) (Operator, *ApplyResult) {
	// If Aggregator is a derived table, then we should rewrite the ordering before pushing.
	if aggregator.isDerived() {
		for idx, orderExpr := range order.Order {
			ti, err := ctx.SemTable.TableInfoFor(aggregator.DT.TableID)
			if err != nil {
				panic(err)
			}
			newOrderExpr := orderExpr.Map(func(expr sqlparser.Expr) sqlparser.Expr {
				return semantics.RewriteDerivedTableExpression(expr, ti)
			})
			order.Order[idx] = newOrderExpr
		}
	}

	// Step 1: Align the GROUP BY and ORDER BY.
	//         Reorder the GROUP BY columns to match the ORDER BY columns.
	//         Since the GB clause is a set, we can reorder these columns freely.
	var newGrouping []GroupBy
	used := make([]bool, len(aggregator.Grouping))
	for _, orderExpr := range order.Order {
		for grpIdx, by := range aggregator.Grouping {
			if !used[grpIdx] && ctx.SemTable.EqualsExprWithDeps(by.Inner, orderExpr.SimplifiedExpr) {
				newGrouping = append(newGrouping, by)
				used[grpIdx] = true
			}
		}
	}

	// Step 2: Add any missing columns from the ORDER BY.
	//         The ORDER BY column is not a set, but we can add more elements
	//         to the end without changing the semantics of the query.
	if len(newGrouping) != len(aggregator.Grouping) {
		// we are missing some groupings. We need to add them both to the new groupings list, but also to the ORDER BY
		for i, added := range used {
			if !added {
				groupBy := aggregator.Grouping[i]
				newGrouping = append(newGrouping, groupBy)
				order.Order = append(order.Order, groupBy.AsOrderBy())
			}
		}
	}

	aggregator.Grouping = newGrouping
	aggrSource, isOrdering := aggregator.Source.(*Ordering)
	if isOrdering {
		// Transform the query plan tree:
		// From:   Ordering(1)      To: Aggregation
		//               |                 |
		//         Aggregation          Ordering(1)
		//               |                 |
		//         Ordering(2)          <Inputs>
		//               |
		//           <Inputs>
		//
		// Remove Ordering(2) from the plan tree, as it's redundant
		// after pushing down the higher ordering.
		order.Source = aggrSource.Source
		aggrSource.Source = nil // removing from plan tree
		aggregator.Source = order
		return aggregator, Rewrote("push ordering under aggregation, removing extra ordering")
	}
	return Swap(order, aggregator, "push ordering under aggregation")
}

func canPushLeft(ctx *plancontext.PlanningContext, aj *ApplyJoin, order []OrderBy) bool {
	lhs := TableID(aj.LHS)
	for _, order := range order {
		deps := ctx.SemTable.DirectDeps(order.Inner.Expr)
		if !deps.IsSolvedBy(lhs) {
			return false
		}
	}
	return true
}

func isOuterTable(op Operator, ts semantics.TableSet) bool {
	aj, ok := op.(*ApplyJoin)
	if ok && aj.LeftJoin && TableID(aj.RHS).IsOverlapping(ts) {
		return true
	}

	for _, op := range op.Inputs() {
		if isOuterTable(op, ts) {
			return true
		}
	}

	return false
}

func tryPushFilter(ctx *plancontext.PlanningContext, in *Filter) (Operator, *ApplyResult) {
	switch src := in.Source.(type) {
	case *Projection:
		return pushFilterUnderProjection(ctx, in, src)
	case *Route:
		for _, pred := range in.Predicates {
			deps := ctx.SemTable.RecursiveDeps(pred)
			if !isOuterTable(src, deps) {
				// we can only update based on predicates on inner tables
				src.Routing = src.Routing.updateRoutingLogic(ctx, pred)
			}
		}
		return Swap(in, src, "push filter into Route")
	case *SubQuery:
		outerTableID := TableID(src.Outer)
		for _, pred := range in.Predicates {
			deps := ctx.SemTable.RecursiveDeps(pred)
			if !deps.IsSolvedBy(outerTableID) {
				return in, NoRewrite
			}
		}
		src.Outer, in.Source = in, src.Outer
		return src, Rewrote("push filter to outer query in subquery container")
	}

	return in, NoRewrite
}

func pushFilterUnderProjection(ctx *plancontext.PlanningContext, filter *Filter, projection *Projection) (Operator, *ApplyResult) {
	for _, p := range filter.Predicates {
		cantPush := false
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			if !mustFetchFromInput(node) {
				return true, nil
			}

			if projection.needsEvaluation(ctx, node.(sqlparser.Expr)) {
				cantPush = true
				return false, io.EOF
			}

			return true, nil
		}, p)

		if cantPush {
			return filter, NoRewrite
		}
	}
	return Swap(filter, projection, "push filter under projection")

}

func tryPushDistinct(in *Distinct) (Operator, *ApplyResult) {
	if in.Required && in.PushedPerformance {
		return in, NoRewrite
	}
	switch src := in.Source.(type) {
	case *Route:
		if isDistinct(src.Source) && src.IsSingleShard() {
			return src, Rewrote("distinct not needed")
		}
		if src.IsSingleShard() || !in.Required {
			return Swap(in, src, "push distinct under route")
		}

		if isDistinct(src.Source) {
			return in, NoRewrite
		}

		src.Source = &Distinct{Source: src.Source}
		in.PushedPerformance = true

		return in, Rewrote("added distinct under route - kept original")
	case *Distinct:
		src.Required = false
		src.PushedPerformance = false
		return src, Rewrote("remove double distinct")
	case *Union:
		for i := range src.Sources {
			src.Sources[i] = &Distinct{Source: src.Sources[i]}
		}
		in.PushedPerformance = true

		return in, Rewrote("push down distinct under union")
	case JoinOp:
		src.SetLHS(&Distinct{Source: src.GetLHS()})
		src.SetRHS(&Distinct{Source: src.GetRHS()})
		in.PushedPerformance = true

		if in.Required {
			return in, Rewrote("push distinct under join - kept original")
		}

		return in.Source, Rewrote("push distinct under join")
	case *Ordering:
		in.Source = src.Source
		return in, Rewrote("remove ordering under distinct")
	}

	return in, NoRewrite
}

func isDistinct(op Operator) bool {
	switch op := op.(type) {
	case *Distinct:
		return true
	case *Union:
		return op.distinct
	case *Horizon:
		return op.Query.IsDistinct()
	case *Limit:
		return isDistinct(op.Source)
	default:
		return false
	}
}

func tryPushUnion(ctx *plancontext.PlanningContext, op *Union) (Operator, *ApplyResult) {
	if res := compactUnion(op); res != NoRewrite {
		return op, res
	}

	var sources []Operator
	var selects []sqlparser.SelectExprs

	if op.distinct {
		sources, selects = mergeUnionInputInAnyOrder(ctx, op)
	} else {
		sources, selects = mergeUnionInputsInOrder(ctx, op)
	}

	if len(sources) == 1 {
		result := sources[0].(*Route)
		if result.IsSingleShard() || !op.distinct {
			return result, Rewrote("push union under route")
		}

		return &Distinct{
			Source:   result,
			Required: true,
		}, Rewrote("push union under route")
	}

	if len(sources) == len(op.Sources) {
		return op, NoRewrite
	}
	return newUnion(sources, selects, op.unionColumns, op.distinct), Rewrote("merge union inputs")
}

// addTruncationOrProjectionToReturnOutput uses the original Horizon to make sure that the output columns line up with what the user asked for
func addTruncationOrProjectionToReturnOutput(ctx *plancontext.PlanningContext, selExprs sqlparser.SelectExprs, output Operator) Operator {
	if len(selExprs) == 0 {
		return output
	}

	cols := output.GetSelectExprs(ctx)
	sizeCorrect := len(selExprs) == len(cols) || tryTruncateColumnsAt(output, len(selExprs))
	if sizeCorrect && colNamesAlign(selExprs, cols) {
		return output
	}

	return createSimpleProjection(ctx, selExprs, output)
}

func colNamesAlign(expected, actual sqlparser.SelectExprs) bool {
	if len(expected) > len(actual) {
		// if we expect more columns than we have, we can't align
		return false
	}

	for i, seE := range expected {
		switch se := seE.(type) {
		case *sqlparser.AliasedExpr:
			if !areColumnNamesAligned(se, actual[i]) {
				return false
			}
		case *sqlparser.StarExpr:
			actualStar, isStar := actual[i].(*sqlparser.StarExpr)
			if !isStar {
				panic(vterrors.VT13001(fmt.Sprintf("star expression is expected here, found: %T", actual[i])))
			}
			if !sqlparser.Equals.RefOfStarExpr(se, actualStar) {
				return false
			}
		}
	}
	return true
}

func areColumnNamesAligned(expectation *sqlparser.AliasedExpr, actual sqlparser.SelectExpr) bool {
	_, isCol := expectation.Expr.(*sqlparser.ColName)
	if expectation.As.IsEmpty() && !isCol {
		// is the user didn't specify a name, we don't care
		return true
	}
	actualAE, isAe := actual.(*sqlparser.AliasedExpr)
	if !isAe {
		panic(vterrors.VT13001("used star expression when user did not"))
	}
	return expectation.ColumnName() == actualAE.ColumnName()
}

func stopAtRoute(operator Operator) VisitRule {
	_, isRoute := operator.(*Route)
	return VisitRule(!isRoute)
}

func aeWrap(e sqlparser.Expr) *sqlparser.AliasedExpr {
	return &sqlparser.AliasedExpr{Expr: e}
}
