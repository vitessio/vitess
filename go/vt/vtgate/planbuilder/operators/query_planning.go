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

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/predicates"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func planQuery(ctx *plancontext.PlanningContext, root Operator) Operator {
	var selExpr []sqlparser.SelectExpr
	if horizon, isHorizon := root.(*Horizon); isHorizon {
		sel := getFirstSelect(horizon.Query)
		selExpr = sqlparser.Clone(sel.SelectExprs).Exprs
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
	}

	return addGroupByOnRHSOfJoin(op)
}

func runRewriters(ctx *plancontext.PlanningContext, root Operator) Operator {
	visitor := func(in Operator, _ semantics.TableSet, isRoot bool) (Operator, *ApplyResult) {
		switch in := in.(type) {
		case *Horizon:
			return pushOrExpandHorizon(ctx, in)
		case *ApplyJoin:
			return tryMergeApplyJoin(in, ctx)
		case *Join:
			return optimizeJoin(ctx, in)
		case *Projection:
			return tryPushProjection(ctx, in)
		case *Limit:
			return tryPushLimit(ctx, in)
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
		case *Update:
			return tryPushUpdate(in)
		case *RecurseCTE:
			return tryMergeRecurse(ctx, in)
		default:
			return in, NoRewrite
		}
	}

	if pbm, ok := root.(*PercentBasedMirror); ok {
		pbm.SetInputs([]Operator{
			runRewriters(ctx, pbm.Operator()),
			runRewriters(ctx.UseMirror(), pbm.Target()),
		})
	}

	return FixedPointBottomUp(root, TableID, visitor, stopAtRoute)
}

func tryMergeApplyJoin(in *ApplyJoin, ctx *plancontext.PlanningContext) (_ Operator, res *ApplyResult) {
	preds := slice.Map(in.JoinPredicates.columns, func(col applyJoinColumn) sqlparser.Expr {
		return col.Original
	})

	jm := newJoinMerge(preds, in.JoinType)
	r := jm.mergeJoinInputs(ctx, in.LHS, in.RHS)
	if r == nil {
		return in, NoRewrite
	}
	aj, ok := r.Source.(*ApplyJoin)
	if !ok {
		panic(vterrors.VT13001("expected apply join"))
	}

	// Copy the join predicates from the original ApplyJoin to the new one.
	aj.JoinPredicates = in.JoinPredicates

	//  - Rewrite join predicates already pushed down &&
	//  - Save original join predicates if we have to bail out of the rewrite
	original := map[predicates.ID]sqlparser.Expr{}
	for _, col := range aj.JoinPredicates.columns {
		if col.JoinPredicateID != nil {
			// if we have pushed down a join predicate, we need to restore it to its original shape, without the argument from the LHS
			id := *col.JoinPredicateID
			oldExpr, err := ctx.PredTracker.Get(id)
			if err != nil {
				panic(err)
			}
			original[id] = oldExpr
			ctx.PredTracker.Set(id, col.Original)
		}
	}

	// Defer restoration of original predicates if no successful rewrite happens.
	defer func() {
		if res == NoRewrite {
			for id, expr := range original {
				ctx.PredTracker.Set(id, expr)
			}
		}
	}()

	// After merging joins, routing logic may have changed. Re-evaluate routing decisions.
	// Example scenario:
	// Before merge: routing based on predicates like ':lhs_col = rhs.col'.
	// After merge: predicate rewritten to 'lhs.col = rhs.col', making this predicate invalid for routing.
	r.Routing = r.Routing.resetRoutingLogic(ctx)

	// Verify if the LHS is a Route operator, which is required for this rewrite.
	rb, ok := in.LHS.(*Route)
	success := "pushed ApplyJoin under Route"
	if !ok {
		// Unexpected scenario: LHS is not a Route; abort rewrite.
		return in, NoRewrite
	}

	// Special case: If LHS is a DualRouting AND the join isn't INNER or targeting a single shard,
	// we cannot safely perform this rewrite.
	if _, isDual := rb.Routing.(*DualRouting); isDual &&
		!(jm.joinType.IsInner() || r.Routing.OpCode().IsSingleShard()) {
		// to check the resulting opcode, we've used the original predicates.
		// Since we are not using them, we need to restore the argument versions of the predicates
		return in, NoRewrite
	}

	return r, Rewrote(success)
}

func tryPushDelete(in *Delete) (Operator, *ApplyResult) {
	if src, ok := in.Source.(*Route); ok {
		return pushDMLUnderRoute(in, src, "pushed delete under route")
	}
	return in, NoRewrite
}

func tryPushUpdate(in *Update) (Operator, *ApplyResult) {
	if src, ok := in.Source.(*Route); ok {
		return pushDMLUnderRoute(in, src, "pushed update under route")
	}
	return in, NoRewrite
}

func pushDMLUnderRoute(in Operator, src *Route, msg string) (Operator, *ApplyResult) {
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
	return Swap(in, src, msg)
}

func pushLockAndComment(l *LockAndComment) (Operator, *ApplyResult) {
	switch src := l.Source.(type) {
	case *Horizon, *QueryGraph:
		// we want to wait until the horizons have been pushed under a route or expanded
		// that way we know that we've replaced the QueryGraphs with Routes
		return l, NoRewrite
	case *Route:
		src.Comments = l.Comments
		src.Lock = l.Lock.GetHighestOrderLock(src.Lock)
		return src, Rewrote("put lock and comment into route")
	case *SubQueryContainer:
		src.Outer = newLockAndComment(src.Outer, l.Comments, l.Lock)
		for _, sq := range src.Inner {
			sq.Subquery = newLockAndComment(sq.Subquery, l.Comments, l.Lock)
		}
		return src, Rewrote("push lock and comment into subquery container")
	default:
		inputs := src.Inputs()
		for i, op := range inputs {
			inputs[i] = newLockAndComment(op, l.Comments, l.Lock)
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
		!isDistinctAST(in.selectStatement()) &&
		in.selectStatement().GetLimit() == nil

	if canPush {
		return Swap(in, rb, "push horizon into route")
	}

	return expandHorizon(ctx, in)
}

func tryPushLimit(ctx *plancontext.PlanningContext, in *Limit) (Operator, *ApplyResult) {
	switch src := in.Source.(type) {
	case *Route:
		return tryPushingDownLimitInRoute(ctx, in, src)
	case *Aggregator:
		return in, NoRewrite
	case *ApplyJoin:
		if in.Pushed {
			// This is the Top limit, and it's already pushed down
			return in, NoRewrite
		}
		side := "RHS"
		src.RHS = createPushedLimit(ctx, src.RHS, in)
		if IsOuter(src) {
			// for outer joins, we are guaranteed that all rows from the LHS will be returned,
			// so we can push down the LIMIT to the LHS
			src.LHS = createPushedLimit(ctx, src.LHS, in)
			side = "RHS and LHS"
		}

		if in.Top {
			in.Pushed = true
			return in, Rewrote(fmt.Sprintf("add limit to %s of apply join", side))
		}

		return src, Rewrote(fmt.Sprintf("push limit to %s of apply join", side))
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

func createPushedLimit(ctx *plancontext.PlanningContext, src Operator, orig *Limit) Operator {
	pushedLimit := sqlparser.Clone(orig.AST)
	if pushedLimit.Offset != nil {
		// we can't push down an offset, so we need to convert it to a rowcount
		// by adding it to the already existing rowcount, and then let the LIMIT running on the vtgate do the rest
		// this way we can still limit the number of rows that are returned
		plus := &sqlparser.BinaryExpr{
			Operator: sqlparser.PlusOp,
			Left:     pushedLimit.Rowcount,
			Right:    pushedLimit.Offset,
		}
		pushedLimit.Rowcount = getLimitExpression(ctx, plus)
		pushedLimit.Offset = nil
	}
	return newLimit(src, pushedLimit, false)
}

// getLimitExpression is a helper function to simplify an expression using the evalengine
// if it's not able to simplify the expression to a literal, it will return an argument expression for :__upper_limit
func getLimitExpression(ctx *plancontext.PlanningContext, expr sqlparser.Expr) sqlparser.Expr {
	cfg := evalengine.Config{
		Environment: ctx.VSchema.Environment(),
	}
	translated, err := evalengine.Translate(expr, &cfg)
	if err != nil {
		panic(vterrors.VT13001("failed to translate expression: " + err.Error()))
	}
	_, isLit := translated.(*evalengine.Literal)
	if isLit {
		return translated
	}

	// we were not able to calculate the expression, so we can't push it down
	// the LIMIT above us will set an argument for us that we can use here
	return sqlparser.NewArgument(engine.UpperLimitStr)
}

func tryPushingDownLimitInRoute(ctx *plancontext.PlanningContext, in *Limit, src *Route) (Operator, *ApplyResult) {
	if src.IsSingleShardOrByDestination() {
		return Swap(in, src, "push limit under route")
	}

	if sqlparser.IsDMLStatement(ctx.Statement) {
		return setUpperLimit(in)
	}

	// this limit has already been pushed down, nothing to do here
	if in.Pushed {
		return in, NoRewrite
	}

	// when pushing a LIMIT into a Route that is not single sharded,
	// we leave a LIMIT on top of the Route, and push a LIMIT under the Route
	// This way we can still limit the number of rows that are returned
	// from the Route and that way minimize unneeded processing
	src.Source = createPushedLimit(ctx, src.Source, in)
	in.Pushed = true

	return in, Rewrote("pushed limit under route")
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
			ast := &sqlparser.Limit{Rowcount: sqlparser.NewArgument(engine.UpperLimitStr)}
			op.Source = newLimit(op.Source, ast, false)
			result = result.Merge(Rewrote("push upper limit under route"))
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
	}
	return in, NoRewrite
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
		if !mustFetchFromInput(ctx, by.SimplifiedExpr) {
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

// pushOrderingUnderAggr pushes the ORDER BY clause under the aggregation if possible,
// to optimize the query plan by aligning the GROUP BY and ORDER BY clauses and
// potentially removing redundant ORDER BY clauses.
func pushOrderingUnderAggr(ctx *plancontext.PlanningContext, order *Ordering, aggregator *Aggregator) (Operator, *ApplyResult) {
	// Avoid pushing down too early to allow for aggregation pushdown to MySQL
	if !reachedPhase(ctx, delegateAggregation) {
		return order, NoRewrite
	}

	// Rewrite ORDER BY if Aggregator is a derived table
	if aggregator.isDerived() {
		for idx, orderExpr := range order.Order {
			ti, err := ctx.SemTable.TableInfoFor(aggregator.DT.TableID)
			if err != nil {
				panic(err)
			}
			// Rewrite expressions in ORDER BY to match derived table columns
			newOrderExpr := orderExpr.Map(func(expr sqlparser.Expr) sqlparser.Expr {
				return semantics.RewriteDerivedTableExpression(expr, ti)
			})
			order.Order[idx] = newOrderExpr
		}
	}

	// Align GROUP BY with ORDER BY by reordering GROUP BY columns to match ORDER BY
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

	// Add any missing GROUP BY columns to ORDER BY
	if len(newGrouping) != len(aggregator.Grouping) {
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
		// Optimize query plan by removing redundant ORDER BY
		// From:   Ordering(1)      To: Aggregation
		//               |                 |
		//         Aggregation          Ordering(1)
		//               |                 |
		//         Ordering(2)          <Inputs>
		//               |
		//           <Inputs>
		order.Source = aggrSource.Source
		aggrSource.Source = nil
		aggregator.Source = order
		return aggregator, Rewrote("push ordering under aggregation, removing extra ordering")
	}
	return Swap(order, aggregator, "push ordering under aggregation")
}

func canPushLeft(ctx *plancontext.PlanningContext, aj *ApplyJoin, order []OrderBy) bool {
	lhs := TableID(aj.LHS)
	for _, order := range order {
		deps := ctx.SemTable.RecursiveDeps(order.Inner.Expr)
		if !deps.IsSolvedBy(lhs) {
			return false
		}
	}
	return true
}

func isOuterTable(op Operator, ts semantics.TableSet) bool {
	aj, ok := op.(*ApplyJoin)
	if ok && !aj.IsInner() && TableID(aj.RHS).IsOverlapping(ts) {
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
	case *Filter:
		if len(in.Predicates) == 0 {
			return in.Source, Rewrote("filter with no predicates removed")
		}

		other, isFilter := in.Source.(*Filter)
		if !isFilter {
			return in, NoRewrite
		}
		in.Source = other.Source
		in.Predicates = append(in.Predicates, other.Predicates...)
		return in, Rewrote("two filters merged into one")
	}

	return in, NoRewrite
}

func pushFilterUnderProjection(ctx *plancontext.PlanningContext, filter *Filter, projection *Projection) (Operator, *ApplyResult) {
	for _, p := range filter.Predicates {
		cantPush := false
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			if !mustFetchFromInput(ctx, node) {
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

		src.Source = newDistinct(src.Source, nil, false)
		in.PushedPerformance = true

		return in, Rewrote("added distinct under route - kept original")
	case *Distinct:
		src.Required = false
		src.PushedPerformance = false
		return src, Rewrote("remove double distinct")
	case *Union:
		for i := range src.Sources {
			src.Sources[i] = newDistinct(src.Sources[i], nil, false)
		}
		in.PushedPerformance = true

		return in, Rewrote("push down distinct under union")
	case JoinOp:
		src.SetLHS(newDistinct(src.GetLHS(), nil, false))
		src.SetRHS(newDistinct(src.GetRHS(), nil, false))
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
		return isDistinctAST(op.Query)
	case *Limit:
		return isDistinct(op.Source)
	default:
		return false
	}
}

func isDistinctAST(s sqlparser.Statement) bool {
	if d, ok := s.(sqlparser.Distinctable); ok {
		return d.IsDistinct()
	}
	return false
}

func tryPushUnion(ctx *plancontext.PlanningContext, op *Union) (Operator, *ApplyResult) {
	if res := compactUnion(op); res != NoRewrite {
		return op, res
	}

	var sources []Operator
	var selects [][]sqlparser.SelectExpr

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

		return newDistinct(result, nil, true), Rewrote("push union under route")
	}

	if len(sources) == len(op.Sources) {
		return op, NoRewrite
	}
	return newUnion(sources, selects, op.unionColumns, op.distinct), Rewrote("merge union inputs")
}

// addTruncationOrProjectionToReturnOutput uses the original Horizon to make sure that the output columns line up with what the user asked for
func addTruncationOrProjectionToReturnOutput(ctx *plancontext.PlanningContext, selExprs []sqlparser.SelectExpr, output Operator) Operator {
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

func colNamesAlign(expected, actual []sqlparser.SelectExpr) bool {
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
