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

	"vitess.io/vitess/go/vt/vtgate/engine"

	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/vt/sqlparser"
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
		default:
			return in, NoRewrite
		}
	}

	return FixedPointBottomUp(root, TableID, visitor, stopAtRoute)
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
		src.Outer = &LockAndComment{
			Source:   src.Outer,
			Comments: l.Comments,
			Lock:     l.Lock,
		}
		for _, sq := range src.Inner {
			sq.Subquery = &LockAndComment{
				Source:   sq.Subquery,
				Comments: l.Comments,
				Lock:     l.Lock,
			}
		}
		return src, Rewrote("push lock and comment into subquery container")
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
	default:
		return setUpperLimit(in)
	}
}

func createPushedLimit(ctx *plancontext.PlanningContext, src Operator, orig *Limit) Operator {
	pushedLimit := sqlparser.CloneRefOfLimit(orig.AST)
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
	return &Limit{
		Source: src,
		AST:    pushedLimit,
	}
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
		case *Route:
			newSrc := &Limit{
				Source: op.Source,
				AST:    &sqlparser.Limit{Rowcount: sqlparser.NewArgument(engine.UpperLimitStr)},
			}
			op.Source = newSrc
			result = result.Merge(Rewrote("push upper limit under route"))
			return SkipChildren
		default:
			return VisitChildren
		}
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
			// ApplyJoin is stable in regard to the columns coming from the LHS,
			// so if all the ordering columns come from the LHS, we can push down the Ordering there
			src.LHS, in.Source = in, src.LHS
			return src, Rewrote("push down ordering on the LHS of a join")
		}
	case *Ordering:
		// we'll just remove the order underneath. The top order replaces whatever was incoming
		in.Source = src.Source
		return in, Rewrote("remove double ordering")
	case *Projection:
		// we can move ordering under a projection if it's not introducing a column we're sorting by
		for _, by := range in.Order {
			if !mustFetchFromInput(ctx, by.SimplifiedExpr) {
				return in, NoRewrite
			}
		}
		return Swap(in, src, "push ordering under projection")
	case *Aggregator:
		if !src.QP.AlignGroupByAndOrderBy(ctx) && !overlaps(ctx, in.Order, src.Grouping) {
			return in, NoRewrite
		}

		return pushOrderingUnderAggr(ctx, in, src)
	case *SubQueryContainer:
		outerTableID := TableID(src.Outer)
		for _, order := range in.Order {
			deps := ctx.SemTable.RecursiveDeps(order.Inner.Expr)
			if !deps.IsSolvedBy(outerTableID) {
				return in, NoRewrite
			}
		}
		src.Outer, in.Source = in, src.Outer
		return src, Rewrote("push ordering into outer side of subquery")
	case *SubQuery:
		outerTableID := TableID(src.Outer)
		for _, order := range in.Order {
			deps := ctx.SemTable.RecursiveDeps(order.Inner.Expr)
			if !deps.IsSolvedBy(outerTableID) {
				return in, NoRewrite
			}
		}
		src.Outer, in.Source = in, src.Outer
		return src, Rewrote("push ordering into outer side of subquery")
	}
	return in, NoRewrite
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
	for i, seE := range expected {
		switch se := seE.(type) {
		case *sqlparser.AliasedExpr:
			if !areColumnNamesAligned(se, actual[i]) {
				return false
			}
		case *sqlparser.StarExpr:
			actualStar, isStar := actual[i].(*sqlparser.StarExpr)
			if !isStar {
				panic("I DONT THINK THIS CAN HAPPEN")
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
