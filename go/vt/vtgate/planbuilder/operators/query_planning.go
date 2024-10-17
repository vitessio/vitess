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
	"slices"
	"strconv"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	projector struct {
		columns               []*ProjExpr
		columnAliases         []string
		explicitColumnAliases bool
		tableName             sqlparser.TableName
	}
)

func planQuery(ctx *plancontext.PlanningContext, root ops.Operator) (output ops.Operator, err error) {
	output, err = runPhases(ctx, root)
	if err != nil {
		return nil, err
	}

	output, err = planOffsets(ctx, output)
	if err != nil {
		return nil, err
	}

	if rewrite.DebugOperatorTree {
		fmt.Println("After offset planning:")
		fmt.Println(ops.ToTree(output))
	}

	output, err = compact(ctx, output)
	if err != nil {
		return nil, err
	}

	return addTruncationOrProjectionToReturnOutput(ctx, root, output)
}

// runPhases is the process of figuring out how to perform the operations in the Horizon
// If we can push it under a route - done.
// If we can't, we will instead expand the Horizon into
// smaller operators and try to push these down as far as possible
func runPhases(ctx *plancontext.PlanningContext, root ops.Operator) (op ops.Operator, err error) {
	op = root
	for _, phase := range getPhases(ctx) {
		ctx.CurrentPhase = int(phase)
		if rewrite.DebugOperatorTree {
			fmt.Printf("PHASE: %s\n", phase.String())
		}

		op, err = phase.act(ctx, op)
		if err != nil {
			return nil, err
		}

		op, err = runRewriters(ctx, op)
		if err != nil {
			return nil, err
		}

		op, err = compact(ctx, op)
		if err != nil {
			return nil, err
		}
	}

	return addGroupByOnRHSOfJoin(op)
}

func runRewriters(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	visitor := func(in ops.Operator, _ semantics.TableSet, isRoot bool) (ops.Operator, *rewrite.ApplyResult, error) {
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
		default:
			return in, rewrite.SameTree, nil
		}
	}

	return rewrite.FixedPointBottomUp(root, TableID, visitor, stopAtRoute)
}

func pushLockAndComment(l *LockAndComment) (ops.Operator, *rewrite.ApplyResult, error) {
	switch src := l.Source.(type) {
	case *Horizon, *QueryGraph:
		// we want to wait until the horizons have been pushed under a route or expanded
		// that way we know that we've replaced the QueryGraphs with Routes
		return src, rewrite.SameTree, nil
	case *Route:
		src.Comments = l.Comments
		src.Lock = l.Lock
		return src, rewrite.NewTree("put lock and comment into route", l), nil
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
		return src, rewrite.NewTree("pushed down lock and comments", l), nil
	}
}

func pushOrExpandHorizon(ctx *plancontext.PlanningContext, in *Horizon) (ops.Operator, *rewrite.ApplyResult, error) {
	if in.IsDerived() {
		newOp, result, err := pushDerived(ctx, in)
		if err != nil {
			return nil, nil, err
		}
		if result != rewrite.SameTree {
			return newOp, result, nil
		}
	}

	if !reachedPhase(ctx, initialPlanning) {
		return in, rewrite.SameTree, nil
	}

	if ctx.SemTable.QuerySignature.SubQueries {
		return expandHorizon(ctx, in)
	}

	rb, isRoute := in.src().(*Route)
	if isRoute && rb.IsSingleShard() {
		return rewrite.Swap(in, rb, "push horizon into route")
	}

	sel, isSel := in.selectStatement().(*sqlparser.Select)

	qp, err := in.getQP(ctx)
	if err != nil {
		return nil, nil, err
	}

	needsOrdering := len(qp.OrderExprs) > 0
	hasHaving := isSel && sel.Having != nil

	canPush := isRoute &&
		!hasHaving &&
		!needsOrdering &&
		!qp.NeedsAggregation() &&
		!in.selectStatement().IsDistinct() &&
		in.selectStatement().GetLimit() == nil

	if canPush {
		return rewrite.Swap(in, rb, "push horizon into route")
	}

	return expandHorizon(ctx, in)
}

func tryPushProjection(
	ctx *plancontext.PlanningContext,
	p *Projection,
) (ops.Operator, *rewrite.ApplyResult, error) {
	switch src := p.Source.(type) {
	case *Route:
		return rewrite.Swap(p, src, "push projection under route")
	case *ApplyJoin:
		if p.FromAggr || !p.canPush(ctx) {
			return p, rewrite.SameTree, nil
		}
		return pushProjectionInApplyJoin(ctx, p, src)
	case *Vindex:
		if !p.canPush(ctx) {
			return p, rewrite.SameTree, nil
		}
		return pushProjectionInVindex(ctx, p, src)
	case *SubQueryContainer:
		if !p.canPush(ctx) {
			return p, rewrite.SameTree, nil
		}
		return pushProjectionToOuterContainer(ctx, p, src)
	case *SubQuery:
		return pushProjectionToOuter(ctx, p, src)
	case *Limit:
		return rewrite.Swap(p, src, "push projection under limit")
	default:
		return p, rewrite.SameTree, nil
	}
}

func pushProjectionToOuter(ctx *plancontext.PlanningContext, p *Projection, sq *SubQuery) (ops.Operator, *rewrite.ApplyResult, error) {
	ap, err := p.GetAliasedProjections()
	if err != nil {
		return p, rewrite.SameTree, nil
	}

	if !reachedPhase(ctx, subquerySettling) || err != nil {
		return p, rewrite.SameTree, nil
	}

	outer := TableID(sq.Outer)
	for _, pe := range ap {
		_, isOffset := pe.Info.(*Offset)
		if isOffset {
			continue
		}

		if !ctx.SemTable.RecursiveDeps(pe.EvalExpr).IsSolvedBy(outer) {
			return p, rewrite.SameTree, nil
		}

		se, ok := pe.Info.(SubQueryExpression)
		if ok {
			pe.EvalExpr = rewriteColNameToArgument(ctx, pe.EvalExpr, se, sq)
		}
	}
	// all projections can be pushed to the outer
	sq.Outer, p.Source = p, sq.Outer
	return sq, rewrite.NewTree("push projection into outer side of subquery", p), nil
}

func pushProjectionInVindex(
	ctx *plancontext.PlanningContext,
	p *Projection,
	src *Vindex,
) (ops.Operator, *rewrite.ApplyResult, error) {
	ap, err := p.GetAliasedProjections()
	if err != nil {
		return nil, nil, err
	}
	for _, pe := range ap {
		_, err = src.AddColumn(ctx, true, false, aeWrap(pe.EvalExpr))
		if err != nil {
			return nil, nil, err
		}
	}
	return src, rewrite.NewTree("push projection into vindex", p), nil
}

func (p *projector) add(pe *ProjExpr, alias string) {
	p.columns = append(p.columns, pe)
	if alias != "" && slices.Index(p.columnAliases, alias) > -1 {
		panic("alias already used")
	}
}

// get finds or adds an expression in the projector, returning its SQL representation with the appropriate alias
func (p *projector) get(ctx *plancontext.PlanningContext, expr sqlparser.Expr) sqlparser.Expr {
	for _, column := range p.columns {
		if ctx.SemTable.EqualsExprWithDeps(expr, column.ColExpr) {
			alias := p.claimUnusedAlias(column.Original)
			out := sqlparser.NewColName(alias)
			out.Qualifier = p.tableName

			ctx.SemTable.CopySemanticInfo(expr, out)
			return out
		}
	}

	// we could not find the expression, so we add it
	alias := sqlparser.UnescapedString(expr)
	pe := newProjExpr(sqlparser.NewAliasedExpr(expr, alias))
	p.columns = append(p.columns, pe)
	p.columnAliases = append(p.columnAliases, alias)

	out := sqlparser.NewColName(alias)
	out.Qualifier = p.tableName

	ctx.SemTable.CopySemanticInfo(expr, out)

	return out
}

// claimUnusedAlias generates a unique alias based on the provided expression, ensuring no duplication in the projector
func (p *projector) claimUnusedAlias(ae *sqlparser.AliasedExpr) string {
	bare := ae.ColumnName()
	alias := bare
	for i := int64(0); slices.Index(p.columnAliases, alias) > -1; i++ {
		alias = bare + strconv.FormatInt(i, 10)
	}
	return alias
}

// pushProjectionInApplyJoin pushes down a projection operation into an ApplyJoin operation.
// It processes each input column and creates new JoinPredicates for the ApplyJoin operation based on
// the input column's expression. It also creates new Projection operators for the left and right
// children of the ApplyJoin operation, if needed.
func pushProjectionInApplyJoin(
	ctx *plancontext.PlanningContext,
	p *Projection,
	src *ApplyJoin,
) (ops.Operator, *rewrite.ApplyResult, error) {
	ap, err := p.GetAliasedProjections()
	if src.LeftJoin || err != nil {
		// we can't push down expression evaluation to the rhs if we are not sure if it will even be executed
		return p, rewrite.SameTree, nil
	}
	lhs, rhs := &projector{}, &projector{}
	if p.DT != nil && len(p.DT.Columns) > 0 {
		lhs.explicitColumnAliases = true
		rhs.explicitColumnAliases = true
	}

	src.JoinColumns = nil
	for idx, pe := range ap {
		var alias string
		if p.DT != nil && idx < len(p.DT.Columns) {
			alias = p.DT.Columns[idx].String()
		}
		err := splitProjectionAcrossJoin(ctx, src, lhs, rhs, pe, alias)
		if err != nil {
			return nil, nil, err
		}
	}

	if p.isDerived() {
		err := exposeColumnsThroughDerivedTable(ctx, p, src, lhs, rhs)
		if err != nil {
			return nil, nil, err
		}
	}

	// Create and update the Projection operators for the left and right children, if needed.
	src.LHS, err = createProjectionWithTheseColumns(ctx, src.LHS, lhs, p.DT)
	if err != nil {
		return nil, nil, err
	}

	src.RHS, err = createProjectionWithTheseColumns(ctx, src.RHS, rhs, p.DT)
	if err != nil {
		return nil, nil, err
	}

	return src, rewrite.NewTree("split projection to either side of join", src), nil
}

// splitProjectionAcrossJoin creates JoinPredicates for all projections,
// and pushes down columns as needed between the LHS and RHS of a join
func splitProjectionAcrossJoin(
	ctx *plancontext.PlanningContext,
	join *ApplyJoin,
	lhs, rhs *projector,
	pe *ProjExpr,
	alias string,
) error {

	// Check if the current expression can reuse an existing column in the ApplyJoin.
	if _, found := canReuseColumn(ctx, join.JoinColumns, pe.EvalExpr, joinColumnToExpr); found {
		return nil
	}

	col, err := splitUnexploredExpression(ctx, join, lhs, rhs, pe, alias)
	if err != nil {
		return err
	}

	// Add the new JoinColumn to the ApplyJoin's JoinPredicates.
	join.JoinColumns = append(join.JoinColumns, col)
	return nil
}

func splitUnexploredExpression(
	ctx *plancontext.PlanningContext,
	join *ApplyJoin,
	lhs, rhs *projector,
	pe *ProjExpr,
	alias string,
) (JoinColumn, error) {
	// Get a JoinColumn for the current expression.
	col, err := join.getJoinColumnFor(ctx, pe.Original, pe.ColExpr, false)
	if err != nil {
		return JoinColumn{}, err
	}

	// Update the left and right child columns and names based on the JoinColumn type.
	switch {
	case col.IsPureLeft():
		lhs.add(pe, alias)
	case col.IsPureRight():
		rhs.add(pe, alias)
	case col.IsMixedLeftAndRight():
		for _, lhsExpr := range col.LHSExprs {
			lhsAlias := ""
			if alias != "" {
				// we need to add an explicit column alias here. let's try just the ColName as is first
				lhsAlias = sqlparser.String(lhsExpr.Expr)
			}
			lhs.add(newProjExpr(aeWrap(lhsExpr.Expr)), lhsAlias)
		}
		innerPE := newProjExprWithInner(pe.Original, col.RHSExpr)
		innerPE.ColExpr = col.RHSExpr
		innerPE.Info = pe.Info
		rhs.add(innerPE, alias)
	}
	return col, nil
}

// exposeColumnsThroughDerivedTable rewrites expressions within a join that is inside a derived table
// in order to make them accessible outside the derived table. This is necessary when swapping the
// positions of the derived table and join operation.
//
// For example, consider the input query:
// select ... from (select T1.foo from T1 join T2 on T1.id = T2.id) as t
// If we push the derived table under the join, with T1 on the LHS of the join, we need to expose
// the values of T1.id through the derived table, or they will not be accessible on the RHS.
//
// The function iterates through each join predicate, rewriting the expressions in the predicate's
// LHS expressions to include the derived table. This allows the expressions to be accessed outside
// the derived table.
func exposeColumnsThroughDerivedTable(ctx *plancontext.PlanningContext, p *Projection, src *ApplyJoin, lhs, rhs *projector) error {
	derivedTbl, err := ctx.SemTable.TableInfoFor(p.DT.TableID)
	if err != nil {
		return err
	}
	derivedTblName, err := derivedTbl.Name()
	if err != nil {
		return err
	}
	lhs.tableName = derivedTblName
	rhs.tableName = derivedTblName

	lhsIDs := TableID(src.LHS)
	rhsIDs := TableID(src.RHS)
	rewriteColumnsForJoin(ctx, src.JoinPredicates, lhsIDs, rhsIDs, lhs, rhs, true)
	rewriteColumnsForJoin(ctx, src.JoinColumns, lhsIDs, rhsIDs, lhs, rhs, true)
	return nil
}

func rewriteColumnsForJoin(
	ctx *plancontext.PlanningContext,
	columns []JoinColumn,
	lhsIDs, rhsIDs semantics.TableSet,
	lhs, rhs *projector,
	exposeRHS bool, // we only want to expose the returned columns from the RHS.
	// For predicates, we don't need to expose the RHS columns
) {
	for colIdx, column := range columns {
		for lhsIdx, bve := range column.LHSExprs {
			// since this is on the LHSExprs, we know that dependencies are from that side of the join
			column.LHSExprs[lhsIdx].Expr = lhs.get(ctx, bve.Expr)
		}
		if column.IsPureLeft() {
			continue
		}

		// now we need to go over the predicate and find
		var rewriteTo sqlparser.Expr

		pre := func(node, _ sqlparser.SQLNode) bool {
			_, isSQ := node.(*sqlparser.Subquery)
			if isSQ {
				return false
			}
			expr, ok := node.(sqlparser.Expr)
			if !ok {
				return true
			}
			deps := ctx.SemTable.RecursiveDeps(expr)

			switch {
			case deps.IsEmpty():
				return true
			case deps.IsSolvedBy(lhsIDs):
				rewriteTo = lhs.get(ctx, expr)
				return false
			case deps.IsSolvedBy(rhsIDs):
				if exposeRHS {
					rewriteTo = rhs.get(ctx, expr)
				}
				return false
			default:
				return true
			}
		}

		post := func(cursor *sqlparser.CopyOnWriteCursor) {
			if rewriteTo != nil {
				cursor.Replace(rewriteTo)
				rewriteTo = nil
				return
			}
		}
		newOriginal := sqlparser.CopyOnRewrite(column.Original.Expr, pre, post, ctx.SemTable.CopySemanticInfo).(sqlparser.Expr)
		column.Original.Expr = newOriginal

		columns[colIdx] = column
	}
}

// prefixColNames adds qualifier prefixes to all ColName:s.
// We want to be more explicit than the user was to make sure we never produce invalid SQL
func prefixColNames(tblName sqlparser.TableName, e sqlparser.Expr) sqlparser.Expr {
	return sqlparser.CopyOnRewrite(e, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
		col, ok := cursor.Node().(*sqlparser.ColName)
		if !ok {
			return
		}
		col.Qualifier = tblName
	}, nil).(sqlparser.Expr)
}

func createProjectionWithTheseColumns(
	ctx *plancontext.PlanningContext,
	src ops.Operator,
	p *projector,
	dt *DerivedTable,
) (ops.Operator, error) {
	if len(p.columns) == 0 {
		return src, nil
	}
	proj, err := createProjection(ctx, src, "")
	if err != nil {
		return nil, err
	}
	proj.Columns = AliasedProjections(p.columns)
	if dt != nil {
		kopy := *dt
		proj.DT = &kopy
	}

	return proj, nil
}

func tryPushLimit(in *Limit) (ops.Operator, *rewrite.ApplyResult, error) {
	switch src := in.Source.(type) {
	case *Route:
		return tryPushingDownLimitInRoute(in, src)
	case *Aggregator:
		return in, rewrite.SameTree, nil
	default:
		return setUpperLimit(in)
	}
}

func tryPushingDownLimitInRoute(in *Limit, src *Route) (ops.Operator, *rewrite.ApplyResult, error) {
	if src.IsSingleShard() {
		return rewrite.Swap(in, src, "push limit under route")
	}

	return setUpperLimit(in)
}

func setUpperLimit(in *Limit) (ops.Operator, *rewrite.ApplyResult, error) {
	if in.Pushed {
		return in, rewrite.SameTree, nil
	}
	in.Pushed = true
	visitor := func(op ops.Operator, _ semantics.TableSet, _ bool) (ops.Operator, *rewrite.ApplyResult, error) {
		return op, rewrite.SameTree, nil
	}
	var result *rewrite.ApplyResult
	shouldVisit := func(op ops.Operator) rewrite.VisitRule {
		switch op := op.(type) {
		case *Join, *ApplyJoin, *SubQueryContainer, *SubQuery:
			// we can't push limits down on either side
			return rewrite.SkipChildren
		case *Aggregator:
			if len(op.Grouping) > 0 {
				// we can't push limits down if we have a group by
				return rewrite.SkipChildren
			}
		case *Route:
			newSrc := &Limit{
				Source: op.Source,
				AST:    &sqlparser.Limit{Rowcount: sqlparser.NewArgument("__upper_limit")},
				Pushed: false,
			}
			op.Source = newSrc
			result = result.Merge(rewrite.NewTree("push limit under route", newSrc))
			return rewrite.SkipChildren
		}
		return rewrite.VisitChildren
	}

	_, err := rewrite.TopDown(in.Source, TableID, visitor, shouldVisit)
	if err != nil {
		return nil, nil, err
	}
	return in, result, nil
}

func tryPushOrdering(ctx *plancontext.PlanningContext, in *Ordering) (ops.Operator, *rewrite.ApplyResult, error) {
	switch src := in.Source.(type) {
	case *Route:
		return rewrite.Swap(in, src, "push ordering under route")
	case *Filter:
		return rewrite.Swap(in, src, "push ordering under filter")
	case *ApplyJoin:
		if canPushLeft(ctx, src, in.Order) {
			// ApplyJoin is stable in regard to the columns coming from the LHS,
			// so if all the ordering columns come from the LHS, we can push down the Ordering there
			src.LHS, in.Source = in, src.LHS
			return src, rewrite.NewTree("push down ordering on the LHS of a join", in), nil
		}
	case *Ordering:
		// we'll just remove the order underneath. The top order replaces whatever was incoming
		in.Source = src.Source
		return in, rewrite.NewTree("remove double ordering", src), nil
	case *Projection:
		// we can move ordering under a projection if it's not introducing a column we're sorting by
		for _, by := range in.Order {
			if !fetchByOffset(by.SimplifiedExpr) {
				return in, rewrite.SameTree, nil
			}
		}
		return rewrite.Swap(in, src, "push ordering under projection")
	case *Aggregator:
		if !src.QP.AlignGroupByAndOrderBy(ctx) && !overlaps(ctx, in.Order, src.Grouping) {
			return in, rewrite.SameTree, nil
		}

		return pushOrderingUnderAggr(ctx, in, src)
	case *SubQueryContainer:
		outerTableID := TableID(src.Outer)
		for _, order := range in.Order {
			deps := ctx.SemTable.RecursiveDeps(order.Inner.Expr)
			if !deps.IsSolvedBy(outerTableID) {
				return in, rewrite.SameTree, nil
			}
		}
		src.Outer, in.Source = in, src.Outer
		return src, rewrite.NewTree("push ordering into outer side of subquery", in), nil
	case *SubQuery:
		outerTableID := TableID(src.Outer)
		for _, order := range in.Order {
			deps := ctx.SemTable.RecursiveDeps(order.Inner.Expr)
			if !deps.IsSolvedBy(outerTableID) {
				return in, rewrite.SameTree, nil
			}
		}
		src.Outer, in.Source = in, src.Outer
		return src, rewrite.NewTree("push ordering into outer side of subquery", in), nil
	}
	return in, rewrite.SameTree, nil
}

func overlaps(ctx *plancontext.PlanningContext, order []ops.OrderBy, grouping []GroupBy) bool {
ordering:
	for _, orderBy := range order {
		for _, groupBy := range grouping {
			if ctx.SemTable.EqualsExprWithDeps(orderBy.SimplifiedExpr, groupBy.SimplifiedExpr) {
				continue ordering
			}
		}
		return false
	}

	return true
}

func pushOrderingUnderAggr(ctx *plancontext.PlanningContext, order *Ordering, aggregator *Aggregator) (ops.Operator, *rewrite.ApplyResult, error) {
	// If Aggregator is a derived table, then we should rewrite the ordering before pushing.
	if aggregator.isDerived() {
		for idx, orderExpr := range order.Order {
			ti, err := ctx.SemTable.TableInfoFor(aggregator.DT.TableID)
			if err != nil {
				return nil, nil, err
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
			if !used[grpIdx] && ctx.SemTable.EqualsExprWithDeps(by.SimplifiedExpr, orderExpr.SimplifiedExpr) {
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
		return aggregator, rewrite.NewTree("push ordering under aggregation, removing extra ordering", aggregator), nil
	}
	return rewrite.Swap(order, aggregator, "push ordering under aggregation")
}

func canPushLeft(ctx *plancontext.PlanningContext, aj *ApplyJoin, order []ops.OrderBy) bool {
	lhs := TableID(aj.LHS)
	for _, order := range order {
		deps := ctx.SemTable.DirectDeps(order.Inner.Expr)
		if !deps.IsSolvedBy(lhs) {
			return false
		}
	}
	return true
}

func isOuterTable(op ops.Operator, ts semantics.TableSet) bool {
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

func tryPushFilter(ctx *plancontext.PlanningContext, in *Filter) (ops.Operator, *rewrite.ApplyResult, error) {
	switch src := in.Source.(type) {
	case *Projection:
		return pushFilterUnderProjection(ctx, in, src)
	case *Route:
		for _, pred := range in.Predicates {
			var err error
			deps := ctx.SemTable.RecursiveDeps(pred)
			if !isOuterTable(src, deps) {
				// we can only update based on predicates on inner tables
				src.Routing, err = src.Routing.updateRoutingLogic(ctx, pred)
				if err != nil {
					return nil, nil, err
				}
			}
		}
		return rewrite.Swap(in, src, "push filter into Route")
	case *SubQuery:
		outerTableID := TableID(src.Outer)
		for _, pred := range in.Predicates {
			deps := ctx.SemTable.RecursiveDeps(pred)
			if !deps.IsSolvedBy(outerTableID) {
				return in, rewrite.SameTree, nil
			}
		}
		src.Outer, in.Source = in, src.Outer
		return src, rewrite.NewTree("push filter to outer query in subquery container", in), nil
	}

	return in, rewrite.SameTree, nil
}

func pushFilterUnderProjection(ctx *plancontext.PlanningContext, filter *Filter, projection *Projection) (ops.Operator, *rewrite.ApplyResult, error) {
	for _, p := range filter.Predicates {
		cantPush := false
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			if !fetchByOffset(node) {
				return true, nil
			}

			if projection.needsEvaluation(ctx, node.(sqlparser.Expr)) {
				cantPush = true
				return false, io.EOF
			}

			return true, nil
		}, p)

		if cantPush {
			return filter, rewrite.SameTree, nil
		}
	}
	return rewrite.Swap(filter, projection, "push filter under projection")

}

func tryPushDistinct(in *Distinct) (ops.Operator, *rewrite.ApplyResult, error) {
	if in.Required && in.PushedPerformance {
		return in, rewrite.SameTree, nil
	}
	switch src := in.Source.(type) {
	case *Route:
		if isDistinct(src.Source) && src.IsSingleShard() {
			return src, rewrite.NewTree("distinct not needed", in), nil
		}
		if src.IsSingleShard() || !in.Required {
			return rewrite.Swap(in, src, "push distinct under route")
		}

		if isDistinct(src.Source) {
			return in, rewrite.SameTree, nil
		}

		src.Source = &Distinct{Source: src.Source}
		in.PushedPerformance = true

		return in, rewrite.NewTree("added distinct under route - kept original", src), nil
	case *Distinct:
		src.Required = false
		src.PushedPerformance = false
		return src, rewrite.NewTree("remove double distinct", src), nil
	case *Union:
		for i := range src.Sources {
			src.Sources[i] = &Distinct{Source: src.Sources[i]}
		}
		in.PushedPerformance = true

		return in, rewrite.NewTree("push down distinct under union", src), nil
	case *ApplyJoin:
		src.LHS = &Distinct{Source: src.LHS}
		src.RHS = &Distinct{Source: src.RHS}
		in.PushedPerformance = true

		if in.Required {
			return in, rewrite.NewTree("push distinct under join - kept original", in.Source), nil
		}

		return in.Source, rewrite.NewTree("push distinct under join", in.Source), nil
	case *Ordering:
		in.Source = src.Source
		return in, rewrite.NewTree("remove ordering under distinct", in), nil
	}

	return in, rewrite.SameTree, nil
}

func isDistinct(op ops.Operator) bool {
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

func tryPushUnion(ctx *plancontext.PlanningContext, op *Union) (ops.Operator, *rewrite.ApplyResult, error) {
	if res := compactUnion(op); res != rewrite.SameTree {
		return op, res, nil
	}

	var sources []ops.Operator
	var selects []sqlparser.SelectExprs
	var err error

	if op.distinct {
		sources, selects, err = mergeUnionInputInAnyOrder(ctx, op)
	} else {
		sources, selects, err = mergeUnionInputsInOrder(ctx, op)
	}
	if err != nil {
		return nil, nil, err
	}

	if len(sources) == 1 {
		result := sources[0].(*Route)
		if result.IsSingleShard() || !op.distinct {
			return result, rewrite.NewTree("push union under route", op), nil
		}

		return &Distinct{
			Source:   result,
			Required: true,
		}, rewrite.NewTree("push union under route", op), nil
	}

	if len(sources) == len(op.Sources) {
		return op, rewrite.SameTree, nil
	}
	return newUnion(sources, selects, op.unionColumns, op.distinct), rewrite.NewTree("merge union inputs", op), nil
}

// addTruncationOrProjectionToReturnOutput uses the original Horizon to make sure that the output columns line up with what the user asked for
func addTruncationOrProjectionToReturnOutput(ctx *plancontext.PlanningContext, oldHorizon ops.Operator, output ops.Operator) (ops.Operator, error) {
	horizon, ok := oldHorizon.(*Horizon)
	if !ok {
		return output, nil
	}

	cols, err := output.GetSelectExprs(ctx)
	if err != nil {
		return nil, err
	}

	sel := sqlparser.GetFirstSelect(horizon.Query)
	if len(sel.SelectExprs) == len(cols) {
		return output, nil
	}

	if tryTruncateColumnsAt(output, len(sel.SelectExprs)) {
		return output, nil
	}

	qp, err := horizon.getQP(ctx)
	if err != nil {
		return nil, err
	}
	proj, err := createSimpleProjection(ctx, qp, output)
	if err != nil {
		return nil, err
	}
	return proj, nil
}

func stopAtRoute(operator ops.Operator) rewrite.VisitRule {
	_, isRoute := operator.(*Route)
	return rewrite.VisitRule(!isRoute)
}

func aeWrap(e sqlparser.Expr) *sqlparser.AliasedExpr {
	return &sqlparser.AliasedExpr{Expr: e}
}
