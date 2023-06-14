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
	"io"

	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	projector struct {
		cols  []ProjExpr
		names []*sqlparser.AliasedExpr
	}

	// horizonLike should be removed. we should use Horizon for both these cases
	horizonLike interface {
		ops.Operator
		selectStatement() sqlparser.SelectStatement
		src() ops.Operator
		getQP(ctx *plancontext.PlanningContext) (*QueryProjection, error)
	}
)

func errHorizonNotPlanned() error {
	return _errHorizonNotPlanned
}

var _errHorizonNotPlanned = vterrors.VT12001("query cannot be fully operator planned")

func tryHorizonPlanning(ctx *plancontext.PlanningContext, root ops.Operator) (output ops.Operator, err error) {
	backup := Clone(root)
	defer func() {
		// If we encounter the _errHorizonNotPlanned error, we'll revert to using the old horizon planning strategy.
		if err == _errHorizonNotPlanned {
			// The only offset planning we did before was on joins.
			// Therefore, we traverse the tree to find all joins and calculate the joinColumns offsets.
			// Our fallback strategy is to clone the original operator tree, compute the join offsets,
			// and allow the legacy horizonPlanner to handle this query using logical plans.
			err = planOffsetsOnJoins(ctx, backup)
			if err == nil {
				output = backup
			}
		}
	}()

	_, ok := root.(*Horizon)

	if !ok || len(ctx.SemTable.SubqueryMap) > 0 || len(ctx.SemTable.SubqueryRef) > 0 {
		// we are not ready to deal with subqueries yet
		return root, errHorizonNotPlanned()
	}

	output, err = planHorizons(ctx, root)
	if err != nil {
		return nil, err
	}

	output, err = planOffsets(ctx, output)
	if err != nil {
		return nil, err
	}

	output, err = makeSureOutputIsCorrect(ctx, root, output)
	if err != nil {
		return nil, err
	}

	return
}

// Phase defines the different planning phases to go through to produce an optimized plan for the input query.
type Phase struct {
	Name string
	// preOptimizeAction is the action to be taken before calling plan optimization operation.
	preOptimizeAction func(ctx *plancontext.PlanningContext, op ops.Operator) (ops.Operator, error)
}

// getPhases returns the phases the planner will go through.
// It's used to control so rewriters collaborate correctly
func getPhases() []Phase {
	return []Phase{{
		// Initial optimization
		Name: "initial horizon planning optimization phase",
	}, {
		// Adding Ordering Op - Any aggregation that is performed in the VTGate needs the input to be ordered
		// Adding Group by - This is needed if the grouping is performed on a join with a join condition then
		//                   aggregation happening at route needs a group by to ensure only matching rows returns
		//                   the aggregations otherwise returns no result.
		Name:              "add ORDER BY to aggregations above the route and add GROUP BY to aggregations on the RHS of join",
		preOptimizeAction: addOrderBysAndGroupBysForAggregations,
	}}
}

// planHorizons is the process of figuring out how to perform the operations in the Horizon
// If we can push it under a route - done.
// If we can't, we will instead expand the Horizon into
// smaller operators and try to push these down as far as possible
func planHorizons(ctx *plancontext.PlanningContext, root ops.Operator) (op ops.Operator, err error) {
	phases := getPhases()
	op = root

	for _, phase := range phases {
		if phase.preOptimizeAction != nil {
			op, err = phase.preOptimizeAction(ctx, op)
			if err != nil {
				return nil, err
			}
		}
		op, err = optimizeHorizonPlanning(ctx, op)
		if err != nil {
			return nil, err
		}
	}
	return op, nil
}

func optimizeHorizonPlanning(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	visitor := func(in ops.Operator, _ semantics.TableSet, isRoot bool) (ops.Operator, *rewrite.ApplyResult, error) {
		switch in := in.(type) {
		case horizonLike:
			return pushOrExpandHorizon(ctx, in)
		case *Projection:
			return tryPushingDownProjection(ctx, in)
		case *Limit:
			return tryPushingDownLimit(in)
		case *Ordering:
			return tryPushingDownOrdering(ctx, in)
		case *Aggregator:
			return tryPushingDownAggregator(ctx, in)
		case *Filter:
			return tryPushingDownFilter(ctx, in)
		case *Distinct:
			return tryPushingDownDistinct(in)
		default:
			return in, rewrite.SameTree, nil
		}
	}

	newOp, err := rewrite.FixedPointBottomUp(root, TableID, visitor, stopAtRoute)
	if err != nil {
		if vterr, ok := err.(*vterrors.VitessError); ok && vterr.ID == "VT13001" {
			// we encountered a bug. let's try to back out
			return nil, errHorizonNotPlanned()
		}
		return nil, err
	}

	return newOp, nil
}

func tryPushingDownFilter(ctx *plancontext.PlanningContext, in *Filter) (ops.Operator, *rewrite.ApplyResult, error) {
	switch src := in.Source.(type) {
	case *Projection:
		return pushFilterUnderProjection(ctx, in, src)
	case *Route:
		return rewrite.Swap(in, src, "push filter into Route")
	}

	return in, rewrite.SameTree, nil
}

func pushFilterUnderProjection(ctx *plancontext.PlanningContext, filter *Filter, projection *Projection) (ops.Operator, *rewrite.ApplyResult, error) {
	for _, p := range filter.Predicates {
		cantPushDown := false
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			if !fetchByOffset(node) {
				return true, nil
			}

			if projection.needsEvaluation(ctx, node.(sqlparser.Expr)) {
				cantPushDown = true
				return false, io.EOF
			}

			return true, nil
		}, p)

		if cantPushDown {
			return filter, rewrite.SameTree, nil
		}
	}
	return rewrite.Swap(filter, projection, "push filter under projection")

}
func tryPushingDownDistinct(in *Distinct) (ops.Operator, *rewrite.ApplyResult, error) {
	if in.Pushed {
		return in, rewrite.SameTree, nil
	}
	switch src := in.Source.(type) {
	case *Route:
		if src.IsSingleShard() {
			return rewrite.Swap(in, src, "push distinct under route")
		}
	case *Distinct:
		return src, rewrite.NewTree("removed double distinct", src), nil
	case *Aggregator:
		return in, rewrite.SameTree, nil
	}

	cols, err := in.Source.GetColumns()
	if err != nil {
		return nil, nil, err
	}

	aggr := &Aggregator{
		Source:   in.Source,
		QP:       in.QP,
		Original: true,
	}

	for _, col := range cols {
		aggr.addColumnWithoutPushing(col, true)
	}

	return aggr, rewrite.NewTree("replace distinct with aggregator", in), nil
}

// addOrderBysForAggregations runs after we have run horizonPlanning until the op tree stops changing
// this means that we have pushed aggregations and other ops as far down as they'll go
// addOrderBysForAggregations will find Aggregators that have not been pushed under routes and
// add the necessary Ordering operators for them
func addOrderBysAndGroupBysForAggregations(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	visitor := func(in ops.Operator, _ semantics.TableSet, isRoot bool) (ops.Operator, *rewrite.ApplyResult, error) {
		switch op := in.(type) {
		case *ApplyJoin:
			return addLiteralGroupingToRHS(op)
		case *Aggregator:
			return addOrderingForAggregation(ctx, op)
		default:
			return in, rewrite.SameTree, nil
		}
	}

	return rewrite.TopDown(root, TableID, visitor, stopAtRoute)
}

func addLiteralGroupingToRHS(in *ApplyJoin) (ops.Operator, *rewrite.ApplyResult, error) {
	_ = rewrite.Visit(in.RHS, func(op ops.Operator) error {
		aggr, isAggr := op.(*Aggregator)
		if !isAggr {
			return nil
		}
		if len(aggr.Grouping) == 0 {
			gb := sqlparser.NewIntLiteral(".0")
			aggr.Grouping = append(aggr.Grouping, NewGroupBy(gb, gb, aeWrap(gb)))
		}
		return nil
	})
	return in, rewrite.NewTree("added grouping to the RHS", in), nil
}

func addOrderingForAggregation(ctx *plancontext.PlanningContext, in *Aggregator) (ops.Operator, *rewrite.ApplyResult, error) {
	requireOrdering, err := needsOrdering(in, ctx)
	if err != nil {
		return nil, nil, err
	}
	if !requireOrdering {
		return in, rewrite.SameTree, nil
	}
	in.Source = &Ordering{
		Source: in.Source,
		Order: slices2.Map(in.Grouping, func(from GroupBy) ops.OrderBy {
			return from.AsOrderBy()
		}),
	}
	return in, rewrite.NewTree("added ordering before aggregation", in), nil
}

func needsOrdering(in *Aggregator, ctx *plancontext.PlanningContext) (bool, error) {
	if len(in.Grouping) == 0 {
		return false, nil
	}
	srcOrdering, err := in.Source.GetOrdering()
	if err != nil {
		return false, err
	}
	if len(srcOrdering) < len(in.Grouping) {
		return true, nil
	}
	for idx, gb := range in.Grouping {
		if !ctx.SemTable.EqualsExprWithDeps(srcOrdering[idx].SimplifiedExpr, gb.SimplifiedExpr) {
			return true, nil
		}
	}
	return false, nil
}

func tryPushingDownOrdering(ctx *plancontext.PlanningContext, in *Ordering) (ops.Operator, *rewrite.ApplyResult, error) {
	switch src := in.Source.(type) {
	case *Route:
		return rewrite.Swap(in, src, "push ordering under route")
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
		if !(src.QP.AlignGroupByAndOrderBy(ctx) || overlaps(ctx, in.Order, src.Grouping)) {
			return in, rewrite.SameTree, nil
		}

		return pushOrderingUnderAggr(ctx, in, src)

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

func tryPushingDownProjection(
	ctx *plancontext.PlanningContext,
	p *Projection,
) (ops.Operator, *rewrite.ApplyResult, error) {
	switch src := p.Source.(type) {
	case *Route:
		return rewrite.Swap(p, src, "pushed projection under route")
	case *ApplyJoin:
		if p.FromAggr {
			return p, rewrite.SameTree, nil
		}
		return pushDownProjectionInApplyJoin(ctx, p, src)
	case *Vindex:
		return pushDownProjectionInVindex(ctx, p, src)
	default:
		return p, rewrite.SameTree, nil
	}
}

func pushDownProjectionInVindex(
	ctx *plancontext.PlanningContext,
	p *Projection,
	src *Vindex,
) (ops.Operator, *rewrite.ApplyResult, error) {
	for _, column := range p.Projections {
		expr := column.GetExpr()
		_, _, err := src.AddColumn(ctx, aeWrap(expr), true, false)
		if err != nil {
			return nil, nil, err
		}
	}
	return src, rewrite.NewTree("push projection into vindex", p), nil
}

func (p *projector) add(e ProjExpr, alias *sqlparser.AliasedExpr) {
	p.cols = append(p.cols, e)
	p.names = append(p.names, alias)
}

// pushDownProjectionInApplyJoin pushes down a projection operation into an ApplyJoin operation.
// It processes each input column and creates new JoinColumns for the ApplyJoin operation based on
// the input column's expression. It also creates new Projection operators for the left and right
// children of the ApplyJoin operation, if needed.
func pushDownProjectionInApplyJoin(
	ctx *plancontext.PlanningContext,
	p *Projection,
	src *ApplyJoin,
) (ops.Operator, *rewrite.ApplyResult, error) {
	if src.LeftJoin {
		// we can't push down expression evaluation to the rhs if we are not sure if it will even be executed
		return p, rewrite.SameTree, nil
	}
	lhs, rhs := &projector{}, &projector{}

	src.ColumnsAST = nil
	for idx := 0; idx < len(p.Projections); idx++ {
		err := splitProjectionAcrossJoin(ctx, src, lhs, rhs, p.Projections[idx], p.Columns[idx])
		if err != nil {
			return nil, nil, err
		}
	}

	if p.TableID != nil {
		err := exposeColumnsThroughDerivedTable(ctx, p, src, lhs)
		if err != nil {
			return nil, nil, err
		}
	}

	var err error

	// Create and update the Projection operators for the left and right children, if needed.
	src.LHS, err = createProjectionWithTheseColumns(src.LHS, lhs, p.TableID, p.Alias)
	if err != nil {
		return nil, nil, err
	}

	src.RHS, err = createProjectionWithTheseColumns(src.RHS, rhs, p.TableID, p.Alias)
	if err != nil {
		return nil, nil, err
	}

	return src, rewrite.NewTree("split projection to either side of join", src), nil
}

// splitProjectionAcrossJoin creates JoinColumns for all projections,
// and pushes down columns as needed between the LHS and RHS of a join
func splitProjectionAcrossJoin(
	ctx *plancontext.PlanningContext,
	join *ApplyJoin,
	lhs, rhs *projector,
	in ProjExpr,
	colName *sqlparser.AliasedExpr,
) error {
	expr := in.GetExpr()

	// Check if the current expression can reuse an existing column in the ApplyJoin.
	if _, found := canReuseColumn(ctx, join.ColumnsAST, expr, joinColumnToExpr); found {
		return nil
	}

	// Get a JoinColumn for the current expression.
	col, err := join.getJoinColumnFor(ctx, colName, false)
	if err != nil {
		return err
	}

	// Update the left and right child columns and names based on the JoinColumn type.
	switch {
	case col.IsPureLeft():
		lhs.add(in, colName)
	case col.IsPureRight():
		rhs.add(in, colName)
	case col.IsMixedLeftAndRight():
		for _, lhsExpr := range col.LHSExprs {
			lhs.add(&UnexploredExpression{E: lhsExpr}, aeWrap(lhsExpr))
		}
		rhs.add(&UnexploredExpression{E: col.RHSExpr}, &sqlparser.AliasedExpr{Expr: col.RHSExpr, As: colName.As})
	}

	// Add the new JoinColumn to the ApplyJoin's ColumnsAST.
	join.ColumnsAST = append(join.ColumnsAST, col)
	return nil
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
func exposeColumnsThroughDerivedTable(ctx *plancontext.PlanningContext, p *Projection, src *ApplyJoin, lhs *projector) error {
	derivedTbl, err := ctx.SemTable.TableInfoFor(*p.TableID)
	if err != nil {
		return err
	}
	derivedTblName, err := derivedTbl.Name()
	if err != nil {
		return err
	}
	for _, predicate := range src.JoinPredicates {
		for idx, expr := range predicate.LHSExprs {
			tbl, err := ctx.SemTable.TableInfoForExpr(expr)
			if err != nil {
				return err
			}
			tblExpr := tbl.GetExpr()
			tblName, err := tblExpr.TableName()
			if err != nil {
				return err
			}

			expr = semantics.RewriteDerivedTableExpression(expr, derivedTbl)
			out, err := prefixColNames(tblName, expr)
			if err != nil {
				return err
			}

			alias := sqlparser.UnescapedString(out)
			predicate.LHSExprs[idx] = sqlparser.NewColNameWithQualifier(alias, derivedTblName)
			lhs.add(&UnexploredExpression{E: out}, &sqlparser.AliasedExpr{Expr: out, As: sqlparser.NewIdentifierCI(alias)})
		}
	}
	return nil
}

// prefixColNames adds qualifier prefixes to all ColName:s.
// We want to be more explicit than the user was to make sure we never produce invalid SQL
func prefixColNames(tblName sqlparser.TableName, e sqlparser.Expr) (out sqlparser.Expr, err error) {
	out = sqlparser.CopyOnRewrite(e, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
		col, ok := cursor.Node().(*sqlparser.ColName)
		if !ok {
			return
		}
		col.Qualifier = tblName
	}, nil).(sqlparser.Expr)
	return
}

func createProjectionWithTheseColumns(
	src ops.Operator,
	p *projector,
	tableID *semantics.TableSet,
	alias string,
) (ops.Operator, error) {
	if len(p.cols) == 0 {
		return src, nil
	}
	proj, err := createProjection(src)
	if err != nil {
		return nil, err
	}
	proj.Columns = p.names
	proj.Projections = p.cols
	proj.TableID = tableID
	proj.Alias = alias
	return proj, nil
}

func stopAtRoute(operator ops.Operator) rewrite.VisitRule {
	_, isRoute := operator.(*Route)
	return rewrite.VisitRule(!isRoute)
}

func tryPushingDownLimit(in *Limit) (ops.Operator, *rewrite.ApplyResult, error) {
	switch src := in.Source.(type) {
	case *Route:
		return tryPushingDownLimitInRoute(in, src)
	case *Projection:
		return rewrite.Swap(in, src, "push limit under projection")
	case *Aggregator:
		return in, rewrite.SameTree, nil
	default:
		return setUpperLimit(in)
	}
}

func setUpperLimit(in *Limit) (ops.Operator, *rewrite.ApplyResult, error) {
	if in.Pushed {
		return in, rewrite.SameTree, nil
	}
	in.Pushed = true
	visitor := func(op ops.Operator, _ semantics.TableSet, _ bool) (ops.Operator, *rewrite.ApplyResult, error) {
		return op, rewrite.SameTree, nil
	}
	shouldVisit := func(op ops.Operator) rewrite.VisitRule {
		switch op := op.(type) {
		case *Join, *ApplyJoin:
			// we can't push limits down on either side
			return rewrite.SkipChildren
		case *Route:
			newSrc := &Limit{
				Source: op.Source,
				AST:    &sqlparser.Limit{Rowcount: sqlparser.NewArgument("__upper_limit")},
				Pushed: false,
			}
			op.Source = newSrc
			return rewrite.SkipChildren
		default:
			return rewrite.VisitChildren
		}
	}

	_, err := rewrite.TopDown(in.Source, TableID, visitor, shouldVisit)
	if err != nil {
		return nil, nil, err
	}
	return in, rewrite.SameTree, nil
}

func tryPushingDownLimitInRoute(in *Limit, src *Route) (ops.Operator, *rewrite.ApplyResult, error) {
	if src.IsSingleShard() {
		return rewrite.Swap(in, src, "limit pushed into single sharded route")
	}

	return setUpperLimit(in)
}

func pushOrExpandHorizon(ctx *plancontext.PlanningContext, in horizonLike) (ops.Operator, *rewrite.ApplyResult, error) {
	if derived, ok := in.(*Derived); ok {
		if len(derived.ColumnAliases) > 0 {
			return nil, nil, errHorizonNotPlanned()
		}
	}
	rb, isRoute := in.src().(*Route)
	if isRoute && rb.IsSingleShard() {
		return rewrite.Swap(in, rb, "push horizon into route")
	}

	sel, isSel := in.selectStatement().(*sqlparser.Select)
	if !isSel {
		return nil, nil, errHorizonNotPlanned()
	}

	qp, err := in.getQP(ctx)
	if err != nil {
		return nil, nil, err
	}

	needsOrdering := len(qp.OrderExprs) > 0
	canPushDown := isRoute && sel.Having == nil && !needsOrdering && !qp.NeedsAggregation() && !sel.Distinct && sel.Limit == nil

	if canPushDown {
		return rewrite.Swap(in, rb, "push horizon into route")
	}

	return expandHorizon(ctx, in)
}

func aeWrap(e sqlparser.Expr) *sqlparser.AliasedExpr {
	return &sqlparser.AliasedExpr{Expr: e}
}

// makeSureOutputIsCorrect uses the original Horizon to make sure that the output columns line up with what the user asked for
func makeSureOutputIsCorrect(ctx *plancontext.PlanningContext, oldHorizon ops.Operator, output ops.Operator) (ops.Operator, error) {
	cols, err := output.GetColumns()
	if err != nil {
		return nil, err
	}

	horizon := oldHorizon.(*Horizon)

	sel := sqlparser.GetFirstSelect(horizon.Select)

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
