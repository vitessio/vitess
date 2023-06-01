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
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func tryPushingDownAggregator(ctx *plancontext.PlanningContext, aggregator *Aggregator) (output ops.Operator, applyResult *rewrite.ApplyResult, err error) {
	if aggregator.Pushed {
		return aggregator, rewrite.SameTree, nil
	}
	aggregator.Pushed = true
	switch src := aggregator.Source.(type) {
	case *Route:
		output, applyResult, err = pushDownAggregationThroughRoute(ctx, aggregator, src)
	case *ApplyJoin:
		output, applyResult, err = pushDownAggregationThroughJoin(ctx, aggregator, src)
	case *Filter:
		output, applyResult, err = pushDownAggregationThroughFilter(ctx, aggregator, src)
	default:
		if aggregator.Original {
			err = vterrors.VT12001(fmt.Sprintf("using aggregation on top of a %T plan", src))
			return
		}
		return aggregator, rewrite.SameTree, nil
	}

	if applyResult != rewrite.SameTree && aggregator.Original {
		aggregator.aggregateTheAggregates()
	}

	return
}

func (a *Aggregator) aggregateTheAggregates() {
	for i, aggr := range a.Aggregations {
		// Handle different aggregation operations when pushing down through a sharded route.
		switch aggr.OpCode {
		case opcode.AggregateCount, opcode.AggregateCountStar, opcode.AggregateCountDistinct:
			// All count variations turn into SUM above the Route.
			// Think of it as we are SUMming together a bunch of distributed COUNTs.
			aggr.OriginalOpCode, aggr.OpCode = aggr.OpCode, opcode.AggregateSum
			a.Aggregations[i] = aggr
		}
	}
}

func pushDownAggregationThroughRoute(
	ctx *plancontext.PlanningContext,
	aggregator *Aggregator,
	route *Route,
) (ops.Operator, *rewrite.ApplyResult, error) {
	// If the route is single-shard, or we are grouping by sharding keys, we can just push down the aggregation
	if route.IsSingleShard() || overlappingUniqueVindex(ctx, aggregator.Grouping) {
		return rewrite.Swap(aggregator, route, "push down aggregation under route - remove original")
	}

	// Create a new aggregator to be placed below the route.
	aggrBelowRoute := aggregator.Clone([]ops.Operator{route.Source}).(*Aggregator)
	aggrBelowRoute.Pushed = false
	aggrBelowRoute.Original = false

	// Set the source of the route to the new aggregator placed below the route.
	route.Source = aggrBelowRoute

	if !aggregator.Original {
		// we only keep the root aggregation, if this aggregator was created
		// by splitting one and pushing under a join, we can get rid of this one
		return aggregator.Source, rewrite.NewTree("push aggregation under route - remove original", aggregator), nil
	}

	return aggregator, rewrite.NewTree("push aggregation under route - keep original", aggregator), nil
}

func pushDownAggregationThroughFilter(
	ctx *plancontext.PlanningContext,
	aggregator *Aggregator,
	filter *Filter,
) (ops.Operator, *rewrite.ApplyResult, error) {

	for _, predicate := range filter.Predicates {
		if sqlparser.ContainsAggregation(predicate) {
			return nil, nil, errHorizonNotPlanned()
		}
	}

	columnsNeeded := collectColNamesNeeded(ctx, filter)

	// Create a new aggregator to be placed below the route.
	pushedAggr := aggregator.Clone([]ops.Operator{filter.Source}).(*Aggregator)
	pushedAggr.Pushed = false
	pushedAggr.Original = false

withNextColumn:
	for _, col := range columnsNeeded {
		for _, gb := range pushedAggr.Grouping {
			if ctx.SemTable.EqualsExpr(col, gb.SimplifiedExpr) {
				continue withNextColumn
			}
		}
		pushedAggr.addNoPushCol(aeWrap(col), true)
	}

	// Set the source of the filter to the new aggregator placed below the route.
	filter.Source = pushedAggr

	if !aggregator.Original {
		// we only keep the root aggregation, if this aggregator was created
		// by splitting one and pushing under a join, we can get rid of this one
		return aggregator.Source, rewrite.NewTree("push aggregation under filter - remove original", aggregator), nil
	}

	return aggregator, rewrite.NewTree("push aggregation under filter - keep original", aggregator), nil
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
		if exprHasUniqueVindex(ctx, groupByExpr.SimplifiedExpr) {
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
We push down aggregations using the logic from the paper Orthogonal Optimization of Subqueries and Aggregation, by
Cesar A. Galindo-Legaria and Milind M. Joshi from Microsoft Corp.

It explains how one can split an aggregation into local aggregates that depend on only one side of the join.
The local aggregates can then be gathered together to produce the global
group by/aggregate query that the user asked for.

In Vitess, this is particularly useful because it allows us to push aggregation down to the routes, even when
we have to join the results at the vtgate level. Instead of doing all the grouping and aggregation at the
vtgate level, we can offload most of the work to MySQL, and at the vtgate just summarize the results.

# For a query, such as

select count(*) from R1 JOIN R2 on R1.id = R2.id

Original:

		 GB         <- This is the original grouping, doing count(*)
		 |
		JOIN
		/  \
	  R1   R2

Transformed:

		  rootAggr  <- This grouping is now SUMing together the distributed `count(*)` we got back
			  |
			Proj    <- This projection makes sure that the columns are lined up as expected
			  |
			Sort    <- Here we are sorting the input so that the OrderedAggregate can do its thing
			  |
			JOIN
		   /    \
	   lAggr    rAggr
		/         \
	   R1          R2
*/
func pushDownAggregationThroughJoin(ctx *plancontext.PlanningContext, rootAggr *Aggregator, join *ApplyJoin) (ops.Operator, *rewrite.ApplyResult, error) {
	lhs := &joinPusher{
		orig: rootAggr,
		pushed: &Aggregator{
			Source: join.LHS,
			QP:     rootAggr.QP,
		},
		columns: initColReUse(len(rootAggr.Columns)),
		tableID: TableID(join.LHS),
	}
	rhs := &joinPusher{
		orig: rootAggr,
		pushed: &Aggregator{
			Source: join.RHS,
			QP:     rootAggr.QP,
		},
		columns: initColReUse(len(rootAggr.Columns)),
		tableID: TableID(join.RHS),
	}

	joinColumns, output, err := splitAggrColumnsToLeftAndRight(ctx, rootAggr, join, lhs, rhs)
	if err != nil {
		return nil, nil, err
	}

	groupingJCs, err := splitGroupingToLeftAndRight(ctx, rootAggr, lhs, rhs)
	if err != nil {
		return nil, nil, err
	}
	joinColumns = append(joinColumns, groupingJCs...)

	// We need to add any columns coming from the lhs of the join to the group by on that side
	// If we don't, the LHS will not be able to return the column, and it can't be used to send down to the RHS
	err = addColumnsFromLHSInJoinPredicates(ctx, rootAggr, join, lhs)
	if err != nil {
		return nil, nil, err
	}

	join.LHS, join.RHS = lhs.pushed, rhs.pushed
	join.ColumnsAST = joinColumns

	if !rootAggr.Original {
		// we only keep the root aggregation, if this aggregator was created
		// by splitting one and pushing under a join, we can get rid of this one
		return output, rewrite.NewTree("push Aggregation under join - keep original", rootAggr), nil
	}

	rootAggr.Source = output
	return rootAggr, rewrite.NewTree("push Aggregation under join", rootAggr), nil
}

func addColumnsFromLHSInJoinPredicates(ctx *plancontext.PlanningContext, rootAggr *Aggregator, join *ApplyJoin, lhs *joinPusher) error {
	for _, pred := range join.JoinPredicates {
		for _, expr := range pred.LHSExprs {
			wexpr := rootAggr.QP.GetSimplifiedExpr(expr)
			idx, found := canReuseColumn(ctx, lhs.pushed.Columns, expr, extractExpr)
			if !found {
				idx = len(lhs.pushed.Columns)
				lhs.pushed.Columns = append(lhs.pushed.Columns, aeWrap(expr))
			}
			_, found = canReuseColumn(ctx, lhs.pushed.Grouping, wexpr, func(by GroupBy) sqlparser.Expr {
				return by.SimplifiedExpr
			})

			if found {
				continue
			}

			lhs.pushed.Grouping = append(lhs.pushed.Grouping, GroupBy{
				Inner:          expr,
				SimplifiedExpr: wexpr,
				ColOffset:      idx,
				WSOffset:       -1,
			})
		}
	}
	return nil
}

func splitGroupingToLeftAndRight(ctx *plancontext.PlanningContext, rootAggr *Aggregator, lhs, rhs *joinPusher) ([]JoinColumn, error) {
	var groupingJCs []JoinColumn

	for _, groupBy := range rootAggr.Grouping {
		deps := ctx.SemTable.RecursiveDeps(groupBy.Inner)
		expr := groupBy.Inner
		switch {
		case deps.IsSolvedBy(lhs.tableID):
			lhs.addGrouping(ctx, groupBy)
			groupingJCs = append(groupingJCs, JoinColumn{
				Original: aeWrap(groupBy.Inner),
				LHSExprs: []sqlparser.Expr{expr},
			})
		case deps.IsSolvedBy(rhs.tableID):
			rhs.addGrouping(ctx, groupBy)
			groupingJCs = append(groupingJCs, JoinColumn{
				Original: aeWrap(groupBy.Inner),
				RHSExpr:  expr,
			})
		default:
			return nil, vterrors.VT12001("grouping on columns from different sources")
		}
	}
	return groupingJCs, nil
}

// splitAggrColumnsToLeftAndRight pushes all aggregations on the aggregator above a join and
// pushes them to one or both sides of the join, and also provides the projections needed to re-assemble the
// aggregations that have been spread across the join
func splitAggrColumnsToLeftAndRight(
	ctx *plancontext.PlanningContext,
	aggregator *Aggregator,
	join *ApplyJoin,
	lhs, rhs *joinPusher,
) ([]JoinColumn, ops.Operator, error) {
	builder := &aggBuilder{
		lhs:       lhs,
		rhs:       rhs,
		proj:      &Projection{Source: join, FromAggr: true},
		outerJoin: join.LeftJoin,
	}

outer:
	// we prefer adding the aggregations in the same order as the columns are declared
	for colIdx, col := range aggregator.Columns {
		for aggrIdx, aggr := range aggregator.Aggregations {
			if aggr.ColOffset == colIdx {
				aggrToKeep, err := builder.handleAggr(ctx, aggr)
				if err != nil {
					return nil, nil, err
				}
				aggregator.Aggregations[aggrIdx] = aggrToKeep
				continue outer
			}
		}
		builder.proj.addUnexploredExpr(col, col.Expr)
	}
	if builder.projectionRequired {
		return builder.joinColumns, builder.proj, nil
	}

	return builder.joinColumns, join, nil
}

type (
	// aggBuilder is a helper struct that aids in pushing down an Aggregator through a join
	// it accumulates the projections (if any) that need to be evaluated on top of the join
	aggBuilder struct {
		lhs, rhs           *joinPusher
		projectionRequired bool
		joinColumns        []JoinColumn
		proj               *Projection
		outerJoin          bool
	}
	// joinPusher is a helper struct that aids in pushing down an Aggregator into one side of a Join.
	// It creates a new Aggregator that is pushed down and keeps track of the column dependencies that the new Aggregator has.
	joinPusher struct {
		orig    *Aggregator        // The original Aggregator before pushing.
		pushed  *Aggregator        // The new Aggregator created for push-down.
		columns []int              // List of column offsets used in the new Aggregator.
		tableID semantics.TableSet // The TableSet denoting the side of the Join where the new Aggregator is pushed.

		// csAE keeps the copy of the countStar expression that has already been added to split an aggregation.
		// No need to have multiple countStars, so we cache it here
		csAE *sqlparser.AliasedExpr
	}
)

func (ab *aggBuilder) leftCountStar(ctx *plancontext.PlanningContext) *sqlparser.AliasedExpr {
	ae, created := ab.lhs.countStar(ctx)
	if created {
		ab.joinColumns = append(ab.joinColumns, JoinColumn{
			Original: ae,
			LHSExprs: []sqlparser.Expr{ae.Expr},
		})
	}
	return ae
}

func (ab *aggBuilder) rightCountStar(ctx *plancontext.PlanningContext) *sqlparser.AliasedExpr {
	ae, created := ab.rhs.countStar(ctx)
	if created {
		ab.joinColumns = append(ab.joinColumns, JoinColumn{
			Original: ae,
			RHSExpr:  ae.Expr,
		})
	}
	return ae
}

func (p *joinPusher) countStar(ctx *plancontext.PlanningContext) (*sqlparser.AliasedExpr, bool) {
	if p.csAE != nil {
		return p.csAE, false
	}
	cs := &sqlparser.CountStar{}
	ae := aeWrap(cs)
	csAggr := Aggr{
		Original: ae,
		Func:     cs,
		OpCode:   opcode.AggregateCountStar,
	}
	expr := p.addAggr(ctx, csAggr)
	p.csAE = aeWrap(expr)
	return p.csAE, true
}

func (ab *aggBuilder) handleAggr(ctx *plancontext.PlanningContext, aggr Aggr) (Aggr, error) {
	switch aggr.OpCode {
	case opcode.AggregateCountStar:
		return ab.handleCountStar(ctx, aggr)
	case opcode.AggregateMax, opcode.AggregateMin, opcode.AggregateRandom:
		return ab.handlePushThroughAggregation(ctx, aggr)
	case opcode.AggregateCount:
		return ab.handleCount(ctx, aggr)

	case opcode.AggregateUnassigned:
		return Aggr{}, vterrors.VT12001(fmt.Sprintf("in scatter query: aggregation function '%s'", sqlparser.String(aggr.Original)))
	default:
		return Aggr{}, errHorizonNotPlanned()
	}
}

// pushThroughLeft and Right are used for extremums and random,
// which are not split and then arithmetics is used to aggregate the per-shard aggregations.
// For these, we just copy the aggregation to one side of the join and then pick the max of the max:es returned
func (ab *aggBuilder) pushThroughLeft(aggr Aggr) {
	ab.lhs.pushThroughAggr(aggr)
	ab.joinColumns = append(ab.joinColumns, JoinColumn{
		Original: aggr.Original,
		LHSExprs: []sqlparser.Expr{aggr.Original.Expr},
	})
}
func (ab *aggBuilder) pushThroughRight(aggr Aggr) {
	ab.rhs.pushThroughAggr(aggr)
	ab.joinColumns = append(ab.joinColumns, JoinColumn{
		Original: aggr.Original,
		RHSExpr:  aggr.Original.Expr,
	})
}

func (ab *aggBuilder) handlePushThroughAggregation(ctx *plancontext.PlanningContext, aggr Aggr) (Aggr, error) {
	ab.proj.addUnexploredExpr(aggr.Original, aggr.Original.Expr)

	deps := ctx.SemTable.RecursiveDeps(aggr.Original.Expr)
	switch {
	case deps.IsSolvedBy(ab.lhs.tableID):
		ab.pushThroughLeft(aggr)
		return aggr, nil
	case deps.IsSolvedBy(ab.rhs.tableID):
		ab.pushThroughRight(aggr)
		return aggr, nil
	default:
		return Aggr{}, vterrors.VT12001("aggregation on columns from different sources: " + sqlparser.String(aggr.Original.Expr))
	}
}

func (ab *aggBuilder) handleCountStar(ctx *plancontext.PlanningContext, aggr Aggr) (Aggr, error) {
	// Projection is necessary since we are going to need to do arithmetics to summarize the aggregates
	ab.projectionRequired = true

	// Add the aggregate to both sides of the join.
	lhsAE := ab.leftCountStar(ctx)
	rhsAE := ab.rightCountStar(ctx)

	// We expect the expressions to be different on each side of the join, otherwise it's an error.
	if lhsAE.Expr == rhsAE.Expr {
		panic(fmt.Sprintf("Need the two produced expressions to be different. %T %T", lhsAE, rhsAE))
	}

	rhsExpr := rhsAE.Expr

	// When dealing with outer joins, we don't want null values from the RHS to ruin the calculations we are doing,
	// so we use the MySQL `coalesce` after the join is applied to multiply the count from LHS with 1.
	if ab.outerJoin {
		rhsExpr = coalesceFunc(rhsExpr)
	}

	// The final COUNT is obtained by multiplying the counts from both sides.
	// This is equivalent to transforming a "select count(*) from t1 join t2" into
	// "select count_t1*count_t2 from
	//    (select count(*) as count_t1 from t1) as x,
	//    (select count(*) as count_t2 from t2) as y".
	projExpr := &sqlparser.BinaryExpr{
		Operator: sqlparser.MultOp,
		Left:     lhsAE.Expr,
		Right:    rhsExpr,
	}
	projAE := &sqlparser.AliasedExpr{
		Expr: aggr.Original.Expr,
		As:   sqlparser.NewIdentifierCI(aggr.Original.ColumnName()),
	}

	ab.proj.addUnexploredExpr(projAE, projExpr)
	return aggr, nil
}

func (ab *aggBuilder) handleCount(ctx *plancontext.PlanningContext, aggr Aggr) (Aggr, error) {
	ab.projectionRequired = true

	expr := aggr.Original.Expr
	deps := ctx.SemTable.RecursiveDeps(expr)
	var otherSide sqlparser.Expr

	switch {
	case deps.IsSolvedBy(ab.lhs.tableID):
		ab.pushThroughLeft(aggr)
		ae := ab.rightCountStar(ctx)
		otherSide = ae.Expr

	case deps.IsSolvedBy(ab.rhs.tableID):
		ab.pushThroughRight(aggr)
		ae := ab.leftCountStar(ctx)
		otherSide = ae.Expr

	default:
		return Aggr{}, errHorizonNotPlanned()
	}

	if ab.outerJoin {
		otherSide = coalesceFunc(otherSide)
	}

	projAE := &sqlparser.AliasedExpr{
		Expr: aggr.Original.Expr,
		As:   sqlparser.NewIdentifierCI(aggr.Original.ColumnName()),
	}
	ab.proj.addUnexploredExpr(projAE, &sqlparser.BinaryExpr{
		Operator: sqlparser.MultOp,
		Left:     expr,
		Right:    otherSide,
	})
	return aggr, nil
}

func coalesceFunc(e sqlparser.Expr) sqlparser.Expr {
	// `coalesce(e,1)` will return `e` if `e` is not `NULL`, otherwise it will return `1`
	return &sqlparser.FuncExpr{
		Name: sqlparser.NewIdentifierCI("coalesce"),
		Exprs: sqlparser.SelectExprs{
			aeWrap(e),
			aeWrap(sqlparser.NewIntLiteral("1")),
		},
	}
}

// addAggr creates a copy of the given aggregation, updates its column offset to point to the correct location in the new Aggregator,
// and adds it to the list of Aggregations of the new Aggregator. It also updates the semantic analysis information to reflect the new structure.
// It returns the expression of the aggregation as it should be used in the parent Aggregator.
func (p *joinPusher) addAggr(ctx *plancontext.PlanningContext, aggr Aggr) sqlparser.Expr {
	copyAggr := aggr
	expr := sqlparser.CloneExpr(aggr.Original.Expr)
	copyAggr.Original = aeWrap(expr)
	// copy dependencies so we can keep track of which side expressions need to be pushed to
	ctx.SemTable.Direct[expr] = p.tableID
	ctx.SemTable.Recursive[expr] = p.tableID
	copyAggr.ColOffset = len(p.pushed.Columns)
	p.pushed.Columns = append(p.pushed.Columns, copyAggr.Original)
	p.pushed.Aggregations = append(p.pushed.Aggregations, copyAggr)
	return expr
}

// pushThroughAggr pushes through an aggregation without changing dependencies.
// Can be used for aggregations we can push in one piece
func (p *joinPusher) pushThroughAggr(aggr Aggr) {
	p.pushed.Columns = append(p.pushed.Columns, aggr.Original)
	p.pushed.Aggregations = append(p.pushed.Aggregations, aggr)
}

// addGrouping creates a copy of the given GroupBy, updates its column offset to point to the correct location in the new Aggregator,
// and adds it to the list of GroupBy expressions of the new Aggregator. It also updates the semantic analysis information to reflect the new structure.
// It returns the expression of the GroupBy as it should be used in the parent Aggregator.
func (p *joinPusher) addGrouping(ctx *plancontext.PlanningContext, gb GroupBy) sqlparser.Expr {
	copyGB := gb
	expr := sqlparser.CloneExpr(gb.Inner)
	// copy dependencies so we can keep track of which side expressions need to be pushed to
	ctx.SemTable.CopyDependencies(gb.Inner, expr)
	// if the column exists in the selection then copy it down to the pushed aggregator operator.
	if copyGB.ColOffset != -1 {
		offset := p.useColumn(copyGB.ColOffset)
		copyGB.ColOffset = offset
	}
	p.pushed.Grouping = append(p.pushed.Grouping, copyGB)
	return expr
}

// useColumn checks whether the column corresponding to the given offset has been used in the new Aggregator.
// If it has not been used before, it adds the column to the new Aggregator
// and updates the columns mapping to reflect the new location of the column.
// It returns the offset of the column in the new Aggregator.
func (p *joinPusher) useColumn(offset int) int {
	if p.columns[offset] == -1 {
		p.columns[offset] = len(p.pushed.Columns)
		// still haven't used this expression on this side
		p.pushed.Columns = append(p.pushed.Columns, p.orig.Columns[offset])
	}
	return p.columns[offset]
}

func initColReUse(size int) []int {
	cols := make([]int, size)
	for i := 0; i < size; i++ {
		cols[i] = -1
	}
	return cols
}

func extractExpr(expr *sqlparser.AliasedExpr) sqlparser.Expr { return expr.Expr }
