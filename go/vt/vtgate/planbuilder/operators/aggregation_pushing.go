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

func tryPushingDownAggregator(ctx *plancontext.PlanningContext, aggregator *Aggregator) (output ops.Operator, applyResult rewrite.ApplyResult, err error) {
	if aggregator.Pushed {
		return aggregator, rewrite.SameTree, nil
	}
	aggregator.Pushed = true
	switch src := aggregator.Source.(type) {
	case *Route:
		output, applyResult, err = pushDownAggregationThroughRoute(ctx, aggregator, src)
	case *ApplyJoin:
		output, applyResult, err = pushDownAggregationThroughJoin(ctx, aggregator, src)
	default:
		return aggregator, rewrite.SameTree, nil
	}

	if applyResult == rewrite.NewTree && aggregator.Original {
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

func pushDownAggregationThroughRoute(ctx *plancontext.PlanningContext, aggregator *Aggregator, src *Route) (ops.Operator, rewrite.ApplyResult, error) {
	// If the route is single-shard, or we are grouping by sharding keys, we can just push down the aggregation
	if src.IsSingleShard() || overlappingUniqueVindex(ctx, aggregator.Grouping) {
		return swap(aggregator, src)
	}

	// Create a new aggregator to be placed below the route.
	aggrBelowRoute := aggregator.Clone([]ops.Operator{src.Source}).(*Aggregator)
	aggrBelowRoute.Pushed = false
	aggrBelowRoute.Original = false

	// Set the source of the route to the new aggregator placed below the route.
	src.Source = aggrBelowRoute

	if !aggregator.Original {
		// we only keep the root aggregation, if this aggregator was created
		// by splitting one and pushing under a join, we can get rid of this one
		return aggregator.Source, rewrite.NewTree, nil
	}

	return aggregator, rewrite.NewTree, nil
}

func overlappingUniqueVindex(ctx *plancontext.PlanningContext, groupByExprs []GroupBy) bool {
	for _, groupByExpr := range groupByExprs {
		if exprHasUniqueVindex(ctx, groupByExpr.WeightStrExpr) {
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
func pushDownAggregationThroughJoin(ctx *plancontext.PlanningContext, rootAggr *Aggregator, join *ApplyJoin) (ops.Operator, rewrite.ApplyResult, error) {
	lhs := joinPusher{
		orig: rootAggr,
		pushed: &Aggregator{
			Source: join.LHS,
			QP:     rootAggr.QP,
		},
		columns: initColReUse(len(rootAggr.Columns)),
		tableID: TableID(join.LHS),
	}
	rhs := joinPusher{
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
		return nil, false, err
	}

	lhsTS := TableID(join.LHS)
	rhsTS := TableID(join.RHS)
	groupingJCs, err := splitGroupingToLeftAndRight(ctx, rootAggr, lhsTS, rhsTS, lhs, rhs)
	if err != nil {
		return nil, false, err
	}
	joinColumns = append(joinColumns, groupingJCs...)

	// We need to add any columns coming from the lhs of the join to the group by on that side
	// If we don't, the LHS will not be able to return the column, and it can't be used to send down to the RHS
	err = addJoinColumnsToLeft(ctx, rootAggr, join, lhs)
	if err != nil {
		return nil, false, err
	}

	join.LHS, join.RHS = lhs.pushed, rhs.pushed
	join.ColumnsAST = joinColumns

	if !rootAggr.Original {
		// we only keep the root aggregation, if this aggregator was created
		// by splitting one and pushing under a join, we can get rid of this one
		return output, rewrite.NewTree, nil
	}

	rootAggr.Source = output
	return rootAggr, rewrite.NewTree, nil
}

func addJoinColumnsToLeft(ctx *plancontext.PlanningContext, rootAggr *Aggregator, join *ApplyJoin, lhs joinPusher) error {
	for _, pred := range join.JoinPredicates {
		for _, expr := range pred.LHSExprs {
			wexpr := rootAggr.QP.GetSimplifiedExpr(expr)
			idx, found := canReuseColumn(ctx, lhs.pushed.Columns, expr, extractExpr)
			if !found {
				idx = len(lhs.pushed.Columns)
				lhs.pushed.Columns = append(lhs.pushed.Columns, aeWrap(expr))
			}
			_, found = canReuseColumn(ctx, lhs.pushed.Grouping, wexpr, func(by GroupBy) sqlparser.Expr {
				return by.WeightStrExpr
			})

			if found {
				continue
			}

			lhs.pushed.Grouping = append(lhs.pushed.Grouping, GroupBy{
				Inner:         expr,
				WeightStrExpr: wexpr,
				ColOffset:     idx,
				WSOffset:      -1,
			})
		}
	}
	return nil
}

func splitGroupingToLeftAndRight(ctx *plancontext.PlanningContext, rootAggr *Aggregator, lhsTS, rhsTS semantics.TableSet, lhs, rhs joinPusher) ([]JoinColumn, error) {
	var groupingJCs []JoinColumn
	for _, groupBy := range rootAggr.Grouping {
		deps := ctx.SemTable.RecursiveDeps(groupBy.Inner)
		expr := groupBy.Inner
		switch {
		case deps.IsSolvedBy(lhsTS):
			lhs.addGrouping(ctx, groupBy)
			groupingJCs = append(groupingJCs, JoinColumn{
				Original: aeWrap(groupBy.Inner),
				LHSExprs: []sqlparser.Expr{expr},
			})
		case deps.IsSolvedBy(rhsTS):
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
// pushed them to one or both sides of the join, and also provides the projections needed to re-assemble the
// aggregations that have been spread across the join
func splitAggrColumnsToLeftAndRight(
	ctx *plancontext.PlanningContext,
	aggregator *Aggregator,
	join *ApplyJoin,
	lhs, rhs joinPusher,
) ([]JoinColumn, ops.Operator, error) {
	builder := &aggBuilder{
		lhs:       lhs,
		rhs:       rhs,
		proj:      &Projection{Source: join, FromAggr: true},
		outerJoin: join.LeftJoin,
		lhsID:     TableID(join.LHS),
		rhsID:     TableID(join.RHS),
	}

outer:
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

type aggBuilder struct {
	lhsID, rhsID       semantics.TableSet
	lhs, rhs           joinPusher
	projectionRequired bool
	joinColumns        []JoinColumn
	proj               *Projection
	outerJoin          bool
}

func (ab *aggBuilder) handleAggr(ctx *plancontext.PlanningContext, aggr Aggr) (Aggr, error) {
	switch aggr.OpCode {
	case opcode.AggregateCountStar:
		return ab.handleCountStar(ctx, aggr)
	case opcode.AggregateMax, opcode.AggregateMin:
		return ab.handleMinMax(ctx, aggr)

	case opcode.AggregateUnassigned:
		return Aggr{}, vterrors.VT12001(fmt.Sprintf("in scatter query: aggregation function '%s'", sqlparser.String(aggr.Original)))
	default:
		return Aggr{}, errHorizonNotPlanned()
	}
}

func (ab *aggBuilder) handleMinMax(ctx *plancontext.PlanningContext, aggr Aggr) (Aggr, error) {
	ab.proj.addUnexploredExpr(aggr.Original, aggr.Func)

	deps := ctx.SemTable.RecursiveDeps(aggr.Original.Expr)
	switch {
	case deps.IsSolvedBy(ab.lhsID):
		ab.lhs.pushThroughAggr(aggr)
		ab.joinColumns = append(ab.joinColumns, JoinColumn{
			Original: aggr.Original,
			LHSExprs: []sqlparser.Expr{aggr.Func},
		})
		return aggr, nil
	case deps.IsSolvedBy(ab.rhsID):
		ab.rhs.pushThroughAggr(aggr)
		ab.joinColumns = append(ab.joinColumns, JoinColumn{
			Original: aggr.Original,
			RHSExpr:  aggr.Func,
		})
		return aggr, nil
	default:
		return Aggr{}, vterrors.VT12001("aggregation on columns from different sources: " + sqlparser.String(aggr.Original.Expr))
	}
}

func (ab *aggBuilder) handleCountStar(ctx *plancontext.PlanningContext, aggr Aggr) (Aggr, error) {
	// Projection is necessary since we are going to need to do arithmetics to summarize the aggregates
	ab.projectionRequired = true

	// Add the aggregate to both sides of the join.
	lhsExpr := ab.lhs.addAggr(ctx, aggr)
	rhsExpr := ab.rhs.addAggr(ctx, aggr)

	// We expect the expressions to be different on each side of the join, otherwise it's an error.
	if lhsExpr == rhsExpr {
		panic(fmt.Sprintf("Need the two produced expressions to be different. %T %T", lhsExpr, rhsExpr))
	}

	// The joinColumns slice holds the column information from each side of the join.
	// We add a column for each side to facilitate reconstituting the final aggregation later.
	ab.joinColumns = append(ab.joinColumns,
		JoinColumn{
			Original: aeWrap(lhsExpr),
			LHSExprs: []sqlparser.Expr{lhsExpr},
		}, JoinColumn{
			Original: aeWrap(rhsExpr),
			RHSExpr:  rhsExpr,
		})

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
		Left:     lhsExpr,
		Right:    rhsExpr,
	}
	ab.proj.addUnexploredExpr(aggr.Original, projExpr)
	return aggr, nil
}

func coalesceFunc(e sqlparser.Expr) sqlparser.Expr {
	// coalesce(e,1)
	return &sqlparser.FuncExpr{
		Name: sqlparser.NewIdentifierCI("coalesce"),
		Exprs: sqlparser.SelectExprs{
			aeWrap(e),
			aeWrap(sqlparser.NewIntLiteral("1")),
		},
	}
}

// joinPusher is a helper struct that aids in pushing down an Aggregator into one side of a Join.
// It creates a new Aggregator that is pushed down and keeps track of the column dependencies that the new Aggregator has.
type joinPusher struct {
	orig    *Aggregator        // The original Aggregator before pushing.
	pushed  *Aggregator        // The new Aggregator created for push-down.
	columns []int              // List of column offsets used in the new Aggregator.
	tableID semantics.TableSet // The TableSet denoting the side of the Join where the new Aggregator is pushed.
}

// addAggr creates a copy of the given aggregation, updates its column offset to point to the correct location in the new Aggregator,
// and adds it to the list of Aggregations of the new Aggregator. It also updates the semantic analysis information to reflect the new structure.
// It returns the expression of the aggregation as it should be used in the parent Aggregator.
func (p joinPusher) addAggr(ctx *plancontext.PlanningContext, aggr Aggr) sqlparser.Expr {
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
func (p joinPusher) pushThroughAggr(aggr Aggr) {
	p.pushed.Columns = append(p.pushed.Columns, aggr.Original)
	p.pushed.Aggregations = append(p.pushed.Aggregations, aggr)
}

// addGrouping creates a copy of the given GroupBy, updates its column offset to point to the correct location in the new Aggregator,
// and adds it to the list of GroupBy expressions of the new Aggregator. It also updates the semantic analysis information to reflect the new structure.
// It returns the expression of the GroupBy as it should be used in the parent Aggregator.
func (p joinPusher) addGrouping(ctx *plancontext.PlanningContext, gb GroupBy) sqlparser.Expr {
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
func (p joinPusher) useColumn(offset int) int {
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
