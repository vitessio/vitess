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

	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/semantics"

	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func tryPushingDownAggregator(ctx *plancontext.PlanningContext, aggregator *Aggregator) (output ops.Operator, applyResult rewrite.ApplyResult, err error) {
	if aggregator.Pushed {
		return aggregator, rewrite.SameTree, nil
	}
	aggregator.Pushed = true
	switch src := aggregator.Source.(type) {
	case *Route:
		output, applyResult, err = pushDownAggregationThroughRoute(aggregator, src)
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

func pushDownAggregationThroughRoute(aggregator *Aggregator, src *Route) (ops.Operator, rewrite.ApplyResult, error) {
	// If the route is single-shard, just swap the aggregator and route.
	if src.IsSingleShard() {
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
			Sort    <- Here we are sorting the input so that the OrderedAggregate can do it's thing
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

	joinColumns, projections, err := splitAggrColumnsToLeftAndRight(ctx, rootAggr, join, lhs, rhs)
	if err != nil {
		return nil, false, err
	}

	// We need to add any columns coming from the lhs of the join to the group by on that side
	// If we don't, the LHS will not be able to return the column, and it can't be used to send down to the RHS
	for _, pred := range join.JoinPredicates {
		for _, expr := range pred.LHSExprs {
			_, wexpr, err := rootAggr.QP.GetSimplifiedExpr(expr)
			if err != nil {
				return nil, false, err
			}
			lhs.pushed.Grouping = append(lhs.pushed.Grouping, GroupBy{
				Inner:         expr,
				WeightStrExpr: wexpr,
				KeyCol:        len(lhs.pushed.Columns),
			})
			lhs.pushed.Columns = append(lhs.pushed.Columns, aeWrap(expr))
		}
	}

	lhsTS := TableID(join.LHS)
	rhsTS := TableID(join.RHS)
	for _, groupBy := range rootAggr.Grouping {
		deps := ctx.SemTable.RecursiveDeps(groupBy.Inner)
		expr := groupBy.aliasedExpr.Expr
		switch {
		case deps.IsSolvedBy(lhsTS):
			lhs.addGrouping(ctx, groupBy)
			joinColumns = append(joinColumns, JoinColumn{
				Original: groupBy.aliasedExpr,
				LHSExprs: []sqlparser.Expr{expr},
			})
		case deps.IsSolvedBy(rhsTS):
			rhs.addGrouping(ctx, groupBy)
			joinColumns = append(joinColumns, JoinColumn{
				Original: groupBy.aliasedExpr,
				RHSExpr:  expr,
			})
		default:
			return nil, false, vterrors.VT12001("grouping on columns from different sources")
		}
	}

	join.LHS, join.RHS = lhs.pushed, rhs.pushed
	join.ColumnsAST = joinColumns

	var output ops.Operator = join
	if len(projections) > 0 {
		output = &Projection{
			Source:      join,
			ColumnNames: []string{""},
			Columns:     projections,
		}
	}

	if !rootAggr.Original {
		// we only keep the root aggregation, if this aggregator was created
		// by splitting one and pushing under a join, we can get rid of this one
		return output, rewrite.NewTree, nil
	}

	rootAggr.Source = output
	return rootAggr, rewrite.NewTree, nil
}

// splitAggrColumnsToLeftAndRight is responsible for pushing aggregation under a join during query planning.
// It returns separate lists of AggrColumn for the left and right side of the join, as well as joinColumns,
// which contains the JoinColumn information. Additionally, it calculates and returns the projections list,
// containing the ProjExpr required to produce the total aggregation.
//
// The function takes the following parameters:
// - ctx: The planning context.
// - aggregator: The Aggregator that needs to be pushed under the join.
// - join: The ApplyJoin that the aggregation is being pushed under.
//
// The function returns the following values:
// - lhs: A slice of AggrColumn representing the aggregation columns for the left side of the join.
// - rhs: A slice of AggrColumn representing the aggregation columns for the right side of the join.
// - joinColumns: A slice of JoinColumn containing the join column information.
// - projections: A slice of ProjExpr representing the expression evaluations needed to produce the total aggregation.
// - err: An error, if any occurred during the process.
func splitAggrColumnsToLeftAndRight(
	ctx *plancontext.PlanningContext,
	aggregator *Aggregator,
	join *ApplyJoin,
	lhs, rhs joinPusher,
) (joinColumns []JoinColumn, projections []ProjExpr, err error) {

	handleAggr := func(aggr Aggr) (Aggr, error) {
		switch aggr.OpCode {
		case opcode.AggregateCountStar:
			lhsExpr := lhs.addAggr(ctx, aggr)
			rhsExpr := rhs.addAggr(ctx, aggr)

			if lhsExpr == rhsExpr {
				panic(fmt.Sprintf("Need the two produced expressions to be different. %T %T", lhsExpr, rhsExpr))
			}

			joinColumns = append(joinColumns, JoinColumn{
				Original: aggr.Original,
				LHSExprs: []sqlparser.Expr{lhsExpr},
			}, JoinColumn{
				Original: aggr.Original,
				RHSExpr:  rhsExpr,
			})

			if join.LeftJoin {
				rhsExpr = &sqlparser.FuncExpr{
					Name: sqlparser.NewIdentifierCI("coalesce"),
					Exprs: sqlparser.SelectExprs{
						&sqlparser.AliasedExpr{Expr: rhsExpr},
						&sqlparser.AliasedExpr{Expr: sqlparser.NewIntLiteral("1")},
					},
				}
			}

			projExpr := &sqlparser.BinaryExpr{
				Operator: sqlparser.MultOp,
				Left:     lhsExpr,
				Right:    rhsExpr,
			}
			projections = append(projections, Expr{E: projExpr})
			return aggr, nil
		default:
			return Aggr{}, errHorizonNotPlanned()
		}
	}

	for i, aggr := range aggregator.Aggregations {
		aggr, err := handleAggr(aggr)
		if err != nil {
			return nil, nil, err
		}
		aggregator.Aggregations[i] = aggr
	}
	return
}

type joinPusher struct {
	orig    *Aggregator
	pushed  *Aggregator
	columns []int
	tableID semantics.TableSet
}

func (p joinPusher) addAggr(ctx *plancontext.PlanningContext, aggr Aggr) sqlparser.Expr {
	copyAggr := aggr
	expr := sqlparser.CloneExpr(aggr.Original.Expr)
	// copy dependencies so we can keep track of which side expressions need to be pushed to
	ctx.SemTable.Direct[expr] = p.tableID
	ctx.SemTable.Recursive[expr] = p.tableID

	offset := p.useColumn(aggr.ColOffset)
	copyAggr.ColOffset = offset
	p.pushed.Aggregations = append(p.pushed.Aggregations, copyAggr)
	return expr
}

func (p joinPusher) addGrouping(ctx *plancontext.PlanningContext, gb GroupBy) sqlparser.Expr {
	copyGB := gb
	expr := sqlparser.CloneExpr(gb.Inner)
	// copy dependencies so we can keep track of which side expressions need to be pushed to
	ctx.SemTable.CopyDependencies(gb.Inner, expr)
	offset := p.useColumn(copyGB.KeyCol)
	copyGB.KeyCol = offset
	p.pushed.Grouping = append(p.pushed.Grouping, copyGB)
	return expr
}

func (p joinPusher) useColumn(offset int) int {
	if p.columns[offset] == -1 {
		p.columns[offset] = len(p.pushed.Columns)
		// still haven't used this expression on the LHS
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
