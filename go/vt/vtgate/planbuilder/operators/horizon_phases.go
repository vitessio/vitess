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
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// Phase defines the different planning phases to go through to produce an optimized plan for the input query.
type Phase struct {
	Name string
	// preOptimizeAction is the action to be taken before calling plan optimization operation.
	preOptimizeAction func(ctx *plancontext.PlanningContext, op ops.Operator) (ops.Operator, error)
	// preOptimizeAction is the action to be taken before calling plan optimization operation.
	postOptimizeAction func(op ops.Operator) (ops.Operator, error)
}

// getPhases returns the phases the planner will go through.
// It's used to control so rewriters collaborate correctly
func getPhases() []Phase {
	return []Phase{{
		// Initial optimization
		Name: "initial horizon planning optimization phase",
	}, {
		Name: "add columns if needed by filter",
		preOptimizeAction: func(ctx *plancontext.PlanningContext, op ops.Operator) (ops.Operator, error) {
			return addColumnsToFilter(ctx, op)
		},
	}, {
		// Adding Ordering Op - Any aggregation that is performed in the VTGate needs the input to be ordered
		Name: "add ORDER BY to aggregations above the route and add GROUP BY to aggregations on the RHS of join",
		preOptimizeAction: func(ctx *plancontext.PlanningContext, op ops.Operator) (ops.Operator, error) {
			ctx.Phases.PushAggregation = true
			return addOrderBysForAggregations(ctx, op)
		},
		// Adding Group by - This is needed if the grouping is performed on a join with a join condition then
		//                   aggregation happening at route needs a group by to ensure only matching rows returns
		//                   the aggregations otherwise returns no result.
		postOptimizeAction: addGroupByOnRHSOfJoin,
	}}
}

func addColumnsToFilter(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	visitor := func(in ops.Operator, _ semantics.TableSet, isRoot bool) (ops.Operator, *rewrite.ApplyResult, error) {
		filter, ok := in.(*Filter)
		if !ok {
			return in, rewrite.SameTree, nil
		}

		columns, err := filter.GetColumns()
		if err != nil {
			return nil, nil, vterrors.Wrap(err, "while planning filter")
		}
		proj, areOnTopOfProj := filter.Source.(selectExpressions)
		if !areOnTopOfProj {
			// not much we can do here
			return in, rewrite.SameTree, nil
		}
		addedColumns := false
		for _, expr := range filter.Predicates {
			sqlparser.Rewrite(expr, func(cursor *sqlparser.Cursor) bool {
				e, ok := cursor.Node().(sqlparser.Expr)
				if !ok {
					return true
				}
				offset := slices.IndexFunc(columns, func(expr *sqlparser.AliasedExpr) bool {
					return ctx.SemTable.EqualsExprWithDeps(expr.Expr, e)
				})

				if offset >= 0 {
					// this expression can be fetched from the input - we can stop here
					return false
				}

				if fetchByOffset(e) {
					// this expression has to be fetched from the input, but we didn't find it in the input. let's add it
					_, addToGroupBy := e.(*sqlparser.ColName)
					proj.addColumnWithoutPushing(aeWrap(e), addToGroupBy)
					addedColumns = true
					columns, err = proj.GetColumns()
					if err != nil {
						panic("this should not happen")
					}
					return false
				}
				return true
			}, nil)
		}
		if addedColumns {
			return in, rewrite.NewTree("added columns because filter needs it", in), nil
		}

		return in, rewrite.SameTree, nil
	}

	return rewrite.TopDown(root, TableID, visitor, stopAtRoute)
}

// addOrderBysForAggregations runs after we have run horizonPlanning until the op tree stops changing
// this means that we have pushed aggregations and other ops as far down as they'll go
// addOrderBysForAggregations will find Aggregators that have not been pushed under routes and
// add the necessary Ordering operators for them
func addOrderBysForAggregations(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	visitor := func(in ops.Operator, _ semantics.TableSet, isRoot bool) (ops.Operator, *rewrite.ApplyResult, error) {
		aggrOp, ok := in.(*Aggregator)
		if !ok {
			return in, rewrite.SameTree, nil
		}

		requireOrdering, err := needsOrdering(aggrOp, ctx)
		if err != nil {
			return nil, nil, err
		}
		if !requireOrdering {
			return in, rewrite.SameTree, nil
		}
		aggrOp.Source = &Ordering{
			Source: aggrOp.Source,
			Order: slices2.Map(aggrOp.Grouping, func(from GroupBy) ops.OrderBy {
				return from.AsOrderBy()
			}),
		}
		return in, rewrite.NewTree("added ordering before aggregation", in), nil
	}

	return rewrite.TopDown(root, TableID, visitor, stopAtRoute)
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

func addGroupByOnRHSOfJoin(root ops.Operator) (ops.Operator, error) {
	visitor := func(in ops.Operator, _ semantics.TableSet, isRoot bool) (ops.Operator, *rewrite.ApplyResult, error) {
		join, ok := in.(*ApplyJoin)
		if !ok {
			return in, rewrite.SameTree, nil
		}

		return addLiteralGroupingToRHS(join)
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
	return in, rewrite.SameTree, nil
}
