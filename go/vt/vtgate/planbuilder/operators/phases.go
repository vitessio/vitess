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
	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	// Phase defines the different planning phases to go through to produce an optimized plan for the input query.
	Phase struct {
		Name string
		// action is the action to be taken before calling plan optimization operation.
		action func(ctx *plancontext.PlanningContext, op ops.Operator) (ops.Operator, error)
		apply  func(semantics.QuerySignature) bool
	}
)

// getPhases returns the ordered phases that the planner will undergo.
// These phases ensure the appropriate collaboration between rewriters.
func getPhases(ctx *plancontext.PlanningContext) []Phase {
	phases := []Phase{
		{
			// Initial optimization phase.
			Name: "initial horizon planning optimization",
		},
		{
			// Convert UNION with `distinct` to UNION ALL with DISTINCT op on top.
			Name:   "pull distinct from UNION",
			action: pullDistinctFromUNION,
			apply:  func(s semantics.QuerySignature) bool { return s.Union },
		},
		{
			// Split aggregation that has not been pushed under the routes into between work on mysql and vtgate.
			Name:   "split aggregation between vtgate and mysql",
			action: enableDelegateAggregatiion,
			apply:  func(s semantics.QuerySignature) bool { return s.Aggregation },
		},
		{
			// Add ORDER BY for aggregations above the route.
			Name:   "optimize aggregations with ORDER BY",
			action: addOrderBysForAggregations,
			apply:  func(s semantics.QuerySignature) bool { return s.Aggregation },
		},
		{
			// Remove unnecessary Distinct operators above routes.
			Name:   "optimize Distinct operations",
			action: removePerformanceDistinctAboveRoute,
			apply:  func(s semantics.QuerySignature) bool { return s.Distinct },
		},
		{
			// Finalize subqueries after they've been pushed as far as possible.
			Name:   "settle subqueries",
			action: settleSubqueries,
			apply:  func(s semantics.QuerySignature) bool { return s.SubQueries },
		},
	}

	return slice.Filter(phases, func(phase Phase) bool {
		return phase.apply == nil || phase.apply(ctx.SemTable.QuerySignature)
	})
}

func removePerformanceDistinctAboveRoute(_ *plancontext.PlanningContext, op ops.Operator) (ops.Operator, error) {
	return rewrite.BottomUp(op, TableID, func(innerOp ops.Operator, _ semantics.TableSet, _ bool) (ops.Operator, *rewrite.ApplyResult, error) {
		d, ok := innerOp.(*Distinct)
		if !ok || d.Required {
			return innerOp, rewrite.SameTree, nil
		}

		return d.Source, rewrite.NewTree("removed distinct not required that was not pushed under route", d), nil
	}, stopAtRoute)
}

func enableDelegateAggregatiion(ctx *plancontext.PlanningContext, op ops.Operator) (ops.Operator, error) {
	ctx.DelegateAggregation = true
	return addColumnsToInput(ctx, op)
}

func settleSubqueries(ctx *plancontext.PlanningContext, op ops.Operator) (ops.Operator, error) {
	visit := func(op ops.Operator, lhsTables semantics.TableSet, isRoot bool) (ops.Operator, *rewrite.ApplyResult, error) {
		switch op := op.(type) {
		case *SubQueryContainer:
			outer := op.Outer
			for _, subq := range op.Inner {
				newOuter, err := subq.settle(ctx, outer)
				if err != nil {
					return nil, nil, err
				}
				subq.Outer = newOuter
				outer = subq
			}
			return outer, rewrite.NewTree("extracted subqueries from subquery container", outer), nil
		case *Projection:
			for idx, proj := range op.Projections {
				se, ok := proj.(SubQueryExpression)
				if !ok {
					continue
				}
				if isMerged(ctx, se) {
					// if the expression has been merged, there is nothing left we need to do
					continue
				}
				// TODO: this doesn't look correct. what if the
				op.Columns[idx].Expr = se.GetExpr()
			}
			return op, rewrite.SameTree, nil
		default:
			return op, rewrite.SameTree, nil
		}
	}
	return rewrite.BottomUp(op, TableID, visit, nil)
}

func isMerged(ctx *plancontext.PlanningContext, se SubQueryExpression) (merged bool) {
	expr := se.GetExpr()
	for _, sq := range se.sqs {
		for _, sq2 := range ctx.MergedSubqueries {
			if sq._sq == sq2 {
				sqlparser.Rewrite(expr, nil, func(cursor *sqlparser.Cursor) bool {
					switch expr := cursor.Node().(type) {
					case *sqlparser.ColName:
						if expr.Name.String() != sq.ReplacedSqColName.Name.String() {
							return true
						}
					case *sqlparser.Argument:
						if expr.Name != sq.ReplacedSqColName.Name.String() {
							return true
						}
					default:
						return true
					}
					merged = true
					cursor.Replace(sq._sq)
					return false
				})
			}
		}
	}
	return
}

func addOrderBysForAggregations(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	visitor := func(in ops.Operator, _ semantics.TableSet, isRoot bool) (ops.Operator, *rewrite.ApplyResult, error) {
		aggrOp, ok := in.(*Aggregator)
		if !ok {
			return in, rewrite.SameTree, nil
		}

		requireOrdering, err := needsOrdering(ctx, aggrOp)
		if err != nil {
			return nil, nil, err
		}
		if !requireOrdering {
			return in, rewrite.SameTree, nil
		}
		orderBys := slice.Map(aggrOp.Grouping, func(from GroupBy) ops.OrderBy {
			return from.AsOrderBy()
		})
		if aggrOp.DistinctExpr != nil {
			orderBys = append(orderBys, ops.OrderBy{
				Inner: &sqlparser.Order{
					Expr: aggrOp.DistinctExpr,
				},
				SimplifiedExpr: aggrOp.DistinctExpr,
			})
		}
		aggrOp.Source = &Ordering{
			Source: aggrOp.Source,
			Order:  orderBys,
		}
		return in, rewrite.NewTree("added ordering before aggregation", in), nil
	}

	return rewrite.BottomUp(root, TableID, visitor, stopAtRoute)
}

func needsOrdering(ctx *plancontext.PlanningContext, in *Aggregator) (bool, error) {
	requiredOrder := slice.Map(in.Grouping, func(from GroupBy) sqlparser.Expr {
		return from.SimplifiedExpr
	})
	if in.DistinctExpr != nil {
		requiredOrder = append(requiredOrder, in.DistinctExpr)
	}
	if len(requiredOrder) == 0 {
		return false, nil
	}
	srcOrdering, err := in.Source.GetOrdering()
	if err != nil {
		return false, err
	}
	if len(srcOrdering) < len(requiredOrder) {
		return true, nil
	}
	for idx, gb := range requiredOrder {
		if !ctx.SemTable.EqualsExprWithDeps(srcOrdering[idx].SimplifiedExpr, gb) {
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
