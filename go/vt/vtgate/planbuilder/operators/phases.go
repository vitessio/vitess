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
	Phase int
)

const (
	physicalTransform Phase = iota
	initialPlanning
	pullDistinctFromUnion
	delegateAggregation
	addAggrOrdering
	cleanOutPerfDistinct
	subquerySettling
	DONE
)

func (p Phase) String() string {
	switch p {
	case physicalTransform:
		return "physicalTransform"
	case initialPlanning:
		return "initial horizon planning optimization"
	case pullDistinctFromUnion:
		return "pull distinct from UNION1"
	case delegateAggregation:
		return "split aggregation between vtgate and mysql"
	case addAggrOrdering:
		return "optimize aggregations with ORDER BY"
	case cleanOutPerfDistinct:
		return "optimize Distinct operations"
	case subquerySettling:
		return "settle subqueries"
	}

	return "unknown"
}

func (p Phase) shouldRun(s semantics.QuerySignature) bool {
	switch p {
	case pullDistinctFromUnion:
		return s.Union
	case delegateAggregation:
		return s.Aggregation
	case addAggrOrdering:
		return s.Aggregation
	case cleanOutPerfDistinct:
		return s.Distinct
	case subquerySettling:
		return s.SubQueries
	}
	return true
}

func (p Phase) act(ctx *plancontext.PlanningContext, op ops.Operator) (ops.Operator, error) {
	switch p {
	case pullDistinctFromUnion:
		return pullDistinctFromUNION(ctx, op)
	case delegateAggregation:
		return enableDelegateAggregation(ctx, op)
	case addAggrOrdering:
		return addOrderBysForAggregations(ctx, op)
	case cleanOutPerfDistinct:
		return removePerformanceDistinctAboveRoute(ctx, op)
	case subquerySettling:
		return settleSubqueries(ctx, op)
	}

	return op, nil
}

// getPhases returns the ordered phases that the planner will undergo.
// These phases ensure the appropriate collaboration between rewriters.
func getPhases(ctx *plancontext.PlanningContext) (phases []Phase) {
	for p := Phase(0); p < DONE; p++ {
		if p.shouldRun(ctx.SemTable.QuerySignature) {
			phases = append(phases, p)
		}
	}
	return
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

func enableDelegateAggregation(ctx *plancontext.PlanningContext, op ops.Operator) (ops.Operator, error) {
	return addColumnsToInput(ctx, op)
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
