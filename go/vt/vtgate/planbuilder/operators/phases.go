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
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
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

// getPhases returns the phases the planner will go through.
// It's used to control so rewriters collaborate correctly
func getPhases(ctx *plancontext.PlanningContext) []Phase {
	phases := []Phase{{
		// Initial optimization
		Name: "initial horizon planning optimization phase",
	}, {
		Name: "pull distinct from UNION",
		// to make it easier to compact UNIONs together, we keep the `distinct` flag in the UNION op until this
		// phase. Here we will place a DISTINCT op on top of the UNION, and turn the UNION into a UNION ALL
		action: pullDistinctFromUNION,
		apply:  func(s semantics.QuerySignature) bool { return s.Union },
	}, {
		// after the initial pushing down of aggregations and filtering, we add columns for the filter ops that
		// need it their inputs, and then we start splitting the aggregation
		// so parts run on MySQL and parts run on VTGate
		Name:   "add filter columns to projection or aggregation",
		action: enableDelegateAggregatiion,
	}, {
		// addOrderBysForAggregations runs after we have pushed aggregations as far down as they'll go
		// addOrderBysForAggregations will find Aggregators that have not been pushed under routes and
		// add the necessary Ordering operators for them
		Name:   "add ORDER BY to aggregations above the route and add GROUP BY to aggregations on the RHS of join",
		action: addOrderBysForAggregations,
		apply:  func(s semantics.QuerySignature) bool { return s.Aggregation },
	}, {
		Name:   "remove Distinct operator that are not required and still above a route",
		action: removePerformanceDistinctAboveRoute,
		apply:  func(s semantics.QuerySignature) bool { return s.Distinct },
	}, {
		// This phase runs late, so subqueries have by this point been pushed down as far as they'll go.
		// Next step is to extract the subqueries from the slices in the SubQueryContainer
		// and plan for how to run them on the vtgate
		Name:   "settle subqueries above the route",
		action: settleSubqueries,
		apply:  func(s semantics.QuerySignature) bool { return s.SubQueries },
	}}

	return slice.Filter(phases, func(phase Phase) bool {
		if phase.apply == nil {
			// if no apply function is defined, we always apply the phase
			return true
		}
		return phase.apply(ctx.SemTable.QuerySignature)
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
		sqc, ok := op.(*SubQueryContainer)
		if !ok {
			return op, rewrite.SameTree, nil
		}
		outer := sqc.Outer
		for _, subq := range sqc.Inner {
			newOuter, err := settleSubquery(ctx, outer, subq)
			if err != nil {
				return nil, nil, err
			}
			outer = newOuter
		}
		return outer, rewrite.NewTree("extracted subqueries from subquery container", outer), nil
	}
	return rewrite.BottomUp(op, TableID, visit, stopAtRoute)
}

// settleSubquery is run when the subqueries have been pushed as far down as they can go.
// At this point, we know that the subqueries will not be pushed under a Route, so we need to
// plan for how to run them on the vtgate
func settleSubquery(ctx *plancontext.PlanningContext, outer ops.Operator, subq SubQuery) (ops.Operator, error) {
	var err error
	// TODO: here we have the chance of using a different subquery for how we actually run the query. Here is an example:
	// select * from user where id = 5 and foo in (select bar from music where baz = 13)
	// this query is equivalent to
	// select * from user where id = 5 and exists(select 1 from music where baz = 13 and user.id = bar)
	// Long term, we should have a cost based optimizer that can make this decision for us.
	switch subq := subq.(type) {
	case *SubQueryFilter:
		outer, err = settleSubqueryFilter(ctx, subq, outer)
		if err != nil {
			return nil, err
		}
	default:
		return nil, vterrors.VT13001("unexpected subquery type")
	}

	subq.SetOuter(outer)

	return subq, nil
}

func settleSubqueryFilter(ctx *plancontext.PlanningContext, sj *SubQueryFilter, outer ops.Operator) (ops.Operator, error) {
	if len(sj.JoinVars) > 0 {
		if sj.FilterType != opcode.PulloutExists {
			return nil, vterrors.VT12001("correlated subquery in WHERE clause")
		}
		sj.Subquery = &Filter{
			Source:     sj.Subquery,
			Predicates: []sqlparser.Expr{sj.corrSubPredicate},
		}
		return outer, nil
	}

	resultArg, hasValuesArg := ctx.ReservedVars.ReserveSubQueryWithHasValues()
	dontEnterSubqueries := func(node, _ sqlparser.SQLNode) bool {
		if _, ok := node.(*sqlparser.Subquery); ok {
			return false
		}
		return true
	}
	post := func(cursor *sqlparser.CopyOnWriteCursor) {
		node := cursor.Node()
		if _, ok := node.(*sqlparser.Subquery); !ok {
			return
		}

		var arg sqlparser.Expr
		if sj.FilterType == opcode.PulloutIn || sj.FilterType == opcode.PulloutNotIn {
			arg = sqlparser.NewListArg(resultArg)
		} else {
			arg = sqlparser.NewArgument(resultArg)
		}
		cursor.Replace(arg)
	}
	rhsPred := sqlparser.CopyOnRewrite(sj.Original, dontEnterSubqueries, post, ctx.SemTable.CopyDependenciesOnSQLNodes).(sqlparser.Expr)

	var predicates []sqlparser.Expr
	switch sj.FilterType {
	case opcode.PulloutExists:
		predicates = append(predicates, sqlparser.NewArgument(hasValuesArg))
	case opcode.PulloutIn, opcode.PulloutNotIn:
		predicates = append(predicates, sqlparser.NewArgument(hasValuesArg), rhsPred)
	case opcode.PulloutValue:
		predicates = append(predicates, rhsPred)
	}
	return &Filter{
		Source:     outer,
		Predicates: predicates,
	}, nil
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
