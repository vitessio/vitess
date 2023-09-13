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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func isMergeable(ctx *plancontext.PlanningContext, query sqlparser.SelectStatement, op ops.Operator) bool {
	validVindex := func(expr sqlparser.Expr) bool {
		sc := findColumnVindex(ctx, op, expr)
		return sc != nil && sc.IsUnique()
	}

	if query.GetLimit() != nil {
		return false
	}

	sel, ok := query.(*sqlparser.Select)
	if !ok {
		return false
	}

	if len(sel.GroupBy) > 0 {
		// iff we are grouping, we need to check that we can perform the grouping inside a single shard, and we check that
		// by checking that one of the grouping expressions used is a unique single column vindex.
		// TODO: we could also support the case where all the columns of a multi-column vindex are used in the grouping
		for _, gb := range sel.GroupBy {
			if validVindex(gb) {
				return true
			}
		}
		return false
	}

	// if we have grouping, we have already checked that it's safe, and don't need to check for aggregations
	// but if we don't have groupings, we need to check if there are aggregations that will mess with us
	if sqlparser.ContainsAggregation(sel.SelectExprs) {
		return false
	}

	if sqlparser.ContainsAggregation(sel.Having) {
		return false
	}

	return true
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
			ap, err := op.GetAliasedProjections()
			if err != nil {
				return nil, nil, err
			}

			for _, pe := range ap {
				se, ok := pe.Info.(*SubQueryExpression)
				if !ok {
					continue
				}
				newExpr, rewritten := rewriteMergedSubqueryExpr(ctx, se, pe.EvalExpr)
				if rewritten {
					pe.Info = nil
					pe.EvalExpr = newExpr
				}
			}
			return op, rewrite.SameTree, nil
		default:
			return op, rewrite.SameTree, nil
		}
	}
	ctx.SubqueriesSettled = true
	return rewrite.BottomUp(op, TableID, visit, nil)
}

func rewriteMergedSubqueryExpr(ctx *plancontext.PlanningContext, se *SubQueryExpression, expr sqlparser.Expr) (sqlparser.Expr, bool) {
	rewritten := false
	for _, sq := range se.sqs {
		for _, sq2 := range ctx.MergedSubqueries {
			if sq._sq == sq2 {
				expr = sqlparser.Rewrite(expr, nil, func(cursor *sqlparser.Cursor) bool {
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
					rewritten = true
					cursor.Replace(sq._sq)
					return false
				}).(sqlparser.Expr)
			}
		}
	}
	return expr, rewritten
}

// tryPushDownSubQueryInJoin attempts to push down a SubQuery into an ApplyJoin
func tryPushDownSubQueryInJoin(ctx *plancontext.PlanningContext, inner *SubQuery, outer *ApplyJoin) (ops.Operator, *rewrite.ApplyResult, error) {
	lhs := TableID(outer.LHS)
	rhs := TableID(outer.RHS)
	joinID := TableID(outer)
	innerID := TableID(inner.Subquery)

	deps := semantics.EmptyTableSet()
	for _, predicate := range inner.GetMergePredicates() {
		deps = deps.Merge(ctx.SemTable.RecursiveDeps(predicate))
	}
	deps = deps.Remove(innerID)

	if deps.IsSolvedBy(lhs) {
		// we can safely push down the subquery on the LHS
		outer.LHS = addSubQuery(outer.LHS, inner)
		return outer, rewrite.NewTree("push subquery into LHS of join", inner), nil
	}

	if outer.LeftJoin {
		return nil, rewrite.SameTree, nil
	}

	// in general, we don't want to push down uncorrelated subqueries into the RHS of a join,
	// since this side is executed once per row from the LHS, so we would unnecessarily execute
	// the subquery multiple times. The exception is if we can merge the subquery with the RHS of the join.
	merged, result, err := tryMergeWithRHS(ctx, inner, outer)
	if err != nil {
		return nil, nil, err
	}
	if merged != nil {
		return merged, result, nil
	}

	if len(inner.Predicates) == 0 {
		// we don't want to push uncorrelated subqueries to the RHS of a join
		return nil, rewrite.SameTree, nil
	}

	if deps.IsSolvedBy(rhs) {
		// we can push down the subquery filter on RHS of the join
		outer.RHS = addSubQuery(outer.RHS, inner)
		return outer, rewrite.NewTree("push subquery into RHS of join", inner), nil
	}

	if deps.IsSolvedBy(joinID) {
		// we can rewrite the predicate to not use the values from the lhs,
		// and instead use arguments for these dependencies.
		// this way we can push the subquery into the RHS of this join
		var updatedPred sqlparser.Exprs
		for _, predicate := range inner.Predicates {
			col, err := BreakExpressionInLHSandRHS(ctx, predicate, lhs)
			if err != nil {
				return nil, rewrite.SameTree, nil
			}
			outer.Predicate = ctx.SemTable.AndExpressions(predicate, outer.Predicate)
			outer.JoinPredicates = append(outer.JoinPredicates, col)
			updatedPred = append(updatedPred, col.RHSExpr)
			for idx, expr := range col.LHSExprs {
				argName := col.BvNames[idx]
				newOrg := replaceSingleExpr(ctx, inner.MergeExpression, expr, sqlparser.NewArgument(argName))
				inner.MergeExpression = newOrg
			}
		}
		inner.Predicates = updatedPred
		// we can't push down filter on outer joins
		outer.RHS = addSubQuery(outer.RHS, inner)
		return outer, rewrite.NewTree("push subquery into RHS of join removing LHS expr", inner), nil
	}

	return nil, rewrite.SameTree, nil
}

// tryMergeWithRHS attempts to merge a subquery with the RHS of a join
func tryMergeWithRHS(ctx *plancontext.PlanningContext, inner *SubQuery, outer *ApplyJoin) (ops.Operator, *rewrite.ApplyResult, error) {
	// both sides need to be routes
	outerRoute, ok := outer.RHS.(*Route)
	if !ok {
		return nil, nil, nil
	}
	innerRoute, ok := inner.Subquery.(*Route)
	if !ok {
		return nil, nil, nil
	}

	newExpr, err := rewriteOriginalPushedToRHS(ctx, inner.MergeExpression, outer)
	if err != nil {
		return nil, nil, err
	}
	sqm := &subqueryRouteMerger{
		outer:    outerRoute,
		original: newExpr,
		subq:     inner,
	}
	newOp, err := mergeJoinInputs(ctx, innerRoute, outerRoute, inner.GetMergePredicates(), sqm)
	if err != nil || newOp == nil {
		return nil, nil, err
	}

	outer.RHS = newOp
	ctx.MergedSubqueries = append(ctx.MergedSubqueries, inner._sq)
	return outer, rewrite.NewTree("merged subquery with rhs of join", inner), nil
}

// addSubQuery adds a SubQuery to the given operator. If the operator is a SubQueryContainer,
// it will add the SubQuery to the SubQueryContainer. If the operator is something else,	it will
// create a new SubQueryContainer with the given operator as the outer and the SubQuery as the inner.
func addSubQuery(in ops.Operator, inner *SubQuery) ops.Operator {
	sql, ok := in.(*SubQueryContainer)
	if !ok {
		return &SubQueryContainer{
			Outer: in,
			Inner: []*SubQuery{inner},
		}
	}

	sql.Inner = append(sql.Inner, inner)
	return sql
}

// rewriteOriginalPushedToRHS rewrites the original expression to use the argument names instead of the column names
// this is necessary because we are pushing the subquery into the RHS of the join, and we need to use the argument names
// instead of the column names
func rewriteOriginalPushedToRHS(ctx *plancontext.PlanningContext, expression sqlparser.Expr, outer *ApplyJoin) (sqlparser.Expr, error) {
	var err error
	outerID := TableID(outer.LHS)
	result := sqlparser.CopyOnRewrite(expression, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
		col, ok := cursor.Node().(*sqlparser.ColName)
		if !ok || ctx.SemTable.RecursiveDeps(col) != outerID {
			// we are only interested in columns that are coming from the LHS of the join
			return
		}
		// this is a dependency we are being fed from the LHS of the join, so we
		// need to find the argument name for it and use that instead
		// we can't use the column name directly, because we're in the RHS of the join
		name, innerErr := outer.findOrAddColNameBindVarName(ctx, col)
		if err != nil {
			err = innerErr
			cursor.StopTreeWalk()
			return
		}
		cursor.Replace(sqlparser.NewArgument(name))
	}, nil)
	if err != nil {
		return nil, err
	}
	return result.(sqlparser.Expr), nil
}

func pushProjectionToOuter(ctx *plancontext.PlanningContext, p *Projection, src *SubQueryContainer) (ops.Operator, *rewrite.ApplyResult, error) {
	ap, err := p.GetAliasedProjections()
	if err != nil {
		return p, rewrite.SameTree, nil
	}

	outer := TableID(src.Outer)
	for _, pe := range ap {
		_, isOffset := pe.Info.(*Offset)
		if isOffset {
			continue
		}

		if !ctx.SemTable.RecursiveDeps(pe.EvalExpr).IsSolvedBy(outer) {
			return p, rewrite.SameTree, nil
		}

		if se, ok := pe.Info.(*SubQueryExpression); ok {
			pe.EvalExpr = rewriteColNameToArgument(pe.EvalExpr, se, src.Inner...)
		}
	}
	// all projections can be pushed to the outer
	src.Outer, p.Source = p, src.Outer
	return src, rewrite.NewTree("push projection into outer side of subquery container", p), nil
}

func rewriteColNameToArgument(in sqlparser.Expr, se *SubQueryExpression, subqueries ...*SubQuery) sqlparser.Expr {
	cols := make(map[*sqlparser.ColName]any)
	for _, sq1 := range se.sqs {
		for _, sq2 := range subqueries {
			if sq1.ReplacedSqColName == sq2.ReplacedSqColName && sq1.ReplacedSqColName != nil {
				cols[sq1.ReplacedSqColName] = nil
			}
		}
	}
	if len(cols) <= 0 {
		return in
	}

	// replace the ColNames with Argument inside the subquery
	result := sqlparser.Rewrite(in, nil, func(cursor *sqlparser.Cursor) bool {
		col, ok := cursor.Node().(*sqlparser.ColName)
		if !ok {
			return true
		}
		if _, ok := cols[col]; !ok {
			return true
		}
		arg := sqlparser.NewArgument(col.Name.String())
		cursor.Replace(arg)
		return true
	})
	return result.(sqlparser.Expr)
}

func pushOrMergeSubQueryContainer(ctx *plancontext.PlanningContext, in *SubQueryContainer) (ops.Operator, *rewrite.ApplyResult, error) {
	var remaining []*SubQuery
	var result *rewrite.ApplyResult
	for _, inner := range in.Inner {
		newOuter, _result, err := pushOrMerge(ctx, in.Outer, inner)
		if err != nil {
			return nil, nil, err
		}
		if _result == rewrite.SameTree {
			remaining = append(remaining, inner)
			continue
		}

		in.Outer = newOuter
		result = result.Merge(_result)
	}

	if len(remaining) == 0 {
		return in.Outer, result, nil
	}

	in.Inner = remaining

	return in, result, nil
}

func tryPushDownSubQueryInRoute(ctx *plancontext.PlanningContext, subQuery *SubQuery, outer *Route) (newOuter ops.Operator, result *rewrite.ApplyResult, err error) {
	switch inner := subQuery.Subquery.(type) {
	case *Route:
		return tryMergeSubqueryWithOuter(ctx, subQuery, outer, inner)
	case *SubQueryContainer:
		return tryMergeSubqueriesRecursively(ctx, subQuery, outer, inner)
	}
	return outer, rewrite.SameTree, nil
}

// tryMergeSubqueriesRecursively attempts to merge a SubQueryContainer with the outer Route.
func tryMergeSubqueriesRecursively(
	ctx *plancontext.PlanningContext,
	subQuery *SubQuery,
	outer *Route,
	inner *SubQueryContainer,
) (ops.Operator, *rewrite.ApplyResult, error) {
	exprs := subQuery.GetMergePredicates()
	merger := &subqueryRouteMerger{
		outer:    outer,
		original: subQuery.MergeExpression,
		subq:     subQuery,
	}
	op, err := mergeJoinInputs(ctx, inner.Outer, outer, exprs, merger)
	if err != nil {
		return nil, nil, err
	}
	if op == nil {
		return outer, rewrite.SameTree, nil
	}

	op = Clone(op).(*Route)
	op.Source = outer.Source
	var finalResult *rewrite.ApplyResult
	for _, subq := range inner.Inner {
		newOuter, res, err := tryPushDownSubQueryInRoute(ctx, subq, op)
		if err != nil {
			return nil, nil, err
		}
		if res == rewrite.SameTree {
			// we failed to merge one of the inners - we need to abort
			return nil, rewrite.SameTree, nil
		}
		op = newOuter.(*Route)
		finalResult = finalResult.Merge(res)
	}

	op.Source = &Filter{Source: outer.Source, Predicates: []sqlparser.Expr{subQuery.MergeExpression}}
	return op, finalResult.Merge(rewrite.NewTree("merge outer of two subqueries", subQuery)), nil
}

func tryMergeSubqueryWithOuter(ctx *plancontext.PlanningContext, subQuery *SubQuery, outer *Route, inner ops.Operator) (ops.Operator, *rewrite.ApplyResult, error) {
	exprs := subQuery.GetMergePredicates()
	merger := &subqueryRouteMerger{
		outer:    outer,
		original: subQuery.MergeExpression,
		subq:     subQuery,
	}
	op, err := mergeJoinInputs(ctx, inner, outer, exprs, merger)
	if err != nil {
		return nil, nil, err
	}
	if op == nil {
		return outer, rewrite.SameTree, nil
	}
	if !subQuery.IsProjection() {
		op.Source = &Filter{Source: outer.Source, Predicates: []sqlparser.Expr{subQuery.MergeExpression}}
	}
	ctx.MergedSubqueries = append(ctx.MergedSubqueries, subQuery._sq)
	return op, rewrite.NewTree("merged subquery with outer", subQuery), nil
}

type subqueryRouteMerger struct {
	outer    *Route
	original sqlparser.Expr
	subq     *SubQuery
}

func (s *subqueryRouteMerger) mergeShardedRouting(r1, r2 *ShardedRouting, old1, old2 *Route) (*Route, error) {
	return s.merge(old1, old2, mergeShardedRouting(r1, r2))
}

func (s *subqueryRouteMerger) merge(old1, old2 *Route, r Routing) (*Route, error) {
	mergedWith := append(old1.MergedWith, old1, old2)
	mergedWith = append(mergedWith, old2.MergedWith...)
	src := s.outer.Source
	if !s.subq.IsProjection() {
		src = &Filter{
			Source:     s.outer.Source,
			Predicates: []sqlparser.Expr{s.original},
		}
	}
	return &Route{
		Source:        src,
		MergedWith:    mergedWith,
		Routing:       r,
		Ordering:      s.outer.Ordering,
		ResultColumns: s.outer.ResultColumns,
	}, nil
}

var _ merger = (*subqueryRouteMerger)(nil)
