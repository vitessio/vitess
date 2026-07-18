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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// mergeUnionInputInAnyOrder merges sources the sources of the union in any order
// can be used for UNION DISTINCT
func mergeUnionInputInAnyOrder(ctx *plancontext.PlanningContext, op *Union) ([]Operator, [][]sqlparser.SelectExpr) {
	sources := op.Sources
	selects := op.Selects

	// next we'll go over all the plans from and check if any two can be merged. if they can, they are merged,
	// and we continue checking for pairs of plans that can be merged into a single route
	idx := 0
	for idx < len(sources) {
		keep := make([]bool, len(sources))
		srcA := sources[idx]
		merged := false
		for j, srcB := range sources {
			if j <= idx {
				continue
			}
			selA := selects[idx]
			selB := selects[j]
			newPlan, sel := mergeUnionInputs(ctx, srcA, srcB, selA, selB, op.distinct)
			if newPlan != nil {
				sources[idx] = newPlan
				selects[idx] = sel
				srcA = newPlan
				merged = true
			} else {
				keep[j] = true
			}
		}
		if !merged {
			return sources, selects
		}

		var newSources []Operator
		var newSelects [][]sqlparser.SelectExpr
		for i, source := range sources {
			if keep[i] || i <= idx {
				newSources = append(newSources, source)
				newSelects = append(newSelects, selects[i])
			}
		}
		idx++
		sources = newSources
		selects = newSelects
	}

	return sources, selects
}

func mergeUnionInputsInOrder(ctx *plancontext.PlanningContext, op *Union) ([]Operator, [][]sqlparser.SelectExpr) {
	sources := op.Sources
	selects := op.Selects
	for {
		merged := false
		for i := 0; i < len(sources)-1; i++ {
			j := i + 1
			srcA, selA := sources[i], selects[i]
			srcB, selB := sources[j], selects[j]
			newPlan, sel := mergeUnionInputs(ctx, srcA, srcB, selA, selB, op.distinct)
			if newPlan != nil {
				sources[i] = newPlan
				selects[i] = sel
				merged = true
				sources = append(sources[:i+1], sources[j+1:]...)
				selects = append(selects[:i+1], selects[j+1:]...)
			}
		}
		if !merged {
			break
		}
	}

	return sources, selects
}

// mergeUnionInputs checks whether two operators can be merged into a single one.
// If they can be merged, a new operator with the merged routing is returned
// If they cannot be merged, nil is returned.
// this function is very similar to mergeJoinInputs
func mergeUnionInputs(
	ctx *plancontext.PlanningContext,
	lhs, rhs Operator,
	lhsExprs, rhsExprs []sqlparser.SelectExpr,
	distinct bool,
) (Operator, []sqlparser.SelectExpr) {
	lhsRoute, rhsRoute, routingA, routingB, a, b, sameKeyspace := prepareInputRoutes(ctx, lhs, rhs)
	if lhsRoute == nil {
		checkCrossKeyspaceOp(ctx, lhs, rhs, "UNION")
		return nil, nil
	}

	switch {
	// a none routing resolves to no shards at execution time, so its side of the
	// union contributes no rows. These cases must be handled before the dual and
	// anyShard cases below: those would retain the none routing and incorrectly
	// discard the other side's rows. A dual side has no keyspace and no tables of
	// its own, so any shard in the none side's keyspace can produce its rows.
	// Otherwise the other side's routing is retained, but only when it targets
	// the none side's keyspace: the none side's tables exist nowhere else.
	case a == none && b == dual:
		return createTrivialMergedUnion(ctx, lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct, &AnyShardRouting{keyspace: routingA.Keyspace()}, lhsRoute)
	case b == none && a == dual:
		return createTrivialMergedUnion(ctx, lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct, &AnyShardRouting{keyspace: routingB.Keyspace()}, rhsRoute)
	case a == none && sameKeyspace:
		return createTrivialMergedUnion(ctx, lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct, routingB, rhsRoute)
	case b == none && sameKeyspace:
		return createTrivialMergedUnion(ctx, lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct, routingA, lhsRoute)

	// if either side is a dual query, we can always merge them together
	// an unsharded/reference route can be merged with anything going to that keyspace
	case b == dual || (b == anyShard && sameKeyspace):
		return createTrivialMergedUnion(ctx, lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct, routingA, lhsRoute)
	case a == dual || (a == anyShard && sameKeyspace):
		return createTrivialMergedUnion(ctx, lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct, routingB, rhsRoute)

	case a == sharded && b == sharded && sameKeyspace:
		res, exprs := tryMergeUnionShardedRouting(ctx, lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct)
		if res != nil {
			return res, exprs
		}
	}

	// Check cross-keyspace restrictions for UNIONs that cannot be merged.
	checkCrossKeyspaceOp(ctx, lhs, rhs, "UNION")

	return nil, nil
}

func tryMergeUnionShardedRouting(
	ctx *plancontext.PlanningContext,
	routeA, routeB *Route,
	exprsA, exprsB []sqlparser.SelectExpr,
	distinct bool,
) (Operator, []sqlparser.SelectExpr) {
	tblA := routeA.Routing.(*ShardedRouting)
	tblB := routeB.Routing.(*ShardedRouting)

	scatterA := tblA.RouteOpCode == engine.Scatter
	scatterB := tblB.RouteOpCode == engine.Scatter

	switch {
	case scatterA:
		return createMergedUnion(ctx, routeA, routeB, exprsA, exprsB, distinct, tblA, nil)

	case scatterB:
		return createMergedUnion(ctx, routeA, routeB, exprsA, exprsB, distinct, tblB, nil)

	case tblA.RouteOpCode == engine.EqualUnique && tblB.RouteOpCode == engine.EqualUnique:
		// If both sides route to the same shard even without the join predicates
		// pushed down from an ApplyJoin above, prefer that routing: it does not
		// depend on join arguments, so the merged route can later be merged with
		// the join producing those arguments. The routing an argument-based merge
		// would have installed is kept as a fallback, so sources routed elsewhere
		// can still merge with this route the way they otherwise would have.
		if canReplayWithoutJoinPredicates(routeA, tblA) && canReplayWithoutJoinPredicates(routeB, tblB) {
			freeA := tblA.withoutJoinPredicates(ctx)
			freeB := tblB.withoutJoinPredicates(ctx)
			if freeA != nil && freeB != nil &&
				(tblA.hasJoinPredicates() || tblB.hasJoinPredicates()) &&
				freeA.RouteOpCode == tblA.RouteOpCode &&
				freeA.RouteOpCode == freeB.RouteOpCode &&
				freeA.SelectedVindex() == freeB.SelectedVindex() {
				equal, conditions := gen4ValuesEqual(ctx, freeA.VindexExpressions(), freeB.VindexExpressions())
				if equal {
					allCond := append(routeA.Conditions, routeB.Conditions...)
					allCond = append(allCond, conditions...)
					op, exprs := createMergedUnion(ctx, routeA, routeB, exprsA, exprsB, distinct, freeA, allCond)
					if route, ok := op.(*Route); ok {
						if fb := unionMergeFallback(ctx, routeA, routeB, tblA, tblB); fb != freeA {
							route.MergeFallback = fb
						}
					}
					return op, exprs
				}
			}
		}
		fallthrough
	case tblA.RouteOpCode == engine.Equal && tblB.RouteOpCode == engine.Equal:
		fallthrough
	case tblA.RouteOpCode == engine.IN && tblB.RouteOpCode == engine.IN:
		aVdx := tblA.SelectedVindex()
		bVdx := tblB.SelectedVindex()
		aExpr := tblA.VindexExpressions()
		bExpr := tblB.VindexExpressions()
		if aVdx == bVdx {
			equal, conditions := gen4ValuesEqual(ctx, aExpr, bExpr)
			if equal {
				allCond := append(routeA.Conditions, routeB.Conditions...)
				allCond = append(allCond, conditions...)
				op, exprs := createMergedUnion(ctx, routeA, routeB, exprsA, exprsB, distinct, tblA, allCond)
				if routeA.MergeFallback != nil || routeB.MergeFallback != nil {
					if route, ok := op.(*Route); ok {
						if fb := unionMergeFallback(ctx, routeA, routeB, tblA, tblB); fb != tblA {
							route.MergeFallback = fb
						}
					}
				}
				return op, exprs
			}
		}

		// One of the sides may have been merged onto a routing that ignores join
		// predicates and carry the argument-based routing it displaced as a
		// fallback. Comparing the fallbacks merges the routes exactly the way the
		// displaced routings would have merged.
		fbA := unionMergeRouting(routeA, tblA)
		fbB := unionMergeRouting(routeB, tblB)
		if (fbA != tblA || fbB != tblB) &&
			fbA.RouteOpCode == fbB.RouteOpCode &&
			fbA.SelectedVindex() == fbB.SelectedVindex() {
			equal, conditions := gen4ValuesEqual(ctx, fbA.VindexExpressions(), fbB.VindexExpressions())
			if equal {
				allCond := append(routeA.Conditions, routeB.Conditions...)
				allCond = append(allCond, conditions...)
				return createMergedUnion(ctx, routeA, routeB, exprsA, exprsB, distinct, fbA, allCond)
			}
		}
	}

	return nil, nil
}

// unionMergeRouting returns the routing to use when comparing this route against
// another union source: the argument-based routing recorded when a merge pinned
// this route onto a join-predicate-free routing, or the given routing itself.
func unionMergeRouting(route *Route, tbl *ShardedRouting) *ShardedRouting {
	if route.MergeFallback != nil {
		return route.MergeFallback
	}
	return tbl
}

// unionMergeFallback computes the MergeFallback for a pair of union sources that
// is about to be merged onto a join-predicate-free routing: the routing an
// argument-based merge would have installed instead. That routing must provably
// cover both sides, so it is only returned when both sides route identically
// under it, with no extra conditions attached to the proof. Sides that already
// carry a fallback contribute that fallback, keeping the recorded routing valid
// for every source of the merged union. Callers skip recording when the result
// is the very routing they are installing (a routing without join predicates
// falls back to itself), so a recorded fallback is always a separate object
// that predicates pushed into the merged route later cannot mutate.
func unionMergeFallback(ctx *plancontext.PlanningContext, routeA, routeB *Route, tblA, tblB *ShardedRouting) *ShardedRouting {
	fbA := unionMergeRouting(routeA, tblA)
	fbB := unionMergeRouting(routeB, tblB)
	if fbA.RouteOpCode != fbB.RouteOpCode || fbA.SelectedVindex() != fbB.SelectedVindex() {
		return nil
	}
	equal, conditions := gen4ValuesEqual(ctx, fbA.VindexExpressions(), fbB.VindexExpressions())
	if !equal || len(conditions) != 0 {
		return nil
	}
	return fbA
}

// canReplayWithoutJoinPredicates reports whether the route's routing can soundly be
// re-derived from its seen predicates alone. Once a merged UNION sits under a route,
// the routing keeps the seen predicates of just one of the union's sources (see
// createMergedUnion), so replaying them could produce a routing that does not cover
// the other sources - such as a shard pin that only one source satisfies. Merging
// several sources on argument equality and then replaying without join predicates
// would collapse differently pinned sources onto one shard and silently lose rows.
// A routing without join predicates is always safe: withoutJoinPredicates returns
// it unchanged, so no re-derivation takes place.
func canReplayWithoutJoinPredicates(route *Route, tr *ShardedRouting) bool {
	return !tr.hasJoinPredicates() || !containsUnion(route.Source)
}

func containsUnion(op Operator) bool {
	if _, ok := op.(*Union); ok {
		return true
	}
	for _, input := range op.Inputs() {
		if containsUnion(input) {
			return true
		}
	}
	return false
}

// createTrivialMergedUnion merges a union pair where one side trivially adopts
// the owning side's routing: the dual, anyShard and none cases. The owning
// route's MergeFallback must survive the merge, so that a later source routed
// elsewhere can still merge with the result through the argument-based routing
// recorded there.
func createTrivialMergedUnion(
	ctx *plancontext.PlanningContext,
	lhsRoute, rhsRoute *Route,
	lhsExprs, rhsExprs []sqlparser.SelectExpr,
	distinct bool,
	routing Routing,
	owner *Route,
) (Operator, []sqlparser.SelectExpr) {
	op, exprs := createMergedUnion(ctx, lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct, routing, nil)
	if route, ok := op.(*Route); ok {
		route.MergeFallback = owner.MergeFallback
	}
	return op, exprs
}

func createMergedUnion(
	ctx *plancontext.PlanningContext,
	lhsRoute, rhsRoute *Route,
	lhsExprs, rhsExprs []sqlparser.SelectExpr,
	distinct bool,
	routing Routing,
	conditions []engine.Condition,
) (Operator, []sqlparser.SelectExpr) {
	// if there are `*` on either side, or a different number of SelectExpr items,
	// we give up aligning the expressions and trust that we can push everything down
	cols := make([]sqlparser.SelectExpr, len(lhsExprs))
	noDeps := len(lhsExprs) != len(rhsExprs)
	for idx, col := range lhsExprs {
		lae, ok := col.(*sqlparser.AliasedExpr)
		if !ok {
			cols[idx] = col
			noDeps = true
			continue
		}
		col := sqlparser.NewColName(lae.ColumnName())
		cols[idx] = aeWrap(col)
		if noDeps {
			continue
		}

		deps := ctx.SemTable.RecursiveDeps(lae.Expr)
		rae, ok := rhsExprs[idx].(*sqlparser.AliasedExpr)
		if !ok {
			noDeps = true
			continue
		}
		deps = deps.Merge(ctx.SemTable.RecursiveDeps(rae.Expr))
		rt, foundR := ctx.TypeForExpr(rae.Expr)
		lt, foundL := ctx.TypeForExpr(lae.Expr)
		if foundR && foundL {
			collations := ctx.VSchema.Environment().CollationEnv()
			var typer evalengine.TypeAggregator

			if err := typer.Add(rt, collations); err != nil {
				panic(err)
			}
			if err := typer.Add(lt, collations); err != nil {
				panic(err)
			}

			ctx.SemTable.ExprTypes[col] = typer.Type()
		}

		ctx.SemTable.Recursive[col] = deps
	}

	exprs := [][]sqlparser.SelectExpr{lhsExprs, rhsExprs}
	union := newUnion([]Operator{lhsRoute.Source, rhsRoute.Source}, exprs, cols, distinct)
	selectExprs := unionSelects(lhsExprs)
	return &Route{
		unaryOperator: newUnaryOp(union),
		MergedWith:    []*Route{rhsRoute},
		Routing:       routing,
		Conditions:    conditions,
	}, selectExprs
}

func compactUnion(u *Union) *ApplyResult {
	if u.distinct {
		// first we remove unnecessary DISTINCTs
		for idx, source := range u.Sources {
			d, ok := source.(*Distinct)
			if !ok || !d.Required {
				continue
			}
			u.Sources[idx] = d.Source
		}
	}

	var newSources []Operator
	var newSelects [][]sqlparser.SelectExpr
	merged := false

	for idx, source := range u.Sources {
		other, ok := source.(*Union)

		if ok && (u.distinct || !other.distinct) {
			newSources = append(newSources, other.Sources...)
			newSelects = append(newSelects, other.Selects...)
			merged = true
			continue
		}

		newSources = append(newSources, source)
		newSelects = append(newSelects, u.Selects[idx])
	}

	if !merged {
		return NoRewrite
	}

	u.Sources = newSources
	u.Selects = newSelects
	return Rewrote("merged UNIONs")
}
