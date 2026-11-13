/*
Copyright 2024 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/predicates"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func tryMergeRecurse(ctx *plancontext.PlanningContext, in *RecurseCTE) (Operator, *ApplyResult) {
	if !reachedPhase(ctx, initialPlanning) {
		seed, changed := useRecursiveCTESeedAlternates(ctx, in.Seed(), in.Term())
		if changed != NoRewrite {
			in.LHS = seed
			return in, changed
		}
	}
	op := tryMergeCTE(ctx, in.Seed(), in.Term(), in)
	if op == nil {
		return in, NoRewrite
	}

	return op, Rewrote("Merged CTE")
}

func tryMergeCTE(ctx *plancontext.PlanningContext, seed, term Operator, in *RecurseCTE) *Route {
	seedRoute, termRoute, routingA, routingB, a, b, sameKeyspace := prepareInputRoutes(ctx, seed, term)
	if seedRoute == nil {
		return nil
	}

	switch {
	case a == dual:
		return mergeCTE(ctx, seedRoute, termRoute, routingB, in, nil)
	case b == dual:
		return mergeCTE(ctx, seedRoute, termRoute, routingA, in, nil)
	case !sameKeyspace:
		return nil
	case a == anyShard:
		return mergeCTE(ctx, seedRoute, termRoute, routingB, in, nil)
	case b == anyShard:
		return mergeCTE(ctx, seedRoute, termRoute, routingA, in, nil)
	case a == sharded && b == sharded:
		return tryMergeCTESharded(ctx, seedRoute, termRoute, in)
	default:
		return nil
	}
}

func tryMergeCTESharded(ctx *plancontext.PlanningContext, seed, term *Route, in *RecurseCTE) *Route {
	tblA := seed.Routing.(*ShardedRouting)
	tblB := term.Routing.(*ShardedRouting)
	switch tblA.RouteOpCode {
	case engine.EqualUnique:
		// If the two routes fully match, they can be merged together.
		if tblB.RouteOpCode == engine.EqualUnique {
			aVdx := tblA.SelectedVindex()
			bVdx := tblB.SelectedVindex()
			aExpr := tblA.VindexExpressions()
			bExpr := tblB.VindexExpressions()
			if aVdx == bVdx {
				equal, conditions := gen4ValuesEqual(ctx, aExpr, bExpr)
				if equal {
					return mergeCTE(ctx, seed, term, tblA, in, conditions)
				}
			}
		}
	}

	return nil
}

func mergeCTE(ctx *plancontext.PlanningContext, seed, term *Route, r Routing, in *RecurseCTE, conditions []engine.Condition) *Route {
	// Evaluate routing with the original recursive predicates on a copy before
	// committing the merge. A multi-shard route would repeat a replicated seed,
	// including DISTINCT recursion that only deduplicates within each shard.
	if routeContainsSpecialUnionInput(seed) {
		candidate := r.Clone()
		if routing, ok := candidate.(*ShardedRouting); ok {
			for i, expr := range routing.SeenPredicates {
				join, ok := expr.(*predicates.JoinPredicate)
				if !ok {
					continue
				}
				for _, predicate := range in.Predicates {
					if predicate.JoinPredicateID != nil && *predicate.JoinPredicateID == join.ID {
						routing.SeenPredicates[i] = predicate.Original
						break
					}
				}
			}
		}
		candidate = candidate.resetRoutingLogic(ctx)
		if !candidate.OpCode().IsSingleShard() {
			in.SeedMergeRejected = true
			return nil
		}
	}

	in.Def.Merged = true
	hz := in.Horizon
	hz.Source = term.Source
	newTerm, _ := expandHorizon(ctx, hz)
	for _, predicate := range in.Predicates {
		if predicate.JoinPredicateID != nil {
			ctx.PredTracker.Set(*predicate.JoinPredicateID, predicate.Original)
		}
	}

	// Accepted merges retain tracked predicates for subsequent routing rewrites.
	r = r.Clone().resetRoutingLogic(ctx)

	cte := &RecurseCTE{
		binaryOperator: newBinaryOp(seed.Source, newTerm),
		Predicates:     in.Predicates,
		Def:            in.Def,
		LeftID:         in.LeftID,
		OuterID:        in.OuterID,
		Distinct:       in.Distinct,
	}
	return &Route{
		Routing:                   r,
		unaryOperator:             newUnaryOp(cte),
		MergedWith:                []*Route{term},
		ContainsSpecialUnionInput: routeContainsSpecialUnionInput(seed) || routeContainsSpecialUnionInput(term),
		Conditions:                conditions,
	}
}

// Select copies while the seed still has its original Horizon. Replacing a
// planned seed route later would discard its projected CTE output columns.
func useRecursiveCTESeedAlternates(ctx *plancontext.PlanningContext, seed, term Operator) (Operator, *ApplyResult) {
	keyspaces := operatorKeyspaces(term)
	if len(keyspaces) != 1 {
		return seed, NoRewrite
	}
	return topDown(seed, semantics.EmptyTableSet(), TableID, func(op Operator, _ semantics.TableSet, _ bool) (Operator, *ApplyResult) {
		route, ok := op.(*Route)
		if !ok || TableID(route).IsOverlapping(ctx.SemTable.DMLTargets) || !canReplaceCTESeed(route.Source) {
			return op, NoRewrite
		}
		routing, ok := route.Routing.(*AnyShardRouting)
		if !ok {
			return op, NoRewrite
		}
		alternate := routing.AlternateInKeyspace(keyspaces[0])
		if alternate == nil {
			return op, NoRewrite
		}
		return Clone(alternate), Rewrote("select recursive CTE seed reference copy")
	}, stopAtRoute, true)
}

// Alternates at this phase contain table and join planning, not the projected
// outputs of a derived seed. Do not discard a projection when selecting a copy.
func canReplaceCTESeed(op Operator) bool {
	switch op := op.(type) {
	case *Table:
		return true
	case *ApplyJoin:
		return canReplaceCTESeed(op.LHS) && canReplaceCTESeed(op.RHS)
	default:
		return false
	}
}
