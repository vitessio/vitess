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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func tryMergeRecurse(ctx *plancontext.PlanningContext, in *RecurseCTE) (Operator, *ApplyResult) {
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

	// The routing the merge keeps comes from whichever side is not the dual or reference one, so
	// half of these take it from the recursive term. mergeCTE has to know: only the term's routing
	// is rebuilt from a predicate that gets restored afterwards.
	switch {
	case a == dual:
		return mergeCTE(ctx, seedRoute, termRoute, routingB, true, in, nil)
	case b == dual:
		return mergeCTE(ctx, seedRoute, termRoute, routingA, false, in, nil)
	case !sameKeyspace:
		return nil
	case a == anyShard:
		return mergeCTE(ctx, seedRoute, termRoute, routingB, true, in, nil)
	case b == anyShard:
		return mergeCTE(ctx, seedRoute, termRoute, routingA, false, in, nil)
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
					return mergeCTE(ctx, seed, term, tblA, false, in, conditions)
				}
			}
		}
	}

	return nil
}

func mergeCTE(ctx *plancontext.PlanningContext, seed, term *Route, r Routing, routingFromTerm bool, in *RecurseCTE, conditions []engine.Condition) *Route {
	preserved, canMerge := referenceRowsInvariant(r, false, seed, term)
	if preserved && routingFromTerm {
		// The term's routing can be single-shard only because of the recursion's bind predicate,
		// which the loop below restores to its cross-table shape, and nothing recomputes the routing
		// afterwards. Rows that a multi-shard route would duplicate do not get to rely on that.
		// The seed's own routing does not come and go that way, so it is taken at face value.
		_, sharded := r.(*ShardedRouting)
		canMerge = canMerge && !sharded
	}
	if !canMerge {
		debugNoRewrite("CTE merge blocked: %s routing cannot honour a route that has to stay single-shard", r.OpCode().String())
		return nil
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

	cte := &RecurseCTE{
		binaryOperator: newBinaryOp(seed.Source, newTerm),
		Predicates:     in.Predicates,
		Def:            in.Def,
		LeftID:         in.LeftID,
		OuterID:        in.OuterID,
		Distinct:       in.Distinct,
	}
	return &Route{
		Routing:                r,
		unaryOperator:          newUnaryOp(cte),
		MergedWith:             []*Route{term},
		Conditions:             conditions,
		PreservesReferenceRows: preserved,
	}
}
