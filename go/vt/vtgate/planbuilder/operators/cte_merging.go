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
	op := tryMergeCTE(ctx, in.Seed, in.Term, in)
	if op == nil {
		return in, NoRewrite
	}

	return op, Rewrote("Merged CTE")
}

func tryMergeCTE(ctx *plancontext.PlanningContext, seed, term Operator, in *RecurseCTE) *Route {
	seedRoute, termRoute, routingA, routingB, a, b, sameKeyspace := prepareInputRoutes(seed, term)
	if seedRoute == nil {
		return nil
	}

	switch {
	case a == dual:
		return mergeCTE(ctx, seedRoute, termRoute, routingB, in)
	case b == dual:
		return mergeCTE(ctx, seedRoute, termRoute, routingA, in)
	case !sameKeyspace:
		return nil
	case a == anyShard:
		return mergeCTE(ctx, seedRoute, termRoute, routingB, in)
	case b == anyShard:
		return mergeCTE(ctx, seedRoute, termRoute, routingA, in)
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
			if aVdx == bVdx && gen4ValuesEqual(ctx, aExpr, bExpr) {
				return mergeCTE(ctx, seed, term, tblA, in)
			}
		}
	}

	return nil
}

func mergeCTE(ctx *plancontext.PlanningContext, seed, term *Route, r Routing, in *RecurseCTE) *Route {
	in.Def.Merged = true
	hz := in.Horizon
	hz.Source = term.Source
	newTerm, _ := expandHorizon(ctx, hz)
	return &Route{
		Routing: r,
		Source: &RecurseCTE{
			Predicates: in.Predicates,
			Def:        in.Def,
			Seed:       seed.Source,
			Term:       newTerm,
			LeftID:     in.LeftID,
			OuterID:    in.OuterID,
			Distinct:   in.Distinct,
		},
		MergedWith: []*Route{term},
	}
}
