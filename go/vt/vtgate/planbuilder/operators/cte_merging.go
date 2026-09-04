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
	"io"
	"slices"

	"vitess.io/vitess/go/vt/sqlparser"
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
	preserved, canMerge := referenceRowsInvariant(r, false, seed, term)
	if preserved && routedByTheRecursion(r, in) {
		canMerge = false
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

// routedByTheRecursion reports whether this routing was selected by a predicate carrying one of the
// recursion's bind variables. mergeCTE restores those predicates to their cross-table shape and
// nothing recomputes the routing afterwards, so a route that is single-shard because of one is not
// single-shard at all once it is merged. Rows that a multi-shard route would duplicate do not get
// to rely on that. Any other predicate - a literal in the term, the seed's own routing - does not
// come and go that way and is taken at face value.
func routedByTheRecursion(r Routing, in *RecurseCTE) bool {
	sharded, ok := r.(*ShardedRouting)
	if !ok || sharded.Selected == nil {
		return false
	}

	fromRecursion := func(name string) bool {
		return slices.ContainsFunc(in.Predicates, func(recursed *plancontext.RecurseExpression) bool {
			return slices.ContainsFunc(recursed.LeftExprs, func(bve plancontext.BindVarExpr) bool {
				return bve.Name == name
			})
		})
	}

	for _, predicate := range sharded.Selected.Predicates {
		found := false
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
			if arg, ok := node.(*sqlparser.Argument); ok && fromRecursion(arg.Name) {
				found = true
				return false, io.EOF
			}
			return true, nil
		}, predicate)
		if found {
			return true
		}
	}

	return false
}
