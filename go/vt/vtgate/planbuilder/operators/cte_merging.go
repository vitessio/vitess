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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/predicates"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func tryMergeRecurse(ctx *plancontext.PlanningContext, in *RecurseCTE) (Operator, *ApplyResult) {
	if pushedIntoDerivedTable(ctx, in) {
		return in, NoRewrite
	}

	op := tryMergeCTE(ctx, in.Seed(), in.Term(), in)
	if op == nil {
		return in, NoRewrite
	}

	return op, Rewrote("Merged CTE")
}

// pushedIntoDerivedTable reports whether a recursion predicate was rewritten into
// a derived table of the term, where its original columns are out of scope. Its
// current form then reads columns, but none from the tables its original reads.
func pushedIntoDerivedTable(ctx *plancontext.PlanningContext, in *RecurseCTE) bool {
	for _, pred := range in.Predicates {
		jp, ok := pred.RightExpr.(*predicates.JoinPredicate)
		if !ok || nothingNeedsFetching(ctx, jp.Current()) {
			continue
		}
		tables := ctx.SemTable.DirectDeps(pred.Original).Remove(in.LeftID)
		if !ctx.SemTable.DirectDeps(jp.Current()).IsOverlapping(tables) {
			return true
		}
	}
	return false
}

func tryMergeCTE(ctx *plancontext.PlanningContext, seed, term Operator, in *RecurseCTE) *Route {
	seedRoute, termRoute, routingA, routingB, a, b, sameKeyspace := prepareInputRoutes(ctx, seed, term)
	if seedRoute == nil || routesOnRecursion(routingB, in) {
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

// routesOnRecursion reports whether r routes on an argument that only the
// RecurseCTE binds, which a merged route would leave unbound
func routesOnRecursion(r Routing, in *RecurseCTE) bool {
	tr, ok := r.(*ShardedRouting)
	if !ok {
		return false
	}
	found := false
	for _, expr := range tr.VindexExpressions() {
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
			arg, ok := node.(*sqlparser.Argument)
			if !ok {
				return true, nil
			}
			for _, pred := range in.Predicates {
				if slices.ContainsFunc(pred.LeftExprs, func(bve plancontext.BindVarExpr) bool { return bve.Name == arg.Name }) {
					found = true
					return false, io.EOF
				}
			}
			return true, nil
		}, expr)
	}
	return found
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
		Routing:       r,
		unaryOperator: newUnaryOp(cte),
		MergedWith:    []*Route{term},
		Conditions:    conditions,
	}
}
