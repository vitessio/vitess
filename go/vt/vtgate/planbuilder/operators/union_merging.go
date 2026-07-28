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
	"slices"

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

	// a none routing resolves to no shards at execution time, so its side of the
	// union contributes no rows. None pairings must be decided before the dual
	// and anyShard cases below: those would retain the none routing and
	// incorrectly discard the other side's rows.
	if a == none || b == none {
		if op, exprs, merged := tryMergeNoneUnion(ctx, lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct, routingA, routingB, a, b); merged {
			return op, exprs
		}
		checkCrossKeyspaceOp(ctx, lhs, rhs, "UNION")
		return nil, nil
	}

	switch {
	// if either side is a dual query, we can always merge them together
	// an unsharded/reference route can be merged with anything going to that keyspace
	case b == dual || (b == anyShard && sameKeyspace):
		return createMergedUnion(ctx, lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct, routingA, nil)
	case a == dual || (a == anyShard && sameKeyspace):
		return createMergedUnion(ctx, lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct, routingB, nil)

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

// tryMergeNoneUnion merges a union pairing in which at least one side has a
// none routing. The none side contributes no rows, but its query text (and the
// field query derived from it) still references its tables, so a merge may only
// adopt a routing whose keyspace holds every real table of the none side.
func tryMergeNoneUnion(
	ctx *plancontext.PlanningContext,
	lhsRoute, rhsRoute *Route,
	lhsExprs, rhsExprs []sqlparser.SelectExpr,
	distinct bool,
	routingA, routingB Routing,
	a, b routingType,
) (Operator, []sqlparser.SelectExpr, bool) {
	otherRouting, otherType := routingB, b
	noneRoute, otherRoute := lhsRoute, rhsRoute
	if a != none {
		otherRouting, otherType = routingA, a
		noneRoute, otherRoute = rhsRoute, lhsRoute
	}
	noneKeyspaces := realTableKeyspaces(noneRoute.Source)

	var routing Routing
	switch {
	case otherType == none:
		// both sides are empty. They can collapse into a single none route as
		// long as their combined real tables live in one keyspace: the merged
		// route's field query must be executable on a shard of that keyspace.
		for _, ks := range realTableKeyspaces(otherRoute.Source) {
			if !slices.Contains(noneKeyspaces, ks) {
				noneKeyspaces = append(noneKeyspaces, ks)
			}
		}
		switch len(noneKeyspaces) {
		case 0:
			routing = noneRoute.Routing
		case 1:
			routing = &NoneRouting{keyspace: noneKeyspaces[0]}
		default:
			return nil, nil, false
		}
	case hasInfoSchemaTables(noneRoute.Source):
		// an information_schema comparison may have been rewritten to a
		// planner-generated argument (e.g. :__vtschemaname) that only a DBA
		// route binds. Adopting an executable routing would ship that argument
		// to a shard with no binding for it, so this branch must stay separate.
		return nil, nil, false
	case len(noneKeyspaces) == 0:
		// the none side references no real tables, so its keyspace is only a
		// placeholder: adopt the other side's routing unconditionally.
		routing = otherRouting
	case otherType == dual:
		// a dual side has no keyspace and no tables of its own, so any shard
		// in the none side's single keyspace can produce its rows.
		if len(noneKeyspaces) != 1 {
			return nil, nil, false
		}
		routing = &AnyShardRouting{keyspace: noneKeyspaces[0]}
	default:
		// the other side's routing is retained, but only when it targets the
		// keyspace holding the none side's tables: they exist nowhere else.
		if len(noneKeyspaces) != 1 || noneKeyspaces[0] != otherRouting.Keyspace() {
			return nil, nil, false
		}
		routing = otherRouting
	}

	op, exprs := createMergedUnion(ctx, lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct, routing, nil)
	return op, exprs, true
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
				return createMergedUnion(ctx, routeA, routeB, exprsA, exprsB, distinct, tblA, allCond)
			}
		}
	}

	return nil, nil
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
	merged := &Route{
		unaryOperator: newUnaryOp(union),
		MergedWith:    []*Route{rhsRoute},
		Routing:       routing,
		Conditions:    conditions,
	}
	if !merged.inheritFrom(lhsRoute, rhsRoute) {
		debugNoRewrite("union merge blocked: %s routing would widen a route that has to stay single-shard", routing.OpCode().String())
		return nil, nil
	}

	return merged, selectExprs
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
