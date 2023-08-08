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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// mergeUnionInputInAnyOrder merges sources the sources of the union in any order
// can be used for UNION DISTINCT
func mergeUnionInputInAnyOrder(ctx *plancontext.PlanningContext, op *Union) ([]ops.Operator, []sqlparser.SelectExprs, error) {
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
			newPlan, sel, err := mergeUnionInputs(ctx, srcA, srcB, selA, selB, op.distinct)
			if err != nil {
				return nil, nil, err
			}
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
			return sources, selects, nil
		}

		var newSources []ops.Operator
		var newSelects []sqlparser.SelectExprs
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

	return sources, selects, nil
}

func mergeUnionInputsInOrder(ctx *plancontext.PlanningContext, op *Union) ([]ops.Operator, []sqlparser.SelectExprs, error) {
	sources := op.Sources
	selects := op.Selects
	for {
		merged := false
		for i := 0; i < len(sources)-1; i++ {
			j := i + 1
			srcA, selA := sources[i], selects[i]
			srcB, selB := sources[j], selects[j]
			newPlan, sel, err := mergeUnionInputs(ctx, srcA, srcB, selA, selB, op.distinct)
			if err != nil {
				return nil, nil, err
			}
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

	return sources, selects, nil
}

// mergeUnionInputs checks whether two operators can be merged into a single one.
// If they can be merged, a new operator with the merged routing is returned
// If they cannot be merged, nil is returned.
// this function is very similar to mergeJoinInputs
func mergeUnionInputs(
	ctx *plancontext.PlanningContext,
	lhs, rhs ops.Operator,
	lhsExprs, rhsExprs sqlparser.SelectExprs,
	distinct bool,
) (ops.Operator, sqlparser.SelectExprs, error) {
	lhsRoute, rhsRoute, routingA, routingB, a, b, sameKeyspace := prepareInputRoutes(lhs, rhs)
	if lhsRoute == nil {
		return nil, nil, nil
	}

	switch {
	// if either side is a dual query, we can always merge them together
	// an unsharded/reference route can be merged with anything going to that keyspace
	case b == dual || (b == anyShard && sameKeyspace):
		return createMergedUnion(ctx, lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct, routingA)
	case a == dual || (a == anyShard && sameKeyspace):
		return createMergedUnion(ctx, lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct, routingB)

	case a == none:
		return createMergedUnion(ctx, lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct, routingB)
	case b == none:
		return createMergedUnion(ctx, lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct, routingA)

	case a == sharded && b == sharded && sameKeyspace:
		res, exprs, err := tryMergeUnionShardedRouting(ctx, lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct)
		if err != nil || res != nil {
			return res, exprs, err
		}
	}
	return nil, nil, nil
}

func tryMergeUnionShardedRouting(
	ctx *plancontext.PlanningContext,
	routeA, routeB *Route,
	exprsA, exprsB sqlparser.SelectExprs,
	distinct bool,
) (ops.Operator, sqlparser.SelectExprs, error) {
	tblA := routeA.Routing.(*ShardedRouting)
	tblB := routeB.Routing.(*ShardedRouting)

	scatterA := tblA.RouteOpCode == engine.Scatter
	scatterB := tblB.RouteOpCode == engine.Scatter
	uniqueA := tblA.RouteOpCode == engine.EqualUnique
	uniqueB := tblB.RouteOpCode == engine.EqualUnique

	switch {
	case scatterA:
		return createMergedUnion(ctx, routeA, routeB, exprsA, exprsB, distinct, tblA)

	case scatterB:
		return createMergedUnion(ctx, routeA, routeB, exprsA, exprsB, distinct, tblB)

	case uniqueA && uniqueB:
		aVdx := tblA.SelectedVindex()
		bVdx := tblB.SelectedVindex()
		aExpr := tblA.VindexExpressions()
		bExpr := tblB.VindexExpressions()
		if aVdx == bVdx && gen4ValuesEqual(ctx, aExpr, bExpr) {
			return createMergedUnion(ctx, routeA, routeB, exprsA, exprsB, distinct, tblA)
		}
	}

	return nil, nil, nil
}

func createMergedUnion(
	ctx *plancontext.PlanningContext,
	lhsRoute, rhsRoute *Route,
	lhsExprs, rhsExprs sqlparser.SelectExprs,
	distinct bool,
	routing Routing) (ops.Operator, sqlparser.SelectExprs, error) {

	// if there are `*` on either side, or a different number of SelectExpr items,
	// we give up aligning the expressions and trust that we can push everything down
	cols := make(sqlparser.SelectExprs, len(lhsExprs))
	noDeps := len(lhsExprs) != len(rhsExprs)
	for idx, col := range lhsExprs {
		ae, ok := col.(*sqlparser.AliasedExpr)
		if !ok {
			cols[idx] = col
			noDeps = true
			continue
		}
		col := sqlparser.NewColName(ae.ColumnName())
		cols[idx] = aeWrap(col)
		if noDeps {
			continue
		}

		deps := ctx.SemTable.RecursiveDeps(ae.Expr)
		ae, ok = rhsExprs[idx].(*sqlparser.AliasedExpr)
		if !ok {
			noDeps = true
			continue
		}
		deps = deps.Merge(ctx.SemTable.RecursiveDeps(ae.Expr))
		ctx.SemTable.Recursive[col] = deps
	}

	union := newUnion([]ops.Operator{lhsRoute.Source, rhsRoute.Source}, []sqlparser.SelectExprs{lhsExprs, rhsExprs}, cols, distinct)
	selectExprs := unionSelects(lhsExprs)
	return &Route{
		Source:     union,
		MergedWith: []*Route{rhsRoute},
		Routing:    routing,
	}, selectExprs, nil
}

func compactUnion(u *Union) *rewrite.ApplyResult {
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

	var newSources []ops.Operator
	var newSelects []sqlparser.SelectExprs
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
		return rewrite.SameTree
	}

	u.Sources = newSources
	u.Selects = newSelects
	return rewrite.NewTree("merged UNIONs", u)
}
