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
	"io"
	"slices"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
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
	lhsRoute, rhsRoute := operatorsToRoutes(lhs, rhs)
	if lhsRoute == nil || rhsRoute == nil {
		checkCrossKeyspaceOp(ctx, lhs, rhs, "UNION")
		return nil, nil
	}

	if op, exprs := mergeUnionRoutings(ctx, lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct); op != nil {
		return op, exprs
	}
	if op, exprs := tryAlternateUnionMerge(ctx, lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct); op != nil {
		return op, exprs
	}

	// Check cross-keyspace restrictions for UNIONs that cannot be merged.
	checkCrossKeyspaceOp(ctx, lhs, rhs, "UNION")

	return nil, nil
}

// mergeUnionRoutings merges two union sources whose routes are usable as they
// stand, or returns nil when their routings do not allow it.
func mergeUnionRoutings(
	ctx *plancontext.PlanningContext,
	lhsRoute, rhsRoute *Route,
	lhsExprs, rhsExprs []sqlparser.SelectExpr,
	distinct bool,
) (Operator, []sqlparser.SelectExpr) {
	routingA, routingB := lhsRoute.Routing, rhsRoute.Routing
	sameKeyspace := routingA.Keyspace() == routingB.Keyspace()
	a, b := getRoutingType(routingA), getRoutingType(routingB)

	// a none routing resolves to no shards at execution time, so its side of the
	// union contributes no rows. None pairings must be decided before the dual
	// and anyShard cases below: those would retain the none routing and
	// incorrectly discard the other side's rows.
	if a == none || b == none {
		op, exprs, _ := tryMergeNoneUnion(ctx, lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct, routingA, routingB, a, b)
		return op, exprs
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

// tryAlternateUnionMerge retries a declined cross-keyspace pairing by moving
// one side onto the reference copies of its tables in the other side's
// keyspace. The move mutates the moved side's tables in place, so all
// semantic bookkeeping keyed on its expressions stays valid; a retry that
// still cannot merge undoes the move.
func tryAlternateUnionMerge(
	ctx *plancontext.PlanningContext,
	lhsRoute, rhsRoute *Route,
	lhsExprs, rhsExprs []sqlparser.SelectExpr,
	distinct bool,
) (Operator, []sqlparser.SelectExpr) {
	lhsKs, rhsKs := lhsRoute.Routing.Keyspace(), rhsRoute.Routing.Keyspace()
	if lhsKs == nil || rhsKs == nil || lhsKs == rhsKs {
		return nil, nil
	}

	// a side already composed of a merged union carries shapes the remaining
	// phases no longer normalize: moving it can hand offset planning a nested
	// horizon it cannot push into, so only leaf routes are moved
	canRewrite := func(route *Route) bool {
		hasUnion := false
		_ = Visit(route.Source, func(op Operator) error {
			if _, ok := op.(*Union); ok {
				hasUnion = true
				return io.EOF
			}
			return nil
		})
		return !hasUnion
	}

	if canRewrite(lhsRoute) {
		if rewritten, undo := rewriteRouteToAlternate(ctx, lhsRoute, rhsKs); rewritten != nil {
			if op, exprs := mergeUnionRoutings(ctx, rewritten, rhsRoute, lhsExprs, rhsExprs, distinct); op != nil {
				return op, exprs
			}
			undo()
		}
	}
	if canRewrite(rhsRoute) {
		if rewritten, undo := rewriteRouteToAlternate(ctx, rhsRoute, lhsKs); rewritten != nil {
			if op, exprs := mergeUnionRoutings(ctx, lhsRoute, rewritten, lhsExprs, rhsExprs, distinct); op != nil {
				return op, exprs
			}
			undo()
		}
	}
	return nil, nil
}

// rewriteRouteToAlternate points route's tables at the reference copies living
// in ks, or returns nil if the route cannot move there. Every real table under
// the planned tree is resolved to its copy independently, so a route composed
// by earlier merges moves when each of its tables has a copy. The planned
// operators are kept and only the table nodes are swapped, so predicates and
// projections pushed after route creation are preserved and the semantic
// analysis of the tree stays authoritative. The returned undo puts the
// original tables back. Reference and unsharded copies are complete on any
// shard; a single ordinary sharded source keeps its resolved routing.
func rewriteRouteToAlternate(ctx *plancontext.PlanningContext, route *Route, ks *vindexes.Keyspace) (*Route, func()) {
	if _, ok := route.Routing.(*AnyShardRouting); !ok {
		return nil, nil
	}
	if !ctx.SemTable.DMLTargets.IsEmpty() && TableID(route).IsOverlapping(ctx.SemTable.DMLTargets) {
		return nil, nil
	}

	type tableSwap struct {
		tbl        *Table
		altQTable  *QueryTable
		altVTable  *vindexes.BaseTable
		origQTable *QueryTable
		origVTable *vindexes.BaseTable
	}
	var swaps []tableSwap
	var shardedRouting *ShardedRouting
	resolvable := true
	_ = Visit(route.Source, func(op Operator) error {
		tbl, ok := op.(*Table)
		if !ok || tbl.VTable == nil || tbl.QTable == nil {
			return nil
		}
		alt, altRouting := resolveTableCopyIn(ctx, tbl, ks)
		if alt == nil {
			resolvable = false
			return io.EOF
		}
		if alt.VTable.Type != vindexes.TypeReference && alt.VTable.Keyspace.Sharded {
			var ok bool
			shardedRouting, ok = altRouting.(*ShardedRouting)
			if !ok || shardedRouting.Keyspace() != ks {
				resolvable = false
				return io.EOF
			}
		}
		swaps = append(swaps, tableSwap{tbl: tbl, altQTable: alt.QTable, altVTable: alt.VTable, origQTable: tbl.QTable, origVTable: tbl.VTable})
		return nil
	})
	if !resolvable || len(swaps) == 0 ||
		(shardedRouting != nil && (TableID(route).NumberOfTables() != 1 || len(swaps) != 1)) {
		return nil, nil
	}

	for _, s := range swaps {
		s.tbl.QTable, s.tbl.VTable = s.altQTable, s.altVTable
	}
	undo := func() {
		for _, s := range swaps {
			s.tbl.QTable, s.tbl.VTable = s.origQTable, s.origVTable
		}
	}

	rewritten := *route
	if shardedRouting != nil {
		rewritten.Routing = shardedRouting
	} else {
		rewritten.Routing = &AnyShardRouting{keyspace: ks}
	}
	return &rewritten, undo
}

// resolveTableCopyIn resolves the physical copy of tbl's table living in ks:
// the table itself, a reference copy from ReferencedBy, or a declared reference
// source. Candidates are looked up through the planning VSchema and accepted
// only when they resolve in ks. The returned table carries the physical name
// with the original name preserved as an alias, along with its routing.
func resolveTableCopyIn(ctx *plancontext.PlanningContext, tbl *Table, ks *vindexes.Keyspace) (*Table, Routing) {
	for _, name := range copyCandidates(tbl.VTable, ks) {
		src, _, _, _, _, err := ctx.VSchema.FindTableOrVindex(name)
		if err != nil || src == nil || src.Keyspace != ks {
			continue
		}
		altRoute := findVSchemaTableAndCreateRoute(ctx, tbl.QTable, name, false /*planAlternates*/)
		altTbl, ok := altRoute.Source.(*Table)
		if !ok || altTbl.VTable == nil || altTbl.VTable.Keyspace != ks {
			continue
		}
		return altTbl, altRoute.Routing
	}
	return nil, nil
}

// copyCandidates lists the declared names under which vt's data may also live
// in ks. A reference source is returned exactly as declared.
func copyCandidates(vt *vindexes.BaseTable, ks *vindexes.Keyspace) []sqlparser.TableName {
	var candidates []sqlparser.TableName
	if vt.Keyspace == ks {
		candidates = append(candidates, sqlparser.TableName{Name: vt.Name, Qualifier: sqlparser.NewIdentifierCS(ks.Name)})
	}
	if ref, found := vt.ReferencedBy[ks.Name]; found {
		candidates = append(candidates, sqlparser.TableName{Name: ref.Name, Qualifier: sqlparser.NewIdentifierCS(ks.Name)})
	}
	if vt.Source != nil {
		candidates = append(candidates, vt.Source.TableName)
	}
	return candidates
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
