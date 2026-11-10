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
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// mergeJoinInputs checks whether two operators can be merged into a single one.
// If they can be merged, a new operator with the merged routing is returned
// If they cannot be merged, nil is returned.
func (jm *joinMerger) mergeJoinInputs(ctx *plancontext.PlanningContext, lhs, rhs Operator) *Route {
	lhsRoute, rhsRoute, routingA, routingB, a, b, sameKeyspace := prepareInputRoutes(ctx, lhs, rhs)
	if lhsRoute == nil {
		debugNoRewrite("apply join merge blocked: LHS or RHS is not a Route")
		return nil
	}

	switch {
	// We clone the right hand side and try and push all the join predicates that are solved entirely by that side.
	// If a dual is on the left side, and it is a left join (all right joins are changed to left joins), then we can only merge if the right side is a single sharded routing.
	case a == dual:
		newRouting := rhsRoute.Routing.Clone()

		rhsID := TableID(rhsRoute)
		for _, predicate := range jm.predicates {
			if ctx.SemTable.DirectDeps(predicate).IsSolvedBy(rhsID) {
				newRouting = UpdateRoutingLogic(ctx, predicate, newRouting)
			}
		}

		if !jm.joinType.IsInner() && !newRouting.OpCode().IsSingleShard() {
			debugNoRewrite("apply join merge blocked: dual routing with non-inner join type %s and multi-shard routing %s", jm.joinType.ToString(), newRouting.OpCode().String())
			return nil
		}
		return jm.merge(ctx, lhsRoute, rhsRoute, newRouting)

	// If a dual is on the right side.
	case b == dual:
		return jm.merge(ctx, lhsRoute, rhsRoute, routingA)

	// As both are reference route. We need to merge the alternates as well.
	case a == anyShard && b == anyShard && sameKeyspace:
		newrouting := mergeAnyShardRoutings(ctx, routingA.(*AnyShardRouting), routingB.(*AnyShardRouting), jm.predicates, jm.joinType)
		return jm.merge(ctx, lhsRoute, rhsRoute, newrouting)

	// an unsharded/reference route can be merged with anything going to that keyspace
	case a == anyShard && sameKeyspace:
		return jm.merge(ctx, lhsRoute, rhsRoute, routingB)
	case b == anyShard && sameKeyspace:
		return jm.merge(ctx, lhsRoute, rhsRoute, routingA)

	// None routing can always be merged, as long as we are aiming for the same keyspace
	case a == none && sameKeyspace:
		return jm.merge(ctx, lhsRoute, rhsRoute, routingA)
	case b == none && sameKeyspace:
		return jm.merge(ctx, lhsRoute, rhsRoute, routingB)

	// infoSchema routing is complex, so we handle it in a separate method
	case a == infoSchema && b == infoSchema:
		result := tryMergeInfoSchemaRoutings(ctx, routingA, routingB, jm, lhsRoute, rhsRoute)
		if result == nil {
			debugNoRewrite("apply join merge blocked: info schema routing merge failed")
		}
		return result

	// sharded routing is complex, so we handle it in a separate method
	case a == sharded && b == sharded:
		result := tryMergeShardedRouting(ctx, lhsRoute, rhsRoute, jm, jm.predicates)
		if result == nil {
			debugNoRewrite("apply join merge blocked: sharded routing merge failed (different keyspaces or incompatible vindex predicates)")
		}
		return result

	default:
		if !sameKeyspace {
			debugNoRewrite("apply join merge blocked: routes target different keyspaces (LHS: %s %s, RHS: %s %s)",
				a.String(), getKeyspaceName(routingA), b.String(), getKeyspaceName(routingB))
		} else {
			debugNoRewrite("apply join merge blocked: incompatible routing types (LHS: %s, RHS: %s)", a.String(), b.String())
		}
		return nil
	}
}

// owesReferenceRows reports whether a merge of these two routes has to produce the rows that an
// outer join preserves on a reference side exactly once. A reference table has the same rows on
// every shard, so a route that reads more than one returns each unmatched preserved row once per
// shard. Checking the opcode leaves unsharded routes out, and the routing type leaves out dual,
// which keeps its own merge rule from main.
func owesReferenceRows(joinType sqlparser.JoinType, preserved *Route) bool {
	return !joinType.IsInner() && canCarryReferenceRows(preserved.Routing)
}

// isDirectDMLRowSource reports whether a merge of these routes is the row source of a multi-table
// DML sent to the shards as it is, rather than something that reads it. Nothing returns the
// preserved rows then. When only the other side is written, an unmatched preserved row has no row
// to write; a write to the reference table itself is decided by each shard on its own, as on main.
// A target among these routes is what says the DML writes through this join rather than reading it from
// further down, where an outer join in a subquery does have its rows read. A DML that is known to
// read its rows first, through a SELECT, has no row source. findDMLAboveRoute can still turn a DML
// into one that reads its rows after the joins are planned, and that SELECT keeps the exemption.
func isDirectDMLRowSource(ctx *plancontext.PlanningContext, routes ...*Route) bool {
	targets := ctx.SemTable.DMLTargets
	if ctx.DMLReadsItsRows {
		return false
	}

	var id semantics.TableSet
	for _, route := range routes {
		id = id.Merge(TableID(route))
	}
	return id.IsOverlapping(targets)
}

func mergeAnyShardRoutings(ctx *plancontext.PlanningContext, a, b *AnyShardRouting, joinPredicates []sqlparser.Expr, joinType sqlparser.JoinType) *AnyShardRouting {
	alternates := make(map[*vindexes.Keyspace]*Route)
	for ak, av := range a.Alternates {
		for bk, bv := range b.Alternates {
			// only same keyspace alternates can be merged.
			if ak != bk {
				continue
			}
			op, _ := mergeOrJoin(ctx, av, bv, joinPredicates, joinType)
			if r, ok := op.(*Route); ok {
				alternates[ak] = r
			}
		}
	}
	return &AnyShardRouting{
		keyspace:   a.keyspace,
		Alternates: alternates,
	}
}

func prepareInputRoutes(ctx *plancontext.PlanningContext, lhs Operator, rhs Operator) (*Route, *Route, Routing, Routing, routingType, routingType, bool) {
	lhsRoute, rhsRoute := operatorsToRoutes(lhs, rhs)
	if lhsRoute == nil || rhsRoute == nil {
		return nil, nil, nil, nil, 0, 0, false
	}

	lhsRoute, rhsRoute, routingA, routingB, sameKeyspace := getRoutesOrAlternates(ctx, lhsRoute, rhsRoute)

	a, b := getRoutingType(routingA), getRoutingType(routingB)
	return lhsRoute, rhsRoute, routingA, routingB, a, b, sameKeyspace
}

type (
	merger interface {
		mergeShardedRouting(ctx *plancontext.PlanningContext, r1, r2 *ShardedRouting, op1, op2 *Route, conditions ...engine.Condition) *Route
		merge(ctx *plancontext.PlanningContext, op1, op2 *Route, r Routing, conditions ...engine.Condition) *Route
	}

	joinMerger struct {
		predicates []sqlparser.Expr
		// joinType is permitted to store only 3 of the possible values
		// NormalJoinType, StraightJoinType and LeftJoinType.
		joinType sqlparser.JoinType
	}

	routingType int
)

const (
	sharded routingType = iota
	infoSchema
	anyShard
	none
	dual
	targeted
)

func (rt routingType) String() string {
	switch rt {
	case sharded:
		return "sharded"
	case infoSchema:
		return "infoSchema"
	case anyShard:
		return "anyShard"
	case none:
		return "none"
	case dual:
		return "dual"
	case targeted:
		return "targeted"
	}
	panic("switch should be exhaustive")
}

// getRoutesOrAlternates gets the Routings from each Route. If they are from different keyspaces,
// we check if this is a table with alternates in other keyspaces that we can use
func getRoutesOrAlternates(ctx *plancontext.PlanningContext, lhsRoute, rhsRoute *Route) (*Route, *Route, Routing, Routing, bool) {
	return routesOrAlternates(ctx, lhsRoute, rhsRoute, func(*Route) bool { return true })
}

// routesOrAlternates is getRoutesOrAlternates for callers that only trust the alternates of the
// routes that the filter accepts.
func routesOrAlternates(ctx *plancontext.PlanningContext, lhsRoute, rhsRoute *Route, usable func(*Route) bool) (*Route, *Route, Routing, Routing, bool) {
	routingA := lhsRoute.Routing
	routingB := rhsRoute.Routing
	sameKeyspace := routingA.Keyspace() == routingB.Keyspace()

	if sameKeyspace ||
		// if either of these is missing a keyspace, we are not going to be able to find an alternative
		routingA.Keyspace() == nil ||
		routingB.Keyspace() == nil {
		return lhsRoute, rhsRoute, routingA, routingB, sameKeyspace
	}

	// If we have a reference route, we will try to find an alternate route in same keyspace as other routing keyspace.
	// If the reference route is part of DML table update target, alternate keyspace route cannot be considered.
	if refA, ok := routingA.(*AnyShardRouting); ok &&
		!TableID(lhsRoute).IsOverlapping(ctx.SemTable.DMLTargets) && usable(lhsRoute) {
		if altARoute := refA.AlternateInKeyspace(routingB.Keyspace()); altARoute != nil {
			return altARoute, rhsRoute, altARoute.Routing, routingB, true
		}
	}

	if refB, ok := routingB.(*AnyShardRouting); ok &&
		!TableID(rhsRoute).IsOverlapping(ctx.SemTable.DMLTargets) && usable(rhsRoute) {
		if altBRoute := refB.AlternateInKeyspace(routingA.Keyspace()); altBRoute != nil {
			return lhsRoute, altBRoute, routingA, altBRoute.Routing, true
		}
	}

	return lhsRoute, rhsRoute, routingA, routingB, sameKeyspace
}

func getRoutingType(r Routing) routingType {
	switch r.(type) {
	case *InfoSchemaRouting:
		return infoSchema
	case *AnyShardRouting:
		return anyShard
	case *DualRouting:
		return dual
	case *ShardedRouting:
		return sharded
	case *NoneRouting:
		return none
	case *TargetedRouting:
		return targeted
	}
	panic(fmt.Sprintf("switch should be exhaustive, got %T", r))
}

func newJoinMerge(predicates []sqlparser.Expr, joinType sqlparser.JoinType) *joinMerger {
	return &joinMerger{
		predicates: predicates,
		joinType:   joinType,
	}
}

func (jm *joinMerger) mergeShardedRouting(ctx *plancontext.PlanningContext, r1, r2 *ShardedRouting, op1, op2 *Route, conditions ...engine.Condition) *Route {
	return jm.merge(ctx, op1, op2, mergeShardedRouting(r1, r2), conditions...)
}

func mergeShardedRouting(r1 *ShardedRouting, r2 *ShardedRouting) *ShardedRouting {
	tr := &ShardedRouting{
		VindexPreds:    append(r1.VindexPreds, r2.VindexPreds...),
		keyspace:       r1.keyspace,
		RouteOpCode:    r1.RouteOpCode,
		SeenPredicates: append(r1.SeenPredicates, r2.SeenPredicates...),
	}
	if r1.SelectedVindex() == r2.SelectedVindex() {
		tr.Selected = r1.Selected
	} else {
		tr.PickBestAvailableVindex()
	}
	return tr
}

// passesOnReferenceRows reports whether the reference rows an input preserved still have to come
// back once from this merge. Only a reference route carries them, and it returns the same rows on
// every shard, so pairing each of them with a row of a sharded table, which lives on one shard,
// returns each pairing once wherever the route runs. The preserved side of an outer join pairs its
// rows with nothing, and owesReferenceRows already covers it.
func passesOnReferenceRows(input, other *Route) bool {
	_, sharded := other.Routing.(*ShardedRouting)
	return input.PreservesReferenceRows && !sharded
}

func getKeyspaceName(routing Routing) string {
	if ks := routing.Keyspace(); ks != nil {
		return ks.Name
	}
	return "<unknown>"
}

func (jm *joinMerger) merge(ctx *plancontext.PlanningContext, op1, op2 *Route, r Routing, conditions ...engine.Condition) *Route {
	// Only a reference routing returns the preserved rows once, so the merge keeps them on one. A
	// sharded routing would have to stay single-shard through every later reset and find a
	// destination even when its predicates match none, so the rows are read on their own instead.
	owed := owesReferenceRows(jm.joinType, op1) || passesOnReferenceRows(op1, op2) || passesOnReferenceRows(op2, op1)
	if owed && isDirectDMLRowSource(ctx, op1, op2) {
		owed = false
	}
	// A recursive term cannot be an ApplyJoin, and one that reads a single shard returns the
	// preserved rows once, so it keeps merging as on main.
	inTerm := TableID(op1).Merge(TableID(op2)).IsSolvedBy(ctx.RecursiveTermTables)
	if owed && !canCarryReferenceRows(r) && !(inTerm && pinnedToOneShard(ctx, r)) {
		debugNoRewrite("apply join merge blocked: the reference rows an outer join preserves cannot be read through %s routing", r.OpCode().String())
		ctx.RefusedReferenceMerge = ctx.RefusedReferenceMerge.Merge(TableID(op1)).Merge(TableID(op2))
		return nil
	}

	aj := NewApplyJoin(ctx, op1.Source, op2.Source, ctx.SemTable.AndExpressions(jm.predicates...), jm.joinType, false)
	for _, column := range aj.JoinPredicates.columns {
		if column.JoinPredicateID != nil {
			ctx.PredTracker.Set(*column.JoinPredicateID, column.Original)
		}
	}

	return &Route{
		unaryOperator:          newUnaryOp(aj),
		MergedWith:             []*Route{op2},
		Routing:                r,
		Conditions:             conditions,
		PreservesReferenceRows: owed,
	}
}
