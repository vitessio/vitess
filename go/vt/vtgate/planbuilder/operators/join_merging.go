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
	"reflect"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// mergeJoinInputs checks whether two operators can be merged into a single one.
// If they can be merged, a new operator with the merged routing is returned
// If they cannot be merged, nil is returned.
func mergeJoinInputs(ctx *plancontext.PlanningContext, lhs, rhs Operator, joinPredicates []sqlparser.Expr, m merger) *Route {
	lhsRoute, rhsRoute, routingA, routingB, a, b, sameKeyspace := prepareInputRoutes(lhs, rhs)
	if lhsRoute == nil {
		return nil
	}

	switch {
	// if either side is a dual query, we can always merge them together
	case a == dual:
		return m.merge(ctx, lhsRoute, rhsRoute, routingB)
	case b == dual:
		return m.merge(ctx, lhsRoute, rhsRoute, routingA)

	// an unsharded/reference route can be merged with anything going to that keyspace
	case a == anyShard && sameKeyspace:
		return m.merge(ctx, lhsRoute, rhsRoute, routingB)
	case b == anyShard && sameKeyspace:
		return m.merge(ctx, lhsRoute, rhsRoute, routingA)

	// None routing can always be merged, as long as we are aiming for the same keyspace
	case a == none && sameKeyspace:
		return m.merge(ctx, lhsRoute, rhsRoute, routingA)
	case b == none && sameKeyspace:
		return m.merge(ctx, lhsRoute, rhsRoute, routingB)

	// infoSchema routing is complex, so we handle it in a separate method
	case a == infoSchema && b == infoSchema:
		return tryMergeInfoSchemaRoutings(ctx, routingA, routingB, m, lhsRoute, rhsRoute)

	// sharded routing is complex, so we handle it in a separate method
	case a == sharded && b == sharded:
		return tryMergeJoinShardedRouting(ctx, lhsRoute, rhsRoute, m, joinPredicates)

	default:
		return nil
	}
}

func prepareInputRoutes(lhs Operator, rhs Operator) (*Route, *Route, Routing, Routing, routingType, routingType, bool) {
	lhsRoute, rhsRoute := operatorsToRoutes(lhs, rhs)
	if lhsRoute == nil || rhsRoute == nil {
		return nil, nil, nil, nil, 0, 0, false
	}

	lhsRoute, rhsRoute, routingA, routingB, sameKeyspace := getRoutesOrAlternates(lhsRoute, rhsRoute)

	a, b := getRoutingType(routingA), getRoutingType(routingB)
	if getTypeName(routingA) < getTypeName(routingB) {
		// while deciding if two routes can be merged, the LHS/RHS order of the routes is not important.
		// for the actual merging, we still need to remember which side was inner and which was outer for subqueries
		a, b = b, a
		routingA, routingB = routingB, routingA
	}

	return lhsRoute, rhsRoute, routingA, routingB, a, b, sameKeyspace
}

type (
	merger interface {
		mergeShardedRouting(ctx *plancontext.PlanningContext, r1, r2 *ShardedRouting, op1, op2 *Route) *Route
		merge(ctx *plancontext.PlanningContext, op1, op2 *Route, r Routing) *Route
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
func getRoutesOrAlternates(lhsRoute, rhsRoute *Route) (*Route, *Route, Routing, Routing, bool) {
	routingA := lhsRoute.Routing
	routingB := rhsRoute.Routing
	sameKeyspace := routingA.Keyspace() == routingB.Keyspace()

	if sameKeyspace ||
		// if either of these is missing a keyspace, we are not going to be able to find an alternative
		routingA.Keyspace() == nil ||
		routingB.Keyspace() == nil {
		return lhsRoute, rhsRoute, routingA, routingB, sameKeyspace
	}

	if refA, ok := routingA.(*AnyShardRouting); ok {
		if altARoute := refA.AlternateInKeyspace(routingB.Keyspace()); altARoute != nil {
			return altARoute, rhsRoute, altARoute.Routing, routingB, true
		}
	}

	if refB, ok := routingB.(*AnyShardRouting); ok {
		if altBRoute := refB.AlternateInKeyspace(routingA.Keyspace()); altBRoute != nil {
			return lhsRoute, altBRoute, routingA, altBRoute.Routing, true
		}
	}

	return lhsRoute, rhsRoute, routingA, routingB, sameKeyspace
}

func getTypeName(myvar interface{}) string {
	return reflect.TypeOf(myvar).String()
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

func newJoinMerge(predicates []sqlparser.Expr, joinType sqlparser.JoinType) merger {
	return &joinMerger{
		predicates: predicates,
		joinType:   joinType,
	}
}

func (jm *joinMerger) mergeShardedRouting(ctx *plancontext.PlanningContext, r1, r2 *ShardedRouting, op1, op2 *Route) *Route {
	return jm.merge(ctx, op1, op2, mergeShardedRouting(r1, r2))
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

func (jm *joinMerger) getApplyJoin(ctx *plancontext.PlanningContext, op1, op2 *Route) *ApplyJoin {
	return NewApplyJoin(ctx, op1.Source, op2.Source, ctx.SemTable.AndExpressions(jm.predicates...), jm.joinType)
}

func (jm *joinMerger) merge(ctx *plancontext.PlanningContext, op1, op2 *Route, r Routing) *Route {
	return &Route{
		Source:     jm.getApplyJoin(ctx, op1, op2),
		MergedWith: []*Route{op2},
		Routing:    r,
	}
}
