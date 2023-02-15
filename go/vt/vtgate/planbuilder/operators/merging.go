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

	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
)

// Merge checks whether two operators can be merged into a single one.
// If they can be merged, a new operator with the merged routing is returned
// If they cannot be merged, nil is returned.
func Merge(ctx *plancontext.PlanningContext, lhs, rhs ops.Operator, joinPredicates []sqlparser.Expr, m merger) (ops.Operator, error) {
	lhsRoute, rhsRoute := operatorsToRoutes(lhs, rhs)
	if lhsRoute == nil || rhsRoute == nil {
		return nil, nil
	}

	lhsRoute, rhsRoute, routingA, routingB, sameKeyspace := getRoutesOrAlternates(lhsRoute, rhsRoute)

	a, b := getRoutingType(routingA), getRoutingType(routingB)
	if getTypeName(routingA) < getTypeName(routingB) {
		// while deciding if two routes can be merged, the LHS/RHS order of the routes is not important.
		// for the actual merging, we still need to remember which side was inner and which was outer for subqueries
		a, b = b, a
		routingA, routingB = routingB, routingA
	}

	switch {
	// if either side is a dual query, we can always merge them together
	case a == dual:
		return m.merge(lhsRoute, rhsRoute, routingB)
	case b == dual:
		return m.merge(lhsRoute, rhsRoute, routingA)

	// an unsharded/reference route can be merged with anything going to that keyspace
	case a == anyShard && sameKeyspace:
		return m.merge(lhsRoute, rhsRoute, routingB)
	case b == anyShard && sameKeyspace:
		return m.merge(lhsRoute, rhsRoute, routingA)

	// None routing can always be merged, as long as we are aiming for the same keyspace
	case a == none && sameKeyspace:
		return m.merge(lhsRoute, rhsRoute, routingA)
	case b == none && sameKeyspace:
		return m.merge(lhsRoute, rhsRoute, routingB)

	// infoSchema routing is complex, so we handle it in a separate method
	case a == infoSchema && b == infoSchema:
		return tryMergeInfoSchemaRoutings(routingA, routingB, m, lhsRoute, rhsRoute)

	// sharded routing is complex, so we handle it in a separate method
	case a == sharded && b == sharded:
		return tryMergeShardedRouting(ctx, lhsRoute, rhsRoute, m, joinPredicates)

	default:
		return nil, nil
	}
}

type (
	merger interface {
		mergeTables(r1, r2 *ShardedRouting, op1, op2 *Route) (*Route, error)
		merge(op1, op2 *Route, r Routing) (*Route, error)
	}

	joinMerger struct {
		ctx        *plancontext.PlanningContext
		predicates []sqlparser.Expr
		innerJoin  bool
	}

	subQueryMerger struct {
		ctx  *plancontext.PlanningContext
		subq *SubQueryInner
	}

	// mergeDecorator runs the inner merge and also runs the additional function f.
	mergeDecorator struct {
		inner merger
		f     func() error
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

func newJoinMerge(ctx *plancontext.PlanningContext, predicates []sqlparser.Expr, innerJoin bool) merger {
	return &joinMerger{
		ctx:        ctx,
		predicates: predicates,
		innerJoin:  innerJoin,
	}
}

func (jm *joinMerger) mergeTables(r1, r2 *ShardedRouting, op1, op2 *Route) (*Route, error) {
	tr := &ShardedRouting{
		VindexPreds:    append(r1.VindexPreds, r2.VindexPreds...),
		keyspace:       r1.keyspace,
		RouteOpCode:    r1.RouteOpCode,
		SeenPredicates: r1.SeenPredicates,
	}
	if r1.SelectedVindex() == r2.SelectedVindex() {
		tr.Selected = r1.Selected
	} else {
		tr.PickBestAvailableVindex()
	}

	return &Route{
		Source:     jm.getApplyJoin(op1, op2),
		MergedWith: []*Route{op2},
		Routing:    tr,
	}, nil
}

func (jm *joinMerger) getApplyJoin(op1, op2 *Route) *ApplyJoin {
	return NewApplyJoin(op1.Source, op2.Source, jm.ctx.SemTable.AndExpressions(jm.predicates...), !jm.innerJoin)
}

func (jm *joinMerger) merge(op1, op2 *Route, r Routing) (*Route, error) {
	return &Route{
		Source:     jm.getApplyJoin(op1, op2),
		MergedWith: []*Route{op2},
		Routing:    r,
	}, nil
}

func newSubQueryMerge(ctx *plancontext.PlanningContext, subq *SubQueryInner) merger {
	return &subQueryMerger{ctx: ctx, subq: subq}
}

// markPredicateInOuterRouting merges a subquery with the outer routing.
// If the subquery was a predicate on the outer side, we see if we can use
// predicates from the subquery to help with routing
func (s *subQueryMerger) markPredicateInOuterRouting(outer *ShardedRouting, inner Routing) (Routing, error) {
	// When merging an inner query with its outer query, we can remove the
	// inner query from the list of predicates that can influence routing of
	// the outer query.
	//
	// Note that not all inner queries necessarily are part of the routing
	// predicates list, so this might be a no-op.
	subQueryWasPredicate := false
	for i, predicate := range outer.SeenPredicates {
		if s.ctx.SemTable.EqualsExpr(predicate, s.subq.ExtractedSubquery) {
			outer.SeenPredicates = append(outer.SeenPredicates[:i], outer.SeenPredicates[i+1:]...)

			subQueryWasPredicate = true

			// The `ExtractedSubquery` of an inner query is unique (due to the uniqueness of bind variable names)
			// so we can stop after the first match.
			break
		}
	}

	if !subQueryWasPredicate {
		// if the subquery was not a predicate, we are done here
		return outer, nil
	}

	switch inner := inner.(type) {
	case *ShardedRouting:
		// Copy Vindex predicates from the inner route to the upper route.
		// If we can route based on some of these predicates, the routing can improve
		outer.VindexPreds = append(outer.VindexPreds, inner.VindexPreds...)
		outer.SeenPredicates = append(outer.SeenPredicates, inner.SeenPredicates...)
		routing, err := outer.ResetRoutingLogic(s.ctx)
		if err != nil {
			return nil, err
		}
		return routing, nil
	case *NoneRouting:
		// if we have an ANDed subquery, and we know that it will not find anything,
		// we can safely assume that the outer query will also not return anything
		return &NoneRouting{keyspace: outer.keyspace}, nil
	default:
		return outer, nil
	}
}

func (s *subQueryMerger) mergeTables(outer, inner *ShardedRouting, op1, op2 *Route) (*Route, error) {
	s.subq.ExtractedSubquery.Merged = true

	routing, err := s.markPredicateInOuterRouting(outer, inner)
	if err != nil {
		return nil, err
	}
	op1.Routing = routing
	op1.MergedWith = append(op1.MergedWith, op2)
	return op1, nil
}

func (s *subQueryMerger) merge(outer, inner *Route, routing Routing) (*Route, error) {
	s.subq.ExtractedSubquery.Merged = true

	if outerSR, ok := outer.Routing.(*ShardedRouting); ok {
		var err error
		routing, err = s.markPredicateInOuterRouting(outerSR, inner.Routing)
		if err != nil {
			return nil, err
		}
	}

	outer.Routing = routing
	outer.MergedWith = append(outer.MergedWith, inner)
	return outer, nil
}

func (d *mergeDecorator) mergeTables(outer, inner *ShardedRouting, op1, op2 *Route) (*Route, error) {
	merged, err := d.inner.mergeTables(outer, inner, op1, op2)
	if err != nil {
		return nil, err
	}
	if err := d.f(); err != nil {
		return nil, err
	}
	return merged, nil
}

func (d *mergeDecorator) merge(outer, inner *Route, r Routing) (*Route, error) {
	merged, err := d.inner.merge(outer, inner, r)
	if err != nil {
		return nil, err
	}
	if err := d.f(); err != nil {
		return nil, err
	}
	return merged, nil
}
