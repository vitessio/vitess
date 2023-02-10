package operators

import (
	"reflect"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/test/dbg"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
)

/*
select 1 from t1 where exists(select 1 from t2 where t1.id = t2.id and t1.foo = t2.foo)
select 1 from t1 where (id, foo) in (select id, foo from t2)
*/
type (
	merger interface {
		mergeTables(r1, r2 *ShardedRouting, op1, op2 *Route) (ops.Operator, error)
		merge(op1, op2 *Route, r Routing) (ops.Operator, error)
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
	ref
	none
	unsharded
	dual
	targeted
)

func (rt routingType) String() string {
	switch rt {
	case sharded:
		return "sharded"
	case infoSchema:
		return "infoSchema"
	case ref:
		return "ref"
	case none:
		return "none"
	case unsharded:
		return "unsharded"
	case dual:
		return "dual"
	case targeted:
		return "targeted"
	}
	panic("switch should be exhaustive")
}

// Merge checks whether two operators can be merged into a single one.
// If they can be merged, the new Routing for the merged operator is returned.
// If they cannot be merged, nil is returned.
func Merge(ctx *plancontext.PlanningContext, lhs, rhs ops.Operator, joinPredicates []sqlparser.Expr, m merger) (ops.Operator, error) {
	lhsRoute, rhsRoute := operatorsToRoutes(lhs, rhs)
	if lhsRoute == nil || rhsRoute == nil {
		return nil, nil
	}

	//
	// if !sameKeyspace {
	// 	if altARoute := routeA.AlternateInKeyspace(rhsRoute.Keyspace); altARoute != nil {
	// 		routeA = altARoute
	// 		sameKeyspace = true
	// 	} else if altBRoute := rhsRoute.AlternateInKeyspace(routeA.Keyspace); altBRoute != nil {
	// 		rhsRoute = altBRoute
	// 		sameKeyspace = true
	// 	}
	// }
	routingA := lhsRoute.Routing
	routingB := rhsRoute.Routing
	sameKeyspace := routingA.Keyspace() == routingB.Keyspace()
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
	case (a == unsharded || a == ref) && sameKeyspace:
		return m.merge(lhsRoute, rhsRoute, routingB)
	case (b == unsharded || b == ref) && sameKeyspace:
		return m.merge(lhsRoute, rhsRoute, routingA)

	case (a == unsharded || a == ref || b == unsharded || b == ref) && !sameKeyspace:
		return nil, nil

	case a == unsharded && b == sharded || b == unsharded && a == sharded:
		return nil, nil

	case a == sharded && b == sharded:
		return mergeTables(ctx, lhsRoute, rhsRoute, m, joinPredicates)

	// table and reference routing can be merged if they are going to the same keyspace
	case a == sharded && b == ref && sameKeyspace:
		return m.merge(lhsRoute, rhsRoute, routingA)

	// info schema routings are hard to merge with anything else
	case a == infoSchema || b == infoSchema:
		return nil, nil

	default:
		panic(dbg.S(a, b))
	}
}

func mergeTables(ctx *plancontext.PlanningContext, routeA *Route, routeB *Route, m merger, joinPredicates []sqlparser.Expr) (ops.Operator, error) {
	sameKeyspace := routeA.Routing.Keyspace() == routeB.Routing.Keyspace()
	tblA := routeA.Routing.(*ShardedRouting)
	tblB := routeB.Routing.(*ShardedRouting)

	switch tblA.RouteOpCode {
	case engine.EqualUnique:
		// If the two routes fully match, they can be merged together.
		if tblB.RouteOpCode == engine.EqualUnique {
			aVdx := tblA.SelectedVindex()
			bVdx := tblB.SelectedVindex()
			aExpr := tblA.VindexExpressions()
			bExpr := tblB.VindexExpressions()
			if aVdx == bVdx && gen4ValuesEqual(ctx, aExpr, bExpr) {
				return m.mergeTables(tblA, tblB, routeA, routeB)
			}
		}

		// If the two routes don't match, fall through to the next case and see if we
		// can merge via join predicates instead.
		fallthrough

	case engine.Scatter, engine.IN, engine.None:
		if len(joinPredicates) == 0 {
			// If we are doing two Scatters, we have to make sure that the
			// joins are on the correct vindex to allow them to be merged
			// no join predicates - no vindex
			return nil, nil
		}

		if !sameKeyspace {
			return nil, vterrors.VT12001("cross-shard correlated subquery")
		}

		canMerge := canMergeOnFilters(ctx, routeA, routeB, joinPredicates)
		if !canMerge {
			return nil, nil
		}
		return m.mergeTables(tblA, tblB, routeA, routeB)
	}
	return nil, nil
}

func getTypeName(myvar interface{}) string {
	return reflect.TypeOf(myvar).String()
}

func getRoutingType(r Routing) routingType {
	switch r.(type) {
	case *InfoSchemaRouting:
		return infoSchema
	case *ReferenceRouting:
		return ref
	case *DualRouting:
		return dual
	case *ShardedRouting:
		return sharded
	case *NoneRouting:
		return none
	case *UnshardedRouting:
		return unsharded
	case *TargetedRouting:
		return targeted
	}
	panic("switch should be exhaustive")
}

func newJoinMerge(ctx *plancontext.PlanningContext, predicates []sqlparser.Expr, innerJoin bool) merger {
	return &joinMerger{
		ctx:        ctx,
		predicates: predicates,
		innerJoin:  innerJoin,
	}
}

func (jm *joinMerger) mergeTables(r1, r2 *ShardedRouting, op1, op2 *Route) (ops.Operator, error) {
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

func (jm *joinMerger) merge(op1, op2 *Route, r Routing) (ops.Operator, error) {
	return &Route{
		Source:     jm.getApplyJoin(op1, op2),
		MergedWith: []*Route{op2},
		Routing:    r,
	}, nil
}

func newSubQueryMerge(ctx *plancontext.PlanningContext, subq *SubQueryInner) merger {
	return &subQueryMerger{ctx: ctx, subq: subq}
}

func (s *subQueryMerger) mergeTables(outer, inner *ShardedRouting, op1, op2 *Route) (ops.Operator, error) {
	s.subq.ExtractedSubquery.NeedsRewrite = true

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

	err := outer.resetRoutingSelections(s.ctx)
	if err != nil {
		return nil, err
	}

	if subQueryWasPredicate { // TODO: should this not be done before the `resetRoutingSelection`?
		// Copy Vindex predicates from the inner route to the upper route.
		// If we can route based on some of these predicates, the routing can improve
		outer.VindexPreds = append(outer.VindexPreds, inner.VindexPreds...)
	}

	op1.MergedWith = append(op1.MergedWith, op2)
	return op1, nil
}

func (s *subQueryMerger) merge(outer, inner *Route, r Routing) (ops.Operator, error) {
	s.subq.ExtractedSubquery.NeedsRewrite = true
	outer.Routing = r
	outer.MergedWith = append(outer.MergedWith, inner)
	return outer, nil
}

func (d *mergeDecorator) mergeTables(outer, inner *ShardedRouting, op1, op2 *Route) (ops.Operator, error) {
	merged, err := d.inner.mergeTables(outer, inner, op1, op2)
	if err != nil {
		return nil, err
	}
	if err := d.f(); err != nil {
		return nil, err
	}
	return merged, nil
}

func (d *mergeDecorator) merge(outer, inner *Route, r Routing) (ops.Operator, error) {
	merged, err := d.inner.merge(outer, inner, r)
	if err != nil {
		return nil, err
	}
	if err := d.f(); err != nil {
		return nil, err
	}
	return merged, nil
}
