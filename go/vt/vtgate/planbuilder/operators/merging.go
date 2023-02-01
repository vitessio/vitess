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

// Merge checks whether two operators can be merged into a single one.
// If they can be merged, the new Routing for the merged operator is returned.
// If they cannot be merged, nil is returned.
func Merge(ctx *plancontext.PlanningContext, opA, opB ops.Operator, joinPredicates []sqlparser.Expr, m merger) (ops.Operator, error) {
	routeA, routeB := operatorsToRoutes(opA, opB)
	if routeA == nil || routeB == nil {
		return nil, nil
	}

	if getTypeName(routeA.Routing) < getTypeName(routeB.Routing) {
		// merging is commutative, and we only want to have to think about one of the two orders, so we do this
		routeA, routeB = routeB, routeA
	}

	//
	// if !sameKeyspace {
	// 	if altARoute := routeA.AlternateInKeyspace(routeB.Keyspace); altARoute != nil {
	// 		routeA = altARoute
	// 		sameKeyspace = true
	// 	} else if altBRoute := routeB.AlternateInKeyspace(routeA.Keyspace); altBRoute != nil {
	// 		routeB = altBRoute
	// 		sameKeyspace = true
	// 	}
	// }
	sameKeyspace := routeA.Routing.Keyspace() == routeB.Routing.Keyspace()
	a, b := getR(routeA.Routing), getR(routeB.Routing)
	switch {

	// if either side is a dual query, we can always merge them together
	case a.dual:
		return m.merge(routeA, routeB, routeB.Routing), nil
	case b.dual:
		return m.merge(routeA, routeB, routeA.Routing), nil

	// an unsharded/reference route can be merged with anything going to that keyspace
	case (a.unsharded || a.ref) && sameKeyspace:
		return m.merge(routeA, routeB, routeB.Routing), nil
	case (b.unsharded || b.ref) && sameKeyspace:
		return m.merge(routeA, routeB, routeA.Routing), nil

	case (a.unsharded || a.ref || b.unsharded || b.ref) && !sameKeyspace:
		return nil, nil

	case a.unsharded && b.table || b.unsharded && a.table:
		return nil, nil

	case a.table && b.table:
		return mergeTables(ctx, routeA, routeB, m, joinPredicates)

	// table and reference routing can be merged if they are going to the same keyspace
	case a.table && b.ref && sameKeyspace:
		return m.merge(routeA, routeB, routeA.Routing), nil

	// info schema routings are hard to merge with anything else
	case a.infoSchema || b.infoSchema:
		return nil, nil

	default:
		panic(dbg.S(a, b))
	}
}

func mergeTables(ctx *plancontext.PlanningContext, routeA *Route, routeB *Route, m merger, joinPredicates []sqlparser.Expr) (ops.Operator, error) {
	sameKeyspace := routeA.Routing.Keyspace() == routeB.Routing.Keyspace()
	tblA := routeA.Routing.(*TableRouting)
	tblB := routeB.Routing.(*TableRouting)

	switch tblA.RouteOpCode {
	case engine.EqualUnique:
		// If the two routes fully match, they can be merged together.
		if tblB.RouteOpCode == engine.EqualUnique {
			aVdx := tblA.SelectedVindex()
			bVdx := tblB.SelectedVindex()
			aExpr := tblA.VindexExpressions()
			bExpr := tblB.VindexExpressions()
			if aVdx == bVdx && gen4ValuesEqual(ctx, aExpr, bExpr) {
				return m.mergeTables(tblA, tblB, routeA, routeB), nil
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
		return m.mergeTables(tblA, tblB, routeA, routeB), nil
	}
	return nil, nil
}

func newJoinMerge(ctx *plancontext.PlanningContext, predicates []sqlparser.Expr, innerJoin bool) merger {
	return &joinMerger{
		ctx:        ctx,
		predicates: predicates,
		innerJoin:  innerJoin,
	}
}

type joinMerger struct {
	ctx        *plancontext.PlanningContext
	predicates []sqlparser.Expr
	innerJoin  bool
}

type merger interface {
	mergeTables(r1, r2 *TableRouting, op1, op2 *Route) ops.Operator
	merge(op1, op2 *Route, r Routing) ops.Operator
}

func (jm *joinMerger) mergeTables(r1, r2 *TableRouting, op1, op2 *Route) ops.Operator {
	tr := &TableRouting{
		VindexPreds: append(r1.VindexPreds, r2.VindexPreds...),
		keyspace:    r1.keyspace,
		RouteOpCode: r1.RouteOpCode,
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
	}
}

func (jm *joinMerger) getApplyJoin(op1, op2 *Route) *ApplyJoin {
	return NewApplyJoin(op1.Source, op2.Source, jm.ctx.SemTable.AndExpressions(jm.predicates...), !jm.innerJoin)
}

func (jm *joinMerger) merge(op1, op2 *Route, r Routing) ops.Operator {
	return &Route{
		Source:     jm.getApplyJoin(op1, op2),
		MergedWith: []*Route{op2},
		Routing:    r,
	}
}

func getTypeName(myvar interface{}) string {
	return reflect.TypeOf(myvar).String()
}

func getR(r Routing) rBools {
	switch r.(type) {
	case *InfoSchemaRouting:
		return rBools{infoSchema: true}
	case *ReferenceRouting:
		return rBools{ref: true}
	case *DualRouting:
		return rBools{dual: true}
	case *TableRouting:
		return rBools{table: true}
	case *NoneRouting:
		return rBools{none: true}
	case *UnshardedRouting:
		return rBools{unsharded: true}
	case *TargetedRouting:
		return rBools{targeted: true}
	}
	return rBools{}
}

type rBools struct {
	table,
	infoSchema,
	ref,
	none,
	unsharded,
	dual,
	targeted bool
}
