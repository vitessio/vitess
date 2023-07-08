package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// mergeUnionInputInAnyOrder merges sources the sources of the union in any order
// can be used for UNION DISTINCT
func mergeUnionInputInAnyOrder(ctx *plancontext.PlanningContext, op *Union) ([]ops.Operator, []sqlparser.SelectExprs, error) {
	sources := op.Sources

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
			newPlan, err := mergeUnionInputs(ctx, srcA, srcB, nil, nil, op.distinct)
			if err != nil {
				return nil, nil, err
			}
			if newPlan != nil {
				sources[idx] = newPlan
				srcA = newPlan
				merged = true
			} else {
				keep[j] = true
			}
		}
		if !merged {
			return sources, nil, nil
		}

		var phase []ops.Operator
		for i, source := range sources {
			if keep[i] || i <= idx {
				phase = append(phase, source)
			}
		}
		idx++
		sources = phase
	}

	return sources, nil, nil
}

// mergeUnionInputs checks whether two operators can be merged into a single one.
// If they can be merged, a new operator with the merged routing is returned
// If they cannot be merged, nil is returned.
// this function is very similar to mergeJoinInputs
func mergeUnionInputs(ctx *plancontext.PlanningContext, lhs, rhs ops.Operator, lhsExprs, rhsExprs sqlparser.SelectExprs, distinct bool) (ops.Operator, error) {
	lhsRoute, rhsRoute, routingA, _, _, b, _ := prepareInputRoutes(lhs, rhs)
	if lhsRoute == nil {
		return nil, nil
	}

	switch {
	// if either side is a dual query, we can always merge them together
	case b == dual:
		//rhsRoute.Source = &Union{
		//	Sources:  []ops.Operator{lhsRoute.Source, rhsRoute.Source},
		//	Selects:  []sqlparser.SelectExprs{lhsExprs, rhsExprs},
		//	distinct: false,
		//}
		//rhsRoute.MergedWith = append(rhsRoute.MergedWith, lhsRoute)
		//return rhsRoute, nil
		return &Route{
			Source: &Union{
				Sources:  []ops.Operator{lhsRoute.Source, rhsRoute.Source},
				Selects:  []sqlparser.SelectExprs{lhsExprs, rhsExprs},
				distinct: distinct,
			},
			MergedWith: []*Route{rhsRoute},
			Routing:    routingA,
		}, nil
	default:
		return nil, nil
	}
}
