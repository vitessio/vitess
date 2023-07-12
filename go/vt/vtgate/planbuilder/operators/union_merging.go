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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
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

// mergeUnionInputs checks whether two operators can be merged into a single one.
// If they can be merged, a new operator with the merged routing is returned
// If they cannot be merged, nil is returned.
// this function is very similar to mergeJoinInputs
func mergeUnionInputs(ctx *plancontext.PlanningContext, lhs, rhs ops.Operator, lhsExprs, rhsExprs sqlparser.SelectExprs, distinct bool) (ops.Operator, error) {
	lhsRoute, rhsRoute, routingA, routingB, a, b, sameKeyspace := prepareInputRoutes(lhs, rhs)
	if lhsRoute == nil {
		return nil, nil
	}

	switch {
	// if either side is a dual query, we can always merge them together
	case b == dual:
		return createMergedUnion(lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct, routingA), nil
	case a == dual:
		return createMergedUnion(lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct, routingB), nil
	case a == sharded && b == sharded && sameKeyspace:
		return createMergedUnion(lhsRoute, rhsRoute, lhsExprs, rhsExprs, distinct, routingB), nil
	default:
		return nil, nil
	}
}

func createMergedUnion(
	lhsRoute, rhsRoute *Route,
	lhsExprs, rhsExprs sqlparser.SelectExprs,
	distinct bool,
	routing Routing) *Route {
	return &Route{
		Source: &Union{
			Sources:  []ops.Operator{lhsRoute.Source, rhsRoute.Source},
			Selects:  []sqlparser.SelectExprs{lhsExprs, rhsExprs},
			distinct: distinct,
		},
		MergedWith: []*Route{rhsRoute},
		Routing:    routing,
	}
}
