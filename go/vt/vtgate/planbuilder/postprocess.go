/*
Copyright 2019 The Vitess Authors.

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

package planbuilder

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// This file has functions to analyze postprocessing
// clauses like ORDER BY, etc.

// make sure we have the right signature for this function
var _ planVisitor = setUpperLimit

// setUpperLimit is an optimization hint that tells that primitive
// that it does not need to return more than the specified number of rows.
// A primitive that cannot perform this can ignore the request.
func setUpperLimit(plan logicalPlan) (bool, logicalPlan, error) {
	switch node := plan.(type) {
	case *join, *hashJoin:
		return false, node, nil
	case *memorySort:
		pv := evalengine.NewBindVar("__upper_limit", sqltypes.Int64, collations.CollationBinaryID)
		node.eMemorySort.UpperLimit = pv
		// we don't want to go down to the rest of the tree
		return false, node, nil
	case *pulloutSubquery:
		// we control the visitation manually here -
		// we don't want to visit the subQuery side of this plan
		newUnderlying, err := visit(node.underlying, setUpperLimit)
		if err != nil {
			return false, nil, err
		}

		node.underlying = newUnderlying
		return false, node, nil
	case *route:
		// The route pushes the limit regardless of the plan.
		// If it's a scatter query, the rows returned will be
		// more than the upper limit, but enough for the limit
		node.Select.SetLimit(&sqlparser.Limit{Rowcount: sqlparser.NewArgument("__upper_limit")})
	}
	return true, plan, nil
}

func createLimit(input logicalPlan, limit *sqlparser.Limit) (logicalPlan, error) {
	plan := newLimit(input)
	pv, err := evalengine.Translate(limit.Rowcount, nil)
	if err != nil {
		return nil, vterrors.Wrap(err, "unexpected expression in LIMIT")
	}
	plan.elimit.Count = pv

	if limit.Offset != nil {
		pv, err = evalengine.Translate(limit.Offset, nil)
		if err != nil {
			return nil, vterrors.Wrap(err, "unexpected expression in OFFSET")
		}
		plan.elimit.Offset = pv
	}

	return plan, nil
}
