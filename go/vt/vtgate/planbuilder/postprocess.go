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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

// This file has functions to analyze postprocessing
// clauses like ORDER BY, etc.

// pushGroupBy processes the group by clause. It resolves all symbols
// and ensures that there are no subqueries.
func (pb *primitiveBuilder) pushGroupBy(sel *sqlparser.Select) error {
	if sel.Distinct {
		newBuilder, err := planDistinct(pb, pb.bldr)
		if err != nil {
			return err
		}
		pb.bldr = newBuilder
	}

	if err := pb.st.ResolveSymbols(sel.GroupBy); err != nil {
		return err
	}

	newInput, err := planGroupBy(pb, pb.bldr, sel.GroupBy)
	if err != nil {
		return err
	}
	pb.bldr = newInput

	return nil
}

// pushOrderBy pushes the order by clause into the primitives.
// It resolves all symbols and ensures that there are no subqueries.
func (pb *primitiveBuilder) pushOrderBy(orderBy sqlparser.OrderBy) error {
	if err := pb.st.ResolveSymbols(orderBy); err != nil {
		return err
	}
	bldr, err := planOrdering(pb, pb.bldr, orderBy)
	if err != nil {
		return err
	}
	pb.bldr = bldr
	pb.bldr.Reorder(0)
	return nil
}

func (pb *primitiveBuilder) pushLimit(limit *sqlparser.Limit) error {
	if limit == nil {
		return nil
	}
	rb, ok := pb.bldr.(*route)
	if ok && rb.isSingleShard() {
		rb.SetLimit(limit)
		return nil
	}

	lb, err := createLimit(pb.bldr, limit)
	if err != nil {
		return err
	}

	bldr, err := visit(lb, setUpperLimit)
	if err != nil {
		return err
	}

	pb.bldr = bldr
	pb.bldr.Reorder(0)
	return nil
}

// make sure we have the right signature for this function
var _ builderVisitor = setUpperLimit

// setUpperLimit is an optimization hint that tells that primitive
// that it does not need to return more than the specified number of rows.
// A primitive that cannot perform this can ignore the request.
func setUpperLimit(bldr builder) (bool, builder, error) {
	arg := sqlparser.NewArgument([]byte(":__upper_limit"))
	switch node := bldr.(type) {
	case *join:
		return false, node, nil
	case *memorySort:
		pv, err := sqlparser.NewPlanValue(arg)
		if err != nil {
			return false, nil, err
		}
		node.eMemorySort.UpperLimit = pv
		// we don't want to go down to the rest of the tree
		return false, node, nil
	case *pulloutSubquery:
		// we control the visitation manually here -
		// we don't want to visit the subQuery side of this builder
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
		node.Select.SetLimit(&sqlparser.Limit{Rowcount: arg})
	case *concatenate:
		return false, node, nil
	}
	return true, bldr, nil
}

// SetLimit sets the limit for the primitive. It calls the underlying
// primitive's SetUpperLimit, which is an optimization hint that informs
// the underlying primitive that it doesn't need to return more rows than
// specified.
func createLimit(input builder, limit *sqlparser.Limit) (builder, error) {
	bldr := newLimit(input)
	pv, err := sqlparser.NewPlanValue(limit.Rowcount)
	if err != nil {
		return nil, vterrors.Wrap(err, "unexpected expression in LIMIT")
	}
	bldr.elimit.Count = pv

	if limit.Offset != nil {
		pv, err = sqlparser.NewPlanValue(limit.Offset)
		if err != nil {
			return nil, vterrors.Wrap(err, "unexpected expression in OFFSET")
		}
		bldr.elimit.Offset = pv
	}

	return bldr, nil
}
