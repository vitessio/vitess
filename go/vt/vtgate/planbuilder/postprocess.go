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
)

// This file has functions to analyze postprocessing
// clauses like ORDER BY, etc.

// pushGroupBy processes the group by clause. It resolves all symbols
// and ensures that there are no subqueries.
func (pb *primitiveBuilder) pushGroupBy(sel *sqlparser.Select) error {
	if sel.Distinct != "" {
		if err := pb.bldr.MakeDistinct(); err != nil {
			return err
		}
	}

	if err := pb.st.ResolveSymbols(sel.GroupBy); err != nil {
		return err
	}
	return pb.bldr.PushGroupBy(sel.GroupBy)
}

// pushOrderBy pushes the order by clause into the primitives.
// It resolves all symbols and ensures that there are no subqueries.
func (pb *primitiveBuilder) pushOrderBy(orderBy sqlparser.OrderBy) error {
	if err := pb.st.ResolveSymbols(orderBy); err != nil {
		return err
	}
	bldr, err := pb.bldr.PushOrderBy(orderBy)
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
	if ok && rb.removeMultishardOptions() {
		rb.SetLimit(limit)
		return nil
	}
	lb := newLimit(pb.bldr)
	if err := lb.SetLimit(limit); err != nil {
		return err
	}
	pb.bldr = lb
	pb.bldr.Reorder(0)
	return nil
}
