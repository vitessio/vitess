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
	"errors"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ builder = (*mergeSort)(nil)

// mergeSort is a pseudo-primitive. It amends the
// the underlying Route to perform a merge sort.
// It's differentiated as a separate primitive
// because some operations cannot be pushed down,
// which would otherwise be possible with a simple route.
// Since ORDER BY happens near the end of the SQL processing,
// most functions of this primitive are unreachable.
type mergeSort struct {
	resultsBuilder
	truncateColumnCount int
}

// newMergeSort builds a new mergeSort.
func newMergeSort(rb *route) *mergeSort {
	ms := &mergeSort{
		resultsBuilder: newResultsBuilder(rb, nil),
	}
	ms.truncater = ms
	return ms
}

// SetTruncateColumnCount satisfies the truncater interface.
// This function records the truncate column count and sets
// it later on the eroute during wire-up phase.
func (ms *mergeSort) SetTruncateColumnCount(count int) {
	ms.truncateColumnCount = count
}

// Primitive satisfies the builder interface.
func (ms *mergeSort) Primitive() engine.Primitive {
	return ms.input.Primitive()
}

// PushFilter satisfies the builder interface.
func (ms *mergeSort) PushFilter(pb *primitiveBuilder, expr sqlparser.Expr, whereType string, origin builder) error {
	return ms.input.PushFilter(pb, expr, whereType, origin)
}

// PushSelect satisfies the builder interface.
func (ms *mergeSort) PushSelect(pb *primitiveBuilder, expr *sqlparser.AliasedExpr, origin builder) (rc *resultColumn, colNumber int, err error) {
	return ms.input.PushSelect(pb, expr, origin)
}

// MakeDistinct satisfies the builder interface.
func (ms *mergeSort) MakeDistinct() error {
	return ms.input.MakeDistinct()
}

// PushGroupBy satisfies the builder interface.
func (ms *mergeSort) PushGroupBy(groupBy sqlparser.GroupBy) error {
	return ms.input.PushGroupBy(groupBy)
}

// PushOrderBy satisfies the builder interface.
// A merge sort is created due to the push of an ORDER BY clause.
// So, this function should never get called.
func (ms *mergeSort) PushOrderBy(orderBy sqlparser.OrderBy) (builder, error) {
	return nil, errors.New("mergeSort.PushOrderBy: unreachable")
}

// Wireup satisfies the builder interface.
func (ms *mergeSort) Wireup(bldr builder, jt *jointab) error {
	// If the route has to do the ordering, and if any columns are Text,
	// we have to request the corresponding weight_string from mysql
	// and use that value instead. This is because we cannot mimic
	// mysql's collation behavior yet.
	rb := ms.input.(*route)
	rb.finalizeOptions()
	ro := rb.routeOptions[0]
	for i, orderby := range ro.eroute.OrderBy {
		rc := ms.resultColumns[orderby.Col]
		if sqltypes.IsText(rc.column.typ) {
			// If a weight string was previously requested, reuse it.
			if colNumber, ok := ms.weightStrings[rc]; ok {
				ro.eroute.OrderBy[i].Col = colNumber
				continue
			}
			var err error
			ro.eroute.OrderBy[i].Col, err = rb.SupplyWeightString(orderby.Col)
			if err != nil {
				return err
			}
			ms.truncateColumnCount = len(ms.resultColumns)
		}
	}
	ro.eroute.TruncateColumnCount = ms.truncateColumnCount
	return ms.input.Wireup(bldr, jt)
}
