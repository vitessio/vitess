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
	builderCommon
}

// newMergeSort builds a new mergeSort.
func newMergeSort(rb *route) *mergeSort {
	return &mergeSort{
		builderCommon: newBuilderCommon(rb),
	}
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
