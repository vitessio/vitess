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
// the underlying Route to performa a merge sort.
// It's differentiated as a separate primitive
// because some operations cannot be pushed down,
// which would otherwise be possible with a simple route.
// Since ORDER BY happens near the end of the SQL processing,
// most functions of this primitive are unreachable.
type mergeSort struct {
	order int
	input *route
}

// newMergeSort builds a new mergeSort.
func newMergeSort(rb *route) *mergeSort {
	return &mergeSort{
		input: rb,
	}
}

// Order satisfies the builder interface.
func (ms *mergeSort) Order() int {
	return ms.order
}

// Reorder satisfies the builder interface.
func (ms *mergeSort) Reorder(order int) {
	ms.input.Reorder(order)
	ms.order = ms.input.Order() + 1
}

// Primitive satisfies the builder interface.
func (ms *mergeSort) Primitive() engine.Primitive {
	return ms.input.Primitive()
}

// First satisfies the builder interface.
func (ms *mergeSort) First() builder {
	return ms.input.First()
}

// ResultColumns satisfies the builder interface.
func (ms *mergeSort) ResultColumns() []*resultColumn {
	return ms.input.ResultColumns()
}

// PushFilter satisfies the builder interface.
func (ms *mergeSort) PushFilter(pb *primitiveBuilder, expr sqlparser.Expr, whereType string, origin builder) error {
	return ms.input.PushFilter(pb, expr, whereType, origin)
}

// PushSelect satisfies the builder interface.
func (ms *mergeSort) PushSelect(expr *sqlparser.AliasedExpr, origin builder) (rc *resultColumn, colnum int, err error) {
	return ms.input.PushSelect(expr, origin)
}

// MakeDistinct satisfies the builder interface.
func (ms *mergeSort) MakeDistinct() error {
	return ms.input.MakeDistinct()
}

// PushGroupBy satisfies the builder interface.
func (ms *mergeSort) PushGroupBy(groupBy sqlparser.GroupBy) error {
	return ms.input.PushGroupBy(groupBy)
}

// PushGroupBy satisfies the builder interface.
// A merge sort is created due to the push of an ORDER BY clause.
// So, the function should never get called.
func (ms *mergeSort) PushOrderBy(orderBy sqlparser.OrderBy) (builder, error) {
	return nil, errors.New("mergeSort.PushOrderBy: unreachable")
}

// SetUpperLimit satisfies the builder interface.
func (ms *mergeSort) SetUpperLimit(count *sqlparser.SQLVal) {
	ms.input.SetUpperLimit(count)
}

// PushMisc satisfies the builder interface.
func (ms *mergeSort) PushMisc(sel *sqlparser.Select) {
	ms.input.PushMisc(sel)
}

// Wireup satisfies the builder interface.
func (ms *mergeSort) Wireup(bldr builder, jt *jointab) error {
	return ms.input.Wireup(bldr, jt)
}

// SupplyVar satisfies the builder interface.
func (ms *mergeSort) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	ms.input.SupplyVar(from, to, col, varname)
}

// SupplyCol satisfies the builder interface.
func (ms *mergeSort) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colnum int) {
	return ms.input.SupplyCol(col)
}
