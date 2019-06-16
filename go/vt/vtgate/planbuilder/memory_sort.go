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
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ builder = (*memorySort)(nil)

// memorySort is the builder for engine.Limit.
// This gets built if a limit needs to be applied
// after rows are returned from an underlying
// operation. Since a limit is the final operation
// of a SELECT, most pushes are not applicable.
type memorySort struct {
	builderCommon
	resultColumns []*resultColumn
	eMemorySort   *engine.MemorySort
}

// newMemorySort builds a new memorySort.
func newMemorySort(bldr builder, orderBy sqlparser.OrderBy) (*memorySort, error) {
	ms := &memorySort{
		builderCommon: builderCommon{input: bldr},
		resultColumns: bldr.ResultColumns(),
		eMemorySort:   &engine.MemorySort{},
	}
	for _, order := range orderBy {
		colnum := -1
		switch expr := order.Expr.(type) {
		case *sqlparser.SQLVal:
			var err error
			if colnum, err = ResultFromNumber(ms.ResultColumns(), expr); err != nil {
				return nil, err
			}
		case *sqlparser.ColName:
			c := expr.Metadata.(*column)
			for i, rc := range ms.ResultColumns() {
				if rc.column == c {
					colnum = i
					break
				}
			}
		default:
			return nil, fmt.Errorf("unsupported: memory sort: complex order by expression: %s", sqlparser.String(expr))
		}
		// If column is not found, then the order by is referencing
		// a column that's not on the select list.
		if colnum == -1 {
			return nil, fmt.Errorf("unsupported: memory sort: order by must reference a column in the select list: %s", sqlparser.String(order))
		}
		ob := engine.OrderbyParams{
			Col:  colnum,
			Desc: order.Direction == sqlparser.DescScr,
		}
		ms.eMemorySort.OrderBy = append(ms.eMemorySort.OrderBy, ob)
	}
	return ms, nil
}

// Primitive satisfies the builder interface.
func (ms *memorySort) Primitive() engine.Primitive {
	ms.eMemorySort.Input = ms.input.Primitive()
	return ms.eMemorySort
}

// ResultColumns satisfies the builder interface.
func (ms *memorySort) ResultColumns() []*resultColumn {
	return ms.resultColumns
}

// PushFilter satisfies the builder interface.
func (ms *memorySort) PushFilter(_ *primitiveBuilder, _ sqlparser.Expr, whereType string, _ builder) error {
	return errors.New("memorySort.PushFilter: unreachable")
}

// PushSelect satisfies the builder interface.
func (ms *memorySort) PushSelect(_ *primitiveBuilder, expr *sqlparser.AliasedExpr, origin builder) (rc *resultColumn, colnum int, err error) {
	return nil, 0, errors.New("memorySort.PushSelect: unreachable")
}

// MakeDistinct satisfies the builder interface.
func (ms *memorySort) MakeDistinct() error {
	return errors.New("memorySort.MakeDistinct: unreachable")
}

// PushGroupBy satisfies the builder interface.
func (ms *memorySort) PushGroupBy(_ sqlparser.GroupBy) error {
	return errors.New("memorySort.PushGroupBy: unreachable")
}

// PushGroupBy satisfies the builder interface.
func (ms *memorySort) PushOrderBy(orderBy sqlparser.OrderBy) (builder, error) {
	return nil, errors.New("memorySort.PushOrderBy: unreachable")
}

// SetLimit satisfies the builder interface.
func (ms *memorySort) SetLimit(limit *sqlparser.Limit) error {
	return errors.New("memorySort.Limit: unreachable")
}

// SetUpperLimit satisfies the builder interface.
// This is a no-op because we actually call SetLimit for this primitive.
// In the future, we may have to honor this call for subqueries.
func (ms *memorySort) SetUpperLimit(count *sqlparser.SQLVal) {
	ms.eMemorySort.UpperLimit, _ = sqlparser.NewPlanValue(count)
}
