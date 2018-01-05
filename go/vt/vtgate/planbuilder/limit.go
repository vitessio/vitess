/*
Copyright 2017 Google Inc.

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

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
)

// limit is the builder for engine.Limit.
// This gets built if a limit needs to be applied
// after rows are returned from an underlying
// operation. Since a limit is the final operation
// of a SELECT, most pushes are not applicable.
type limit struct {
	symtab        *symtab
	maxOrder      int
	resultColumns []*resultColumn
	input         builder
	elimit        *engine.Limit
}

// newLimit builds a new limit.
func newLimit(bldr builder) *limit {
	return &limit{
		symtab:        bldr.Symtab(),
		maxOrder:      bldr.MaxOrder(),
		resultColumns: bldr.ResultColumns(),
		input:         bldr,
		elimit:        &engine.Limit{},
	}
}

// Symtab satisfies the builder interface.
func (l *limit) Symtab() *symtab {
	return l.symtab
}

// MaxOrder satisfies the builder interface.
func (l *limit) MaxOrder() int {
	return l.maxOrder
}

// SetOrder satisfies the builder interface.
func (l *limit) SetOrder(order int) {
	panic("BUG: reordering can only happen within the FROM clause")
}

// Primitive satisfies the builder interface.
func (l *limit) Primitive() engine.Primitive {
	l.elimit.Input = l.input.Primitive()
	return l.elimit
}

// Leftmost satisfies the builder interface.
func (l *limit) Leftmost() columnOriginator {
	return l.input.Leftmost()
}

// ResultColumns satisfies the builder interface.
func (l *limit) ResultColumns() []*resultColumn {
	return l.resultColumns
}

// PushFilter satisfies the builder interface.
func (l *limit) PushFilter(_ sqlparser.Expr, whereType string, _ columnOriginator) error {
	panic("BUG: unreachable")
}

// PushSelect satisfies the builder interface.
func (l *limit) PushSelect(expr *sqlparser.AliasedExpr, origin columnOriginator) (rc *resultColumn, colnum int, err error) {
	panic("BUG: unreachable")
}

// MakeDistinct satisfies the builder interface.
func (l *limit) MakeDistinct() error {
	panic("BUG: unreachable")
}

// SetGroupBy satisfies the builder interface.
func (l *limit) SetGroupBy(groupBy sqlparser.GroupBy) (builder, error) {
	panic("BUG: unreachable")
}

// PushOrderByNull satisfies the builder interface.
func (l *limit) PushOrderByNull() {
	panic("BUG: unreachable")
}

// PushOrderByRand satisfies the builder interface.
func (l *limit) PushOrderByRand() {
	panic("BUG: unreachable")
}

// SetLimit sets the limit for the primitive. It calls the underlying
// primitive's SetUpperLimit, which is an optimization hint that informs
// the underlying primitive that it doesn't need to return more rows than
// specified.
func (l *limit) SetLimit(limit *sqlparser.Limit) error {
	if limit.Offset != nil {
		return errors.New("unsupported: offset limit for cross-shard queries")
	}
	count, ok := limit.Rowcount.(*sqlparser.SQLVal)
	if !ok {
		return fmt.Errorf("unexpected expression in LIMIT: %v", sqlparser.String(limit))
	}
	pv, err := sqlparser.NewPlanValue(count)
	if err != nil {
		return err
	}
	l.elimit.Count = pv
	l.input.SetUpperLimit(count)
	return nil
}

// SetUpperLimit satisfies the builder interface.
// This is a no-op because we actually call SetLimit for this primitive.
// In the future, we may have to honor this call for subqueries.
func (l *limit) SetUpperLimit(count *sqlparser.SQLVal) {
}

// PushMisc satisfies the builder interface.
func (l *limit) PushMisc(sel *sqlparser.Select) {
	l.input.PushMisc(sel)
}

// Wireup satisfies the builder interface.
func (l *limit) Wireup(bldr builder, jt *jointab) error {
	return l.input.Wireup(bldr, jt)
}

// SupplyVar satisfies the builder interface.
func (l *limit) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	l.input.SupplyVar(from, to, col, varname)
}

// SupplyCol satisfies the builder interface.
func (l *limit) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colnum int) {
	panic("BUG: nothing should depend on LIMIT")
}
