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

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
)

var _ builder = (*subquery)(nil)
var _ columnOriginator = (*subquery)(nil)

// subquery is a builder that wraps a subquery.
// TODO(sougou): explain.
type subquery struct {
	order         int
	symtab        *symtab
	resultColumns []*resultColumn
	bldr          builder
	esubquery     *engine.Subquery
}

// newSubquery builds a new subquery.
func newSubquery(alias sqlparser.TableIdent, bldr builder, vschema VSchema) *subquery {
	sq := &subquery{
		order:     bldr.MaxOrder() + 1,
		bldr:      bldr,
		symtab:    newSymtab(vschema, nil),
		esubquery: &engine.Subquery{},
	}

	// Create a 'table' that represents the subquery.
	t := &table{
		alias:  sqlparser.TableName{Name: alias},
		origin: sq,
	}

	// Create column symbols based on the result column names.
	cols := make(map[string]*column)
	for i, rc := range bldr.ResultColumns() {
		cols[rc.alias.Lowered()] = &column{
			origin: sq,
			name:   rc.alias,
			table:  t,
			colnum: i,
		}
	}

	// Populate the table with those columns and add it to symtab.
	t.columns = cols
	if err := sq.symtab.AddTable(t); err != nil {
		panic(err)
	}

	return sq
}

// Symtab returns the associated symtab.
func (sq *subquery) Symtab() *symtab {
	return sq.symtab.Resolve()
}

// Order returns the order of the route.
func (sq *subquery) Order() int {
	return sq.order
}

// MaxOrder returns the max order of the node.
func (sq *subquery) MaxOrder() int {
	return sq.order
}

// SetOrder sets the order to one above the specified number.
func (sq *subquery) SetOrder(order int) {
	sq.bldr.SetOrder(order)
	sq.order = sq.bldr.MaxOrder() + 1
}

// Primitve returns the built primitive.
func (sq *subquery) Primitive() engine.Primitive {
	sq.esubquery.Subquery = sq.bldr.Primitive()
	return sq.esubquery
}

// Leftmost returns the current subquery.
func (sq *subquery) Leftmost() columnOriginator {
	return sq
}

// ResultColumns returns the result columns.
func (sq *subquery) ResultColumns() []*resultColumn {
	return sq.resultColumns
}

// PushFilter is unreachable.
func (sq *subquery) PushFilter(filter sqlparser.Expr, whereType string, rb *route) error {
	panic("unreachable")
}

// PushSelect pushes the select expression to the subquery.
// Only a trivial column expression that reference a result
// column of the subquery is allowed. Anything else results
// in an unsupported error.
func (sq *subquery) PushSelect(expr *sqlparser.AliasedExpr, _ columnOriginator) (rc *resultColumn, colnum int, err error) {
	col, ok := expr.Expr.(*sqlparser.ColName)
	if !ok {
		return nil, 0, errors.New("unsupported: expression on results of a cross-shard subquery")
	}

	// colnum should already be set for subquery columns.
	inner := col.Metadata.(*column).colnum
	sq.esubquery.Cols = append(sq.esubquery.Cols, inner)

	// Build a new column reference to represent the result column.
	rc = sq.Symtab().NewResultColumn(expr, sq)
	sq.resultColumns = append(sq.resultColumns, rc)

	return rc, len(sq.resultColumns) - 1, nil
}

// PushOrderBy is unreachable.
func (sq *subquery) PushOrderBy(order *sqlparser.Order, rb *route) error {
	panic("unreachable")
}

// PushOrderByNull is ignored.
func (sq *subquery) PushOrderByNull() {
}

// PushMisc is ignored.
func (sq *subquery) PushMisc(sel *sqlparser.Select) {
}

// Wireup performs the wireup.
func (sq *subquery) Wireup(bldr builder, jt *jointab) error {
	return sq.bldr.Wireup(bldr, jt)
}

// SupplyVar delegates the request to the inner primitive.
// This can be called if there are dependencies between
// the primitives in the subquery.
func (sq *subquery) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	sq.bldr.SupplyVar(from, to, col, varname)
}

// SupplyCol changes the executor to supply the requested column.
func (sq *subquery) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colnum int) {
	c := col.Metadata.(*column)
	for i, rc := range sq.resultColumns {
		if rc.column == c {
			return rc, i
		}
	}

	// columns that reference subqueries will have their colnum set.
	// Let's use it here.
	sq.esubquery.Cols = append(sq.esubquery.Cols, c.colnum)
	sq.resultColumns = append(sq.resultColumns, &resultColumn{
		alias:  col.Name,
		column: c,
	})
	return rc, len(sq.resultColumns) - 1
}
