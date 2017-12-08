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
// This primitive wraps any subquery that results
// in something that's not a route. It builds a
// 'table' for the subquery allowing higher level
// constructs to reference its columns. If a subquery
// results in a route primitive, we instead build
// a new route that keeps the subquery in the FROM
// clause, because a route is more versatile than
// a subquery.
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
		symtab:    newSymtab(vschema),
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
	// AddTable will not fail because symtab is empty.
	_ = sq.symtab.AddTable(t)
	return sq
}

// Symtab satisfies the builder interface.
func (sq *subquery) Symtab() *symtab {
	return sq.symtab.Resolve()
}

// Order returns the order of the subquery.
func (sq *subquery) Order() int {
	return sq.order
}

// MaxOrder satisfies the builder interface.
func (sq *subquery) MaxOrder() int {
	return sq.order
}

// SetOrder satisfies the builder interface.
func (sq *subquery) SetOrder(order int) {
	sq.bldr.SetOrder(order)
	sq.order = sq.bldr.MaxOrder() + 1
}

// Primitive satisfies the builder interface.
func (sq *subquery) Primitive() engine.Primitive {
	sq.esubquery.Subquery = sq.bldr.Primitive()
	return sq.esubquery
}

// Leftmost satisfies the builder interface.
func (sq *subquery) Leftmost() columnOriginator {
	return sq
}

// ResultColumns satisfies the builder interface.
func (sq *subquery) ResultColumns() []*resultColumn {
	return sq.resultColumns
}

// PushFilter satisfies the builder interface.
func (sq *subquery) PushFilter(_ sqlparser.Expr, whereType string, _ columnOriginator) error {
	return errors.New("unsupported: filtering on results of cross-shard subquery")
}

// PushSelect satisfies the builder interface.
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

// PushOrderByNull satisfies the builder interface.
func (sq *subquery) PushOrderByNull() {
}

// PushOrderByRand satisfies the builder interface.
func (sq *subquery) PushOrderByRand() {
}

// SetUpperLimit satisfies the builder interface.
// For now, the call is ignored because the
// repercussions of pushing this limit down
// into a subquery have not been studied yet.
// We can consider doing it in the future.
// TODO(sougou): this could be improved.
func (sq *subquery) SetUpperLimit(_ *sqlparser.SQLVal) {
}

// PushMisc satisfies the builder interface.
func (sq *subquery) PushMisc(sel *sqlparser.Select) {
}

// Wireup satisfies the builder interface.
func (sq *subquery) Wireup(bldr builder, jt *jointab) error {
	return sq.bldr.Wireup(bldr, jt)
}

// SupplyVar satisfies the builder interface.
func (sq *subquery) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	sq.bldr.SupplyVar(from, to, col, varname)
}

// SupplyCol satisfies the builder interface.
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
	sq.resultColumns = append(sq.resultColumns, &resultColumn{column: c})
	return rc, len(sq.resultColumns) - 1
}
