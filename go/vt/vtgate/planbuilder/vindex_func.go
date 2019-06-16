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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ builder = (*vindexFunc)(nil)

// vindexFunc is used to build a VindexFunc primitive.
type vindexFunc struct {
	order int

	// resultColumns represent the columns returned by this route.
	resultColumns []*resultColumn

	// eVindexFunc is the primitive being built.
	eVindexFunc *engine.VindexFunc
}

func newVindexFunc(alias sqlparser.TableName, vindex vindexes.Vindex) (*vindexFunc, *symtab) {
	vf := &vindexFunc{
		order: 1,
		eVindexFunc: &engine.VindexFunc{
			Vindex: vindex,
		},
	}

	// Create a 'table' that represents the vindex.
	t := &table{
		alias:  alias,
		origin: vf,
	}

	// Column names are hard-coded to id, keyspace_id
	t.addColumn(sqlparser.NewColIdent("id"), &column{origin: vf})
	t.addColumn(sqlparser.NewColIdent("keyspace_id"), &column{origin: vf})
	t.addColumn(sqlparser.NewColIdent("range_start"), &column{origin: vf})
	t.addColumn(sqlparser.NewColIdent("range_end"), &column{origin: vf})
	t.isAuthoritative = true

	st := newSymtab()
	// AddTable will not fail because symtab is empty.
	_ = st.AddTable(t)
	return vf, st
}

// Order satisfies the builder interface.
func (vf *vindexFunc) Order() int {
	return vf.order
}

// Reorder satisfies the builder interface.
func (vf *vindexFunc) Reorder(order int) {
	vf.order = order + 1
}

// Primitive satisfies the builder interface.
func (vf *vindexFunc) Primitive() engine.Primitive {
	return vf.eVindexFunc
}

// First satisfies the builder interface.
func (vf *vindexFunc) First() builder {
	return vf
}

// ResultColumns satisfies the builder interface.
func (vf *vindexFunc) ResultColumns() []*resultColumn {
	return vf.resultColumns
}

// PushFilter satisfies the builder interface.
// Only some where clauses are allowed.
func (vf *vindexFunc) PushFilter(pb *primitiveBuilder, filter sqlparser.Expr, whereType string, _ builder) error {
	if vf.eVindexFunc.Opcode != engine.VindexNone {
		return errors.New("unsupported: where clause for vindex function must be of the form id = <val> (multiple filters)")
	}

	// Check LHS.
	comparison, ok := filter.(*sqlparser.ComparisonExpr)
	if !ok {
		return errors.New("unsupported: where clause for vindex function must be of the form id = <val> (not a comparison)")
	}
	if comparison.Operator != sqlparser.EqualStr {
		return errors.New("unsupported: where clause for vindex function must be of the form id = <val> (not equality)")
	}
	colname, ok := comparison.Left.(*sqlparser.ColName)
	if !ok {
		return errors.New("unsupported: where clause for vindex function must be of the form id = <val> (lhs is not a column)")
	}
	if !colname.Name.EqualString("id") {
		return errors.New("unsupported: where clause for vindex function must be of the form id = <val> (lhs is not id)")
	}

	// Check RHS.
	// We have to check before calling NewPlanValue because NewPlanValue allows lists also.
	if !sqlparser.IsValue(comparison.Right) {
		return errors.New("unsupported: where clause for vindex function must be of the form id = <val> (rhs is not a value)")
	}
	var err error
	vf.eVindexFunc.Value, err = sqlparser.NewPlanValue(comparison.Right)
	if err != nil {
		return fmt.Errorf("unsupported: where clause for vindex function must be of the form id = <val>: %v", err)
	}
	vf.eVindexFunc.Opcode = engine.VindexMap
	return nil
}

// PushSelect satisfies the builder interface.
func (vf *vindexFunc) PushSelect(_ *primitiveBuilder, expr *sqlparser.AliasedExpr, _ builder) (rc *resultColumn, colNumber int, err error) {
	// Catch the case where no where clause was specified. If so, the opcode
	// won't be set.
	if vf.eVindexFunc.Opcode == engine.VindexNone {
		return nil, 0, errors.New("unsupported: where clause for vindex function must be of the form id = <val> (where clause missing)")
	}
	col, ok := expr.Expr.(*sqlparser.ColName)
	if !ok {
		return nil, 0, errors.New("unsupported: expression on results of a vindex function")
	}
	rc = newResultColumn(expr, vf)
	vf.resultColumns = append(vf.resultColumns, rc)
	vf.eVindexFunc.Fields = append(vf.eVindexFunc.Fields, &querypb.Field{
		Name: rc.alias.String(),
		Type: querypb.Type_VARBINARY,
	})
	vf.eVindexFunc.Cols = append(vf.eVindexFunc.Cols, col.Metadata.(*column).colNumber)
	return rc, len(vf.resultColumns) - 1, nil
}

// MakeDistinct satisfies the builder interface.
func (vf *vindexFunc) MakeDistinct() error {
	return errors.New("unsupported: distinct on vindex function")
}

// PushGroupBy satisfies the builder interface.
func (vf *vindexFunc) PushGroupBy(groupBy sqlparser.GroupBy) error {
	if (groupBy) == nil {
		return nil
	}
	return errors.New("unupported: group by on vindex function")
}

// PushOrderBy satisfies the builder interface.
func (vf *vindexFunc) PushOrderBy(orderBy sqlparser.OrderBy) (builder, error) {
	if len(orderBy) == 0 {
		return vf, nil
	}
	return newMemorySort(vf, orderBy)
}

// SetUpperLimit satisfies the builder interface.
func (vf *vindexFunc) SetUpperLimit(_ *sqlparser.SQLVal) {
}

// PushMisc satisfies the builder interface.
func (vf *vindexFunc) PushMisc(sel *sqlparser.Select) {
}

// Wireup satisfies the builder interface.
func (vf *vindexFunc) Wireup(bldr builder, jt *jointab) error {
	return nil
}

// SupplyVar satisfies the builder interface.
func (vf *vindexFunc) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	// vindexFunc is an atomic primitive. So, SupplyVar cannot be
	// called on it.
	panic("BUG: vindexFunc is an atomic node.")
}

// SupplyCol satisfies the builder interface.
func (vf *vindexFunc) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	c := col.Metadata.(*column)
	for i, rc := range vf.resultColumns {
		if rc.column == c {
			return rc, i
		}
	}

	vf.resultColumns = append(vf.resultColumns, &resultColumn{column: c})
	vf.eVindexFunc.Fields = append(vf.eVindexFunc.Fields, &querypb.Field{
		Name: col.Name.String(),
		Type: querypb.Type_VARBINARY,
	})

	// columns that reference vindexFunc will have their colNumber set.
	// Let's use it here.
	vf.eVindexFunc.Cols = append(vf.eVindexFunc.Cols, c.colNumber)
	return rc, len(vf.resultColumns) - 1
}
