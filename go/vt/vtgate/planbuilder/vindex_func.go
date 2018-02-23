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
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

var _ builder = (*vindexFunc)(nil)
var _ columnOriginator = (*vindexFunc)(nil)

// vindexFunc is used to build a VindexFunc primitive.
type vindexFunc struct {
	order  int
	symtab *symtab

	// resultColumns represent the columns returned by this route.
	resultColumns []*resultColumn

	// eVindexFunc is the primitive being built.
	eVindexFunc *engine.VindexFunc
}

func newVindexFunc(alias sqlparser.TableName, vindex vindexes.Vindex, vschema VSchema) *vindexFunc {
	vf := &vindexFunc{
		symtab: newSymtab(vschema),
		order:  1,
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
	t.columns = map[string]*column{
		"id": {
			origin: vf,
			name:   sqlparser.NewColIdent("id"),
			table:  t,
			colnum: 0,
		},
		"keyspace_id": {
			origin: vf,
			name:   sqlparser.NewColIdent("keyspace_id"),
			table:  t,
			colnum: 1,
		},
	}

	// AddTable will not fail because symtab is empty.
	_ = vf.symtab.AddTable(t)
	return vf
}

// Symtab satisfies the builder interface.
func (vf *vindexFunc) Symtab() *symtab {
	return vf.symtab.Resolve()
}

// Order returns the order of the subquery.
func (vf *vindexFunc) Order() int {
	return vf.order
}

// MaxOrder satisfies the builder interface.
func (vf *vindexFunc) MaxOrder() int {
	return vf.order
}

// SetOrder satisfies the builder interface.
func (vf *vindexFunc) SetOrder(order int) {
	vf.order = order + 1
}

// Primitive satisfies the builder interface.
func (vf *vindexFunc) Primitive() engine.Primitive {
	return vf.eVindexFunc
}

// Leftmost satisfies the builder interface.
func (vf *vindexFunc) Leftmost() columnOriginator {
	return vf
}

// ResultColumns satisfies the builder interface.
func (vf *vindexFunc) ResultColumns() []*resultColumn {
	return vf.resultColumns
}

// PushFilter satisfies the builder interface.
// Only some where clauses are allowed.
func (vf *vindexFunc) PushFilter(filter sqlparser.Expr, whereType string, _ columnOriginator) error {
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
func (vf *vindexFunc) PushSelect(expr *sqlparser.AliasedExpr, _ columnOriginator) (rc *resultColumn, colnum int, err error) {
	// Catch the case where no where clause was specified. If so, the opcode
	// won't be set.
	if vf.eVindexFunc.Opcode == engine.VindexNone {
		return nil, 0, errors.New("unsupported: where clause for vindex function must be of the form id = <val> (where clause missing)")
	}
	col, ok := expr.Expr.(*sqlparser.ColName)
	if !ok {
		return nil, 0, errors.New("unsupported: expression on results of a vindex function")
	}
	rc = vf.symtab.NewResultColumn(expr, vf)
	vf.resultColumns = append(vf.resultColumns, rc)
	vf.eVindexFunc.Fields = append(vf.eVindexFunc.Fields, &querypb.Field{
		Name: rc.alias.String(),
		Type: querypb.Type_VARBINARY,
	})
	switch {
	case col.Name.EqualString("id"):
		vf.eVindexFunc.Cols = append(vf.eVindexFunc.Cols, 0)
	case col.Name.EqualString("keyspace_id"):
		vf.eVindexFunc.Cols = append(vf.eVindexFunc.Cols, 1)
	case col.Name.EqualString("range_start"):
		vf.eVindexFunc.Cols = append(vf.eVindexFunc.Cols, 2)
	case col.Name.EqualString("range_end"):
		vf.eVindexFunc.Cols = append(vf.eVindexFunc.Cols, 3)
	default:
		return nil, 0, fmt.Errorf("unrecognized column %s for vindex: %s", col.Name, vf.eVindexFunc.Vindex)
	}
	return rc, len(vf.resultColumns) - 1, nil
}

// PushOrderByNull satisfies the builder interface.
func (vf *vindexFunc) PushOrderByNull() {
}

// PushOrderByRand satisfies the builder interface.
func (vf *vindexFunc) PushOrderByRand() {
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
	panic("BUG: route is an atomic node.")
}

// SupplyCol satisfies the builder interface.
func (vf *vindexFunc) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colnum int) {
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

	// columns that reference vindexFunc will have their colnum set.
	// Let's use it here.
	vf.eVindexFunc.Cols = append(vf.eVindexFunc.Cols, c.colnum)
	return rc, len(vf.resultColumns) - 1
}
