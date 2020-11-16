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

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ logicalPlan = (*vindexFunc)(nil)

// vindexFunc is used to build a VindexFunc primitive.
type vindexFunc struct {
	order int

	// resultColumns represent the columns returned by this route.
	resultColumns []*resultColumn

	// eVindexFunc is the primitive being built.
	eVindexFunc *engine.VindexFunc
}

func newVindexFunc(alias sqlparser.TableName, vindex vindexes.SingleColumn) (*vindexFunc, *symtab) {
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

// Order satisfies the logicalPlan interface.
func (vf *vindexFunc) Order() int {
	return vf.order
}

// Reorder satisfies the logicalPlan interface.
func (vf *vindexFunc) Reorder(order int) {
	vf.order = order + 1
}

// Primitive satisfies the logicalPlan interface.
func (vf *vindexFunc) Primitive() engine.Primitive {
	return vf.eVindexFunc
}

// PushLock satisfies the logicalPlan interface.
func (vf *vindexFunc) PushLock(lock sqlparser.Lock) error {
	return nil
}

// ResultColumns satisfies the logicalPlan interface.
func (vf *vindexFunc) ResultColumns() []*resultColumn {
	return vf.resultColumns
}

// Wireup satisfies the logicalPlan interface.
func (vf *vindexFunc) Wireup(bldr logicalPlan, jt *jointab) error {
	return nil
}

// SupplyVar satisfies the logicalPlan interface.
func (vf *vindexFunc) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	// vindexFunc is an atomic primitive. So, SupplyVar cannot be
	// called on it.
	panic("BUG: vindexFunc is an atomic node.")
}

// SupplyCol satisfies the logicalPlan interface.
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

// SupplyWeightString satisfies the logicalPlan interface.
func (vf *vindexFunc) SupplyWeightString(colNumber int) (weightcolNumber int, err error) {
	return 0, errors.New("cannot do collation on vindex function")
}

func (vf *vindexFunc) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 0 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vindexFunc: wrong number of inputs")
	}

	return nil
}

func (vf *vindexFunc) Inputs() []logicalPlan {
	return []logicalPlan{}
}
