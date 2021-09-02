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
	"fmt"

	"vitess.io/vitess/go/vt/vtgate/semantics"

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
	t.addColumn(sqlparser.NewColIdent("hex_keyspace_id"), &column{origin: vf})
	t.addColumn(sqlparser.NewColIdent("shard"), &column{origin: vf})
	t.isAuthoritative = true

	st := newSymtab()
	// AddTable will not fail because symtab is empty.
	_ = st.AddTable(t)
	return vf, st
}

// Order implements the logicalPlan interface
func (vf *vindexFunc) Order() int {
	return vf.order
}

// Reorder implements the logicalPlan interface
func (vf *vindexFunc) Reorder(order int) {
	vf.order = order + 1
}

// Primitive implements the logicalPlan interface
func (vf *vindexFunc) Primitive() engine.Primitive {
	return vf.eVindexFunc
}

// ResultColumns implements the logicalPlan interface
func (vf *vindexFunc) ResultColumns() []*resultColumn {
	return vf.resultColumns
}

// Wireup implements the logicalPlan interface
func (vf *vindexFunc) Wireup(logicalPlan, *jointab) error {
	return nil
}

// Wireup2 implements the logicalPlan interface
func (vf *vindexFunc) WireupGen4(*semantics.SemTable) error {
	return nil
}

// SupplyVar implements the logicalPlan interface
func (vf *vindexFunc) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	// vindexFunc is an atomic primitive. So, SupplyVar cannot be
	// called on it.
	panic("BUG: vindexFunc is an atomic node.")
}

// SupplyCol implements the logicalPlan interface
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

// SupplyProjection pushes the given aliased expression into the fields and cols slices of the
// vindexFunc engine primitive. The method returns the offset of the new expression in the columns
// list. If the column already exists, -1 will be returned as the offset along with a nil error.
func (vf *vindexFunc) SupplyProjection(expr *sqlparser.AliasedExpr) (int, error) {
	colName, isColName := expr.Expr.(*sqlparser.ColName)
	if !isColName {
		return 0, vterrors.New(vtrpcpb.Code_INTERNAL, "unsupported: expression on results of a vindex function")
	}

	for _, field := range vf.eVindexFunc.Fields {
		if field.Name == colName.Name.String() {
			return -1, nil
		}
	}
	vf.eVindexFunc.Fields = append(vf.eVindexFunc.Fields, &querypb.Field{
		Name: colName.Name.String(),
		Type: querypb.Type_VARBINARY,
	})
	index := vindexColumnToIndex(colName)
	if index == -1 {
		return 0, semantics.Gen4NotSupportedF("unknown vindex column: %s", colName.Name.String())
	}
	vf.eVindexFunc.Cols = append(vf.eVindexFunc.Cols, index)
	return len(vf.eVindexFunc.Fields) - 1, nil
}

// UnsupportedSupplyWeightString represents the error where the supplying a weight string is not supported
type UnsupportedSupplyWeightString struct {
	Type string
}

// Error function implements the error interface
func (err UnsupportedSupplyWeightString) Error() string {
	return fmt.Sprintf("cannot do collation on %s", err.Type)
}

// SupplyWeightString implements the logicalPlan interface
func (vf *vindexFunc) SupplyWeightString(colNumber int) (weightcolNumber int, err error) {
	return 0, UnsupportedSupplyWeightString{Type: "vindex function"}
}

// Rewrite implements the logicalPlan interface
func (vf *vindexFunc) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 0 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vindexFunc: wrong number of inputs")
	}
	return nil
}

// ContainsTables implements the logicalPlan interface
func (vf *vindexFunc) ContainsTables() semantics.TableSet {
	return 0
}

// Inputs implements the logicalPlan interface
func (vf *vindexFunc) Inputs() []logicalPlan {
	return []logicalPlan{}
}

func vindexColumnToIndex(column *sqlparser.ColName) int {
	switch column.Name.String() {
	case "hex_keyspace_id":
		return 0
	case "id":
		return 1
	case "keyspace_id":
		return 2
	case "range_start":
		return 3
	case "range_end":
		return 4
	case "shard":
		return 5
	default:
		return -1
	}
}
