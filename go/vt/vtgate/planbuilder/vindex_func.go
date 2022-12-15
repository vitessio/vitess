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

	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/vt/vtgate/semantics"

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

	// the tableID field is only used by the gen4 planner
	tableID semantics.TableSet

	// resultColumns represent the columns returned by this route.
	resultColumns []*resultColumn

	// eVindexFunc is the primitive being built.
	eVindexFunc *engine.VindexFunc
}

var colnames = []string{
	"id",
	"keyspace_id",
	"range_start",
	"range_end",
	"hex_keyspace_id",
	"shard",
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

	for _, colName := range colnames {
		t.addColumn(sqlparser.NewIdentifierCI(colName), &column{origin: vf})
	}
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

// WireupGen4 implements the logicalPlan interface
func (vf *vindexFunc) WireupGen4(*plancontext.PlanningContext) error {
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
// list.
func (vf *vindexFunc) SupplyProjection(expr *sqlparser.AliasedExpr, reuse bool) (int, error) {
	colName, isColName := expr.Expr.(*sqlparser.ColName)
	if !isColName {
		return 0, vterrors.VT12001("expression on results of a vindex function")
	}

	enum := vindexColumnToIndex(colName)
	if enum == -1 {
		return 0, vterrors.VT03016(colName.Name.String())
	}

	if reuse {
		for i, col := range vf.eVindexFunc.Cols {
			if col == enum {
				return i, nil
			}
		}
	}

	vf.eVindexFunc.Fields = append(vf.eVindexFunc.Fields, &querypb.Field{
		Name: expr.ColumnName(),
		Type: querypb.Type_VARBINARY,
	})
	vf.eVindexFunc.Cols = append(vf.eVindexFunc.Cols, enum)
	return len(vf.eVindexFunc.Cols) - 1, nil
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
func (vf *vindexFunc) SupplyWeightString(colNumber int, alsoAddToGroupBy bool) (weightcolNumber int, err error) {
	return 0, UnsupportedSupplyWeightString{Type: "vindex function"}
}

// Rewrite implements the logicalPlan interface
func (vf *vindexFunc) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 0 {
		return vterrors.VT13001("vindexFunc: wrong number of inputs")
	}
	return nil
}

// ContainsTables implements the logicalPlan interface
func (vf *vindexFunc) ContainsTables() semantics.TableSet {
	return vf.tableID
}

// Inputs implements the logicalPlan interface
func (vf *vindexFunc) Inputs() []logicalPlan {
	return []logicalPlan{}
}

func vindexColumnToIndex(column *sqlparser.ColName) int {
	switch column.Name.String() {
	case "id":
		return 0
	case "keyspace_id":
		return 1
	case "range_start":
		return 2
	case "range_end":
		return 3
	case "hex_keyspace_id":
		return 4
	case "shard":
		return 5
	default:
		return -1
	}
}

// OutputColumns implements the logicalPlan interface
func (vf *vindexFunc) OutputColumns() []sqlparser.SelectExpr {
	exprs := make([]sqlparser.SelectExpr, 0, len(colnames))
	for _, field := range vf.eVindexFunc.Fields {
		exprs = append(exprs, &sqlparser.AliasedExpr{Expr: sqlparser.NewColName(field.Name)})
	}
	return exprs
}
