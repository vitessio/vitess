/*
Copyright 2021 The Vitess Authors.

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

package operators

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	Table struct {
		QTable  *QueryTable
		VTable  *vindexes.Table
		Columns []*sqlparser.ColName

		noInputs
	}
	ColNameColumns interface {
		GetColumns() []*sqlparser.ColName
		AddCol(*sqlparser.ColName)
	}
)

var _ PhysicalOperator = (*Table)(nil)

// IPhysical implements the PhysicalOperator interface
func (to *Table) IPhysical() {}

// Clone implements the Operator interface
func (to *Table) Clone(inputs []Operator) Operator {
	checkSize(inputs, 0)
	var columns []*sqlparser.ColName
	for _, name := range to.Columns {
		columns = append(columns, sqlparser.CloneRefOfColName(name))
	}
	return &Table{
		QTable:  to.QTable,
		VTable:  to.VTable,
		Columns: columns,
	}
}

// Introduces implements the PhysicalOperator interface
func (to *Table) Introduces() semantics.TableSet {
	return to.QTable.ID
}

// AddPredicate implements the PhysicalOperator interface
func (to *Table) AddPredicate(_ *plancontext.PlanningContext, expr sqlparser.Expr) (Operator, error) {
	return newFilter(to, expr), nil
}

func (to *Table) AddColumn(_ *plancontext.PlanningContext, e sqlparser.Expr) (int, error) {
	return addColumn(to, e)
}

func (to *Table) GetColumns() []*sqlparser.ColName {
	return to.Columns
}
func (to *Table) AddCol(col *sqlparser.ColName) {
	to.Columns = append(to.Columns, col)
}

func addColumn(op ColNameColumns, e sqlparser.Expr) (int, error) {
	col, ok := e.(*sqlparser.ColName)
	if !ok {
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "can't push this expression to a table/vindex")
	}
	cols := op.GetColumns()
	for idx, column := range cols {
		if col.Name.Equal(column.Name) {
			return idx, nil
		}
	}
	offset := len(cols)
	op.AddCol(col)
	return offset, nil
}
