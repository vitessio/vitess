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
	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
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
		GetColNames() []*sqlparser.ColName
		AddCol(*sqlparser.ColName)
	}
)

// Clone implements the Operator interface
func (to *Table) Clone([]ops.Operator) ops.Operator {
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
func (to *Table) AddPredicate(_ *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	return newFilter(to, expr), nil
}

func (to *Table) AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr) (ops.Operator, int, error) {
	offset, err := addColumn(ctx, to, expr.Expr)
	if err != nil {
		return nil, 0, err
	}

	return to, offset, nil
}

func (to *Table) GetColumns() ([]*sqlparser.AliasedExpr, error) {
	return slices2.Map(to.Columns, colNameToExpr), nil
}

func (to *Table) GetOrdering() ([]ops.OrderBy, error) {
	return nil, nil
}

func (to *Table) GetColNames() []*sqlparser.ColName {
	return to.Columns
}
func (to *Table) AddCol(col *sqlparser.ColName) {
	to.Columns = append(to.Columns, col)
}

func (to *Table) TablesUsed() []string {
	if sqlparser.SystemSchema(to.QTable.Table.Qualifier.String()) {
		return nil
	}
	return SingleQualifiedIdentifier(to.VTable.Keyspace, to.VTable.Name)
}

func addColumn(ctx *plancontext.PlanningContext, op ColNameColumns, e sqlparser.Expr) (int, error) {
	col, ok := e.(*sqlparser.ColName)
	if !ok {
		return 0, vterrors.VT13001("cannot push this expression to a table/vindex")
	}
	sqlparser.RemoveKeyspaceFromColName(col)
	cols := op.GetColNames()
	colAsExpr := func(c *sqlparser.ColName) sqlparser.Expr { return c }
	if offset, found := canReuseColumn(ctx, cols, e, colAsExpr); found {
		return offset, nil
	}
	offset := len(cols)
	op.AddCol(col)
	return offset, nil
}

func (to *Table) Description() ops.OpDescription {
	var columns []string
	for _, col := range to.Columns {
		columns = append(columns, sqlparser.String(col))
	}
	return ops.OpDescription{
		OperatorType: "Table",
		Other:        map[string]any{"Columns": columns},
	}
}

func (to *Table) ShortDescription() string {
	return to.VTable.String()
}
