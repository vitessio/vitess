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
	"fmt"

	"vitess.io/vitess/go/slice"
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
		GetColNames() []*sqlparser.ColName
		AddCol(*sqlparser.ColName)
	}
)

// Clone implements the Operator interface
func (to *Table) Clone([]Operator) Operator {
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
func (to *Table) introducesTableID() semantics.TableSet {
	return to.QTable.ID
}

// AddPredicate implements the PhysicalOperator interface
func (to *Table) AddPredicate(_ *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	return newFilter(to, expr)
}

func (to *Table) AddColumn(*plancontext.PlanningContext, bool, bool, *sqlparser.AliasedExpr) int {
	panic(vterrors.VT13001("did not expect this method to be called"))
}

func (to *Table) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	colToFind, ok := expr.(*sqlparser.ColName)
	if !ok {
		return -1
	}

	for idx, colName := range to.Columns {
		if colName.Name.Equal(colToFind.Name) {
			return idx
		}
	}

	return -1
}

func (to *Table) GetColumns(*plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return slice.Map(to.Columns, colNameToExpr)
}

func (to *Table) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	return transformColumnsToSelectExprs(ctx, to)
}

func (to *Table) GetOrdering(*plancontext.PlanningContext) []OrderBy {
	return nil
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

func addColumn(ctx *plancontext.PlanningContext, op ColNameColumns, e sqlparser.Expr) int {
	col, ok := e.(*sqlparser.ColName)
	if !ok {
		panic(vterrors.VT09018(fmt.Sprintf("cannot add '%s' expression to a table/vindex", sqlparser.String(e))))
	}
	sqlparser.RemoveKeyspaceInCol(col)
	cols := op.GetColNames()
	colAsExpr := func(c *sqlparser.ColName) sqlparser.Expr { return c }
	if offset, found := canReuseColumn(ctx, cols, e, colAsExpr); found {
		return offset
	}
	offset := len(cols)
	op.AddCol(col)
	return offset
}

func (to *Table) ShortDescription() string {
	tbl := to.VTable.String()
	var alias, where string
	if to.QTable.Alias.As.NotEmpty() {
		alias = " AS " + to.QTable.Alias.As.String()
	}

	if len(to.QTable.Predicates) > 0 {
		where = " WHERE " + sqlparser.String(sqlparser.AndExpressions(to.QTable.Predicates...))
	}

	return tbl + alias + where
}
