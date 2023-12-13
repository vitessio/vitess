/*
Copyright 2022 The Vitess Authors.

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
	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	Vindex struct {
		OpCode  engine.VindexOpcode
		Table   VindexTable
		Vindex  vindexes.Vindex
		Solved  semantics.TableSet
		Columns []*sqlparser.ColName
		Value   sqlparser.Expr

		noInputs
	}

	// VindexTable contains information about the vindex table we want to query
	VindexTable struct {
		TableID    semantics.TableSet
		Alias      *sqlparser.AliasedTableExpr
		Table      sqlparser.TableName
		Predicates []sqlparser.Expr
		VTable     *vindexes.Table
	}
)

const wrongWhereCond = "WHERE clause for vindex function must be of the form id = <val> or id in(<val>,...)"

// Introduces implements the Operator interface
func (v *Vindex) introducesTableID() semantics.TableSet {
	return v.Solved
}

// Clone implements the Operator interface
func (v *Vindex) Clone([]Operator) Operator {
	clone := *v
	return &clone
}

func (v *Vindex) AddColumn(ctx *plancontext.PlanningContext, reuse bool, gb bool, ae *sqlparser.AliasedExpr) int {
	if gb {
		panic(vterrors.VT13001("tried to add group by to a table"))
	}
	if reuse {
		offset := v.FindCol(ctx, ae.Expr, true)
		if offset > -1 {
			return offset
		}
	}

	return addColumn(ctx, v, ae.Expr)
}

func colNameToExpr(c *sqlparser.ColName) *sqlparser.AliasedExpr {
	return &sqlparser.AliasedExpr{
		Expr: c,
		As:   sqlparser.IdentifierCI{},
	}
}

func (v *Vindex) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	for idx, col := range v.Columns {
		if ctx.SemTable.EqualsExprWithDeps(expr, col) {
			return idx
		}
	}

	return -1
}

func (v *Vindex) GetColumns(*plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return slice.Map(v.Columns, colNameToExpr)
}

func (v *Vindex) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	return transformColumnsToSelectExprs(ctx, v)
}

func (v *Vindex) GetOrdering(*plancontext.PlanningContext) []OrderBy {
	return nil
}

func (v *Vindex) GetColNames() []*sqlparser.ColName {
	return v.Columns
}

func (v *Vindex) AddCol(col *sqlparser.ColName) {
	v.Columns = append(v.Columns, col)
}

func (v *Vindex) CheckValid() {
	if len(v.Table.Predicates) == 0 {
		panic(vterrors.VT09018(wrongWhereCond + " (where clause missing)"))
	}
}

func (v *Vindex) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	for _, e := range sqlparser.SplitAndExpression(nil, expr) {
		deps := ctx.SemTable.RecursiveDeps(e)
		if deps.NumberOfTables() > 1 {
			panic(vterrors.VT09018(wrongWhereCond + " (multiple tables involved)"))
		}
		// check if we already have a predicate
		if v.OpCode != engine.VindexNone {
			panic(vterrors.VT09018(wrongWhereCond + " (multiple filters)"))
		}

		// check LHS
		comparison, ok := e.(*sqlparser.ComparisonExpr)
		if !ok {
			panic(vterrors.VT09018(wrongWhereCond + " (not a comparison)"))
		}
		if comparison.Operator != sqlparser.EqualOp && comparison.Operator != sqlparser.InOp {
			panic(vterrors.VT09018(wrongWhereCond + " (not equality)"))
		}
		colname, ok := comparison.Left.(*sqlparser.ColName)
		if !ok {
			panic(vterrors.VT09018(wrongWhereCond + " (lhs is not a column)"))
		}
		if !colname.Name.EqualString("id") {
			panic(vterrors.VT09018(wrongWhereCond + " (lhs is not id)"))
		}

		// check RHS
		if sqlparser.IsValue(comparison.Right) || sqlparser.IsSimpleTuple(comparison.Right) {
			v.Value = comparison.Right
		} else {
			panic(vterrors.VT09018(wrongWhereCond + " (rhs is not a value)"))
		}

		v.OpCode = engine.VindexMap
		v.Table.Predicates = append(v.Table.Predicates, e)
	}
	return v
}

// TablesUsed implements the Operator interface.
// It is not keyspace-qualified.
func (v *Vindex) TablesUsed() []string {
	return []string{v.Table.Table.Name.String()}
}

func (v *Vindex) ShortDescription() string {
	return v.Vindex.String()
}
