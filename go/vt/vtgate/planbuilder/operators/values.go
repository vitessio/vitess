/*
Copyright 2025 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type Values struct {
	unaryOperator

	Name    string
	TableID semantics.TableSet
}

func (v *Values) Clone(inputs []Operator) Operator {
	clone := *v

	if len(inputs) > 0 {
		clone.Source = inputs[0]
	}
	return &clone
}

func (v *Values) AddPredicate(_ *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	return newFilter(v, expr)
}

func (v *Values) AddColumn(*plancontext.PlanningContext, bool, bool, *sqlparser.AliasedExpr) int {
	panic(vterrors.VT13001("we cannot add new columns to a Values operator"))
}

func (v *Values) AddWSColumn(*plancontext.PlanningContext, int, bool) int {
	panic(vterrors.VT13001("we cannot add new columns to a Values operator"))
}

func (v *Values) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, _ bool) int {
	for i, column := range v.getExprsFromCtx(ctx) {
		if ctx.SemTable.EqualsExpr(column, expr) {
			return i
		}
	}
	return -1
}

func (v *Values) getColumnNamesFromCtx(ctx *plancontext.PlanningContext) sqlparser.Columns {
	columns, found := ctx.ValuesJoinColumns[v.Name]
	if !found {
		panic(vterrors.VT13001("columns not found"))
	}
	return slice.Map(columns, func(ae *sqlparser.AliasedExpr) sqlparser.IdentifierCI {
		return sqlparser.NewIdentifierCI(ae.ColumnName())
	})
}

func (v *Values) getExprsFromCtx(ctx *plancontext.PlanningContext) []sqlparser.Expr {
	columns := ctx.ValuesJoinColumns[v.Name]
	return slice.Map(columns, func(ae *sqlparser.AliasedExpr) sqlparser.Expr {
		return ae.Expr
	})
}

func (v *Values) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	columns := ctx.ValuesJoinColumns[v.Name]
	return columns
}

func (v *Values) GetSelectExprs(ctx *plancontext.PlanningContext) []sqlparser.SelectExpr {
	r := v.GetColumns(ctx)
	var selectExprs []sqlparser.SelectExpr
	for _, expr := range r {
		selectExprs = append(selectExprs, expr)
	}
	return selectExprs
}

func (v *Values) ShortDescription() string {
	return v.Name
}

func (v *Values) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return v.Source.GetOrdering(ctx)
}

func (v *Values) introducesTableID() semantics.TableSet {
	return v.TableID
}
