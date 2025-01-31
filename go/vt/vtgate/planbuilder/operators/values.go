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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type Values struct {
	unaryOperator

	Name string
	Arg  string
}

func (v *Values) Clone(inputs []Operator) Operator {
	clone := *v
	return &clone
}

func (v *Values) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	return newFilter(v, expr)
}

func (v *Values) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, expr *sqlparser.AliasedExpr) int {
	panic(vterrors.VT13001("we cannot add new columns to a Values operator"))
}

func (v *Values) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	panic(vterrors.VT13001("we cannot add new columns to a Values operator"))
}

func (v *Values) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	col, ok := expr.(*sqlparser.ColName)
	if !ok {
		return -1
	}
	for i, column := range v.getColsFromCtx(ctx) {
		if col.Name.Equal(column) {
			return i
		}
	}
	return -1
}

func (v *Values) getColsFromCtx(ctx *plancontext.PlanningContext) sqlparser.Columns {
	columns, found := ctx.ValuesJoinColumns[v.Arg]
	if !found {
		panic(vterrors.VT13001("columns not found"))
	}
	return columns
}

func (v *Values) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	var cols []*sqlparser.AliasedExpr
	for _, column := range v.getColsFromCtx(ctx) {
		cols = append(cols, sqlparser.NewAliasedExpr(sqlparser.NewColName(column.String()), ""))
	}
	return cols
}

func (v *Values) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	r := v.GetColumns(ctx)
	var selectExprs sqlparser.SelectExprs
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
