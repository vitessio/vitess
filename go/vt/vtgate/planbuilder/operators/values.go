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
	"fmt"
	"slices"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type Values struct {
	unaryOperator

	Columns sqlparser.Columns
	Name    string
	Arg     string

	// TODO: let's see if we want to have noColumns or no
	// noColumns
}

func (v *Values) Clone(inputs []Operator) Operator {
	clone := *v
	clone.Columns = slices.Clone(v.Columns)
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
	for i, column := range v.Columns {
		if col.Name.Equal(column) {
			return i
		}
	}
	return -1
}

func (v *Values) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	var cols []*sqlparser.AliasedExpr
	for _, column := range v.Columns {
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
	return fmt.Sprintf("%s (%s)", v.Name, sqlparser.String(v.Columns))
}

func (v *Values) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return v.Source.GetOrdering(ctx)
}
