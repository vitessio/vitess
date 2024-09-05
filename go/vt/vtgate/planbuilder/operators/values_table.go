/*
Copyright 2024 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

var _ Operator = (*ValuesTable)(nil)

// ValuesTable is used to represent a VALUES clause in a query
type ValuesTable struct {
	unaryOperator

	ListArgName string
	TableName   string
}

func (v *ValuesTable) Clone(inputs []Operator) Operator {
	nv := *v
	nv.Source = inputs[0]
	return &nv
}

func (v *ValuesTable) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	v.Source = v.Source.AddPredicate(ctx, expr)
	return v
}

func (v *ValuesTable) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, expr *sqlparser.AliasedExpr) int {
	return v.Source.AddColumn(ctx, reuseExisting, addToGroupBy, expr)
}

func (v *ValuesTable) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	return v.Source.AddWSColumn(ctx, offset, underRoute)
}

func (v *ValuesTable) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return v.Source.FindCol(ctx, expr, underRoute)
}

func (v *ValuesTable) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return v.Source.GetColumns(ctx)
}

func (v *ValuesTable) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	return v.Source.GetSelectExprs(ctx)
}

func (v *ValuesTable) ShortDescription() string {
	return ""
}

func (v *ValuesTable) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return v.Source.GetOrdering(ctx)
}
