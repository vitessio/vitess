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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// Values is used to represent a VALUES derived table clause in a query.
// Its only function is to add the `FROM (VALUES ::ARG) AS values(tbl_col1, tbl_col2, ...)` clause to the query.
// That is why we pass everything through it. Also - since it can only add itself to the SQL query,
// it _must_ be pushed under a route.
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

func (v *Values) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	v.Source = v.Source.AddPredicate(ctx, expr)
	return v
}

func (v *Values) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, expr *sqlparser.AliasedExpr) int {
	return v.Source.AddColumn(ctx, reuseExisting, addToGroupBy, expr)
}

func (v *Values) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	return v.Source.AddWSColumn(ctx, offset, underRoute)
}

func (v *Values) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return v.Source.FindCol(ctx, expr, underRoute)
}

func (v *Values) getColumnNamesFromCtx(ctx *plancontext.PlanningContext) sqlparser.Columns {
	columns := ctx.GetValuesColumns(v.Name)
	return slice.Map(columns, func(ae *sqlparser.AliasedExpr) sqlparser.IdentifierCI {
		return sqlparser.NewIdentifierCI(ae.ColumnName())
	})
}

func (v *Values) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return v.Source.GetColumns(ctx)
}

func (v *Values) GetSelectExprs(ctx *plancontext.PlanningContext) []sqlparser.SelectExpr {
	return v.Source.GetSelectExprs(ctx)
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
