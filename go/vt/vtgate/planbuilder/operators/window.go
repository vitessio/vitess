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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type Window struct {
	unaryOperator
	QP *QueryProjection
}

func newWindow(source Operator, qp *QueryProjection) *Window {
	return &Window{
		unaryOperator: newUnaryOp(source),
		QP:            qp,
	}
}

func (w *Window) Clone(inputs []Operator) Operator {
	return newWindow(inputs[0], w.QP)
}

func (w *Window) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	w.Source = w.Source.AddPredicate(ctx, expr)
	return w
}

func (w *Window) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, expr *sqlparser.AliasedExpr) int {
	return w.Source.AddColumn(ctx, reuseExisting, addToGroupBy, expr)
}

func (w *Window) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	return w.Source.AddWSColumn(ctx, offset, underRoute)
}

func (w *Window) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return w.Source.FindCol(ctx, expr, underRoute)
}

func (w *Window) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return w.Source.GetColumns(ctx)
}

func (w *Window) GetSelectExprs(ctx *plancontext.PlanningContext) []sqlparser.SelectExpr {
	return w.Source.GetSelectExprs(ctx)
}

func (w *Window) ShortDescription() string {
	return "Window"
}

func (w *Window) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return w.Source.GetOrdering(ctx)
}
