/*
Copyright 2023 The Vitess Authors.

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
	"strconv"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type Limit struct {
	Source Operator
	AST    *sqlparser.Limit

	// Pushed marks whether the limit has been pushed down to the inputs but still need to keep the operator around.
	// For example, `select * from user order by id limit 10`. Even after we push the limit to the route, we need a limit on top
	// since it is a scatter.
	Pushed bool
}

func (l *Limit) Clone(inputs []Operator) Operator {
	return &Limit{
		Source: inputs[0],
		AST:    sqlparser.CloneRefOfLimit(l.AST),
		Pushed: l.Pushed,
	}
}

func (l *Limit) Inputs() []Operator {
	return []Operator{l.Source}
}

func (l *Limit) SetInputs(operators []Operator) {
	l.Source = operators[0]
}

func (l *Limit) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	l.Source = l.Source.AddPredicate(ctx, expr)
	return l
}

func (l *Limit) AddColumn(ctx *plancontext.PlanningContext, reuse bool, gb bool, expr *sqlparser.AliasedExpr) int {
	return l.Source.AddColumn(ctx, reuse, gb, expr)
}

func (l *Limit) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return l.Source.FindCol(ctx, expr, underRoute)
}

func (l *Limit) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return l.Source.GetColumns(ctx)
}

func (l *Limit) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	return l.Source.GetSelectExprs(ctx)
}

func (l *Limit) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return l.Source.GetOrdering(ctx)
}

func (l *Limit) ShortDescription() string {
	return sqlparser.String(l.AST) + " Pushed:" + strconv.FormatBool(l.Pushed)
}
