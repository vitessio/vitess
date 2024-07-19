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
	"slices"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// Recurse is used to represent a recursive CTE
type Recurse struct {
	// Name is the name of the recursive CTE
	Name string

	// ColumnNames is the list of column names that are sent between the two parts of the recursive CTE
	ColumnNames []string

	// ColumnOffsets is the list of column offsets that are sent between the two parts of the recursive CTE
	Offsets []int

	Init, Tail Operator
}

var _ Operator = (*Recurse)(nil)

func newRecurse(name string, init, tail Operator) *Recurse {
	return &Recurse{
		Name: name,
		Init: init,
		Tail: tail,
	}
}

func (r *Recurse) Clone(inputs []Operator) Operator {
	return &Recurse{
		Name:        r.Name,
		ColumnNames: slices.Clone(r.ColumnNames),
		Offsets:     slices.Clone(r.Offsets),
		Init:        inputs[0],
		Tail:        inputs[1],
	}
}

func (r *Recurse) Inputs() []Operator {
	return []Operator{r.Init, r.Tail}
}

func (r *Recurse) SetInputs(operators []Operator) {
	r.Init = operators[0]
	r.Tail = operators[1]
}

func (r *Recurse) AddPredicate(_ *plancontext.PlanningContext, e sqlparser.Expr) Operator {
	r.Tail = newFilter(r, e)
	return r
}

func (r *Recurse) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, expr *sqlparser.AliasedExpr) int {
	return r.Init.AddColumn(ctx, reuseExisting, addToGroupBy, expr)
}

func (r *Recurse) AddWSColumn(*plancontext.PlanningContext, int, bool) int {
	panic("implement me")
}

func (r *Recurse) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return r.Init.FindCol(ctx, expr, underRoute)
}

func (r *Recurse) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return r.Init.GetColumns(ctx)
}

func (r *Recurse) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	return r.Init.GetSelectExprs(ctx)
}

func (r *Recurse) ShortDescription() string { return "" }

func (r *Recurse) GetOrdering(*plancontext.PlanningContext) []OrderBy {
	// Recurse is a special case. It never guarantees any ordering.
	return nil
}
