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
	"fmt"
	"slices"
	"strings"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/vtgate/semantics"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// RecurseCTE is used to represent a recursive CTE
type RecurseCTE struct {
	Init, Tail Operator

	// Def is the CTE definition according to the semantics
	Def *semantics.CTE

	// ColumnNames is the list of column names that are sent between the two parts of the recursive CTE
	ColumnNames []string

	// ColumnOffsets is the list of column offsets that are sent between the two parts of the recursive CTE
	Offsets []int

	// Expressions are the expressions that are needed on the recurse side of the CTE
	Expressions []*plancontext.RecurseExpression

	// Vars is the map of variables that are sent between the two parts of the recursive CTE
	// It's filled in at offset planning time
	Vars map[string]int
}

var _ Operator = (*RecurseCTE)(nil)

func newRecurse(def *semantics.CTE, init, tail Operator, expressions []*plancontext.RecurseExpression) *RecurseCTE {
	return &RecurseCTE{
		Def:         def,
		Init:        init,
		Tail:        tail,
		Expressions: expressions,
	}
}

func (r *RecurseCTE) Clone(inputs []Operator) Operator {
	return &RecurseCTE{
		Def:         r.Def,
		ColumnNames: slices.Clone(r.ColumnNames),
		Offsets:     slices.Clone(r.Offsets),
		Expressions: slices.Clone(r.Expressions),
		Init:        inputs[0],
		Tail:        inputs[1],
	}
}

func (r *RecurseCTE) Inputs() []Operator {
	return []Operator{r.Init, r.Tail}
}

func (r *RecurseCTE) SetInputs(operators []Operator) {
	r.Init = operators[0]
	r.Tail = operators[1]
}

func (r *RecurseCTE) AddPredicate(_ *plancontext.PlanningContext, e sqlparser.Expr) Operator {
	r.Tail = newFilter(r, e)
	return r
}

func (r *RecurseCTE) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, expr *sqlparser.AliasedExpr) int {
	return r.Init.AddColumn(ctx, reuseExisting, addToGroupBy, expr)
}

func (r *RecurseCTE) AddWSColumn(*plancontext.PlanningContext, int, bool) int {
	panic("implement me")
}

func (r *RecurseCTE) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return r.Init.FindCol(ctx, expr, underRoute)
}

func (r *RecurseCTE) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return r.Init.GetColumns(ctx)
}

func (r *RecurseCTE) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	return r.Init.GetSelectExprs(ctx)
}

func (r *RecurseCTE) ShortDescription() string {
	if len(r.Vars) > 0 {
		return fmt.Sprintf("%v", r.Vars)
	}
	exprs := slice.Map(r.Expressions, func(expr *plancontext.RecurseExpression) string {
		return sqlparser.String(expr.Original)
	})
	return fmt.Sprintf("%v %v", r.Def.Name, strings.Join(exprs, ", "))
}

func (r *RecurseCTE) GetOrdering(*plancontext.PlanningContext) []OrderBy {
	// RecurseCTE is a special case. It never guarantees any ordering.
	return nil
}

func (r *RecurseCTE) planOffsets(ctx *plancontext.PlanningContext) Operator {
	r.Vars = make(map[string]int)
	columns := r.Init.GetColumns(ctx)
	for _, expr := range r.Expressions {
	outer:
		for _, lhsExpr := range expr.LeftExprs {
			_, found := r.Vars[lhsExpr.Name]
			if found {
				continue
			}

			for offset, column := range columns {
				if lhsExpr.Expr.Name.EqualString(column.ColumnName()) {
					r.Vars[lhsExpr.Name] = offset
					continue outer
				}
			}

			panic("couldn't find column")
		}
	}
	return r
}
