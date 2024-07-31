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
	Seed, // used to describe the non-recursive part that initializes the result set
	Term Operator // the part that repeatedly applies the recursion, processing the result set

	// Def is the CTE definition according to the semantics
	Def *semantics.CTE

	// Expressions are the expressions that are needed on the recurse side of the CTE
	Expressions []*plancontext.RecurseExpression

	// Vars is the map of variables that are sent between the two parts of the recursive CTE
	// It's filled in at offset planning time
	Vars map[string]int
}

var _ Operator = (*RecurseCTE)(nil)

func newRecurse(def *semantics.CTE, seed, term Operator, expressions []*plancontext.RecurseExpression) *RecurseCTE {
	return &RecurseCTE{
		Def:         def,
		Seed:        seed,
		Term:        term,
		Expressions: expressions,
	}
}

func (r *RecurseCTE) Clone(inputs []Operator) Operator {
	return &RecurseCTE{
		Def:         r.Def,
		Expressions: slices.Clone(r.Expressions),
		Seed:        inputs[0],
		Term:        inputs[1],
	}
}

func (r *RecurseCTE) Inputs() []Operator {
	return []Operator{r.Seed, r.Term}
}

func (r *RecurseCTE) SetInputs(operators []Operator) {
	r.Seed = operators[0]
	r.Term = operators[1]
}

func (r *RecurseCTE) AddPredicate(_ *plancontext.PlanningContext, e sqlparser.Expr) Operator {
	r.Term = newFilter(r, e)
	return r
}

func (r *RecurseCTE) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, expr *sqlparser.AliasedExpr) int {
	return r.Seed.AddColumn(ctx, reuseExisting, addToGroupBy, expr)
}

func (r *RecurseCTE) AddWSColumn(*plancontext.PlanningContext, int, bool) int {
	panic("implement me")
}

func (r *RecurseCTE) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return r.Seed.FindCol(ctx, expr, underRoute)
}

func (r *RecurseCTE) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return r.Seed.GetColumns(ctx)
}

func (r *RecurseCTE) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	return r.Seed.GetSelectExprs(ctx)
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
	columns := r.Seed.GetColumns(ctx)
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
