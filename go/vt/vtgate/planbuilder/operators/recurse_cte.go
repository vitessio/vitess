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
	"maps"
	"slices"
	"strings"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// RecurseCTE is used to represent a recursive CTE
type RecurseCTE struct {
	binaryOperator

	// Def is the CTE definition according to the semantics
	Def *semantics.CTE

	// Expressions are the predicates that are needed on the recurse side of the CTE
	Predicates  []*plancontext.RecurseExpression
	Projections []*plancontext.RecurseExpression

	// Vars is the map of variables that are sent between the two parts of the recursive CTE
	// It's filled in at offset planning time
	Vars map[string]int

	// Horizon is stored here until we either expand it or push it under a route
	Horizon *Horizon

	// The LeftID is the id of the left side of the CTE
	LeftID,

	// The OuterID is the id for this use of the CTE
	OuterID semantics.TableSet

	// Distinct is used to determine if the result set should be distinct
	Distinct bool
}

var _ Operator = (*RecurseCTE)(nil)

func newRecurse(
	def *semantics.CTE,
	seed, term Operator,
	predicates []*plancontext.RecurseExpression,
	horizon *Horizon,
	leftID, outerID semantics.TableSet,
	distinct bool,
) *RecurseCTE {
	return &RecurseCTE{
		binaryOperator: newBinaryOp(seed, term),
		Def:            def,
		Predicates:     predicates,
		Horizon:        horizon,
		LeftID:         leftID,
		OuterID:        outerID,
		Distinct:       distinct,
	}
}

func (r *RecurseCTE) Clone(inputs []Operator) Operator {
	klone := *r
	klone.LHS = inputs[0]
	klone.RHS = inputs[1]
	klone.Vars = maps.Clone(r.Vars)
	klone.Predicates = slices.Clone(r.Predicates)
	klone.Projections = slices.Clone(r.Projections)
	return &klone
}

func (r *RecurseCTE) AddPredicate(_ *plancontext.PlanningContext, e sqlparser.Expr) Operator {
	return newFilter(r, e)
}

func (r *RecurseCTE) AddColumn(ctx *plancontext.PlanningContext, _, _ bool, expr *sqlparser.AliasedExpr) int {
	offset := r.columnOffset(ctx, expr.Expr)
	if offset == -1 {
		// the seed may project an equal expression, but the term rows can hold other values there
		panic(vterrors.VT12001("expression over recursive CTE columns: " + sqlparser.String(expr.Expr)))
	}
	return offset
}

func (r *RecurseCTE) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	seed := r.Seed().AddWSColumn(ctx, offset, underRoute)
	term := r.Term().AddWSColumn(ctx, offset, underRoute)
	if seed != term {
		panic(vterrors.VT13001("CTE columns don't match"))
	}
	return seed
}

func (r *RecurseCTE) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, _ bool) int {
	return r.columnOffset(ctx, expr)
}

// columnOffset resolves a column of the CTE by its position, since distinct
// columns can share the same seed expression
func (r *RecurseCTE) columnOffset(ctx *plancontext.PlanningContext, expr sqlparser.Expr) int {
	col, ok := expr.(*sqlparser.ColName)
	if !ok {
		return -1
	}
	if len(r.Def.Columns) > 0 {
		for offset, column := range r.Def.Columns {
			if col.Name.Equal(column) {
				return offset
			}
		}
		return -1
	}
	for offset, column := range r.Seed().GetColumns(ctx) {
		if col.Name.EqualString(column.ColumnName()) {
			return offset
		}
	}
	return -1
}

func (r *RecurseCTE) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return r.Seed().GetColumns(ctx)
}

func (r *RecurseCTE) GetSelectExprs(ctx *plancontext.PlanningContext) []sqlparser.SelectExpr {
	return r.Seed().GetSelectExprs(ctx)
}

func (r *RecurseCTE) ShortDescription() string {
	distinct := ""
	if r.Distinct {
		distinct = "distinct "
	}
	if len(r.Vars) > 0 {
		return fmt.Sprintf("%s%v", distinct, r.Vars)
	}
	expressions := slice.Map(r.expressions(), func(expr *plancontext.RecurseExpression) string {
		return sqlparser.String(expr.Original)
	})
	return fmt.Sprintf("%s%v %v", distinct, r.Def.Name, strings.Join(expressions, ", "))
}

func (r *RecurseCTE) GetOrdering(*plancontext.PlanningContext) []OrderBy {
	// RecurseCTE is a special case. It never guarantees any ordering.
	return nil
}

func (r *RecurseCTE) expressions() []*plancontext.RecurseExpression {
	return append(r.Predicates, r.Projections...)
}

func (r *RecurseCTE) planOffsets(ctx *plancontext.PlanningContext) Operator {
	r.Vars = make(map[string]int)
	columns := r.Seed().GetColumns(ctx)
	for _, expr := range r.expressions() {
	outer:
		for _, lhsExpr := range expr.LeftExprs {
			_, found := r.Vars[lhsExpr.Name]
			if found {
				continue
			}

			// a declared column list renames every output column of the CTE, so
			// its names are matched positionally against the seed columns first
			for offset, column := range r.Def.Columns {
				if lhsExpr.Expr.Name.Equal(column) {
					r.Vars[lhsExpr.Name] = offset
					continue outer
				}
			}

			for offset, column := range columns {
				if lhsExpr.Expr.Name.EqualString(column.ColumnName()) {
					r.Vars[lhsExpr.Name] = offset
					continue outer
				}
			}

			panic(vterrors.VT13001("couldn't find column"))
		}
	}
	return r
}

func (r *RecurseCTE) introducesTableID() semantics.TableSet {
	return r.OuterID
}

func (r *RecurseCTE) Seed() Operator {
	return r.LHS
}

func (r *RecurseCTE) Term() Operator {
	return r.RHS
}
