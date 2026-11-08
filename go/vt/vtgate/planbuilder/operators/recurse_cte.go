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
	"vitess.io/vitess/go/vt/vtgate/engine"
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

	// MyTableID is the id of the CTE
	MyTableInfo *semantics.CTETable

	// Horizon is stored here until we either expand it or push it under a route
	Horizon *Horizon

	// The LeftID is the id of the left side of the CTE
	LeftID,

	// The OuterID is the id for this use of the CTE
	OuterID semantics.TableSet

	// Distinct is used to determine if the result set should be distinct
	Distinct bool

	// CheckCols are the columns used to deduplicate rows when Distinct is set.
	// TermCheckCols is the same for the term, where a weight string can be at another offset.
	CheckCols     []engine.CheckCol
	TermCheckCols []engine.CheckCol

	// Truncate is the number of columns that parents see, without the weight strings added for the deduplication
	Truncate int

	// TermFromCTE maps the term's columns that only pass on a column of the previous row to it.
	// Their weight string is taken from that row, as ApplyJoin does for the left side.
	TermFromCTE map[int]int
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
	klone.CheckCols = slices.Clone(r.CheckCols)
	klone.TermCheckCols = slices.Clone(r.TermCheckCols)
	klone.TermFromCTE = maps.Clone(r.TermFromCTE)
	return &klone
}

func (r *RecurseCTE) AddPredicate(_ *plancontext.PlanningContext, e sqlparser.Expr) Operator {
	return newFilter(r, e)
}

func (r *RecurseCTE) AddColumn(ctx *plancontext.PlanningContext, _, _ bool, expr *sqlparser.AliasedExpr) int {
	r.makeSureWeHaveTableInfo(ctx)
	e := semantics.RewriteDerivedTableExpression(expr.Expr, r.MyTableInfo)
	offset := r.Seed().FindCol(ctx, e, false)
	if offset != -1 {
		return offset
	}
	// A UNION seed that got wrapped in a derived table no longer has the same dependencies,
	// so fall back to the name of the CTE column that was asked for, as planOffsets does for Vars.
	// The seed's own expression can carry the name of another column when the seed renames them.
	if col, ok := expr.Expr.(*sqlparser.ColName); ok {
		for offset, column := range r.Seed().GetColumns(ctx) {
			if col.Name.EqualString(column.ColumnName()) {
				return offset
			}
		}
	}
	panic(vterrors.VT13001("CTE column not found"))
}

func (r *RecurseCTE) makeSureWeHaveTableInfo(ctx *plancontext.PlanningContext) {
	if r.MyTableInfo == nil {
		for _, table := range ctx.SemTable.Tables {
			cte, ok := table.(*semantics.CTETable)
			if !ok {
				continue
			}
			if cte.CTE == r.Def {
				r.MyTableInfo = cte
				break
			}
		}
		if r.MyTableInfo == nil {
			panic(vterrors.VT13001("CTE not found"))
		}
	}
}

func (r *RecurseCTE) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	seed := r.Seed().AddWSColumn(ctx, offset, underRoute)
	term := r.Term().AddWSColumn(ctx, offset, underRoute)
	if seed != term {
		panic(vterrors.VT13001("CTE columns don't match"))
	}
	return seed
}

func (r *RecurseCTE) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	r.makeSureWeHaveTableInfo(ctx)
	expr = semantics.RewriteDerivedTableExpression(expr, r.MyTableInfo)
	return r.Seed().FindCol(ctx, expr, underRoute)
}

func (r *RecurseCTE) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return r.Seed().GetColumns(ctx)
}

func (r *RecurseCTE) GetSelectExprs(ctx *plancontext.PlanningContext) []sqlparser.SelectExpr {
	return truncateTo(r.Truncate, r.Seed().GetSelectExprs(ctx))
}

func truncateTo[K any](count int, slice []K) []K {
	if count == 0 {
		return slice
	}
	return slice[:count]
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

			for offset, column := range columns {
				if lhsExpr.Expr.Name.EqualString(column.ColumnName()) {
					r.Vars[lhsExpr.Name] = offset
					continue outer
				}
			}

			panic(vterrors.VT13001("couldn't find column"))
		}
	}

	if r.Distinct {
		r.planCheckCols(ctx, columns)
	}
	return r
}

// planCheckCols prepares the columns used to deduplicate a UNION DISTINCT CTE, like Distinct does.
// Only the CTE's own columns are checked, not the weight strings that parents added after them,
// and the weight strings added here are truncated.
func (r *RecurseCTE) planCheckCols(ctx *plancontext.PlanningContext, columns []*sqlparser.AliasedExpr) {
	if r.Truncate > 0 {
		// Offsets can be planned twice, as for INSERT ... SELECT. The weight strings added the
		// first time are among the seed's columns then, and must stay truncated. The term can
		// have lost its own, so they are planned again.
		columns = columns[:r.Truncate]
	}
	r.CheckCols, r.TermCheckCols, r.TermFromCTE = nil, nil, nil
	collationEnv := ctx.VSchema.Environment().CollationEnv()
	termColumns := r.Term().GetColumns(ctx)
	r.makeSureWeHaveTableInfo(ctx)
	cteColumns := min(r.MyTableInfo.Query.GetColumnCount(), len(columns))
	for idx, col := range columns[:cteColumns] {
		typ, _ := ctx.TypeForExpr(col.Expr)
		var seedWs, termWs *int
		if ctx.NeedsWeightString(col.Expr) {
			seedWs = new(r.Seed().AddWSColumn(ctx, idx, false))
			r.Truncate = len(columns)
			cteCol, fromCTE := r.cteColumnFor(ctx, termColumns[idx].Expr)
			switch {
			case fromCTE:
				// the weight string is only fetched to have a column to copy the previous row's into
				termWs = new(r.Term().AddWSColumn(ctx, idx, false))
				if r.TermFromCTE == nil {
					r.TermFromCTE = make(map[int]int)
				}
				r.TermFromCTE[idx] = cteCol
			case sameTableColumn(ctx, col.Expr, termColumns[idx].Expr):
				// the same column as the seed's has the same collation, so its weight string matches
				termWs = new(r.Term().AddWSColumn(ctx, idx, false))
			default:
				// Any other term value is compared by its bytes. Its weight string would use the
				// collation of the connection or of another column, not of the CTE's column, and
				// could drop rows that MySQL keeps. Bytes can only keep a row too many.
				termWs = new(idx)
			}
		}
		r.CheckCols = append(r.CheckCols, engine.CheckCol{Col: idx, WsCol: seedWs, Type: typ, CollationEnv: collationEnv})
		r.TermCheckCols = append(r.TermCheckCols, engine.CheckCol{Col: idx, WsCol: termWs, Type: typ, CollationEnv: collationEnv})
	}
}

// sameTableColumn reports whether both expressions read the same column of the same table.
func sameTableColumn(ctx *plancontext.PlanningContext, a, b sqlparser.Expr) bool {
	colA, okA := a.(*sqlparser.ColName)
	colB, okB := b.(*sqlparser.ColName)
	if !okA || !okB || !colA.Name.Equal(colB.Name) {
		return false
	}
	tableA, errA := ctx.SemTable.TableInfoForExpr(colA)
	tableB, errB := ctx.SemTable.TableInfoForExpr(colB)
	if errA != nil || errB != nil {
		return false
	}
	vtable := tableA.GetVindexTable()
	return vtable != nil && vtable == tableB.GetVindexTable()
}

// cteColumnFor returns the column of the previous row that the expression only passes on.
func (r *RecurseCTE) cteColumnFor(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (int, bool) {
	for _, recurseExpr := range r.expressions() {
		for _, lhsExpr := range recurseExpr.LeftExprs {
			if ctx.SemTable.EqualsExprWithDeps(expr, lhsExpr.Expr) {
				offset, found := r.Vars[lhsExpr.Name]
				return offset, found
			}
		}
	}
	return 0, false
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
