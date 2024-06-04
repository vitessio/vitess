/*
Copyright 2021 The Vitess Authors.

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
)

type (
	// ApplyJoin is a nested loop join - for each row on the LHS,
	// we'll execute the plan on the RHS, feeding data from left to right
	ApplyJoin struct {
		LHS, RHS Operator

		// JoinType is permitted to store only 3 of the possible values
		// NormalJoinType, StraightJoinType and LeftJoinType.
		JoinType sqlparser.JoinType
		// LeftJoin will be true in the case of an outer join
		LeftJoin bool

		// JoinColumns keeps track of what AST expression is represented in the Columns array
		JoinColumns *applyJoinColumns

		// JoinPredicates are join predicates that have been broken up into left hand side and right hand side parts.
		JoinPredicates *applyJoinColumns

		// ExtraVars are columns we need to copy from left to right not needed by any predicates or projections,
		// these are needed by other operators further down the right hand side of the join
		ExtraLHSVars []BindVarExpr

		// After offset planning

		// Columns stores the column indexes of the columns coming from the left and right side
		// negative value comes from LHS and positive from RHS
		Columns []int

		// Vars are the arguments that need to be copied from the LHS to the RHS
		Vars map[string]int
	}

	// applyJoinColumn is where we store information about columns passing through the join operator
	// It can be in one of three possible configurations:
	//   - Pure left
	//     We are projecting a column that comes from the left. The RHSExpr will be nil for these
	//   - Pure right
	//     We are projecting a column that comes from the right. The LHSExprs will be empty for these
	//   - Mix of data from left and right
	//     Here we need to transmit columns from the LHS to the RHS,
	//     so they can be used for the result of this expression that is using data from both sides.
	//     All fields will be used for these
	applyJoinColumn struct {
		Original  sqlparser.Expr     // this is the original expression being passed through
		LHSExprs  []BindVarExpr      // These are the expressions we are pushing to the left hand side which we'll receive as bind variables
		RHSExpr   sqlparser.Expr     // This the expression that we'll evaluate on the right hand side. This is nil, if the right hand side has nothing.
		DTColName *sqlparser.ColName // This is the output column name that the parent of JOIN will be seeing. If this is unset, then the colname is the String(Original). We set this when we push Projections with derived tables underneath a Join.
		GroupBy   bool               // if this is true, we need to push this down to our inputs with addToGroupBy set to true
	}

	// BindVarExpr is an expression needed from one side of a join/subquery, and the argument name for it.
	// TODO: Do we really need to store the name here? it could be found in the semantic state instead
	BindVarExpr struct {
		Name string
		Expr sqlparser.Expr
	}
)

func NewApplyJoin(ctx *plancontext.PlanningContext, lhs, rhs Operator, predicate sqlparser.Expr, joinType sqlparser.JoinType) *ApplyJoin {
	aj := &ApplyJoin{
		LHS:            lhs,
		RHS:            rhs,
		Vars:           map[string]int{},
		JoinType:       joinType,
		JoinColumns:    &applyJoinColumns{},
		JoinPredicates: &applyJoinColumns{},
	}
	aj.AddJoinPredicate(ctx, predicate)
	return aj
}

// Clone implements the Operator interface
func (aj *ApplyJoin) Clone(inputs []Operator) Operator {
	kopy := *aj
	kopy.LHS = inputs[0]
	kopy.RHS = inputs[1]
	kopy.Columns = slices.Clone(aj.Columns)
	kopy.JoinColumns = aj.JoinColumns.clone()
	kopy.JoinPredicates = aj.JoinPredicates.clone()
	kopy.Vars = maps.Clone(aj.Vars)
	kopy.ExtraLHSVars = slices.Clone(aj.ExtraLHSVars)
	return &kopy
}

func (aj *ApplyJoin) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	return AddPredicate(ctx, aj, expr, false, newFilterSinglePredicate)
}

// Inputs implements the Operator interface
func (aj *ApplyJoin) Inputs() []Operator {
	return []Operator{aj.LHS, aj.RHS}
}

// SetInputs implements the Operator interface
func (aj *ApplyJoin) SetInputs(inputs []Operator) {
	aj.LHS, aj.RHS = inputs[0], inputs[1]
}

func (aj *ApplyJoin) GetLHS() Operator {
	return aj.LHS
}

func (aj *ApplyJoin) GetRHS() Operator {
	return aj.RHS
}

func (aj *ApplyJoin) SetLHS(operator Operator) {
	aj.LHS = operator
}

func (aj *ApplyJoin) SetRHS(operator Operator) {
	aj.RHS = operator
}

func (aj *ApplyJoin) MakeInner() {
	if aj.IsInner() {
		return
	}
	aj.JoinType = sqlparser.NormalJoinType
}

func (aj *ApplyJoin) IsInner() bool {
	return aj.JoinType.IsInner()
}

func (aj *ApplyJoin) AddJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) {
	if expr == nil {
		return
	}
	rhs := aj.RHS
	predicates := sqlparser.SplitAndExpression(nil, expr)
	for _, pred := range predicates {
		col := breakExpressionInLHSandRHS(ctx, pred, TableID(aj.LHS))
		aj.JoinPredicates.add(col)
		ctx.AddJoinPredicates(pred, col.RHSExpr)
		rhs = rhs.AddPredicate(ctx, col.RHSExpr)
	}
	aj.RHS = rhs
}

func (aj *ApplyJoin) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	colSize := len(aj.Columns)
	if colSize == 0 {
		// we've yet to do offset planning - let's return what we have for now
		return slice.Map(aj.JoinColumns.columns, func(from applyJoinColumn) *sqlparser.AliasedExpr {
			return aeWrap(from.Original)
		})
	}
	cols := make([]*sqlparser.AliasedExpr, colSize)
	var lhsCols, rhsCols []*sqlparser.AliasedExpr
	for idx, column := range aj.Columns {
		if column < 0 {
			if lhsCols == nil {
				lhsCols = aj.LHS.GetColumns(ctx)
			}
			cols[idx] = lhsCols[FromLeftOffset(column)]
		} else {
			if rhsCols == nil {
				rhsCols = aj.RHS.GetColumns(ctx)
			}
			cols[idx] = rhsCols[FromRightOffset(column)]
		}
	}
	return cols
}

func (aj *ApplyJoin) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	return transformColumnsToSelectExprs(ctx, aj)
}

func (aj *ApplyJoin) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return aj.LHS.GetOrdering(ctx)
}

func joinColumnToExpr(column applyJoinColumn) sqlparser.Expr {
	return column.Original
}

func (aj *ApplyJoin) getJoinColumnFor(ctx *plancontext.PlanningContext, orig *sqlparser.AliasedExpr, e sqlparser.Expr, addToGroupBy bool) (col applyJoinColumn) {
	defer func() {
		col.Original = orig.Expr
	}()
	lhs := TableID(aj.LHS)
	rhs := TableID(aj.RHS)
	both := lhs.Merge(rhs)
	deps := ctx.SemTable.RecursiveDeps(e)
	col.GroupBy = addToGroupBy

	switch {
	case deps.IsSolvedBy(lhs):
		col.LHSExprs = []BindVarExpr{{Expr: e}}
	case deps.IsSolvedBy(rhs):
		col.RHSExpr = e
	case deps.IsSolvedBy(both):
		col = breakExpressionInLHSandRHS(ctx, e, TableID(aj.LHS))
	default:
		panic(vterrors.VT13002(sqlparser.String(e)))
	}

	return
}

func applyJoinCompare(ctx *plancontext.PlanningContext, expr sqlparser.Expr) func(e applyJoinColumn) bool {
	return func(e applyJoinColumn) bool {
		// e.DTColName is how the outside world will be using this expression. So we should check for an equality with that too.
		return ctx.SemTable.EqualsExprWithDeps(e.Original, expr) || ctx.SemTable.EqualsExprWithDeps(e.DTColName, expr)
	}
}

func (aj *ApplyJoin) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, _ bool) int {
	return slices.IndexFunc(aj.JoinColumns.columns, applyJoinCompare(ctx, expr))
}

func (aj *ApplyJoin) AddColumn(
	ctx *plancontext.PlanningContext,
	reuse bool,
	groupBy bool,
	expr *sqlparser.AliasedExpr,
) int {
	if reuse {
		offset := aj.FindCol(ctx, expr.Expr, false)
		if offset != -1 {
			return offset
		}
	}
	col := aj.getJoinColumnFor(ctx, expr, expr.Expr, groupBy)
	offset := len(aj.JoinColumns.columns)
	aj.JoinColumns.add(col)
	return offset
}

func (aj *ApplyJoin) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	if len(aj.Columns) == 0 {
		aj.planOffsets(ctx)
	}

	if len(aj.Columns) <= offset {
		panic(vterrors.VT13001("offset out of range"))
	}

	wsExpr := weightStringFor(aj.JoinColumns.columns[offset].Original)
	if index := aj.FindCol(ctx, wsExpr, false); index != -1 {
		// nice, we already have this column. no need to add anything
		return index
	}

	i := aj.Columns[offset]
	out := 0
	if i < 0 {
		out = aj.LHS.AddWSColumn(ctx, FromLeftOffset(i), underRoute)
		out = ToLeftOffset(out)
		aj.JoinColumns.addLeft(wsExpr)
	} else {
		out = aj.RHS.AddWSColumn(ctx, FromRightOffset(i), underRoute)
		out = ToRightOffset(out)
		aj.JoinColumns.addRight(wsExpr)
	}

	if out >= 0 {
		aj.addOffset(out)
	} else {
		col := aj.getJoinColumnFor(ctx, aeWrap(wsExpr), wsExpr, !ContainsAggr(ctx, wsExpr))
		aj.JoinColumns.add(col)
		aj.planOffsetFor(ctx, col)
	}

	return len(aj.Columns) - 1
}

func (aj *ApplyJoin) planOffsets(ctx *plancontext.PlanningContext) Operator {
	if len(aj.Columns) > 0 {
		// we've already done offset planning
		return aj
	}
	for _, col := range aj.JoinColumns.columns {
		// Read the type description for applyJoinColumn to understand the following code
		aj.planOffsetFor(ctx, col)
	}

	for _, col := range aj.JoinPredicates.columns {
		for _, lhsExpr := range col.LHSExprs {
			offset := aj.LHS.AddColumn(ctx, true, false, aeWrap(lhsExpr.Expr))
			aj.Vars[lhsExpr.Name] = offset
		}
	}

	for _, lhsExpr := range aj.ExtraLHSVars {
		offset := aj.LHS.AddColumn(ctx, true, false, aeWrap(lhsExpr.Expr))
		aj.Vars[lhsExpr.Name] = offset
	}

	return nil
}

func (aj *ApplyJoin) planOffsetFor(ctx *plancontext.PlanningContext, col applyJoinColumn) {
	if col.DTColName != nil {
		// If DTColName is set, then we already pushed the parts of the expression down while planning.
		// We need to use this name and ask the correct side of the join for it. Nothing else is required.
		if col.IsPureLeft() {
			offset := aj.LHS.AddColumn(ctx, true, col.GroupBy, aeWrap(col.DTColName))
			aj.addOffset(ToLeftOffset(offset))
		} else {
			for _, lhsExpr := range col.LHSExprs {
				offset := aj.LHS.AddColumn(ctx, true, col.GroupBy, aeWrap(lhsExpr.Expr))
				aj.Vars[lhsExpr.Name] = offset
			}
			offset := aj.RHS.AddColumn(ctx, true, col.GroupBy, aeWrap(col.DTColName))
			aj.addOffset(ToRightOffset(offset))
		}
		return
	}
	for _, lhsExpr := range col.LHSExprs {
		offset := aj.LHS.AddColumn(ctx, true, col.GroupBy, aeWrap(lhsExpr.Expr))
		if col.RHSExpr == nil {
			// if we don't have an RHS expr, it means that this is a pure LHS expression
			aj.addOffset(ToLeftOffset(offset))
		} else {
			aj.Vars[lhsExpr.Name] = offset
		}
	}
	if col.RHSExpr != nil {
		offset := aj.RHS.AddColumn(ctx, true, col.GroupBy, aeWrap(col.RHSExpr))
		aj.addOffset(ToRightOffset(offset))
	}
}

func (aj *ApplyJoin) addOffset(offset int) {
	aj.Columns = append(aj.Columns, offset)
}

func (aj *ApplyJoin) ShortDescription() string {
	fn := func(cols *applyJoinColumns) string {
		out := slice.Map(cols.columns, func(jc applyJoinColumn) string {
			return jc.String()
		})
		return strings.Join(out, ", ")
	}

	firstPart := fmt.Sprintf("on %s columns: %s", fn(aj.JoinPredicates), fn(aj.JoinColumns))
	if len(aj.ExtraLHSVars) == 0 {
		return firstPart
	}
	extraCols := slice.Map(aj.ExtraLHSVars, func(s BindVarExpr) string { return s.String() })

	return firstPart + " extra: " + strings.Join(extraCols, ", ")
}

func (aj *ApplyJoin) isColNameMovedFromL2R(bindVarName string) bool {
	for _, jc := range aj.JoinColumns.columns {
		for _, bve := range jc.LHSExprs {
			if bve.Name == bindVarName {
				return true
			}
		}
	}
	for _, jp := range aj.JoinPredicates.columns {
		for _, bve := range jp.LHSExprs {
			if bve.Name == bindVarName {
				return true
			}
		}
	}
	for _, bve := range aj.ExtraLHSVars {
		if bve.Name == bindVarName {
			return true
		}
	}
	return false
}

// findOrAddColNameBindVarName goes through the JoinColumns and looks for the given colName coming from the LHS of the join
// and returns the argument name if found. if it's not found, a new applyJoinColumn passing this through will be added
func (aj *ApplyJoin) findOrAddColNameBindVarName(ctx *plancontext.PlanningContext, col *sqlparser.ColName) string {
	for i, thisCol := range aj.JoinColumns.columns {
		idx := slices.IndexFunc(thisCol.LHSExprs, func(e BindVarExpr) bool {
			return ctx.SemTable.EqualsExpr(e.Expr, col)
		})

		if idx != -1 {
			if len(thisCol.LHSExprs) == 1 && thisCol.RHSExpr == nil {
				// this is a ColName that was not being sent to the RHS, so it has no bindvar name.
				// let's add one.
				expr := thisCol.LHSExprs[idx]
				bvname := ctx.GetReservedArgumentFor(expr.Expr)
				expr.Name = bvname
				aj.JoinColumns.columns[i].LHSExprs[idx] = expr
			}
			return thisCol.LHSExprs[idx].Name
		}
	}
	for _, thisCol := range aj.JoinPredicates.columns {
		idx := slices.IndexFunc(thisCol.LHSExprs, func(e BindVarExpr) bool {
			return ctx.SemTable.EqualsExpr(e.Expr, col)
		})
		if idx != -1 {
			return thisCol.LHSExprs[idx].Name
		}
	}

	idx := slices.IndexFunc(aj.ExtraLHSVars, func(e BindVarExpr) bool {
		return ctx.SemTable.EqualsExpr(e.Expr, col)
	})
	if idx != -1 {
		return aj.ExtraLHSVars[idx].Name
	}

	// we didn't find it, so we need to add it
	bvName := ctx.GetReservedArgumentFor(col)
	aj.ExtraLHSVars = append(aj.ExtraLHSVars, BindVarExpr{
		Name: bvName,
		Expr: col,
	})
	return bvName
}

func (a *ApplyJoin) LHSColumnsNeeded(ctx *plancontext.PlanningContext) (needed sqlparser.Exprs) {
	f := func(from BindVarExpr) sqlparser.Expr {
		return from.Expr
	}
	for _, jc := range a.JoinColumns.columns {
		needed = append(needed, slice.Map(jc.LHSExprs, f)...)
	}
	for _, jc := range a.JoinPredicates.columns {
		needed = append(needed, slice.Map(jc.LHSExprs, f)...)
	}
	needed = append(needed, slice.Map(a.ExtraLHSVars, f)...)
	return ctx.SemTable.Uniquify(needed)
}

func (jc applyJoinColumn) String() string {
	rhs := sqlparser.String(jc.RHSExpr)
	lhs := slice.Map(jc.LHSExprs, func(e BindVarExpr) string {
		return sqlparser.String(e.Expr)
	})
	return fmt.Sprintf("[%s | %s | %s]", strings.Join(lhs, ", "), rhs, sqlparser.String(jc.Original))
}

func (jc applyJoinColumn) IsPureLeft() bool {
	return jc.RHSExpr == nil
}

func (jc applyJoinColumn) IsPureRight() bool {
	return len(jc.LHSExprs) == 0
}

func (jc applyJoinColumn) IsMixedLeftAndRight() bool {
	return len(jc.LHSExprs) > 0 && jc.RHSExpr != nil
}

func (bve BindVarExpr) String() string {
	if bve.Name == "" {
		return sqlparser.String(bve.Expr)
	}

	return fmt.Sprintf(":%s|`%s`", bve.Name, sqlparser.String(bve.Expr))
}
