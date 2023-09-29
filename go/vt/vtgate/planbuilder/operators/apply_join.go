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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	// ApplyJoin is a nested loop join - for each row on the LHS,
	// we'll execute the plan on the RHS, feeding data from left to right
	ApplyJoin struct {
		LHS, RHS ops.Operator

		// LeftJoin will be true in the case of an outer join
		LeftJoin bool

		// Before offset planning
		Predicate sqlparser.Expr

		// JoinColumns keeps track of what AST expression is represented in the Columns array
		JoinColumns []JoinColumn

		// JoinPredicates are join predicates that have been broken up into left hand side and right hand side parts.
		JoinPredicates []JoinColumn

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

	// JoinColumn is where we store information about columns passing through the join operator
	// It can be in one of three possible configurations:
	//   - Pure left
	//     We are projecting a column that comes from the left. The RHSExpr will be nil for these
	//   - Pure right
	//     We are projecting a column that comes from the right. The LHSExprs will be empty for these
	//   - Mix of data from left and right
	//     Here we need to transmit columns from the LHS to the RHS,
	//     so they can be used for the result of this expression that is using data from both sides.
	//     All fields will be used for these
	JoinColumn struct {
		Original *sqlparser.AliasedExpr // this is the original expression being passed through
		LHSExprs []BindVarExpr
		RHSExpr  sqlparser.Expr
		GroupBy  bool // if this is true, we need to push this down to our inputs with addToGroupBy set to true
	}

	// BindVarExpr is an expression needed from one side of a join/subquery, and the argument name for it.
	// TODO: Do we really need to store the name here? it could be found in the semantic state instead
	BindVarExpr struct {
		Name string
		Expr sqlparser.Expr
	}
)

func NewApplyJoin(lhs, rhs ops.Operator, predicate sqlparser.Expr, leftOuterJoin bool) *ApplyJoin {
	return &ApplyJoin{
		LHS:       lhs,
		RHS:       rhs,
		Vars:      map[string]int{},
		Predicate: predicate,
		LeftJoin:  leftOuterJoin,
	}
}

// Clone implements the Operator interface
func (aj *ApplyJoin) Clone(inputs []ops.Operator) ops.Operator {
	kopy := *aj
	kopy.LHS = inputs[0]
	kopy.RHS = inputs[1]
	kopy.Columns = slices.Clone(aj.Columns)
	kopy.JoinColumns = slices.Clone(aj.JoinColumns)
	kopy.JoinPredicates = slices.Clone(aj.JoinPredicates)
	kopy.Vars = maps.Clone(aj.Vars)
	kopy.Predicate = sqlparser.CloneExpr(aj.Predicate)
	kopy.ExtraLHSVars = slices.Clone(aj.ExtraLHSVars)
	return &kopy
}

func (aj *ApplyJoin) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	return AddPredicate(ctx, aj, expr, false, newFilter)
}

// Inputs implements the Operator interface
func (aj *ApplyJoin) Inputs() []ops.Operator {
	return []ops.Operator{aj.LHS, aj.RHS}
}

// SetInputs implements the Operator interface
func (aj *ApplyJoin) SetInputs(inputs []ops.Operator) {
	aj.LHS, aj.RHS = inputs[0], inputs[1]
}

func (aj *ApplyJoin) GetLHS() ops.Operator {
	return aj.LHS
}

func (aj *ApplyJoin) GetRHS() ops.Operator {
	return aj.RHS
}

func (aj *ApplyJoin) SetLHS(operator ops.Operator) {
	aj.LHS = operator
}

func (aj *ApplyJoin) SetRHS(operator ops.Operator) {
	aj.RHS = operator
}

func (aj *ApplyJoin) MakeInner() {
	aj.LeftJoin = false
}

func (aj *ApplyJoin) IsInner() bool {
	return !aj.LeftJoin
}

func (aj *ApplyJoin) AddJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) error {
	aj.Predicate = ctx.SemTable.AndExpressions(expr, aj.Predicate)

	col, err := BreakExpressionInLHSandRHS(ctx, expr, TableID(aj.LHS))
	if err != nil {
		return err
	}
	aj.JoinPredicates = append(aj.JoinPredicates, col)
	rhs, err := aj.RHS.AddPredicate(ctx, col.RHSExpr)
	if err != nil {
		return err
	}
	aj.RHS = rhs

	return nil
}

func (aj *ApplyJoin) pushColLeft(ctx *plancontext.PlanningContext, e *sqlparser.AliasedExpr, addToGroupBy bool) (int, error) {
	offset, err := aj.LHS.AddColumn(ctx, true, addToGroupBy, e)
	if err != nil {
		return 0, err
	}
	return offset, nil
}

func (aj *ApplyJoin) pushColRight(ctx *plancontext.PlanningContext, e *sqlparser.AliasedExpr, addToGroupBy bool) (int, error) {
	offset, err := aj.RHS.AddColumn(ctx, true, addToGroupBy, e)
	if err != nil {
		return 0, err
	}
	return offset, nil
}

func (aj *ApplyJoin) GetColumns(*plancontext.PlanningContext) ([]*sqlparser.AliasedExpr, error) {
	return slice.Map(aj.JoinColumns, joinColumnToAliasedExpr), nil
}

func (aj *ApplyJoin) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	return transformColumnsToSelectExprs(ctx, aj)
}

func (aj *ApplyJoin) GetOrdering() ([]ops.OrderBy, error) {
	return aj.LHS.GetOrdering()
}

func joinColumnToAliasedExpr(c JoinColumn) *sqlparser.AliasedExpr {
	return c.Original
}

func joinColumnToExpr(column JoinColumn) sqlparser.Expr {
	return column.Original.Expr
}

func (aj *ApplyJoin) getJoinColumnFor(ctx *plancontext.PlanningContext, orig *sqlparser.AliasedExpr, e sqlparser.Expr, addToGroupBy bool) (col JoinColumn, err error) {
	defer func() {
		col.Original = orig
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
		col, err = BreakExpressionInLHSandRHS(ctx, e, TableID(aj.LHS))
		if err != nil {
			return JoinColumn{}, err
		}
	default:
		return JoinColumn{}, vterrors.VT13002(sqlparser.String(e))
	}

	return
}

func (aj *ApplyJoin) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, _ bool) (int, error) {
	offset, found := canReuseColumn(ctx, aj.JoinColumns, expr, joinColumnToExpr)
	if !found {
		return -1, nil
	}
	return offset, nil
}

func (aj *ApplyJoin) AddColumn(
	ctx *plancontext.PlanningContext,
	reuse bool,
	groupBy bool,
	expr *sqlparser.AliasedExpr,
) (int, error) {
	if reuse {
		offset, err := aj.FindCol(ctx, expr.Expr, false)
		if err != nil {
			return 0, err
		}
		if offset != -1 {
			return offset, nil
		}
	}
	col, err := aj.getJoinColumnFor(ctx, expr, expr.Expr, groupBy)
	if err != nil {
		return 0, err
	}
	offset := len(aj.JoinColumns)
	aj.JoinColumns = append(aj.JoinColumns, col)
	return offset, nil
}

func (aj *ApplyJoin) planOffsets(ctx *plancontext.PlanningContext) (err error) {
	for _, col := range aj.JoinColumns {
		// Read the type description for JoinColumn to understand the following code
		for _, lhsExpr := range col.LHSExprs {
			offset, err := aj.pushColLeft(ctx, aeWrap(lhsExpr.Expr), col.GroupBy)
			if err != nil {
				return err
			}
			if col.RHSExpr == nil {
				// if we don't have an RHS expr, it means that this is a pure LHS expression
				aj.addOffset(-offset - 1)
			} else {
				aj.Vars[lhsExpr.Name] = offset
			}
		}
		if col.RHSExpr != nil {
			offset, err := aj.pushColRight(ctx, aeWrap(col.RHSExpr), col.GroupBy)
			if err != nil {
				return err
			}
			aj.addOffset(offset + 1)
		}
	}

	for _, col := range aj.JoinPredicates {
		for _, lhsExpr := range col.LHSExprs {
			offset, err := aj.pushColLeft(ctx, aeWrap(lhsExpr.Expr), false)
			if err != nil {
				return err
			}
			aj.Vars[lhsExpr.Name] = offset
		}
		if err != nil {
			return err
		}
	}

	for _, lhsExpr := range aj.ExtraLHSVars {
		offset, err := aj.pushColLeft(ctx, aeWrap(lhsExpr.Expr), false)
		if err != nil {
			return err
		}
		aj.Vars[lhsExpr.Name] = offset
	}
	return nil
}

func (aj *ApplyJoin) addOffset(offset int) {
	aj.Columns = append(aj.Columns, offset)
}

func (aj *ApplyJoin) ShortDescription() string {
	pred := sqlparser.String(aj.Predicate)
	columns := slice.Map(aj.JoinColumns, func(from JoinColumn) string {
		return sqlparser.String(from.Original)
	})
	firstPart := fmt.Sprintf("on %s columns: %s", pred, strings.Join(columns, ", "))
	if len(aj.ExtraLHSVars) == 0 {
		return firstPart
	}
	extraCols := slice.Map(aj.ExtraLHSVars, func(s BindVarExpr) string { return s.String() })

	return firstPart + " extra: " + strings.Join(extraCols, ", ")
}

func (aj *ApplyJoin) isColNameMovedFromL2R(bindVarName string) bool {
	for _, jc := range aj.JoinColumns {
		for _, bve := range jc.LHSExprs {
			if bve.Name == bindVarName {
				return true
			}
		}
	}
	for _, jp := range aj.JoinPredicates {
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
// and returns the argument name if found. if it's not found, a new JoinColumn passing this through will be added
func (aj *ApplyJoin) findOrAddColNameBindVarName(ctx *plancontext.PlanningContext, col *sqlparser.ColName) (string, error) {
	for i, thisCol := range aj.JoinColumns {
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
				aj.JoinColumns[i].LHSExprs[idx] = expr
			}
			return thisCol.LHSExprs[idx].Name, nil
		}
	}
	for _, thisCol := range aj.JoinPredicates {
		idx := slices.IndexFunc(thisCol.LHSExprs, func(e BindVarExpr) bool {
			return ctx.SemTable.EqualsExpr(e.Expr, col)
		})
		if idx != -1 {
			return thisCol.LHSExprs[idx].Name, nil
		}
	}

	idx := slices.IndexFunc(aj.ExtraLHSVars, func(e BindVarExpr) bool {
		return ctx.SemTable.EqualsExpr(e.Expr, col)
	})
	if idx != -1 {
		return aj.ExtraLHSVars[idx].Name, nil
	}

	// we didn't find it, so we need to add it
	bvName := ctx.GetReservedArgumentFor(col)
	aj.ExtraLHSVars = append(aj.ExtraLHSVars, BindVarExpr{
		Name: bvName,
		Expr: col,
	})
	return bvName, nil
}

func (a *ApplyJoin) LHSColumnsNeeded(ctx *plancontext.PlanningContext) (needed sqlparser.Exprs) {
	f := func(from BindVarExpr) sqlparser.Expr {
		return from.Expr
	}
	for _, jc := range a.JoinColumns {
		needed = append(needed, slice.Map(jc.LHSExprs, f)...)
	}
	for _, jc := range a.JoinPredicates {
		needed = append(needed, slice.Map(jc.LHSExprs, f)...)
	}
	needed = append(needed, slice.Map(a.ExtraLHSVars, f)...)
	return ctx.SemTable.Uniquify(needed)
}

func (jc JoinColumn) IsPureLeft() bool {
	return jc.RHSExpr == nil
}

func (jc JoinColumn) IsPureRight() bool {
	return len(jc.LHSExprs) == 0
}

func (jc JoinColumn) IsMixedLeftAndRight() bool {
	return len(jc.LHSExprs) > 0 && jc.RHSExpr != nil
}

func (bve BindVarExpr) String() string {
	if bve.Name == "" {
		return sqlparser.String(bve.Expr)
	}

	return fmt.Sprintf(":%s|`%s`", bve.Name, sqlparser.String(bve.Expr))
}
