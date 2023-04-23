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
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/slices2"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// ApplyJoin is a nested loop join - for each row on the LHS,
// we'll execute the plan on the RHS, feeding data from left to right
type ApplyJoin struct {
	LHS, RHS ops.Operator

	// LeftJoin will be true in the case of an outer join
	LeftJoin bool

	// JoinCols are the columns from the LHS used for the join.
	// These are the same columns pushed on the LHS that are now used in the Vars field
	LHSColumns []*sqlparser.ColName

	// Before offset planning
	Predicate sqlparser.Expr

	// ColumnsAST keeps track of what AST expression is represented in the Columns array
	ColumnsAST []JoinColumn

	// JoinPredicates are join predicates that have been broken up into left hand side and right hand side parts.
	JoinPredicates []JoinColumn

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
type JoinColumn struct {
	Original sqlparser.Expr // this is the original expression being passed through
	BvNames  []string       // the BvNames and LHSCols line up
	LHSExprs []sqlparser.Expr
	RHSExpr  sqlparser.Expr
}

var _ ops.PhysicalOperator = (*ApplyJoin)(nil)

func NewApplyJoin(lhs, rhs ops.Operator, predicate sqlparser.Expr, leftOuterJoin bool) *ApplyJoin {
	return &ApplyJoin{
		LHS:       lhs,
		RHS:       rhs,
		Vars:      map[string]int{},
		Predicate: predicate,
		LeftJoin:  leftOuterJoin,
	}
}

// IPhysical implements the PhysicalOperator interface
func (a *ApplyJoin) IPhysical() {}

// Clone implements the Operator interface
func (a *ApplyJoin) Clone(inputs []ops.Operator) ops.Operator {
	return &ApplyJoin{
		LHS:            inputs[0],
		RHS:            inputs[1],
		Columns:        slices.Clone(a.Columns),
		ColumnsAST:     slices.Clone(a.ColumnsAST),
		JoinPredicates: slices.Clone(a.JoinPredicates),
		Vars:           maps.Clone(a.Vars),
		LeftJoin:       a.LeftJoin,
		Predicate:      sqlparser.CloneExpr(a.Predicate),
		LHSColumns:     slices.Clone(a.LHSColumns),
	}
}

func (a *ApplyJoin) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	return AddPredicate(ctx, a, expr, false, newFilter)
}

// Inputs implements the Operator interface
func (a *ApplyJoin) Inputs() []ops.Operator {
	return []ops.Operator{a.LHS, a.RHS}
}

// SetInputs implements the Operator interface
func (a *ApplyJoin) SetInputs(inputs []ops.Operator) {
	a.LHS, a.RHS = inputs[0], inputs[1]
}

var _ JoinOp = (*ApplyJoin)(nil)

func (a *ApplyJoin) GetLHS() ops.Operator {
	return a.LHS
}

func (a *ApplyJoin) GetRHS() ops.Operator {
	return a.RHS
}

func (a *ApplyJoin) SetLHS(operator ops.Operator) {
	a.LHS = operator
}

func (a *ApplyJoin) SetRHS(operator ops.Operator) {
	a.RHS = operator
}

func (a *ApplyJoin) MakeInner() {
	a.LeftJoin = false
}

func (a *ApplyJoin) IsInner() bool {
	return !a.LeftJoin
}

func (a *ApplyJoin) AddJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) error {
	a.Predicate = ctx.SemTable.AndExpressions(expr, a.Predicate)

	col, err := BreakExpressionInLHSandRHS(ctx, expr, TableID(a.LHS))
	if err != nil {
		return err
	}
	a.JoinPredicates = append(a.JoinPredicates, col)
	rhs, err := a.RHS.AddPredicate(ctx, col.RHSExpr)
	if err != nil {
		return err
	}
	a.RHS = rhs

	return nil
}

func (a *ApplyJoin) pushColLeft(ctx *plancontext.PlanningContext, e *sqlparser.AliasedExpr) (int, error) {
	newLHS, offset, err := a.LHS.AddColumn(ctx, e)
	if err != nil {
		return 0, err
	}
	a.LHS = newLHS
	return offset, nil
}

func (a *ApplyJoin) pushColRight(ctx *plancontext.PlanningContext, e *sqlparser.AliasedExpr) (int, error) {
	newRHS, offset, err := a.RHS.AddColumn(ctx, e)
	if err != nil {
		return 0, err
	}
	a.RHS = newRHS
	return offset, nil
}

func (a *ApplyJoin) GetColumns() ([]sqlparser.Expr, error) {
	return slices2.Map(a.ColumnsAST, jcToExpr), nil
}

func jcToExpr(c JoinColumn) sqlparser.Expr { return c.Original }

func (a *ApplyJoin) getJoinColumnFor(ctx *plancontext.PlanningContext, e sqlparser.Expr) (col JoinColumn, err error) {
	col.Original = e
	lhs := TableID(a.LHS)
	rhs := TableID(a.RHS)
	both := lhs.Merge(rhs)
	deps := ctx.SemTable.RecursiveDeps(e)

	switch {
	case deps.IsSolvedBy(lhs):
		col.LHSExprs = []sqlparser.Expr{e}
	case deps.IsSolvedBy(rhs):
		col.RHSExpr = e
	case deps.IsSolvedBy(both):
		col, err = BreakExpressionInLHSandRHS(ctx, e, TableID(a.LHS))
		if err != nil {
			return JoinColumn{}, err
		}
	default:
		return JoinColumn{}, vterrors.VT13002(sqlparser.String(e))
	}

	return
}

func (a *ApplyJoin) AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr) (ops.Operator, int, error) {
	if offset, found := canReuseColumn(ctx, a.ColumnsAST, expr.Expr, jcToExpr); found {
		return a, offset, nil
	}
	col, err := a.getJoinColumnFor(ctx, expr.Expr)
	if err != nil {
		return nil, 0, err
	}
	a.ColumnsAST = append(a.ColumnsAST, col)
	return a, len(a.ColumnsAST) - 1, nil
}

func (a *ApplyJoin) planOffsets(ctx *plancontext.PlanningContext) (err error) {
	for _, col := range a.ColumnsAST {
		// Read the type description for JoinColumn to understand the following code
		for i, lhsExpr := range col.LHSExprs {
			offset, err := a.pushColLeft(ctx, aeWrap(lhsExpr))
			if err != nil {
				return err
			}
			if col.RHSExpr == nil {
				// if we don't have a RHS expr, it means that this is a pure LHS expression
				a.addOffset(-offset - 1)
			} else {
				a.Vars[col.BvNames[i]] = offset
			}
		}
		if col.RHSExpr != nil {
			offset, err := a.pushColRight(ctx, aeWrap(col.RHSExpr))
			if err != nil {
				return err
			}
			a.addOffset(offset + 1)
		}
	}

	for _, col := range a.JoinPredicates {
		for i, lhsExpr := range col.LHSExprs {
			offset, err := a.pushColLeft(ctx, aeWrap(lhsExpr))
			if err != nil {
				return err
			}
			a.Vars[col.BvNames[i]] = offset
		}
		lhsColumns := slices2.Map(col.LHSExprs, func(from sqlparser.Expr) *sqlparser.ColName {
			col, ok := from.(*sqlparser.ColName)
			if !ok {
				// todo: there is no good reason to keep this limitation around
				err = vterrors.VT13001("joins can only compare columns: %s", sqlparser.String(from))
			}
			return col
		})
		if err != nil {
			return err
		}
		a.LHSColumns = append(a.LHSColumns, lhsColumns...)
	}
	return nil
}

func (a *ApplyJoin) addOffset(offset int) {
	index := slices.Index(a.Columns, offset)
	if index != -1 {
		panic("should never pass through the same column")
	}
	a.Columns = append(a.Columns, offset)
}

func (a *ApplyJoin) Description() ops.OpDescription {
	return ops.OpDescription{
		OperatorType: "Join",
		Variant:      "Apply",
		Other: map[string]any{
			"Predicate":     sqlparser.String(a.Predicate),
			"OutputColumns": a.Columns,
		},
	}
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
