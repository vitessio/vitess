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

	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
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

	// ColumnsAST keeps track of what AST expression is represented in the Columns array
	ColumnsAST []JoinColumn

	// After offset planning

	// Columns stores the column indexes of the columns coming from the left and right side
	// negative value comes from LHS and positive from RHS
	Columns []int

	// Vars are the arguments that need to be copied from the LHS to the RHS
	Vars map[string]int

	Predicate sqlparser.Expr
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
	Original       sqlparser.Expr // this is the original expression being passed through
	BvNames        []string       // the BvNames and LHSCols line up
	LHSExprs       []sqlparser.Expr
	RHSExpr        sqlparser.Expr
	IncomingOffset int
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
		LHS:        inputs[0],
		RHS:        inputs[1],
		Columns:    slices.Clone(a.Columns),
		ColumnsAST: slices.Clone(a.ColumnsAST),
		Vars:       maps.Clone(a.Vars),
		LeftJoin:   a.LeftJoin,
		Predicate:  sqlparser.CloneExpr(a.Predicate),
		LHSColumns: slices.Clone(a.LHSColumns),
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
	columns := slices2.Map(a.ColumnsAST, jcToExpr)
	return columns, nil
}

func jcToExpr(c JoinColumn) sqlparser.Expr { return c.Original }

func (a *ApplyJoin) AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr) (ops.Operator, int, error) {
	if offset, found := canReuseColumn(ctx, a.ColumnsAST, expr.Expr, jcToExpr); found {
		return a, offset, nil
	}

	side := chooseSide{
		Expr: expr.Expr,
		left: func() error {
			offset, err := a.pushColLeft(ctx, expr)
			if err != nil {
				return err
			}
			a.Columns = append(a.Columns, -offset-1)
			return nil
		},
		right: func() error {
			offset, err := a.pushColRight(ctx, expr)
			if err != nil {
				return err
			}
			a.Columns = append(a.Columns, offset+1)
			return nil
		},
		both: func() error {
			col, err := BreakExpressionInLHSandRHS(ctx, expr.Expr, TableID(a.LHS))
			if err != nil {
				return err
			}
			for i, lhsExpr := range col.LHSExprs {
				offset, err := a.pushColLeft(ctx, aeWrap(lhsExpr))
				if err != nil {
					return err
				}
				a.Vars[col.BvNames[i]] = offset
			}
			offset, err := a.pushColRight(ctx, aeWrap(col.RHSExpr))
			if err != nil {
				return err
			}
			a.Columns = append(a.Columns, offset+1)
			return nil
		},
	}
	err := a.Push(ctx, side)
	if err != nil {
		return nil, 0, err
	}
	return a, len(a.Columns) - 1, nil
}

func (a *ApplyJoin) Push(ctx *plancontext.PlanningContext, chooser chooseSide) error {
	lhs := TableID(a.LHS)
	rhs := TableID(a.RHS)
	both := lhs.Merge(rhs)
	deps := ctx.SemTable.RecursiveDeps(chooser.Expr)

	switch {
	case deps.IsSolvedBy(lhs):
		return chooser.left()
	case deps.IsSolvedBy(rhs):
		return chooser.right()
	case deps.IsSolvedBy(both):
		return chooser.both()
	default:
		return vterrors.VT13002(sqlparser.String(chooser.Expr))
	}
}

type chooseSide struct {
	Expr              sqlparser.Expr
	left, right, both func() error
}
