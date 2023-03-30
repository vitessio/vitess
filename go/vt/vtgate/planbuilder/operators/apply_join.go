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

	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// ApplyJoin is a nested loop join - for each row on the LHS,
// we'll execute the plan on the RHS, feeding data from left to right
type ApplyJoin struct {
	LHS, RHS ops.Operator

	// Columns stores the column indexes of the columns coming from the left and right side
	// negative value comes from LHS and positive from RHS
	Columns []int

	// ColumnsAST keeps track of what AST expression is represented in the Columns array
	ColumnsAST []sqlparser.Expr

	// Vars are the arguments that need to be copied from the LHS to the RHS
	Vars map[string]int

	// LeftJoin will be true in the case of an outer join
	LeftJoin bool

	// JoinCols are the columns from the LHS used for the join.
	// These are the same columns pushed on the LHS that are now used in the Vars field
	LHSColumns []*sqlparser.ColName

	Predicate sqlparser.Expr
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
	return AddPredicate(a, ctx, expr, false, newFilter)
}

// Inputs implements the Operator interface
func (a *ApplyJoin) Inputs() []ops.Operator {
	return []ops.Operator{a.LHS, a.RHS}
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
	bvName, cols, predicate, err := BreakExpressionInLHSandRHS(ctx, expr, TableID(a.LHS))
	if err != nil {
		return err
	}
	for i, col := range cols {
		offset, err := a.pushColLeft(ctx, aeWrap(col))
		if err != nil {
			return err
		}
		a.Vars[bvName[i]] = offset
	}
	a.LHSColumns = append(a.LHSColumns, cols...)

	rhs, err := a.RHS.AddPredicate(ctx, predicate)
	if err != nil {
		return err
	}
	a.RHS = rhs

	a.Predicate = ctx.SemTable.AndExpressions(expr, a.Predicate)
	return nil
}

func (a *ApplyJoin) pushColLeft(ctx *plancontext.PlanningContext, e *sqlparser.AliasedExpr) (int, error) {
	newLHS, offset, err := a.LHS.AddColumn(ctx, e, true)
	if err != nil {
		return 0, err
	}
	a.LHS = newLHS
	return offset, nil
}

func (a *ApplyJoin) pushColRight(ctx *plancontext.PlanningContext, e *sqlparser.AliasedExpr) (int, error) {
	newRHS, offset, err := a.RHS.AddColumn(ctx, e, true)
	if err != nil {
		return 0, err
	}
	a.RHS = newRHS
	return offset, nil
}

func (a *ApplyJoin) GetColumns() ([]sqlparser.Expr, error) {
	return a.ColumnsAST, nil
}

func (a *ApplyJoin) AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, reuseCol bool) (ops.Operator, int, error) {
	if offset, found := canReuseColumn(ctx, reuseCol, a.ColumnsAST, expr.Expr); found {
		return a, offset, nil
	}

	lhs := TableID(a.LHS)
	rhs := TableID(a.RHS)
	both := lhs.Merge(rhs)
	deps := ctx.SemTable.RecursiveDeps(expr.Expr)

	// if we get here, it's a new expression we are dealing with.
	// We need to decide if we can push it all on either side,
	// or if we have to break the expression into left and right parts
	switch {
	case deps.IsSolvedBy(lhs):
		offset, err := a.pushColLeft(ctx, expr)
		if err != nil {
			return nil, 0, err
		}
		a.Columns = append(a.Columns, -offset-1)
	case deps.IsSolvedBy(rhs):
		offset, err := a.pushColRight(ctx, expr)
		if err != nil {
			return nil, 0, err
		}
		a.Columns = append(a.Columns, offset+1)
	case deps.IsSolvedBy(both):
		bvNames, lhsExprs, rhsExpr, err := BreakExpressionInLHSandRHS(ctx, expr.Expr, lhs)
		if err != nil {
			return nil, 0, err
		}
		for i, lhsExpr := range lhsExprs {
			offset, err := a.pushColLeft(ctx, aeWrap(lhsExpr))
			if err != nil {
				return nil, 0, err
			}
			a.Vars[bvNames[i]] = offset
		}
		offset, err := a.pushColRight(ctx, aeWrap(rhsExpr))
		if err != nil {
			return nil, 0, err
		}
		a.Columns = append(a.Columns, offset+1)
	default:
		return nil, 0, vterrors.VT13002(sqlparser.String(expr))
	}

	// the expression wasn't already there - let's add it
	a.ColumnsAST = append(a.ColumnsAST, expr.Expr)
	return a, len(a.Columns) - 1, nil
}
