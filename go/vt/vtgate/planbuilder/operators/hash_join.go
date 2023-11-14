/*
Copyright 2023 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	HashJoin struct {
		LHS, RHS ops.Operator

		// LeftJoin will be true in the case of an outer join
		LeftJoin bool

		// Before offset planning
		JoinComparisons []Comparison

		// These columns are the output columns of the hash join. While in operator mode we keep track of complex expression,
		// but once we move to the engine primitives, the hash join only passes through column from either left or right.
		// anything more complex will be solved by a projection on top of the hash join
		columns []sqlparser.Expr

		// After offset planning

		// Columns stores the column indexes of the columns coming from the left and right side
		// negative value comes from LHS and positive from RHS
		ColumnOffsets []int

		// These are the values that will be hashed together
		LHSKeys, RHSKeys []int

		offset bool
	}

	Comparison struct {
		LHS, RHS sqlparser.Expr
	}
)

var _ ops.Operator = (*HashJoin)(nil)
var _ JoinOp = (*HashJoin)(nil)

func NewHashJoin(lhs, rhs ops.Operator, outerJoin bool) *HashJoin {
	hj := &HashJoin{
		LHS:      lhs,
		RHS:      rhs,
		LeftJoin: outerJoin,
	}
	return hj
}

func (hj *HashJoin) Clone(inputs []ops.Operator) ops.Operator {
	kopy := *hj
	kopy.LHS, kopy.RHS = inputs[0], inputs[1]
	kopy.columns = slices.Clone(hj.columns)
	kopy.LHSKeys = slices.Clone(hj.LHSKeys)
	kopy.RHSKeys = slices.Clone(hj.RHSKeys)
	return &kopy
}

func (hj *HashJoin) Inputs() []ops.Operator {
	return []ops.Operator{hj.LHS, hj.RHS}
}

func (hj *HashJoin) SetInputs(operators []ops.Operator) {
	hj.LHS, hj.RHS = operators[0], operators[1]
}

func (hj *HashJoin) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) ops.Operator {
	return AddPredicate(ctx, hj, expr, false, newFilter)
}

func (hj *HashJoin) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, expr *sqlparser.AliasedExpr) int {
	if reuseExisting {
		offset := hj.FindCol(ctx, expr.Expr, false)
		if offset >= 0 {
			return offset
		}
	}

	hj.columns = append(hj.columns, expr.Expr)
	return len(hj.columns) - 1
}

func (hj *HashJoin) planOffsets(ctx *plancontext.PlanningContext) ops.Operator {
	if hj.offset {
		return nil
	}
	hj.offset = true
	for _, cmp := range hj.JoinComparisons {
		lOffset := hj.LHS.AddColumn(ctx, true, false, aeWrap(cmp.LHS))
		hj.LHSKeys = append(hj.LHSKeys, lOffset)
		rOffset := hj.LHS.AddColumn(ctx, true, false, aeWrap(cmp.RHS))
		hj.RHSKeys = append(hj.RHSKeys, rOffset)
	}

	eexprs := slice.Map(hj.columns, func(in sqlparser.Expr) *ProjExpr {
		return hj.addColumn(ctx, in)
	})

	proj := newAliasedProjection(hj)
	_, err := proj.addProjExpr(eexprs...)
	if err != nil {
		panic(err)
	}

	return proj
}

func (hj *HashJoin) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	for offset, col := range hj.columns {
		if ctx.SemTable.EqualsExprWithDeps(expr, col) {
			return offset
		}
	}
	return -1
}

func (hj *HashJoin) GetColumns(*plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return slice.Map(hj.columns, aeWrap)
}

func (hj *HashJoin) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	return transformColumnsToSelectExprs(ctx, hj)
}

func (hj *HashJoin) ShortDescription() string {
	comparisons := slice.Map(hj.JoinComparisons, func(from Comparison) string {
		return from.String()
	})
	cmp := strings.Join(comparisons, " AND ")

	if len(hj.columns) > 0 {
		return fmt.Sprintf("%s columns %v", cmp, hj.columns)
	}

	return cmp
}

func (hj *HashJoin) GetOrdering(ctx *plancontext.PlanningContext) []ops.OrderBy {
	return nil // hash joins will never promise an output order
}

func (hj *HashJoin) GetLHS() ops.Operator {
	return hj.LHS
}

func (hj *HashJoin) GetRHS() ops.Operator {
	return hj.RHS
}

func (hj *HashJoin) SetLHS(op ops.Operator) {
	hj.LHS = op
}

func (hj *HashJoin) SetRHS(op ops.Operator) {
	hj.RHS = op
}

func (hj *HashJoin) MakeInner() {
	hj.LeftJoin = false
}

func (hj *HashJoin) IsInner() bool {
	return !hj.LeftJoin
}

func (hj *HashJoin) AddJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) {
	cmp, ok := expr.(*sqlparser.ComparisonExpr)
	if !ok || !canBeSolvedWithHashJoin(cmp.Operator) {
		panic(vterrors.VT12001(fmt.Sprintf("can't use [%s] with hash joins", sqlparser.String(expr))))
	}
	lExpr := cmp.Left
	lDeps := ctx.SemTable.RecursiveDeps(lExpr)
	rExpr := cmp.Right
	rDeps := ctx.SemTable.RecursiveDeps(rExpr)
	lID := TableID(hj.LHS)
	rID := TableID(hj.RHS)
	if !lDeps.IsSolvedBy(lID) || !rDeps.IsSolvedBy(rID) {
		// we'll switch and see if things work out then
		lExpr, rExpr = rExpr, lExpr
		lDeps, rDeps = rDeps, lDeps
	}

	if !lDeps.IsSolvedBy(lID) || !rDeps.IsSolvedBy(rID) {
		panic(vterrors.VT12001(fmt.Sprintf("can't use [%s] with hash joins", sqlparser.String(expr))))
	}

	hj.JoinComparisons = append(hj.JoinComparisons, Comparison{
		LHS: lExpr,
		RHS: rExpr,
	})
}

func canBeSolvedWithHashJoin(op sqlparser.ComparisonExprOperator) bool {
	switch op {
	case sqlparser.EqualOp, sqlparser.NullSafeEqualOp:
		return true
	default:
		return false
	}
}

func (c Comparison) String() string {
	return sqlparser.String(c.LHS) + " = " + sqlparser.String(c.RHS)
}

func (hj *HashJoin) addColumn(ctx *plancontext.PlanningContext, in sqlparser.Expr) *ProjExpr {
	lId, rId := TableID(hj.LHS), TableID(hj.RHS)
	var replaceExpr sqlparser.Expr // this is the expression we will put in instead of whatever we find there
	pre := func(node, parent sqlparser.SQLNode) bool {
		expr, ok := node.(sqlparser.Expr)
		if !ok {
			return true
		}
		deps := ctx.SemTable.RecursiveDeps(expr)
		check := func(id semantics.TableSet, op ops.Operator, offsetter func(int) int) int {
			if !deps.IsSolvedBy(id) {
				return -1
			}
			inOffset := op.FindCol(ctx, expr, false)
			if inOffset == -1 {
				if !fetchByOffset(expr) {
					return -1
				}

				// aha! this is an expression that we have to get from the input. let's force it in there
				inOffset = op.AddColumn(ctx, false, false, aeWrap(expr))
			}

			// we turn the
			internalOffset := offsetter(inOffset)

			// ok, we have an offset from the input operator. Let's check if we already have it
			// in our list of incoming columns

			for idx, offset := range hj.ColumnOffsets {
				if internalOffset == offset {
					return idx
				}
			}

			hj.ColumnOffsets = append(hj.ColumnOffsets, internalOffset)

			return len(hj.ColumnOffsets) - 1
		}

		f := func(i int) int { return (i * -1) - 1 }
		if lOffset := check(lId, hj.LHS, f); lOffset >= 0 {
			replaceExpr = sqlparser.NewOffset(lOffset, expr)
			return false // we want to stop going down the expression tree and start coming back up again
		}

		f = func(i int) int { return i + 1 }
		if rOffset := check(rId, hj.RHS, f); rOffset >= 0 {
			replaceExpr = sqlparser.NewOffset(rOffset, expr)
			return false
		}

		return true
	}

	post := func(cursor *sqlparser.CopyOnWriteCursor) {
		if replaceExpr != nil {
			node := cursor.Node()
			_, ok := node.(sqlparser.Expr)
			if !ok {
				panic(fmt.Sprintf("can't replace this node with an expression: %s", sqlparser.String(node)))
			}
			cursor.Replace(replaceExpr)
			replaceExpr = nil
		}
	}

	rewrittenExpr := sqlparser.CopyOnRewrite(in, pre, post, ctx.SemTable.CopySemanticInfo).(sqlparser.Expr)
	cfg := &evalengine.Config{
		ResolveType: ctx.SemTable.TypeForExpr,
		Collation:   ctx.SemTable.Collation,
	}
	eexpr, err := evalengine.Translate(rewrittenExpr, cfg)
	if err != nil {
		panic(err)
	}

	return &ProjExpr{
		Original: aeWrap(in),
		EvalExpr: rewrittenExpr,
		ColExpr:  rewrittenExpr,
		Info:     &EvalEngine{EExpr: eexpr},
	}
}

// JoinPredicate produces an AST representation of the join condition this join has
func (hj *HashJoin) JoinPredicate() sqlparser.Expr {
	exprs := slice.Map(hj.JoinComparisons, func(from Comparison) sqlparser.Expr {
		return &sqlparser.ComparisonExpr{
			Left:  from.LHS,
			Right: from.RHS,
		}
	})
	return sqlparser.AndExpressions(exprs...)
}
