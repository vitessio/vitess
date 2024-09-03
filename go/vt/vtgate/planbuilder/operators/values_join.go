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
	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type ValuesJoin struct {
	binaryOperator

	JoinType sqlparser.JoinType

	LHSExprs sqlparser.Exprs

	// Done at offset planning time
	// Vars are the arguments that need to be copied from the LHS to the RHS
	Vars map[string]int
}

func newValuesJoin(lhs, rhs Operator, joinType sqlparser.JoinType) *ValuesJoin {
	return &ValuesJoin{
		binaryOperator: newBinaryOp(lhs, rhs),
		JoinType:       joinType,
	}
}

var _ Operator = (*ValuesJoin)(nil)
var _ JoinOp = (*ValuesJoin)(nil)

func (vj *ValuesJoin) Clone(inputs []Operator) Operator {
	nvj := *vj
	nvj.LHS = inputs[0]
	nvj.RHS = inputs[1]
	nvj.Vars = maps.Clone(vj.Vars)
	return &nvj
}

func (vj *ValuesJoin) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	return AddPredicate(ctx, vj, expr, false, newFilterSinglePredicate)
}

func (vj *ValuesJoin) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, expr *sqlparser.AliasedExpr) int {
	col := breakExpressionInLHSandRHS(ctx, expr.Expr, TableID(vj.LHS))

	for _, exp := range col.LHSExprs {
		vj.LHSExprs = append(vj.LHSExprs, exp.Expr)
	}
	return vj.RHS.AddColumn(ctx, reuseExisting, addToGroupBy, expr)
}

func (vj *ValuesJoin) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	return vj.RHS.AddWSColumn(ctx, offset, underRoute)
}

func (vj *ValuesJoin) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return vj.RHS.FindCol(ctx, expr, underRoute)
}

func (vj *ValuesJoin) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return vj.RHS.GetColumns(ctx)
}

func (vj *ValuesJoin) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	return append(vj.LHS.GetSelectExprs(ctx), vj.RHS.GetSelectExprs(ctx)...)
}

func (vj *ValuesJoin) ShortDescription() string {
	return ""
}

func (vj *ValuesJoin) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return vj.RHS.GetOrdering(ctx)
}

func (vj *ValuesJoin) MakeInner() {
	if vj.IsInner() {
		return
	}
	vj.JoinType = sqlparser.NormalJoinType
}

func (vj *ValuesJoin) IsInner() bool {
	return vj.JoinType.IsInner()
}

func (vj *ValuesJoin) AddJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) {
	if expr == nil {
		return
	}
	predicates := sqlparser.SplitAndExpression(nil, expr)
	for _, pred := range predicates {
		col := breakExpressionInLHSandRHS(ctx, pred, TableID(vj.LHS))

		if col.IsPureLeft() {
			vj.LHS = vj.LHS.AddPredicate(ctx, pred)
		} else {
			for _, exp := range col.LHSExprs {
				vj.LHSExprs = append(vj.LHSExprs, exp.Expr)
			}
			err := ctx.SkipJoinPredicates(pred)
			if err != nil {
				panic(err)
			}
			vj.RHS = vj.RHS.AddPredicate(ctx, pred)
		}
	}
}
