/*
Copyright 2025 The Vitess Authors.

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
	"strings"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	ValuesJoin struct {
		binaryOperator

		ValuesDestination string

		JoinPredicates []valuesJoinColumn
	}

	// select tbl1.baz+tbl2.baz from tbl1, tbl2
	valuesJoinColumn struct {
		Original sqlparser.Expr           // tbl1.baz+tbl2.baz
		RHS      sqlparser.Expr           // tbl1_baz+tbl2.baz
		LHS      []leftHandSideExpression // tbl1.baz|tbl1_baz
	}

	leftHandSideExpression struct {
		Original         *sqlparser.ColName
		RightHandVersion sqlparser.Expr
	}
)

func (c valuesJoinColumn) String() string {
	return fmt.Sprintf("[%s:%s]", sqlparser.String(c.RHS), sqlparser.String(c.Original))
}

func (c valuesJoinColumn) PureLHS() bool {
	return len(c.LHS) == 1 && sqlparser.Equals.Expr(c.LHS[0].RightHandVersion, c.RHS)
}

var _ Operator = (*ValuesJoin)(nil)
var _ JoinOp = (*ValuesJoin)(nil)

func (vj *ValuesJoin) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, ae *sqlparser.AliasedExpr) int {
	if reuseExisting {
		if offset := vj.FindCol(ctx, ae.Expr, false); offset >= 0 {
			return offset
		}
	}

	jc, _ := breakValuesJoinExpressionInLHS(ctx, ae.Expr, TableID(vj.LHS), vj.ValuesDestination)
	for _, lhsExpr := range jc.LHS {
		_ = vj.LHS.AddColumn(ctx, true, false, aeWrap(lhsExpr.Original))
		ctx.AddValuesColumn(vj.ValuesDestination, lhsExpr.RightHandVersion)
	}

	rhsCol := sqlparser.NewAliasedExpr(jc.RHS, ae.ColumnName())

	return vj.RHS.AddColumn(ctx, reuseExisting, addToGroupBy, rhsCol)
}

// AddWSColumn is used to add a weight_string column to the operator
func (vj *ValuesJoin) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	panic("oh no")
}

func (vj *ValuesJoin) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return vj.RHS.FindCol(ctx, expr, underRoute)
}

func (vj *ValuesJoin) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return vj.RHS.GetColumns(ctx)
}

func (vj *ValuesJoin) GetSelectExprs(ctx *plancontext.PlanningContext) []sqlparser.SelectExpr {
	return vj.RHS.GetSelectExprs(ctx)
}

func (vj *ValuesJoin) GetLHS() Operator {
	return vj.LHS
}

func (vj *ValuesJoin) GetRHS() Operator {
	return vj.RHS
}

func (vj *ValuesJoin) SetLHS(operator Operator) {
	vj.LHS = operator
}

func (vj *ValuesJoin) SetRHS(operator Operator) {
	vj.RHS = operator
}

func (vj *ValuesJoin) MakeInner() {
	// no-op for values-join
}

func (vj *ValuesJoin) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	return AddPredicate(ctx, vj, expr, false, newFilterSinglePredicate)
}

func (vj *ValuesJoin) IsInner() bool {
	return true
}

func (vj *ValuesJoin) addJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr, pushDown bool) valuesJoinColumn {
	if expr == nil {
		return valuesJoinColumn{}
	}
	lID := TableID(vj.LHS)

	jc, pureLHS := breakValuesJoinExpressionInLHS(ctx, expr, lID, vj.ValuesDestination)
	if !pushDown {
		vj.JoinPredicates = append(vj.JoinPredicates, jc)
		return jc
	}

	if pureLHS {
		vj.LHS = vj.LHS.AddPredicate(ctx, expr)
		return jc
	}

	vj.RHS = vj.RHS.AddPredicate(ctx, jc.RHS)
	if len(jc.LHS) == 0 {
		// this is a pure RHS predicate, let's push it there and forget about it
		return jc
	}

	vj.JoinPredicates = append(vj.JoinPredicates, jc)
	return jc
}

func (vj *ValuesJoin) AddJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr, pushDown bool) {
	vj.addJoinPredicate(ctx, expr, pushDown)
}

func (vj *ValuesJoin) Clone(inputs []Operator) Operator {
	clone := *vj
	clone.LHS = inputs[0]
	clone.RHS = inputs[1]
	return &clone
}

func (vj *ValuesJoin) ShortDescription() string {
	fn := func(cols []valuesJoinColumn) string {
		out := slice.Map(cols, func(jc valuesJoinColumn) string {
			return jc.String()
		})
		return strings.Join(out, ", ")
	}

	firstPart := fmt.Sprintf("%s on %s", vj.ValuesDestination, fn(vj.JoinPredicates))

	return firstPart
}

func (vj *ValuesJoin) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return vj.RHS.GetOrdering(ctx)
}

func (vj *ValuesJoin) planOffsets(ctx *plancontext.PlanningContext) Operator {
	for _, jc := range vj.JoinPredicates {
		// for join predicates, we only need to push the LHS dependencies. The RHS expressions are already pushed
		for _, lh := range jc.LHS {
			vj.LHS.AddColumn(ctx, true, false, aeWrap(lh.Original))
			ctx.AddValuesColumn(vj.ValuesDestination, lh.RightHandVersion)
		}
	}
	return vj
}

func getValuesJoinColName(ctx *plancontext.PlanningContext, valuesDestination string, tableID semantics.TableSet, expr sqlparser.Expr) string {
	col, isCol := expr.(*sqlparser.ColName)
	if !isCol {
		panic(fmt.Sprintf("expected a col named '%v'", expr))
	}
	tableName := col.Qualifier.Name.String()
	if tableName == "" {
		ti, err := ctx.SemTable.TableInfoFor(tableID)
		if err != nil {
			tableName = valuesDestination
		} else {
			tblName, err := ti.Name()
			if err != nil {
				tableName = valuesDestination
			} else {
				tableName = tblName.Name.String()
			}
		}
	}
	return tableName + "_" + col.Name.String()
}
