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
	BlockJoin struct {
		binaryOperator

		Destination string

		JoinPredicates []blockJoinColumn
	}

	// select tbl1.baz+tbl2.baz from tbl1, tbl2
	blockJoinColumn struct {
		Original sqlparser.Expr           // tbl1.baz+tbl2.baz
		RHS      sqlparser.Expr           // tbl1_baz+tbl2.baz
		LHS      []leftHandSideExpression // tbl1.baz|tbl1_baz
	}

	leftHandSideExpression struct {
		Original         *sqlparser.ColName
		RightHandVersion sqlparser.Expr
	}
)

func (c blockJoinColumn) String() string {
	return fmt.Sprintf("[%s:%s]", sqlparser.String(c.RHS), sqlparser.String(c.Original))
}

func (c blockJoinColumn) PureLHS() bool {
	return len(c.LHS) == 1 && sqlparser.Equals.Expr(c.LHS[0].RightHandVersion, c.RHS)
}

var _ Operator = (*BlockJoin)(nil)
var _ JoinOp = (*BlockJoin)(nil)

func (vj *BlockJoin) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, ae *sqlparser.AliasedExpr) int {
	if reuseExisting {
		if offset := vj.FindCol(ctx, ae.Expr, false); offset >= 0 {
			return offset
		}
	}

	jc, _ := breakBlockJoinExpressionInLHS(ctx, ae.Expr, TableID(vj.LHS), vj.Destination)
	for _, lhsExpr := range jc.LHS {
		_ = vj.LHS.AddColumn(ctx, true, false, aeWrap(lhsExpr.Original))
		ctx.AddBlockJoinColumn(vj.Destination, lhsExpr.RightHandVersion)
	}

	rhsCol := sqlparser.NewAliasedExpr(jc.RHS, ae.ColumnName())

	return vj.RHS.AddColumn(ctx, reuseExisting, addToGroupBy, rhsCol)
}

// AddWSColumn is used to add a weight_string column to the operator
func (vj *BlockJoin) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	return vj.RHS.AddWSColumn(ctx, offset, underRoute)
}

func (vj *BlockJoin) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return vj.RHS.FindCol(ctx, expr, underRoute)
}

func (vj *BlockJoin) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return vj.RHS.GetColumns(ctx)
}

func (vj *BlockJoin) GetSelectExprs(ctx *plancontext.PlanningContext) []sqlparser.SelectExpr {
	return vj.RHS.GetSelectExprs(ctx)
}

func (vj *BlockJoin) GetLHS() Operator {
	return vj.LHS
}

func (vj *BlockJoin) GetRHS() Operator {
	return vj.RHS
}

func (vj *BlockJoin) SetLHS(operator Operator) {
	vj.LHS = operator
}

func (vj *BlockJoin) SetRHS(operator Operator) {
	vj.RHS = operator
}

func (vj *BlockJoin) MakeInner() {
	// no-op for block-join
}

func (vj *BlockJoin) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	return AddPredicate(ctx, vj, expr, false, newFilterSinglePredicate)
}

func (vj *BlockJoin) IsInner() bool {
	return true
}

func (vj *BlockJoin) addJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr, pushDown bool) blockJoinColumn {
	if expr == nil {
		return blockJoinColumn{}
	}
	lID := TableID(vj.LHS)

	jc, pureLHS := breakBlockJoinExpressionInLHS(ctx, expr, lID, vj.Destination)
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

func (vj *BlockJoin) AddJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr, pushDown bool) {
	vj.addJoinPredicate(ctx, expr, pushDown)
}

func (vj *BlockJoin) Clone(inputs []Operator) Operator {
	clone := *vj
	clone.LHS = inputs[0]
	clone.RHS = inputs[1]
	return &clone
}

func (vj *BlockJoin) ShortDescription() string {
	fn := func(cols []blockJoinColumn) string {
		out := slice.Map(cols, func(jc blockJoinColumn) string {
			return jc.String()
		})
		return strings.Join(out, ", ")
	}

	firstPart := fmt.Sprintf("%s on %s", vj.Destination, fn(vj.JoinPredicates))

	return firstPart
}

func (vj *BlockJoin) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return vj.RHS.GetOrdering(ctx)
}

func (vj *BlockJoin) planOffsets(ctx *plancontext.PlanningContext) Operator {
	for _, jc := range vj.JoinPredicates {
		// for join predicates, we only need to push the LHS dependencies. The RHS expressions are already pushed
		for _, lh := range jc.LHS {
			vj.LHS.AddColumn(ctx, true, false, aeWrap(lh.Original))
			ctx.AddBlockJoinColumn(vj.Destination, lh.RightHandVersion)
		}
	}
	return vj
}

func getBlockJoinColName(ctx *plancontext.PlanningContext, destination string, tableID semantics.TableSet, expr sqlparser.Expr) string {
	col, isCol := expr.(*sqlparser.ColName)
	if !isCol {
		panic(fmt.Sprintf("expected a col named '%v'", expr))
	}
	tableName := col.Qualifier.Name.String()
	if tableName == "" {
		ti, err := ctx.SemTable.TableInfoFor(tableID)
		if err != nil {
			tableName = destination
		} else {
			tblName, err := ti.Name()
			if err != nil {
				tableName = destination
			} else {
				tableName = tblName.Name.String()
			}
		}
	}
	return tableName + "_" + col.Name.String()
}
