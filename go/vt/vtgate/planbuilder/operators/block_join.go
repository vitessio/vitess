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
		Original,
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

func (bj *BlockJoin) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, ae *sqlparser.AliasedExpr) int {
	if reuseExisting {
		if offset := bj.FindCol(ctx, ae.Expr, false); offset >= 0 {
			return offset
		}
	}

	jc, _ := breakBlockJoinExpressionInLHS(ctx, ae.Expr, TableID(bj.LHS), bj.Destination)
	for _, lhsExpr := range jc.LHS {
		_ = bj.LHS.AddColumn(ctx, true, false, aeWrap(lhsExpr.Original))
		ctx.AddBlockJoinColumn(bj.Destination, lhsExpr.RightHandVersion)
	}

	rhsCol := sqlparser.NewAliasedExpr(jc.RHS, ae.ColumnName())

	return bj.RHS.AddColumn(ctx, reuseExisting, addToGroupBy, rhsCol)
}

// AddWSColumn is used to add a weight_string column to the operator
func (bj *BlockJoin) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	return bj.RHS.AddWSColumn(ctx, offset, underRoute)
}

func (bj *BlockJoin) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return bj.RHS.FindCol(ctx, expr, underRoute)
}

func (bj *BlockJoin) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return bj.RHS.GetColumns(ctx)
}

func (bj *BlockJoin) GetSelectExprs(ctx *plancontext.PlanningContext) []sqlparser.SelectExpr {
	return bj.RHS.GetSelectExprs(ctx)
}

func (bj *BlockJoin) GetLHS() Operator {
	return bj.LHS
}

func (bj *BlockJoin) GetRHS() Operator {
	return bj.RHS
}

func (bj *BlockJoin) SetLHS(operator Operator) {
	bj.LHS = operator
}

func (bj *BlockJoin) SetRHS(operator Operator) {
	bj.RHS = operator
}

func (bj *BlockJoin) MakeInner() {
	// no-op for block-join
}

func (bj *BlockJoin) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	return AddPredicate(ctx, bj, expr, false, newFilterSinglePredicate)
}

func (bj *BlockJoin) IsInner() bool {
	return true
}

func (bj *BlockJoin) addJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr, pushDown bool) blockJoinColumn {
	if expr == nil {
		return blockJoinColumn{}
	}
	lID := TableID(bj.LHS)

	jc, pureLHS := breakBlockJoinExpressionInLHS(ctx, expr, lID, bj.Destination)
	if !pushDown {
		bj.JoinPredicates = append(bj.JoinPredicates, jc)
		return jc
	}

	if pureLHS {
		bj.LHS = bj.LHS.AddPredicate(ctx, expr)
		return jc
	}

	bj.RHS = bj.RHS.AddPredicate(ctx, jc.RHS)
	if len(jc.LHS) == 0 {
		// this is a pure RHS predicate, let's push it there and forget about it
		return jc
	}

	bj.JoinPredicates = append(bj.JoinPredicates, jc)
	return jc
}

func (bj *BlockJoin) AddJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr, pushDown bool) {
	bj.addJoinPredicate(ctx, expr, pushDown)
}

func (bj *BlockJoin) Clone(inputs []Operator) Operator {
	clone := *bj
	clone.LHS = inputs[0]
	clone.RHS = inputs[1]
	return &clone
}

func (bj *BlockJoin) ShortDescription() string {
	fn := func(cols []blockJoinColumn) string {
		out := slice.Map(cols, func(jc blockJoinColumn) string {
			return jc.String()
		})
		return strings.Join(out, ", ")
	}

	firstPart := fmt.Sprintf("%s on %s", bj.Destination, fn(bj.JoinPredicates))

	return firstPart
}

func (bj *BlockJoin) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return bj.RHS.GetOrdering(ctx)
}

func (bj *BlockJoin) planOffsets(ctx *plancontext.PlanningContext) Operator {
	for _, jc := range bj.JoinPredicates {
		// for join predicates, we only need to push the LHS dependencies. The RHS expressions are already pushed
		for _, lh := range jc.LHS {
			offset := bj.LHS.FindCol(ctx, lh.Original, false)
			if offset < 0 {
				bj.LHS.AddColumn(ctx, true, false, aeWrap(lh.Original))
				ctx.AddBlockJoinColumn(bj.Destination, lh.RightHandVersion)
			}
		}
	}
	return bj
}

const blockJoinCol = "x_x"

func getBlockJoinColName(ctx *plancontext.PlanningContext, destination string, tableID semantics.TableSet, expr sqlparser.Expr) string {
	col, isCol := expr.(*sqlparser.ColName)
	if isCol {
		return getBlockJoinColNameForColName(ctx, destination, tableID, col)
	}

	return ctx.ReservedVars.ReserveVariable(blockJoinCol)
}

func getBlockJoinColNameForColName(ctx *plancontext.PlanningContext, destination string, tableID semantics.TableSet, col *sqlparser.ColName) string {
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
