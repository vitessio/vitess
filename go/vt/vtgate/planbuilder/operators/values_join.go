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
	"slices"
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

		JoinColumns    []valuesJoinColumn
		JoinPredicates []valuesJoinColumn

		// After offset planning

		// CopyColumnsToRHS are the offsets of columns from LHS we are copying over to the RHS
		// []int{0,2} means that the first column in the t-o-t is the first offset from the left and the second column is the third offset
		CopyColumnsToRHS []int

		Columns    []int
		ColumnName []string
	}

	valuesJoinColumn struct {
		Original sqlparser.Expr
		RHS      sqlparser.Expr
		LHS      []sqlparser.Expr
		PureLHS  bool
	}
)

func (c valuesJoinColumn) String() string {
	return fmt.Sprintf("[%s:%s]", sqlparser.SliceString(c.LHS), sqlparser.String(c.Original))
}

var _ Operator = (*ValuesJoin)(nil)
var _ JoinOp = (*ValuesJoin)(nil)

func (vj *ValuesJoin) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, expr *sqlparser.AliasedExpr) int {
	if reuseExisting {
		if offset := vj.FindCol(ctx, expr.Expr, false); offset >= 0 {
			return offset
		}
	}

	vj.JoinColumns = append(vj.JoinColumns, breakValuesJoinExpressionInLHS(ctx, expr.Expr, TableID(vj.LHS)))
	vj.ColumnName = append(vj.ColumnName, expr.ColumnName())
	return len(vj.JoinColumns) - 1
}

// AddWSColumn is used to add a weight_string column to the operator
func (vj *ValuesJoin) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	panic("oh no")
}

func (vj *ValuesJoin) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	for offset, column := range vj.JoinColumns {
		if ctx.SemTable.EqualsExpr(column.Original, expr) {
			return offset
		}
	}
	return -1
}

func (vj *ValuesJoin) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	results := make([]*sqlparser.AliasedExpr, len(vj.JoinColumns))
	for i, column := range vj.JoinColumns {
		results[i] = sqlparser.NewAliasedExpr(column.Original, vj.ColumnName[i])
	}
	return results
}

func (vj *ValuesJoin) GetSelectExprs(ctx *plancontext.PlanningContext) []sqlparser.SelectExpr {
	return transformColumnsToSelectExprs(ctx, vj)
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

func (vj *ValuesJoin) AddJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr, pushDown bool) {
	if expr == nil {
		return
	}
	lID := TableID(vj.LHS)
	lhsJoinCols := breakValuesJoinExpressionInLHS(ctx, expr, lID)
	if pushDown {
		if lhsJoinCols.PureLHS {
			vj.LHS = vj.LHS.AddPredicate(ctx, expr)
			return
		}
		vj.RHS = vj.RHS.AddPredicate(ctx, expr)
	}
	vj.JoinPredicates = append(vj.JoinPredicates, lhsJoinCols)
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

	firstPart := fmt.Sprintf("%s on %s columns: %s", vj.ValuesDestination, fn(vj.JoinPredicates), fn(vj.JoinColumns))

	return firstPart
}

func (vj *ValuesJoin) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return vj.RHS.GetOrdering(ctx)
}

func (vj *ValuesJoin) planOffsets(ctx *plancontext.PlanningContext) Operator {
	exprs := ctx.GetColumns(vj.ValuesDestination)
	for _, jc := range vj.JoinColumns {
		newExprs := vj.planOffsetsForLHSExprs(ctx, jc.LHS)
		for _, expr := range newExprs {
			offset := vj.RHS.AddColumn(ctx, true, false, expr)
			vj.Columns = append(vj.Columns, ToRightOffset(offset))
		}
		exprs = append(exprs, newExprs...)
	}
	for _, jc := range vj.JoinPredicates {
		// for join predicates, we only need to push the LHS dependencies. The RHS expressions are already pushed
		newExprs := vj.planOffsetsForLHSExprs(ctx, jc.LHS)
		exprs = append(exprs, newExprs...)
	}
	ctx.SetColumns(vj.ValuesDestination, exprs)
	return vj
}

func (vj *ValuesJoin) planOffsetsForLHSExprs(ctx *plancontext.PlanningContext, lhsCols []sqlparser.Expr) (exprs []*sqlparser.AliasedExpr) {
	for _, lhsExpr := range lhsCols {
		offset := vj.LHS.AddColumn(ctx, true, false, aeWrap(lhsExpr))
		// only add it if we don't already have it
		if slices.Index(vj.CopyColumnsToRHS, offset) == -1 {
			vj.CopyColumnsToRHS = append(vj.CopyColumnsToRHS, offset)
			newCol := sqlparser.NewColNameWithQualifier(getValuesJoinColName(ctx, vj.ValuesDestination, TableID(vj.LHS), lhsExpr), sqlparser.NewTableName(vj.ValuesDestination))
			exprs = append(exprs, aeWrap(newCol))
		}
	}
	return exprs
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
