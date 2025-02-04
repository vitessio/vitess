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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	ValuesJoin struct {
		binaryOperator

		BindVarName string

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
		LHS      []*sqlparser.ColName
		PureLHS  bool
	}
)

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

func (vj *ValuesJoin) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
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

func (vj *ValuesJoin) IsInner() bool {
	return true
}

func (vj *ValuesJoin) AddJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) {
	if expr == nil {
		return
	}
	lID := TableID(vj.LHS)
	lhsJoinCols := breakValuesJoinExpressionInLHS(ctx, expr, lID)
	if lhsJoinCols.PureLHS {
		vj.LHS = vj.LHS.AddPredicate(ctx, expr)
		return
	}
	vj.RHS = vj.RHS.AddPredicate(ctx, expr)
	vj.JoinPredicates = append(vj.JoinPredicates, lhsJoinCols)
}

func (vj *ValuesJoin) Clone(inputs []Operator) Operator {
	clone := *vj
	clone.LHS = inputs[0]
	clone.RHS = inputs[1]
	return &clone
}

func (vj *ValuesJoin) ShortDescription() string {
	return ""
}

func (vj *ValuesJoin) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return vj.RHS.GetOrdering(ctx)
}

func (vj *ValuesJoin) planOffsets(ctx *plancontext.PlanningContext) Operator {
	valuesColumns := ctx.ValuesJoinColumns[vj.BindVarName]
	for i, jc := range vj.JoinColumns {
		if jc.PureLHS {
			offset := vj.LHS.AddColumn(ctx, true, false, sqlparser.NewAliasedExpr(jc.Original, vj.ColumnName[i]))
			vj.Columns = append(vj.Columns, ToLeftOffset(offset))
			continue
		}
		vj.planOffsetsForValueJoinPredicate(ctx, jc.LHS, &valuesColumns)
		ctx.ValuesJoinColumns[vj.BindVarName] = valuesColumns

		offset := vj.RHS.AddColumn(ctx, true, false, aeWrap(jc.Original))
		vj.Columns = append(vj.Columns, ToRightOffset(offset))
	}

	for _, predicate := range vj.JoinPredicates {
		vj.planOffsetsForValueJoinPredicate(ctx, predicate.LHS, &valuesColumns)
	}

	ctx.ValuesJoinColumns[vj.BindVarName] = valuesColumns
	return vj
}

func (vj *ValuesJoin) planOffsetsForValueJoinPredicate(ctx *plancontext.PlanningContext, lhsPred []*sqlparser.ColName, valuesColumns *sqlparser.Columns) {
outer:
	for _, lh := range lhsPred {
		for _, ci := range *valuesColumns {
			if ci.Equal(lh.Name) {
				// already there, no need to add it again
				continue outer
			}
		}
		offset := vj.LHS.AddColumn(ctx, true, false, aeWrap(lh))
		vj.CopyColumnsToRHS = append(vj.CopyColumnsToRHS, offset)
		*valuesColumns = append(*valuesColumns, lh.Name)
	}
}
