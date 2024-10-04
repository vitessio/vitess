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
	"encoding/json"
	"fmt"
	"slices"
	"sort"

	"vitess.io/vitess/go/slice"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	Column struct {
		Table string
		Name  string
	}

	ColumnUse struct {
		Column Column
		Uses   sqlparser.ComparisonExprOperator
	}

	VExplainKeys struct {
		StatementType   string
		TableName       []string
		GroupingColumns []Column
		JoinColumns     []ColumnUse
		FilterColumns   []ColumnUse
		SelectColumns   []Column
	}
)

func (c Column) String() string {
	return fmt.Sprintf("%s.%s", c.Table, c.Name)
}

func (c ColumnUse) String() string {
	return fmt.Sprintf("%s %s", c.Column, c.Uses.JSONString())
}

type columnUse struct {
	col *sqlparser.ColName
	use sqlparser.ComparisonExprOperator
}

func GetVExplainKeys(ctx *plancontext.PlanningContext, stmt sqlparser.Statement) (result VExplainKeys) {
	var groupingColumns, selectColumns []*sqlparser.ColName
	var filterColumns, joinColumns []columnUse

	addPredicate := func(predicate sqlparser.Expr) {
		predicates := sqlparser.SplitAndExpression(nil, predicate)
		for _, expr := range predicates {
			switch cmp := expr.(type) {
			case *sqlparser.ComparisonExpr:
				lhs, lhsOK := cmp.Left.(*sqlparser.ColName)
				rhs, rhsOK := cmp.Right.(*sqlparser.ColName)

				var output = &filterColumns
				if lhsOK && rhsOK && ctx.SemTable.RecursiveDeps(lhs) != ctx.SemTable.RecursiveDeps(rhs) {
					// If the columns are from different tables, they are considered join columns
					output = &joinColumns
				}

				if lhsOK {
					*output = append(*output, columnUse{lhs, cmp.Operator})
				}

				if switchedOp, ok := cmp.Operator.SwitchSides(); rhsOK && ok {
					*output = append(*output, columnUse{rhs, switchedOp})
				}
			case *sqlparser.BetweenExpr:
				if col, ok := cmp.Left.(*sqlparser.ColName); ok {
					//a BETWEEN 100 AND 200    is equivalent to    a >= 100 AND a <= 200
					filterColumns = append(filterColumns,
						columnUse{col, sqlparser.GreaterEqualOp},
						columnUse{col, sqlparser.LessEqualOp})
				}
			}

		}
	}

	_ = sqlparser.VisitSQLNode(stmt, func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.Where:
			addPredicate(node.Expr)
		case *sqlparser.JoinCondition:
			addPredicate(node.On)
		case *sqlparser.GroupBy:
			for _, expr := range node.Exprs {
				col, ok := expr.(*sqlparser.ColName)
				if ok {
					groupingColumns = append(groupingColumns, col)
				}
			}
		case *sqlparser.AliasedExpr:
			_ = sqlparser.VisitSQLNode(node, func(e sqlparser.SQLNode) (kontinue bool, err error) {
				if col, ok := e.(*sqlparser.ColName); ok {
					selectColumns = append(selectColumns, col)
				}
				return true, nil
			})
		}

		return true, nil
	})

	return VExplainKeys{
		SelectColumns:   getUniqueColNames(ctx, selectColumns),
		GroupingColumns: getUniqueColNames(ctx, groupingColumns),
		JoinColumns:     getUniqueColUsages(ctx, joinColumns),
		FilterColumns:   getUniqueColUsages(ctx, filterColumns),
		StatementType:   sqlparser.ASTToStatementType(stmt).String(),
	}
}

func getUniqueColNames(ctx *plancontext.PlanningContext, inCols []*sqlparser.ColName) (columns []Column) {
	for _, col := range inCols {
		column := createColumn(ctx, col)
		if column != nil {
			columns = append(columns, *column)
		}
	}
	sort.Slice(columns, func(i, j int) bool {
		return columns[i].String() < columns[j].String()
	})

	return slices.Compact(columns)
}

func getUniqueColUsages(ctx *plancontext.PlanningContext, inCols []columnUse) (columns []ColumnUse) {
	for _, col := range inCols {
		column := createColumn(ctx, col.col)
		if column != nil {
			columns = append(columns, ColumnUse{Column: *column, Uses: col.use})
		}
	}

	sort.Slice(columns, func(i, j int) bool {
		return columns[i].Column.String() < columns[j].Column.String()
	})
	return slices.Compact(columns)
}

func createColumn(ctx *plancontext.PlanningContext, col *sqlparser.ColName) *Column {
	tableInfo, err := ctx.SemTable.TableInfoForExpr(col)
	if err != nil {
		return nil
	}
	table := tableInfo.GetVindexTable()
	if table == nil {
		return nil
	}
	return &Column{Table: table.Name.String(), Name: col.Name.String()}
}

func (v VExplainKeys) MarshalJSON() ([]byte, error) {
	aux := struct {
		StatementType   string   `json:"statementType"`
		TableName       []string `json:"tableName,omitempty"`
		GroupingColumns []string `json:"groupingColumns,omitempty"`
		JoinColumns     []string `json:"joinColumns,omitempty"`
		FilterColumns   []string `json:"filterColumns,omitempty"`
		SelectColumns   []string `json:"selectColumns,omitempty"`
	}{
		StatementType:   v.StatementType,
		TableName:       v.TableName,
		SelectColumns:   slice.Map(v.SelectColumns, Column.String),
		GroupingColumns: slice.Map(v.GroupingColumns, Column.String),
		JoinColumns:     slice.Map(v.JoinColumns, ColumnUse.String),
		FilterColumns:   slice.Map(v.FilterColumns, ColumnUse.String),
	}
	return json.Marshal(aux)
}
