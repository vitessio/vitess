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
	"fmt"
	"slices"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type VExplainKeys struct {
	GroupingColumns []string `json:"Grouping Columns,omitempty"`
	TableName       []string `json:"TableName,omitempty"`
	JoinColumns     []string `json:"JoinColumns,omitempty"`
	FilterColumns   []string `json:"FilterColumns,omitempty"`
	StatementType   string   `json:"StatementType"`
}

func GetVExplainKeys(ctx *plancontext.PlanningContext, stmt sqlparser.Statement) (result VExplainKeys) {
	var filterColumns, joinColumns, groupingColumns []*sqlparser.ColName

	addPredicate := func(predicate sqlparser.Expr) {
		predicates := sqlparser.SplitAndExpression(nil, predicate)
		for _, expr := range predicates {
			cmp, ok := expr.(*sqlparser.ComparisonExpr)
			if !ok {
				continue
			}
			lhs, lhsOK := cmp.Left.(*sqlparser.ColName)
			rhs, rhsOK := cmp.Right.(*sqlparser.ColName)
			if lhsOK && rhsOK && ctx.SemTable.RecursiveDeps(lhs) != ctx.SemTable.RecursiveDeps(rhs) {
				joinColumns = append(joinColumns, lhs, rhs)
				continue
			}
			if lhsOK {
				filterColumns = append(filterColumns, lhs)
			}
			if rhsOK {
				filterColumns = append(filterColumns, rhs)
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
				predicates := sqlparser.SplitAndExpression(nil, expr)
				for _, expr := range predicates {
					col, ok := expr.(*sqlparser.ColName)
					if ok {
						groupingColumns = append(groupingColumns, col)
					}
				}
			}
		}

		return true, nil
	})

	return VExplainKeys{
		GroupingColumns: getUniqueColNames(ctx, groupingColumns),
		JoinColumns:     getUniqueColNames(ctx, joinColumns),
		FilterColumns:   getUniqueColNames(ctx, filterColumns),
		StatementType:   sqlparser.ASTToStatementType(stmt).String(),
	}
}

func getUniqueColNames(ctx *plancontext.PlanningContext, columns []*sqlparser.ColName) []string {
	var colNames []string
	for _, col := range columns {
		tableInfo, err := ctx.SemTable.TableInfoForExpr(col)
		if err != nil {
			panic(err.Error()) // WIP this should not be left before merging
		}
		table := tableInfo.GetVindexTable()
		if table == nil {
			continue
		}
		colNames = append(colNames, fmt.Sprintf("%s.%s", table.Name.String(), col.Name.String()))
	}

	slices.Sort(colNames)
	return slices.Compact(colNames)
}
