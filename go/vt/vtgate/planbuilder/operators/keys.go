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
	"strings"

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
	JoinPredicate struct {
		LHS, RHS Column
		Uses     sqlparser.ComparisonExprOperator
	}
	VExplainKeys struct {
		StatementType   string          `json:"statementType"`
		TableName       []string        `json:"tableName,omitempty"`
		GroupingColumns []Column        `json:"groupingColumns,omitempty"`
		FilterColumns   []ColumnUse     `json:"filterColumns,omitempty"`
		SelectColumns   []Column        `json:"selectColumns,omitempty"`
		JoinPredicates  []JoinPredicate `json:"joinPredicates,omitempty"`
	}
)

func newJoinPredicate(lhs, rhs Column, op sqlparser.ComparisonExprOperator) JoinPredicate {
	// we want to try to keep the columns in the same order, no matter how the query was written
	if lhs.String() > rhs.String() {
		var success bool
		op, success = op.SwitchSides()
		if success {
			lhs, rhs = rhs, lhs
		}
	}
	return JoinPredicate{LHS: lhs, RHS: rhs, Uses: op}
}

func (c Column) MarshalJSON() ([]byte, error) {
	if c.Table != "" {
		return json.Marshal(fmt.Sprintf("%s.%s", c.Table, c.Name))
	}
	return json.Marshal(c.Name)
}

func (c *Column) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	parts := strings.Split(s, ".")
	if len(parts) > 1 {
		c.Table = parts[0]
		c.Name = parts[1]
	} else {
		c.Name = s
	}
	return nil
}

func (cu ColumnUse) MarshalJSON() ([]byte, error) {
	return json.Marshal(fmt.Sprintf("%s %s", cu.Column, cu.Uses.JSONString()))
}

func (cu *ColumnUse) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	spaceIdx := strings.LastIndex(s, " ")
	if spaceIdx == -1 {
		return fmt.Errorf("invalid ColumnUse format: %s", s)
	}

	for i := spaceIdx - 1; i >= 0; i-- {
		// table.column not like
		// table.`tricky not` like
		if s[i] == '`' || s[i] == '.' {
			break
		}
		if s[i] == ' ' {
			spaceIdx = i
			break
		}
		if i == 0 {
			return fmt.Errorf("invalid ColumnUse format: %s", s)
		}
	}

	colStr, opStr := s[:spaceIdx], s[spaceIdx+1:]

	err := cu.Column.UnmarshalJSON([]byte(`"` + colStr + `"`))
	if err != nil {
		return fmt.Errorf("failed to unmarshal column: %w", err)
	}

	cu.Uses, err = sqlparser.ComparisonExprOperatorFromJson(strings.ToLower(opStr))
	if err != nil {
		return fmt.Errorf("failed to unmarshal operator: %w", err)
	}
	return nil
}

func (jp *JoinPredicate) MarshalJSON() ([]byte, error) {
	return json.Marshal(jp.String())
}

func (jp *JoinPredicate) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	subStrings := strings.Split(s, " ")
	if len(subStrings) != 3 {
		return fmt.Errorf("invalid JoinPredicate format: %s", s)
	}

	op, err := sqlparser.ComparisonExprOperatorFromJson(subStrings[1])
	if err != nil {
		return fmt.Errorf("invalid comparison operator: %w", err)
	}
	jp.Uses = op

	if err = jp.LHS.UnmarshalJSON([]byte(`"` + subStrings[0] + `"`)); err != nil {
		return err
	}
	if err = jp.RHS.UnmarshalJSON([]byte(`"` + subStrings[2] + `"`)); err != nil {
		return err
	}
	return nil
}

func (c Column) String() string {
	return fmt.Sprintf("%s.%s", c.Table, c.Name)
}

func (cu ColumnUse) String() string {
	return fmt.Sprintf("%s %s", cu.Column, cu.Uses.JSONString())
}

func (jp JoinPredicate) String() string {
	return fmt.Sprintf("%s %s %s", jp.LHS.String(), jp.Uses.JSONString(), jp.RHS.String())
}

type columnUse struct {
	col *sqlparser.ColName
	use sqlparser.ComparisonExprOperator
}

type joinPredicate struct {
	lhs  *sqlparser.ColName
	rhs  *sqlparser.ColName
	uses sqlparser.ComparisonExprOperator
}

func GetVExplainKeys(ctx *plancontext.PlanningContext, stmt sqlparser.Statement) (result VExplainKeys) {
	var groupingColumns, selectColumns []*sqlparser.ColName
	var filterColumns, joinColumns []columnUse
	var jps []joinPredicate

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
					jps = append(jps, joinPredicate{lhs: lhs, rhs: rhs, uses: cmp.Operator})
				}

				if lhsOK {
					*output = append(*output, columnUse{lhs, cmp.Operator})
				}

				if switchedOp, ok := cmp.Operator.SwitchSides(); rhsOK && ok {
					*output = append(*output, columnUse{rhs, switchedOp})
				}
			case *sqlparser.BetweenExpr:
				if col, ok := cmp.Left.(*sqlparser.ColName); ok {
					// a BETWEEN 100 AND 200    is equivalent to    a >= 100 AND a <= 200
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
		FilterColumns:   getUniqueColUsages(ctx, filterColumns),
		StatementType:   sqlparser.ASTToStatementType(stmt).String(),
		JoinPredicates:  getUniqueJoinPredicates(ctx, jps),
	}
}

func getUniqueJoinPredicates(ctx *plancontext.PlanningContext, joinPredicates []joinPredicate) []JoinPredicate {
	var result []JoinPredicate
	for _, predicate := range joinPredicates {
		lhs := createColumn(ctx, predicate.lhs)
		rhs := createColumn(ctx, predicate.rhs)
		if lhs == nil || rhs == nil {
			continue
		}

		result = append(result, newJoinPredicate(*lhs, *rhs, predicate.uses))
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].LHS.Name == result[j].LHS.Name {
			return result[i].RHS.Name < result[j].RHS.Name
		}
		return result[i].LHS.Name < result[j].LHS.Name
	})

	return slices.Compact(result)
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
	return &Column{
		// we want the escaped versions of the names
		Table: sqlparser.String(table.Name),
		Name:  sqlparser.String(col.Name),
	}
}
