// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

func analyzeSelect(sel *sqlparser.Select, getTable TableGetter) (plan *ExecPlan, err error) {
	// Default plan
	plan = &ExecPlan{
		PlanId:     PLAN_PASS_SELECT,
		FieldQuery: GenerateFieldQuery(sel),
		FullQuery:  GenerateSelectLimitQuery(sel),
	}

	// from
	tableName, hasHints := analyzeFrom(sel.From)
	if tableName == "" {
		plan.Reason = REASON_TABLE
		return plan, nil
	}
	tableInfo, err := plan.setTableInfo(tableName, getTable)
	if err != nil {
		return nil, err
	}

	// There are bind variables in the SELECT list
	if plan.FieldQuery == nil {
		plan.Reason = REASON_SELECT_LIST
		return plan, nil
	}

	if sel.Distinct != "" || sel.GroupBy != nil || sel.Having != nil {
		plan.Reason = REASON_SELECT
		return plan, nil
	}

	// Don't improve the plan if the select is locking the row
	if sel.Lock != "" {
		plan.Reason = REASON_LOCK
		return plan, nil
	}

	// Further improvements possible only if table is row-cached
	if tableInfo.CacheType == schema.CACHE_NONE || tableInfo.CacheType == schema.CACHE_W {
		plan.Reason = REASON_NOCACHE
		return plan, nil
	}

	// Select expressions
	selects, err := analyzeSelectExprs(sel.SelectExprs, tableInfo)
	if err != nil {
		return nil, err
	}
	if selects == nil {
		plan.Reason = REASON_SELECT_LIST
		return plan, nil
	}
	plan.ColumnNumbers = selects

	// where
	conditions := analyzeWhere(sel.Where)
	if conditions == nil {
		plan.Reason = REASON_WHERE
		return plan, nil
	}

	// order
	if sel.OrderBy != nil {
		plan.Reason = REASON_ORDER
		return plan, nil
	}

	// This check should never fail because we only cache tables with primary keys.
	if len(tableInfo.Indexes) == 0 || tableInfo.Indexes[0].Name != "PRIMARY" {
		panic("unexpected")
	}

	planId, pkValues, err := getSelectPKValues(conditions, tableInfo.Indexes[0])
	if err != nil {
		return nil, err
	}
	switch planId {
	case PLAN_PK_EQUAL:
		offset, rowcount, err := sel.Limit.Limits()
		if err != nil {
			return nil, err
		}
		if offset != nil {
			goto nopk
		}
		switch r := rowcount.(type) {
		case nil:
			// no limit clause. Ok to fetch by pk.
		case int64:
			// A rowcount >= 1 is redundant for fetch by pk.
			if r < 1 {
				goto nopk
			}
		default:
			// A more complex limit clause.
			goto nopk
		}
		plan.PlanId = PLAN_PK_EQUAL
		plan.OuterQuery = GenerateSelectOuterQuery(sel, tableInfo)
		plan.PKValues = pkValues
		return plan, nil
	case PLAN_PK_IN:
		if sel.Limit != nil {
			goto nopk
		}
		plan.PlanId = PLAN_PK_IN
		plan.OuterQuery = GenerateSelectOuterQuery(sel, tableInfo)
		plan.PKValues = pkValues
		return plan, nil
	}

nopk:
	if len(tableInfo.Indexes[0].Columns) != 1 {
		plan.Reason = REASON_COMPOSITE_PK
		return plan, nil
	}

	// TODO: Analyze hints to improve plan.
	if hasHints {
		plan.Reason = REASON_HAS_HINTS
		return plan, nil
	}

	plan.IndexUsed = getIndexMatch(conditions, tableInfo.Indexes)
	if plan.IndexUsed == "" {
		plan.Reason = REASON_NOINDEX_MATCH
		return plan, nil
	}
	if plan.IndexUsed == "PRIMARY" {
		plan.Reason = REASON_PKINDEX
		return plan, nil
	}
	// TODO: We can further optimize. Change this to pass-through if select list matches all columns in index.
	plan.PlanId = PLAN_SELECT_SUBQUERY
	plan.OuterQuery = GenerateSelectOuterQuery(sel, tableInfo)
	plan.Subquery = GenerateSelectSubquery(sel, tableInfo, plan.IndexUsed)
	return plan, nil
}

func analyzeSelectExprs(exprs sqlparser.SelectExprs, table *schema.Table) (selects []int, err error) {
	selects = make([]int, 0, len(exprs))
	for _, expr := range exprs {
		switch expr := expr.(type) {
		case *sqlparser.StarExpr:
			// Append all columns.
			for colIndex := range table.Columns {
				selects = append(selects, colIndex)
			}
		case *sqlparser.NonStarExpr:
			name := sqlparser.GetColName(expr.Expr)
			if name == "" {
				// Not a simple column name.
				return nil, nil
			}
			colIndex := table.FindColumn(name)
			if colIndex == -1 {
				return nil, fmt.Errorf("column %s not found in table %s", name, table.Name)
			}
			selects = append(selects, colIndex)
		default:
			panic("unreachable")
		}
	}
	return selects, nil
}

func analyzeFrom(tableExprs sqlparser.TableExprs) (tablename string, hasHints bool) {
	if len(tableExprs) > 1 {
		return "", false
	}
	node, ok := tableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return "", false
	}
	return sqlparser.GetTableName(node.Expr), node.Hints != nil
}

func analyzeWhere(node *sqlparser.Where) (conditions []sqlparser.BoolExpr) {
	if node == nil {
		return nil
	}
	return analyzeBoolean(node.Expr)
}

func analyzeBoolean(node sqlparser.BoolExpr) (conditions []sqlparser.BoolExpr) {
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		left := analyzeBoolean(node.Left)
		right := analyzeBoolean(node.Right)
		if left == nil || right == nil {
			return nil
		}
		if sqlparser.HasINClause(left) && sqlparser.HasINClause(right) {
			return nil
		}
		return append(left, right...)
	case *sqlparser.ParenBoolExpr:
		return analyzeBoolean(node.Expr)
	case *sqlparser.ComparisonExpr:
		switch {
		case sqlparser.StringIn(
			node.Operator,
			sqlparser.AST_EQ,
			sqlparser.AST_LT,
			sqlparser.AST_GT,
			sqlparser.AST_LE,
			sqlparser.AST_GE,
			sqlparser.AST_NSE,
			sqlparser.AST_LIKE):
			if sqlparser.IsColName(node.Left) && sqlparser.IsValue(node.Right) {
				return []sqlparser.BoolExpr{node}
			}
		case node.Operator == sqlparser.AST_IN:
			if sqlparser.IsColName(node.Left) && sqlparser.IsSimpleTuple(node.Right) {
				return []sqlparser.BoolExpr{node}
			}
		}
	case *sqlparser.RangeCond:
		if node.Operator != sqlparser.AST_BETWEEN {
			return nil
		}
		if sqlparser.IsColName(node.Left) && sqlparser.IsValue(node.From) && sqlparser.IsValue(node.To) {
			return []sqlparser.BoolExpr{node}
		}
	}
	return nil
}
