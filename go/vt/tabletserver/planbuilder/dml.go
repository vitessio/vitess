// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"
	"fmt"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

func analyzeUpdate(upd *sqlparser.Update, getTable TableGetter) (plan *ExecPlan, err error) {
	plan = &ExecPlan{
		PlanID:    PlanPassDML,
		FullQuery: GenerateFullQuery(upd),
	}

	tableName := sqlparser.GetTableName(upd.Table)
	if tableName == "" {
		plan.Reason = ReasonTable
		return plan, nil
	}
	tableInfo, err := plan.setTableInfo(tableName, getTable)
	if err != nil {
		return nil, err
	}

	if len(tableInfo.Indexes) == 0 || tableInfo.Indexes[0].Name.Lowered() != "primary" {
		log.Warningf("no primary key for table %s", tableName)
		plan.Reason = ReasonTableNoIndex
		return plan, nil
	}

	plan.SecondaryPKValues, err = analyzeUpdateExpressions(upd.Exprs, tableInfo.Indexes[0])
	if err != nil {
		if err == ErrTooComplex {
			plan.Reason = ReasonPKChange
			return plan, nil
		}
		return nil, err
	}

	plan.OuterQuery = GenerateUpdateOuterQuery(upd)

	if conditions := analyzeWhere(upd.Where); conditions != nil {
		pkValues, err := getPKValues(conditions, tableInfo.Indexes[0])
		if err != nil {
			return nil, err
		}
		if pkValues != nil {
			plan.PlanID = PlanDMLPK
			plan.PKValues = pkValues
			return plan, nil
		}
	}

	plan.PlanID = PlanDMLSubquery
	plan.Subquery = GenerateUpdateSubquery(upd, tableInfo)
	return plan, nil
}

func analyzeDelete(del *sqlparser.Delete, getTable TableGetter) (plan *ExecPlan, err error) {
	plan = &ExecPlan{
		PlanID:    PlanPassDML,
		FullQuery: GenerateFullQuery(del),
	}

	tableName := sqlparser.GetTableName(del.Table)
	if tableName == "" {
		plan.Reason = ReasonTable
		return plan, nil
	}
	tableInfo, err := plan.setTableInfo(tableName, getTable)
	if err != nil {
		return nil, err
	}

	if len(tableInfo.Indexes) == 0 || tableInfo.Indexes[0].Name.Lowered() != "primary" {
		log.Warningf("no primary key for table %s", tableName)
		plan.Reason = ReasonTableNoIndex
		return plan, nil
	}

	plan.OuterQuery = GenerateDeleteOuterQuery(del)

	if conditions := analyzeWhere(del.Where); conditions != nil {
		pkValues, err := getPKValues(conditions, tableInfo.Indexes[0])
		if err != nil {
			return nil, err
		}
		if pkValues != nil {
			plan.PlanID = PlanDMLPK
			plan.PKValues = pkValues
			return plan, nil
		}
	}

	plan.PlanID = PlanDMLSubquery
	plan.Subquery = GenerateDeleteSubquery(del, tableInfo)
	return plan, nil
}

func analyzeSet(set *sqlparser.Set) (plan *ExecPlan) {
	return &ExecPlan{
		PlanID:    PlanSet,
		FullQuery: GenerateFullQuery(set),
	}
}

func analyzeUpdateExpressions(exprs sqlparser.UpdateExprs, pkIndex *schema.Index) (pkValues []interface{}, err error) {
	for _, expr := range exprs {
		index := pkIndex.FindColumn(expr.Name.Original())
		if index == -1 {
			continue
		}
		if !sqlparser.IsValue(expr.Expr) {
			return nil, ErrTooComplex
		}
		if pkValues == nil {
			pkValues = make([]interface{}, len(pkIndex.Columns))
		}
		var err error
		pkValues[index], err = sqlparser.AsInterface(expr.Expr)
		if err != nil {
			return nil, err
		}
	}
	return pkValues, nil
}

func analyzeSelect(sel *sqlparser.Select, getTable TableGetter) (plan *ExecPlan, err error) {
	plan = &ExecPlan{
		PlanID:     PlanPassSelect,
		FieldQuery: GenerateFieldQuery(sel),
		FullQuery:  GenerateSelectLimitQuery(sel),
	}
	if sel.Lock != "" {
		plan.PlanID = PlanSelectLock
	}

	tableName := analyzeFrom(sel.From)
	if tableName == "" {
		return plan, nil
	}
	tableInfo, err := plan.setTableInfo(tableName, getTable)
	if err != nil {
		return nil, err
	}

	// Check if it's a NEXT VALUE statement.
	if _, ok := sel.SelectExprs[0].(sqlparser.Nextval); ok {
		if tableInfo.Type != schema.Sequence {
			return nil, fmt.Errorf("%s is not a sequence", tableName)
		}
		plan.PlanID = PlanNextval
		plan.FieldQuery = nil
		plan.FullQuery = nil
	}
	return plan, nil
}

func analyzeFrom(tableExprs sqlparser.TableExprs) string {
	if len(tableExprs) > 1 {
		return ""
	}
	node, ok := tableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return ""
	}
	return sqlparser.GetTableName(node.Expr)
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
			sqlparser.EqualStr,
			sqlparser.LessThanStr,
			sqlparser.GreaterThanStr,
			sqlparser.LessEqualStr,
			sqlparser.GreaterEqualStr,
			sqlparser.NullSafeEqualStr,
			sqlparser.LikeStr):
			if sqlparser.IsColName(node.Left) && sqlparser.IsValue(node.Right) {
				return []sqlparser.BoolExpr{node}
			}
		case node.Operator == sqlparser.InStr:
			if sqlparser.IsColName(node.Left) && sqlparser.IsSimpleTuple(node.Right) {
				return []sqlparser.BoolExpr{node}
			}
		}
	case *sqlparser.RangeCond:
		if node.Operator != sqlparser.BetweenStr {
			return nil
		}
		if sqlparser.IsColName(node.Left) && sqlparser.IsValue(node.From) && sqlparser.IsValue(node.To) {
			return []sqlparser.BoolExpr{node}
		}
	}
	return nil
}

func analyzeInsert(ins *sqlparser.Insert, getTable TableGetter) (plan *ExecPlan, err error) {
	plan = &ExecPlan{
		PlanID:    PlanPassDML,
		FullQuery: GenerateFullQuery(ins),
	}
	tableName := sqlparser.GetTableName(ins.Table)
	if tableName == "" {
		plan.Reason = ReasonTable
		return plan, nil
	}
	tableInfo, err := plan.setTableInfo(tableName, getTable)
	if err != nil {
		return nil, err
	}

	if len(tableInfo.Indexes) == 0 || tableInfo.Indexes[0].Name.Lowered() != "primary" {
		log.Warningf("no primary key for table %s", tableName)
		plan.Reason = ReasonTableNoIndex
		return plan, nil
	}

	pkColumnNumbers := getInsertPKColumns(ins.Columns, tableInfo)

	if sel, ok := ins.Rows.(sqlparser.SelectStatement); ok {
		if ins.OnDup != nil {
			// Upserts not allowed for subqueries.
			// http://bugs.mysql.com/bug.php?id=58637
			plan.Reason = ReasonUpsert
			return plan, nil
		}
		plan.PlanID = PlanInsertSubquery
		plan.OuterQuery = GenerateInsertOuterQuery(ins)
		plan.Subquery = GenerateSelectLimitQuery(sel)
		if len(ins.Columns) != 0 {
			for _, col := range ins.Columns {
				colIndex := tableInfo.FindColumn(col.Original())
				if colIndex == -1 {
					return nil, fmt.Errorf("column %v not found in table %s", col, tableInfo.Name)
				}
				plan.ColumnNumbers = append(plan.ColumnNumbers, colIndex)
			}
		} else {
			// Add all columns.
			for colIndex := range tableInfo.Columns {
				plan.ColumnNumbers = append(plan.ColumnNumbers, colIndex)
			}
		}
		plan.SubqueryPKColumns = pkColumnNumbers
		return plan, nil
	}

	// If it's not a sqlparser.SelectStatement, it's Values.
	rowList := ins.Rows.(sqlparser.Values)
	pkValues, err := getInsertPKValues(pkColumnNumbers, rowList, tableInfo)
	if err != nil {
		return nil, err
	}
	if pkValues == nil {
		plan.Reason = ReasonComplexExpr
		return plan, nil
	}
	plan.PKValues = pkValues
	if ins.OnDup == nil {
		plan.PlanID = PlanInsertPK
		plan.OuterQuery = sqlparser.GenerateParsedQuery(ins)
		return plan, nil
	}
	if len(rowList) > 1 {
		// Upsert supported only for single row inserts.
		plan.Reason = ReasonUpsert
		return plan, nil
	}
	plan.SecondaryPKValues, err = analyzeUpdateExpressions(sqlparser.UpdateExprs(ins.OnDup), tableInfo.Indexes[0])
	if err != nil {
		if err == ErrTooComplex {
			plan.Reason = ReasonPKChange
			return plan, nil
		}
		return nil, err
	}
	plan.PlanID = PlanUpsertPK
	newins := *ins
	newins.Ignore = ""
	newins.OnDup = nil
	plan.OuterQuery = sqlparser.GenerateParsedQuery(&newins)
	upd := &sqlparser.Update{
		Comments: ins.Comments,
		Table:    ins.Table,
		Exprs:    sqlparser.UpdateExprs(ins.OnDup),
	}
	plan.UpsertQuery = GenerateUpdateOuterQuery(upd)
	return plan, nil
}

func getInsertPKColumns(columns sqlparser.Columns, tableInfo *schema.Table) (pkColumnNumbers []int) {
	if len(columns) == 0 {
		return tableInfo.PKColumns
	}
	pkIndex := tableInfo.Indexes[0]
	pkColumnNumbers = make([]int, len(pkIndex.Columns))
	for i := range pkColumnNumbers {
		pkColumnNumbers[i] = -1
	}
	for i, column := range columns {
		index := pkIndex.FindColumn(column.Original())
		if index == -1 {
			continue
		}
		pkColumnNumbers[index] = i
	}
	return pkColumnNumbers
}

func getInsertPKValues(pkColumnNumbers []int, rowList sqlparser.Values, tableInfo *schema.Table) (pkValues []interface{}, err error) {
	pkValues = make([]interface{}, len(pkColumnNumbers))
	for index, columnNumber := range pkColumnNumbers {
		if columnNumber == -1 {
			pkValues[index] = tableInfo.GetPKColumn(index).Default
			continue
		}
		values := make([]interface{}, len(rowList))
		for j := 0; j < len(rowList); j++ {
			if _, ok := rowList[j].(*sqlparser.Subquery); ok {
				return nil, errors.New("row subquery not supported for inserts")
			}
			row := rowList[j].(sqlparser.ValTuple)
			if columnNumber >= len(row) {
				return nil, errors.New("column count doesn't match value count")
			}
			node := row[columnNumber]
			if !sqlparser.IsNull(node) && !sqlparser.IsValue(node) {
				return nil, nil
			}
			var err error
			values[j], err = sqlparser.AsInterface(node)
			if err != nil {
				return nil, err
			}
		}
		if len(values) == 1 {
			pkValues[index] = values[0]
		} else {
			pkValues[index] = values
		}
	}
	return pkValues, nil
}
