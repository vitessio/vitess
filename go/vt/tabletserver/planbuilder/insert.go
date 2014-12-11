// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"

	log "github.com/golang/glog"
	"github.com/henryanand/vitess/go/vt/schema"
	"github.com/henryanand/vitess/go/vt/sqlparser"
)

func analyzeInsert(ins *sqlparser.Insert, getTable TableGetter) (plan *ExecPlan, err error) {
	plan = &ExecPlan{
		PlanId:    PLAN_PASS_DML,
		FullQuery: GenerateFullQuery(ins),
	}
	tableName := sqlparser.GetTableName(ins.Table)
	if tableName == "" {
		plan.Reason = REASON_TABLE
		return plan, nil
	}
	tableInfo, err := plan.setTableInfo(tableName, getTable)
	if err != nil {
		return nil, err
	}

	if len(tableInfo.Indexes) == 0 || tableInfo.Indexes[0].Name != "PRIMARY" {
		log.Warningf("no primary key for table %s", tableName)
		plan.Reason = REASON_TABLE_NOINDEX
		return plan, nil
	}

	pkColumnNumbers := getInsertPKColumns(ins.Columns, tableInfo)

	if ins.OnDup != nil {
		// Upserts are not safe for statement based replication:
		// http://bugs.mysql.com/bug.php?id=58637
		plan.Reason = REASON_UPSERT
		return plan, nil
	}

	if sel, ok := ins.Rows.(sqlparser.SelectStatement); ok {
		plan.PlanId = PLAN_INSERT_SUBQUERY
		plan.OuterQuery = GenerateInsertOuterQuery(ins)
		plan.Subquery = GenerateSelectLimitQuery(sel)
		if len(ins.Columns) != 0 {
			plan.ColumnNumbers, err = analyzeSelectExprs(sqlparser.SelectExprs(ins.Columns), tableInfo)
			if err != nil {
				return nil, err
			}
		} else {
			// StarExpr node will expand into all columns
			n := sqlparser.SelectExprs{&sqlparser.StarExpr{}}
			plan.ColumnNumbers, err = analyzeSelectExprs(n, tableInfo)
			if err != nil {
				return nil, err
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
	if pkValues != nil {
		plan.PlanId = PLAN_INSERT_PK
		plan.OuterQuery = plan.FullQuery
		plan.PKValues = pkValues
	}
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
		index := pkIndex.FindColumn(sqlparser.GetColName(column.(*sqlparser.NonStarExpr).Expr))
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
			if !sqlparser.IsValue(node) {
				log.Warningf("insert is too complex %v", node)
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
