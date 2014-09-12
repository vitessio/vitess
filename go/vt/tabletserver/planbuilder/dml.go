// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"strconv"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

func analyzeUpdate(upd *sqlparser.Update, getTable TableGetter) (plan *ExecPlan, err error) {
	// Default plan
	plan = &ExecPlan{
		PlanId:    PLAN_PASS_DML,
		FullQuery: GenerateFullQuery(upd),
	}

	tableName := sqlparser.GetTableName(upd.Table)
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

	plan.SecondaryPKValues, err = analyzeUpdateExpressions(upd.Exprs, tableInfo.Indexes[0])
	if err != nil {
		if err == TooComplex {
			plan.Reason = REASON_PK_CHANGE
			return plan, nil
		}
		return nil, err
	}

	plan.PlanId = PLAN_DML_SUBQUERY
	plan.OuterQuery = GenerateUpdateOuterQuery(upd, tableInfo.Indexes[0])
	plan.Subquery = GenerateUpdateSubquery(upd, tableInfo)

	conditions := analyzeWhere(upd.Where)
	if conditions == nil {
		plan.Reason = REASON_WHERE
		return plan, nil
	}

	pkValues, err := getPKValues(conditions, tableInfo.Indexes[0])
	if err != nil {
		return nil, err
	}
	if pkValues != nil {
		plan.PlanId = PLAN_DML_PK
		plan.OuterQuery = plan.FullQuery
		plan.PKValues = pkValues
		return plan, nil
	}

	return plan, nil
}

func analyzeDelete(del *sqlparser.Delete, getTable TableGetter) (plan *ExecPlan, err error) {
	// Default plan
	plan = &ExecPlan{
		PlanId:    PLAN_PASS_DML,
		FullQuery: GenerateFullQuery(del),
	}

	tableName := sqlparser.GetTableName(del.Table)
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

	plan.PlanId = PLAN_DML_SUBQUERY
	plan.OuterQuery = GenerateDeleteOuterQuery(del, tableInfo.Indexes[0])
	plan.Subquery = GenerateDeleteSubquery(del, tableInfo)

	conditions := analyzeWhere(del.Where)
	if conditions == nil {
		plan.Reason = REASON_WHERE
		return plan, nil
	}

	pkValues, err := getPKValues(conditions, tableInfo.Indexes[0])
	if err != nil {
		return nil, err
	}
	if pkValues != nil {
		plan.PlanId = PLAN_DML_PK
		plan.OuterQuery = plan.FullQuery
		plan.PKValues = pkValues
		return plan, nil
	}

	return plan, nil
}

func analyzeSet(set *sqlparser.Set) (plan *ExecPlan) {
	plan = &ExecPlan{
		PlanId:    PLAN_SET,
		FullQuery: GenerateFullQuery(set),
	}
	if len(set.Exprs) > 1 { // Multiple set values
		return plan
	}
	update_expression := set.Exprs[0]
	plan.SetKey = string(update_expression.Name.Name)
	numExpr, ok := update_expression.Expr.(sqlparser.NumVal)
	if !ok {
		return plan
	}
	val := string(numExpr)
	if ival, err := strconv.ParseInt(val, 0, 64); err == nil {
		plan.SetValue = ival
	} else if fval, err := strconv.ParseFloat(val, 64); err == nil {
		plan.SetValue = fval
	}
	return plan
}

func analyzeUpdateExpressions(exprs sqlparser.UpdateExprs, pkIndex *schema.Index) (pkValues []interface{}, err error) {
	for _, expr := range exprs {
		index := pkIndex.FindColumn(sqlparser.GetColName(expr.Name))
		if index == -1 {
			continue
		}
		if !sqlparser.IsValue(expr.Expr) {
			log.Warningf("expression is too complex %v", expr)
			return nil, TooComplex
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
