// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import "github.com/youtube/vitess/go/vt/sqlparser"

type DDLPlan struct {
	Action    string
	TableName string
	NewName   string
}

func DDLParse(sql string) (plan *DDLPlan) {
	statement, err := sqlparser.Parse(sql)
	if err != nil {
		return &DDLPlan{Action: ""}
	}
	stmt, ok := statement.(*sqlparser.DDL)
	if !ok {
		return &DDLPlan{Action: ""}
	}
	return &DDLPlan{
		Action:    stmt.Action,
		TableName: string(stmt.Table),
		NewName:   string(stmt.NewName),
	}
}

func analyzeDDL(ddl *sqlparser.DDL, getTable TableGetter) *ExecPlan {
	plan := &ExecPlan{PlanId: PLAN_DDL}
	tableName := string(ddl.Table)
	// Skip TableName if table is empty (create statements) or not found in schema
	if tableName != "" {
		tableInfo, ok := getTable(tableName)
		if ok {
			plan.TableName = tableInfo.Name
		}
	}
	return plan
}
