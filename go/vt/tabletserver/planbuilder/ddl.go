// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import "github.com/youtube/vitess/go/vt/sqlparser"

// DDLPlan provides a plan for DDLs.
type DDLPlan struct {
	Action    string
	TableName sqlparser.TableIdent
	NewName   sqlparser.TableIdent
}

// DDLParse parses a DDL and produces a DDLPlan.
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
		TableName: stmt.Table,
		NewName:   stmt.NewName,
	}
}

func analyzeDDL(ddl *sqlparser.DDL, getTable TableGetter) *ExecPlan {
	// TODO(sougou): Add support for sequences.
	plan := &ExecPlan{PlanID: PlanDDL}
	tableName := ddl.Table
	// Skip TableName if table is empty (create statements) or not found in schema
	if !tableName.IsEmpty() {
		table, ok := getTable(tableName)
		if ok {
			plan.TableName = table.Name
		}
	}
	return plan
}
