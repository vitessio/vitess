// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"
)

// DDLPlan provides a plan for DDLs.
type DDLPlan struct {
	Action    string
	TableName *sqlparser.TableName
	NewName   *sqlparser.TableName
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

func analyzeDDL(ddl *sqlparser.DDL, tables map[string]*schema.Table) *Plan {
	// TODO(sougou): Add support for sequences.
	plan := &Plan{PlanID: PlanDDL}
	if ddl.Table != nil {
		plan.Table = tables[ddl.Table.Name.String()]
	}
	return plan
}
