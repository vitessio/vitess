/*
Copyright 2019 The Vitess Authors.

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

package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

// DDLPlan provides a plan for DDLs.
type DDLPlan struct {
	Action string
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
		Action: stmt.Action,
	}
}

func analyzeDDL(ddl *sqlparser.DDL, tables map[string]*schema.Table) *Plan {
	// TODO(sougou): Add support for sequences.
	plan := &Plan{
		PlanID: PlanDDL,
	}
	// this can become a whitelist of fully supported ddl actions as support grows
	if ddl.PartitionSpec != nil {
		plan.FullQuery = GenerateFullQuery(ddl)
	}
	return plan
}
