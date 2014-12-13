// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"bytes"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

func buildUpdatePlan(upd *sqlparser.Update, schema *Schema) *Plan {
	plan := &Plan{
		ID:        NoPlan,
		Rewritten: generateQuery(upd),
	}
	tablename := sqlparser.GetTableName(upd.Table)
	plan.Table, plan.Reason = schema.FindTable(tablename)
	if plan.Reason != "" {
		return plan
	}
	if !plan.Table.Keyspace.Sharded {
		plan.ID = UpdateUnsharded
		return plan
	}

	getWhereRouting(upd.Where, plan, true)
	switch plan.ID {
	case SelectEqual:
		plan.ID = UpdateEqual
	case SelectIN, SelectScatter:
		plan.ID = NoPlan
		plan.Reason = "too complex"
		return plan
	default:
		panic("unexpected")
	}
	if isIndexChanging(upd.Exprs, plan.Table.ColVindexes) {
		plan.ID = NoPlan
		plan.Reason = "index is changing"
	}
	return plan
}

func isIndexChanging(setClauses sqlparser.UpdateExprs, colVindexes []*ColVindex) bool {
	vindexCols := make([]string, len(colVindexes))
	for i, index := range colVindexes {
		vindexCols[i] = index.Col
	}
	for _, assignment := range setClauses {
		if sqlparser.StringIn(string(assignment.Name.Name), vindexCols...) {
			return true
		}
	}
	return false
}

func buildDeletePlan(del *sqlparser.Delete, schema *Schema) *Plan {
	plan := &Plan{
		ID:        NoPlan,
		Rewritten: generateQuery(del),
	}
	tablename := sqlparser.GetTableName(del.Table)
	plan.Table, plan.Reason = schema.FindTable(tablename)
	if plan.Reason != "" {
		return plan
	}
	if !plan.Table.Keyspace.Sharded {
		plan.ID = DeleteUnsharded
		return plan
	}

	getWhereRouting(del.Where, plan, true)
	switch plan.ID {
	case SelectEqual:
		plan.ID = DeleteEqual
		plan.Subquery = generateDeleteSubquery(del, plan.Table)
	case SelectIN, SelectScatter:
		plan.ID = NoPlan
		plan.Reason = "too complex"
	default:
		panic("unexpected")
	}
	return plan
}

func generateDeleteSubquery(del *sqlparser.Delete, table *Table) string {
	if len(table.Owned) == 0 {
		return ""
	}
	buf := bytes.NewBuffer(nil)
	buf.WriteString("select ")
	prefix := ""
	for _, cv := range table.Owned {
		buf.WriteString(prefix)
		buf.WriteString(cv.Col)
		prefix = ", "
	}
	buf.WriteString(sqlparser.String(del.Where))
	buf.WriteString(" for update")
	return buf.String()
}
