// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

func buildInsertPlan(ins *sqlparser.Insert, schema *Schema) *Plan {
	plan := &Plan{
		ID:        NoPlan,
		Rewritten: generateQuery(ins),
	}
	tablename := sqlparser.GetTableName(ins.Table)
	plan.Table, plan.Reason = schema.LookupTable(tablename)
	if plan.Reason != "" {
		return plan
	}
	if plan.Table.Keyspace.ShardingScheme == Unsharded {
		plan.ID = InsertUnsharded
		return plan
	}

	if len(ins.Columns) == 0 {
		plan.Reason = "no column list"
		return plan
	}
	var values sqlparser.Values
	switch rows := ins.Rows.(type) {
	case *sqlparser.Select, *sqlparser.Union:
		plan.Reason = "subqueries not allowed"
		return plan
	case sqlparser.Values:
		values = rows
	default:
		panic("unexpected")
	}
	if len(values) != 1 {
		plan.Reason = "multi-row inserts not supported"
		return plan
	}
	switch values[0].(type) {
	case *sqlparser.Subquery:
		plan.Reason = "subqueries not allowed"
		return plan
	}
	row := values[0].(sqlparser.ValTuple)
	if len(ins.Columns) != len(row) {
		plan.Reason = "column list doesn't match values"
		return plan
	}
	indexes := schema.Tables[tablename].Indexes
	plan.ID = InsertSharded
	plan.Values = make([]interface{}, 0, len(indexes))
	for _, index := range indexes {
		if err := buildIndexPlan(ins, tablename, index, plan); err != nil {
			plan.ID = NoPlan
			plan.Reason = err.Error()
			return plan
		}
	}
	// Query was rewritten
	plan.Rewritten = generateQuery(ins)
	return plan
}

func buildIndexPlan(ins *sqlparser.Insert, tablename string, index *Index, plan *Plan) error {
	pos := -1
	for i, column := range ins.Columns {
		if index.Column == sqlparser.GetColName(column.(*sqlparser.NonStarExpr).Expr) {
			pos = i
			break
		}
	}
	if pos == -1 && index.Owner == tablename && index.IsAutoInc {
		pos = len(ins.Columns)
		ins.Columns = append(ins.Columns, &sqlparser.NonStarExpr{Expr: &sqlparser.ColName{Name: []byte(index.Column)}})
		ins.Rows.(sqlparser.Values)[0] = append(ins.Rows.(sqlparser.Values)[0].(sqlparser.ValTuple), &sqlparser.NullVal{})
	}
	if pos == -1 {
		return fmt.Errorf("must supply value for indexed column: %s", index.Column)
	}
	row := ins.Rows.(sqlparser.Values)[0].(sqlparser.ValTuple)
	val, err := sqlparser.AsInterface(row[pos])
	if err != nil {
		return fmt.Errorf("could not convert val: %s, pos: %d", row[pos], pos)
	}
	plan.Values = append(plan.Values.([]interface{}), val)
	if index.Owner == tablename && index.IsAutoInc {
		row[pos] = sqlparser.ValArg([]byte(fmt.Sprintf(":_%s", index.Column)))
	}
	return nil
}
