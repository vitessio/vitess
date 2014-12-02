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
	plan.Table, plan.Reason = schema.FindTable(tablename)
	if plan.Reason != "" {
		return plan
	}
	if !plan.Table.Keyspace.Sharded {
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
	colVindexes := schema.Tables[tablename].ColVindexes
	plan.ID = InsertSharded
	plan.Values = make([]interface{}, 0, len(colVindexes))
	for _, index := range colVindexes {
		if err := buildIndexPlan(ins, tablename, index, plan); err != nil {
			plan.ID = NoPlan
			plan.Reason = err.Error()
			return plan
		}
	}
	plan.Rewritten = generateQuery(ins)
	return plan
}

func buildIndexPlan(ins *sqlparser.Insert, tablename string, colVindex *ColVindex, plan *Plan) error {
	pos := -1
	for i, column := range ins.Columns {
		if colVindex.Col == sqlparser.GetColName(column.(*sqlparser.NonStarExpr).Expr) {
			pos = i
			break
		}
	}
	if pos == -1 {
		pos = len(ins.Columns)
		ins.Columns = append(ins.Columns, &sqlparser.NonStarExpr{Expr: &sqlparser.ColName{Name: []byte(colVindex.Col)}})
		ins.Rows.(sqlparser.Values)[0] = append(ins.Rows.(sqlparser.Values)[0].(sqlparser.ValTuple), &sqlparser.NullVal{})
	}
	row := ins.Rows.(sqlparser.Values)[0].(sqlparser.ValTuple)
	val, err := sqlparser.AsInterface(row[pos])
	if err != nil {
		return fmt.Errorf("could not convert val: %s, pos: %d", row[pos], pos)
	}
	plan.Values = append(plan.Values.([]interface{}), val)
	row[pos] = sqlparser.ValArg([]byte(fmt.Sprintf(":_%s", colVindex.Col)))
	return nil
}
