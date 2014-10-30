// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

func buildInsertPlan(ins *sqlparser.Insert, schema *VTGateSchema) *Plan {
	tablename := sqlparser.GetTableName(ins.Table)
	plan := getTableRouting(tablename, schema)
	if plan != nil {
		if plan.ID == SelectUnsharded {
			plan.ID = InsertUnsharded
		}
		plan.Query = generateQuery(ins)
		return plan
	}
	if len(ins.Columns) == 0 {
		return &Plan{
			ID:        NoPlan,
			Reason:    "no column list",
			TableName: tablename,
			Query:     generateQuery(ins),
		}
	}
	var values sqlparser.Values
	switch rows := ins.Rows.(type) {
	case *sqlparser.Select, *sqlparser.Union:
		return &Plan{
			ID:        NoPlan,
			Reason:    "subqueries not allowed",
			TableName: tablename,
			Query:     generateQuery(ins),
		}
	case sqlparser.Values:
		values = rows
	default:
		panic("unexpected")
	}
	if len(values) != 1 {
		return &Plan{
			ID:        NoPlan,
			Reason:    "multi-row inserts not supported",
			TableName: tablename,
			Query:     generateQuery(ins),
		}
	}
	switch values[0].(type) {
	case *sqlparser.Subquery:
		return &Plan{
			ID:        NoPlan,
			Reason:    "subqueries not allowed",
			TableName: tablename,
			Query:     generateQuery(ins),
		}
	}
	row := values[0].(sqlparser.ValTuple)
	if len(ins.Columns) != len(row) {
		return &Plan{
			ID:        NoPlan,
			Reason:    "column list doesn't match values",
			TableName: tablename,
			Query:     generateQuery(ins),
		}
	}
	indexes := schema.Tables[tablename].Indexes
	plan = &Plan{
		ID:        InsertSharded,
		TableName: tablename,
		Values:    make([]interface{}, 0, len(indexes)),
	}
	for _, index := range indexes {
		if err := buildIndexPlan(ins, tablename, index, plan); err != nil {
			return &Plan{
				ID:        NoPlan,
				Reason:    err.Error(),
				TableName: tablename,
				Query:     generateQuery(ins),
			}
		}
	}
	plan.Query = generateQuery(ins)
	return plan
}

func buildIndexPlan(ins *sqlparser.Insert, tablename string, index *VTGateIndex, plan *Plan) error {
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
		return fmt.Errorf("could not convert val: %v, pos: %d", row[pos], pos)
	}
	plan.Values = append(plan.Values.([]interface{}), val)
	if index.Owner == tablename && index.IsAutoInc {
		row[pos] = sqlparser.ValArg([]byte(fmt.Sprintf(":_%s", index.Column)))
	}
	return nil
}
