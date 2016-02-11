// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

func buildInsertPlan(ins *sqlparser.Insert, vschema *VSchema) (*DMLRoute, error) {
	route := &DMLRoute{
		Query: generateQuery(ins),
	}
	tablename := sqlparser.GetTableName(ins.Table)
	var err error
	route.Table, err = vschema.FindTable(tablename)
	if err != nil {
		return nil, err
	}
	if !route.Table.Keyspace.Sharded {
		route.PlanID = InsertUnsharded
		return route, nil
	}

	if len(ins.Columns) == 0 {
		return nil, errors.New("no column list")
	}
	var values sqlparser.Values
	switch rows := ins.Rows.(type) {
	case *sqlparser.Select, *sqlparser.Union:
		return nil, errors.New("subqueries not allowed")
	case sqlparser.Values:
		values = rows
	default:
		panic("unexpected")
	}
	if len(values) != 1 {
		return nil, errors.New("multi-row inserts not supported")
	}
	switch values[0].(type) {
	case *sqlparser.Subquery:
		return nil, errors.New("subqueries not allowed")
	}
	row := values[0].(sqlparser.ValTuple)
	if len(ins.Columns) != len(row) {
		return nil, errors.New("column list doesn't match values")
	}
	colVindexes := vschema.Tables[tablename].ColVindexes
	route.PlanID = InsertSharded
	route.Values = make([]interface{}, 0, len(colVindexes))
	for _, index := range colVindexes {
		if err := buildIndexPlan(ins, tablename, index, route); err != nil {
			return nil, err
		}
	}
	route.Query = generateQuery(ins)
	return route, nil
}

func buildIndexPlan(ins *sqlparser.Insert, tablename string, colVindex *ColVindex, route *DMLRoute) error {
	pos := -1
	for i, column := range ins.Columns {
		if colVindex.Col == sqlparser.GetColName(column.(*sqlparser.NonStarExpr).Expr) {
			pos = i
			break
		}
	}
	if pos == -1 {
		pos = len(ins.Columns)
		ins.Columns = append(ins.Columns, &sqlparser.NonStarExpr{Expr: &sqlparser.ColName{Name: sqlparser.SQLName(colVindex.Col)}})
		ins.Rows.(sqlparser.Values)[0] = append(ins.Rows.(sqlparser.Values)[0].(sqlparser.ValTuple), &sqlparser.NullVal{})
	}
	row := ins.Rows.(sqlparser.Values)[0].(sqlparser.ValTuple)
	val, err := asInterface(row[pos])
	if err != nil {
		return fmt.Errorf("could not convert val: %s, pos: %d: %v", sqlparser.String(row[pos]), pos, err)
	}
	route.Values = append(route.Values.([]interface{}), val)
	row[pos] = sqlparser.ValArg([]byte(fmt.Sprintf(":_%s", colVindex.Col)))
	return nil
}
