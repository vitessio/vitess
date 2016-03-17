// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"
	"fmt"

	"github.com/youtube/vitess/go/vt/sqlparser"
)

// buildInsertPlan builds the route for an INSERT statement.
func buildInsertPlan(ins *sqlparser.Insert, vschema *VSchema) (*Route, error) {
	route := &Route{
		Query: generateQuery(ins),
	}
	tablename := sqlparser.GetTableName(ins.Table)
	var err error
	route.Table, err = vschema.FindTable(tablename)
	if err != nil {
		return nil, err
	}
	route.Keyspace = route.Table.Keyspace
	if !route.Keyspace.Sharded {
		route.Opcode = InsertUnsharded
		return route, nil
	}

	if len(ins.Columns) == 0 {
		return nil, errors.New("no column list")
	}
	var values sqlparser.Values
	switch rows := ins.Rows.(type) {
	case *sqlparser.Select, *sqlparser.Union:
		return nil, errors.New("unsupported: insert into select")
	case sqlparser.Values:
		values = rows
	default:
		panic("unexpected construct in insert")
	}
	if len(values) != 1 {
		return nil, errors.New("unsupported: multi-row insert")
	}
	switch values[0].(type) {
	case *sqlparser.Subquery:
		return nil, errors.New("unsupported: subqueries in insert")
	}
	row := values[0].(sqlparser.ValTuple)
	if len(ins.Columns) != len(row) {
		return nil, errors.New("column list doesn't match values")
	}
	colVindexes := vschema.Tables[tablename].ColVindexes
	route.Opcode = InsertSharded
	route.Values = make([]interface{}, 0, len(colVindexes))
	for _, index := range colVindexes {
		if err := buildIndexPlan(ins, tablename, index, route); err != nil {
			return nil, err
		}
	}
	route.Query = generateQuery(ins)
	return route, nil
}

// buildIndexPlan adds the insert value to the Values field for the specified ColVindex.
// This value will be used at the time of insert to validate the vindex value.
func buildIndexPlan(ins *sqlparser.Insert, tablename string, colVindex *ColVindex, route *Route) error {
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
	val, err := valConvert(row[pos])
	if err != nil {
		return fmt.Errorf("could not convert val: %s, pos: %d: %v", sqlparser.String(row[pos]), pos, err)
	}
	route.Values = append(route.Values.([]interface{}), val)
	row[pos] = sqlparser.ValArg([]byte(fmt.Sprintf(":_%s", colVindex.Col)))
	return nil
}
