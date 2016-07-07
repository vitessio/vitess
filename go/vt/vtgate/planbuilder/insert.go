// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/youtube/vitess/go/cistring"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

// buildInsertPlan builds the route for an INSERT statement.
func buildInsertPlan(ins *sqlparser.Insert, vschema VSchema) (*engine.Route, error) {
	route := &engine.Route{
		Query: generateQuery(ins),
	}
	var err error
	route.Table, err = vschema.Find(string(ins.Table.Qualifier), string(ins.Table.Name))
	if err != nil {
		return nil, err
	}
	route.Keyspace = route.Table.Keyspace
	if !route.Keyspace.Sharded {
		route.Opcode = engine.InsertUnsharded
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
	route.Opcode = engine.InsertSharded
	for _, value := range values {
		switch value.(type) {
		case *sqlparser.Subquery:
			return nil, errors.New("unsupported: subqueries in insert")
		}
		row := value.(sqlparser.ValTuple)
		if len(ins.Columns) != len(row) {
			return nil, errors.New("column list doesn't match values")
		}
	}
	colVindexes := route.Table.ColumnVindexes
	routeValues := make([]interface{}, 0, len(values))
	autoIncColInsert := false
	for rowNum := 0; rowNum < len(values); rowNum++ {
		value := make([]interface{}, 0, len(colVindexes))
		for _, index := range colVindexes {
			if err := buildIndexPlan(ins, index, &value, rowNum); err != nil {
				return nil, err
			}
		}
		if route.Table.AutoIncrement != nil {
			if err := buildAutoIncrementPlan(ins, route.Table.AutoIncrement, route, &value, rowNum, &autoIncColInsert); err != nil {
				return nil, err
			}
		}
		routeValues = append(routeValues, value)
	}
	route.Values = routeValues
	route.Query = generateQuery(ins)
	return route, nil
}

// buildIndexPlan adds the insert value to the Values field for the specified ColumnVindex.
// This value will be used at the time of insert to validate the vindex value.
func buildIndexPlan(ins *sqlparser.Insert, colVindex *vindexes.ColumnVindex, value *[]interface{}, rowNum int) error {
	fixedFalse := false
	row, pos := findOrInsertPos(ins, colVindex.Column, rowNum, &fixedFalse)

	val, err := valConvert(row[pos])
	if err != nil {
		return fmt.Errorf("could not convert val: %s, pos: %d: %v", sqlparser.String(row[pos]), pos, err)
	}
	*value = append(*value, val)
	row[pos] = sqlparser.ValArg([]byte(":_" + colVindex.Column.Original() + strconv.Itoa(rowNum)))
	return nil
}

func buildAutoIncrementPlan(ins *sqlparser.Insert, autoinc *vindexes.AutoIncrement, route *engine.Route, value *[]interface{}, rowNum int, autoIncColInsert *bool) error {
	route.Generate = &engine.Generate{
		Opcode:   engine.SelectUnsharded,
		Keyspace: autoinc.Sequence.Keyspace,
		Query:    fmt.Sprintf("select next value from `%s`", autoinc.Sequence.Name),
	}
	// If it's also a colvindex, we have to add a redirect from route.Values.
	// Otherwise, we have to redirect from row[pos].
	if autoinc.ColumnVindexNum >= 0 {
		route.Generate.Value = (*value)[autoinc.ColumnVindexNum]
		(*value)[autoinc.ColumnVindexNum] = ":" + engine.SeqVarName + strconv.Itoa(rowNum)
		return nil
	}
	row, pos := findOrInsertPos(ins, autoinc.Column, rowNum, autoIncColInsert)
	val, err := valConvert(row[pos])
	if err != nil {
		return fmt.Errorf("could not convert val: %s, pos: %d: %v", sqlparser.String(row[pos]), pos, err)
	}
	route.Generate.Value = val
	row[pos] = sqlparser.ValArg([]byte(":" + engine.SeqVarName + strconv.Itoa(rowNum)))

	return nil
}

func findOrInsertPos(ins *sqlparser.Insert, col cistring.CIString, rowNum int, autoIncColInsert *bool) (row sqlparser.ValTuple, pos int) {
	pos = -1
	for i, column := range ins.Columns {
		if col.Equal(cistring.CIString(column)) {
			pos = i
			break
		}
	}
	if pos == -1 {
		*autoIncColInsert = true
		pos = len(ins.Columns)
		ins.Columns = append(ins.Columns, sqlparser.ColIdent(col))
	}
	if *autoIncColInsert {
		ins.Rows.(sqlparser.Values)[rowNum] = append(ins.Rows.(sqlparser.Values)[rowNum].(sqlparser.ValTuple), &sqlparser.NullVal{})
	}
	return ins.Rows.(sqlparser.Values)[rowNum].(sqlparser.ValTuple), pos
}
