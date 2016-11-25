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
	table, err := vschema.Find(string(ins.Table.Qualifier), string(ins.Table.Name))
	if err != nil {
		return nil, err
	}
	if !table.Keyspace.Sharded {
		return buildInsertUnshardedPlan(ins, table, vschema)
	}
	return buildInsertShardedPlan(ins, table)
}

func buildInsertUnshardedPlan(ins *sqlparser.Insert, table *vindexes.Table, vschema VSchema) (*engine.Route, error) {
	rt := &engine.Route{
		Opcode:   engine.InsertUnsharded,
		Table:    table,
		Keyspace: table.Keyspace,
	}
	var values sqlparser.Values
	switch rows := ins.Rows.(type) {
	case *sqlparser.Union:
		return nil, errors.New("unsupported: union in insert")
	case *sqlparser.Select:
		bldr, err := processSelect(rows, vschema, nil)
		if err != nil {
			return nil, err
		}
		rb, ok := bldr.(*route)
		if !ok {
			return nil, errors.New("unsupported: complex join in insert")
		}
		if rb.ERoute.Keyspace.Name != rt.Keyspace.Name {
			return nil, errors.New("unsupported: cross-shard select in insert")
		}
		if rt.Table.AutoIncrement != nil {
			return nil, errors.New("unsupported: auto-inc and select in insert")
		}
		rt.Query = generateQuery(ins)
		return rt, nil
	case sqlparser.Values:
		values = rows
		if hasSubquery(values) {
			return nil, errors.New("unsupported: subquery in insert values")
		}
	default:
		panic("unexpected construct in insert")
	}
	if rt.Table.AutoIncrement == nil {
		rt.Query = generateQuery(ins)
		return rt, nil
	}
	if len(ins.Columns) == 0 {
		return nil, errors.New("column list required for tables with auto-inc columns")
	}
	for _, value := range values {
		if len(ins.Columns) != len(value) {
			return nil, errors.New("column list doesn't match values")
		}
	}
	autoIncValues := make([]interface{}, 0, len(values))
	for rowNum := range values {
		autoIncVal, err := handleAutoinc(ins, rt.Table.AutoIncrement, rowNum)
		if err != nil {
			return nil, err
		}
		autoIncValues = append(autoIncValues, autoIncVal)
	}
	if rt.Table.AutoIncrement != nil {
		rt.Generate = &engine.Generate{
			Opcode:   engine.SelectUnsharded,
			Keyspace: rt.Table.AutoIncrement.Sequence.Keyspace,
			Query:    fmt.Sprintf("select next :n values from `%s`", rt.Table.AutoIncrement.Sequence.Name),
			Value:    autoIncValues,
		}
	}
	rt.Query = generateQuery(ins)
	return rt, nil
}

func buildInsertShardedPlan(ins *sqlparser.Insert, table *vindexes.Table) (*engine.Route, error) {
	rt := &engine.Route{
		Opcode:   engine.InsertSharded,
		Table:    table,
		Keyspace: table.Keyspace,
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
		if hasSubquery(values) {
			return nil, errors.New("unsupported: subquery in insert values")
		}
	default:
		panic("unexpected construct in insert")
	}
	for _, value := range values {
		if len(ins.Columns) != len(value) {
			return nil, errors.New("column list doesn't match values")
		}
	}
	colVindexes := rt.Table.ColumnVindexes
	routeValues := make([]interface{}, 0, len(values))
	autoIncValues := make([]interface{}, 0, len(values))
	for rowNum := range values {
		rowValue := make([]interface{}, 0, len(colVindexes))
		for _, index := range colVindexes {
			row, pos := findOrInsertPos(ins, index.Column, rowNum)
			value, err := handleVindexCol(index, rowNum, row, pos)
			if err != nil {
				return nil, err
			}
			rowValue = append(rowValue, value)
		}
		if rt.Table.AutoIncrement != nil {
			autoIncVal, value, err := handleShardedAutoinc(ins, rt.Table.AutoIncrement, rowValue, rowNum)
			if err != nil {
				return nil, err
			}
			rowValue = value
			autoIncValues = append(autoIncValues, autoIncVal)
		}
		routeValues = append(routeValues, rowValue)
	}
	if rt.Table.AutoIncrement != nil {
		rt.Generate = &engine.Generate{
			Opcode:   engine.SelectUnsharded,
			Keyspace: rt.Table.AutoIncrement.Sequence.Keyspace,
			Query:    fmt.Sprintf("select next :n values from `%s`", rt.Table.AutoIncrement.Sequence.Name),
			Value:    autoIncValues,
		}
	}
	rt.Values = routeValues
	rt.Query = generateQuery(ins)
	return rt, nil
}

// handleVindexCol substitutes the insert value with a bind var name and returns
// the converted value, which will be used at the time of insert to validate the vindex value.
func handleVindexCol(colVindex *vindexes.ColumnVindex, rowNum int, row sqlparser.ValTuple, pos int) (interface{}, error) {
	val, err := valConvert(row[pos])
	if err != nil {
		return val, fmt.Errorf("could not convert val: %s, pos: %d: %v", sqlparser.String(row[pos]), pos, err)
	}
	row[pos] = sqlparser.ValArg([]byte(":_" + colVindex.Column.Original() + strconv.Itoa(rowNum)))
	return val, nil
}

// handleShardedAutoinc substitutes the insert value with a bind var and returns
// the converted value, which will be used at the time of decide if a new value should be generated.
// This is for a sharded keyspace which also needs to take care of an additional redirect.
func handleShardedAutoinc(ins *sqlparser.Insert, autoinc *vindexes.AutoIncrement, rowValue []interface{}, rowNum int) (interface{}, []interface{}, error) {
	// If it's also a colvindex, we have to add a redirect from route.Values.
	// Otherwise, we have to redirect from row[pos].
	if autoinc.ColumnVindexNum >= 0 {
		val := rowValue[autoinc.ColumnVindexNum]
		rowValue[autoinc.ColumnVindexNum] = ":" + engine.SeqVarName + strconv.Itoa(rowNum)
		return val, rowValue, nil
	}
	val, err := handleAutoinc(ins, autoinc, rowNum)
	return val, rowValue, err
}

// handleAutoinc substitutes the insert value with a bind var and returns
// the converted value, which will be used at the time of decide if a new value should be generated.
// This works for columns with no vindexes.
func handleAutoinc(ins *sqlparser.Insert, autoinc *vindexes.AutoIncrement, rowNum int) (interface{}, error) {
	row, pos := findOrInsertPos(ins, autoinc.Column, rowNum)
	val, err := valConvert(row[pos])
	if err != nil {
		return nil, fmt.Errorf("could not convert val: %s, pos: %d: %v", sqlparser.String(row[pos]), pos, err)
	}
	row[pos] = sqlparser.ValArg([]byte(":" + engine.SeqVarName + strconv.Itoa(rowNum)))
	return val, nil
}

func findOrInsertPos(ins *sqlparser.Insert, col cistring.CIString, rowNum int) (row sqlparser.ValTuple, pos int) {
	pos = -1
	for i, column := range ins.Columns {
		if col.Equal(cistring.CIString(column)) {
			pos = i
			break
		}
	}
	if pos == -1 {
		pos = len(ins.Columns)
		ins.Columns = append(ins.Columns, sqlparser.ColIdent(col))
	}
	if pos >= len(ins.Rows.(sqlparser.Values)[rowNum]) {
		ins.Rows.(sqlparser.Values)[rowNum] = append(ins.Rows.(sqlparser.Values)[rowNum], &sqlparser.NullVal{})
	}
	return ins.Rows.(sqlparser.Values)[rowNum], pos
}
