// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

// buildInsertPlan builds the route for an INSERT statement.
func buildInsertPlan(ins *sqlparser.Insert, vschema VSchema) (*engine.Route, error) {
	table, err := vschema.Find(ins.Table.Qualifier, ins.Table.Name)
	if err != nil {
		return nil, err
	}
	if !table.Keyspace.Sharded {
		return buildInsertUnshardedPlan(ins, table, vschema)
	}
	return buildInsertShardedPlan(ins, table)
}

func buildInsertUnshardedPlan(ins *sqlparser.Insert, table *vindexes.Table, vschema VSchema) (*engine.Route, error) {
	eRoute := &engine.Route{
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
		innerRoute, ok := bldr.(*route)
		if !ok {
			return nil, errors.New("unsupported: complex join in insert")
		}
		if innerRoute.ERoute.Keyspace.Name != eRoute.Keyspace.Name {
			return nil, errors.New("unsupported: cross-keyspace select in insert")
		}
		if eRoute.Table.AutoIncrement != nil {
			return nil, errors.New("unsupported: auto-inc and select in insert")
		}
		eRoute.Query = generateQuery(ins)
		return eRoute, nil
	case sqlparser.Values:
		values = rows
		if hasSubquery(values) {
			return nil, errors.New("unsupported: subquery in insert values")
		}
	default:
		panic("unexpected construct in insert")
	}
	if eRoute.Table.AutoIncrement == nil {
		eRoute.Query = generateQuery(ins)
		return eRoute, nil
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
		autoIncVal, err := handleAutoinc(ins, eRoute.Table.AutoIncrement, rowNum)
		if err != nil {
			return nil, err
		}
		autoIncValues = append(autoIncValues, autoIncVal)
	}
	if eRoute.Table.AutoIncrement != nil {
		eRoute.Generate = &engine.Generate{
			Opcode:   engine.SelectUnsharded,
			Keyspace: eRoute.Table.AutoIncrement.Sequence.Keyspace,
			Query:    fmt.Sprintf("select next :n values from %s", sqlparser.String(eRoute.Table.AutoIncrement.Sequence.Name)),
			Value:    autoIncValues,
		}
	}
	eRoute.Query = generateQuery(ins)
	return eRoute, nil
}

func buildInsertShardedPlan(ins *sqlparser.Insert, table *vindexes.Table) (*engine.Route, error) {
	eRoute := &engine.Route{
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
	colVindexes := eRoute.Table.ColumnVindexes
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
		if eRoute.Table.AutoIncrement != nil {
			autoIncVal, value, err := handleShardedAutoinc(ins, eRoute.Table.AutoIncrement, rowValue, rowNum)
			if err != nil {
				return nil, err
			}
			rowValue = value
			autoIncValues = append(autoIncValues, autoIncVal)
		}
		routeValues = append(routeValues, rowValue)
	}
	if eRoute.Table.AutoIncrement != nil {
		eRoute.Generate = &engine.Generate{
			Opcode:   engine.SelectUnsharded,
			Keyspace: eRoute.Table.AutoIncrement.Sequence.Keyspace,
			Query:    fmt.Sprintf("select next :n values from %s", sqlparser.String(eRoute.Table.AutoIncrement.Sequence.Name)),
			Value:    autoIncValues,
		}
	}
	eRoute.Values = routeValues
	eRoute.Query = generateQuery(ins)
	generateInsertShardedQuery(ins, eRoute, values)
	return eRoute, nil
}

func generateInsertShardedQuery(node *sqlparser.Insert, eRoute *engine.Route, valueTuples sqlparser.Values) {
	prefixBuf := sqlparser.NewTrackedBuffer(dmlFormatter)
	midBuf := sqlparser.NewTrackedBuffer(dmlFormatter)
	suffixBuf := sqlparser.NewTrackedBuffer(dmlFormatter)
	eRoute.Mid = make([]string, len(valueTuples))
	prefixBuf.Myprintf("insert %v%sinto %v%v values ",
		node.Comments, node.Ignore,
		node.Table, node.Columns)
	eRoute.Prefix = prefixBuf.String()
	for rowNum, val := range valueTuples {
		midBuf.Myprintf("%v", val)
		eRoute.Mid[rowNum] = midBuf.String()
		midBuf.Truncate(0)
	}
	suffixBuf.Myprintf("%v", node.OnDup)
	eRoute.Suffix = suffixBuf.String()
}

// handleVindexCol substitutes the insert value with a bind var name and returns
// the converted value, which will be used at the time of insert to validate the vindex value.
func handleVindexCol(colVindex *vindexes.ColumnVindex, rowNum int, row sqlparser.ValTuple, pos int) (interface{}, error) {
	val, err := valConvert(row[pos])
	if err != nil {
		return val, fmt.Errorf("could not convert val: %s, pos: %d: %v", sqlparser.String(row[pos]), pos, err)
	}
	row[pos] = sqlparser.NewValArg([]byte(":_" + colVindex.Column.CompliantName() + strconv.Itoa(rowNum)))
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
	row[pos] = sqlparser.NewValArg([]byte(":" + engine.SeqVarName + strconv.Itoa(rowNum)))
	return val, nil
}

func findOrInsertPos(ins *sqlparser.Insert, col sqlparser.ColIdent, rowNum int) (row sqlparser.ValTuple, pos int) {
	pos = -1
	for i, column := range ins.Columns {
		if col.Equal(column) {
			pos = i
			break
		}
	}
	if pos == -1 {
		pos = len(ins.Columns)
		ins.Columns = append(ins.Columns, col)
	}
	if pos >= len(ins.Rows.(sqlparser.Values)[rowNum]) {
		ins.Rows.(sqlparser.Values)[rowNum] = append(ins.Rows.(sqlparser.Values)[rowNum], &sqlparser.NullVal{})
	}
	return ins.Rows.(sqlparser.Values)[rowNum], pos
}
