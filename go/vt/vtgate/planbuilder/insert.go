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
	colVindexes := route.Table.ColumnVindexes
	if len(values) != 1 {
		route.Opcode = engine.MultiInsertSharded
		route.Values = make([][]interface{}, len(values))
		for i := 0; i < len(values); i++ {
			route.Values.([][]interface{})[i] = make([]interface{}, len(colVindexes))
		}
	} else {
		route.Opcode = engine.InsertSharded
		route.Values = make([]interface{}, 0, len(colVindexes))
	}
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
	for _, index := range colVindexes {
		if err := buildIndexPlan(ins, index, route); err != nil {
			return nil, err
		}
	}
	if route.Table.AutoIncrement != nil {
		if err := buildAutoIncrementPlan(ins, route.Table.AutoIncrement, route); err != nil {
			return nil, err
		}
	}
	route.Query = generateQuery(ins)
	return route, nil
}

// buildIndexPlan adds the insert value to the Values field for the specified ColumnVindex.
// This value will be used at the time of insert to validate the vindex value.
func buildIndexPlan(ins *sqlparser.Insert, colVindex *vindexes.ColumnVindex, route *engine.Route) error {
	rows, pos := findOrInsertPos(ins, colVindex.Column)
	for i, row := range rows {
		val, err := valConvert(row.(sqlparser.ValTuple)[pos])
		if err != nil {
			return fmt.Errorf("could not convert val: %s, pos: %d: %v", sqlparser.String(row.(sqlparser.ValTuple)[pos]), pos, err)
		}
		switch route.Values.(type) {
		case [][]interface{}:
			route.Values.([][]interface{})[i] = append(route.Values.([][]interface{})[i], val)
		case []interface{}:
			route.Values = append(route.Values.([]interface{}), val)
		}
		row.(sqlparser.ValTuple)[pos] = sqlparser.ValArg([]byte(":_" + colVindex.Column.Original() + strconv.Itoa(i)))
	}
	return nil
}

func buildAutoIncrementPlan(ins *sqlparser.Insert, autoinc *vindexes.AutoIncrement, route *engine.Route) error {
	route.Generate = &engine.Generate{
		Opcode:   engine.SelectUnsharded,
		Keyspace: autoinc.Sequence.Keyspace,
		Query:    fmt.Sprintf("select next value from `%s`", autoinc.Sequence.Name),
	}
	// If it's also a colvindex, we have to add a redirect from route.Values.
	// Otherwise, we have to redirect from row[pos].
	if autoinc.ColumnVindexNum >= 0 {
		route.Generate.Value = route.Values.([]interface{})[autoinc.ColumnVindexNum]
		route.Values.([]interface{})[autoinc.ColumnVindexNum] = ":" + engine.SeqVarName
		return nil
	}
	rows, pos := findOrInsertPos(ins, autoinc.Column)
	for _, row := range rows {
		val, err := valConvert(row.(sqlparser.ValTuple)[pos])
		if err != nil {
			return fmt.Errorf("could not convert val: %s, pos: %d: %v", sqlparser.String(row.(sqlparser.ValTuple)[pos]), pos, err)
		}
		route.Generate.Value = val
		row.(sqlparser.ValTuple)[pos] = sqlparser.ValArg([]byte(":" + engine.SeqVarName))
	}
	return nil
}

func findOrInsertPos(ins *sqlparser.Insert, col cistring.CIString) (rows sqlparser.Values, pos int) {
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
		ins.Rows.(sqlparser.Values)[0] = append(ins.Rows.(sqlparser.Values)[0].(sqlparser.ValTuple), &sqlparser.NullVal{})
	}
	return ins.Rows.(sqlparser.Values), pos
}
