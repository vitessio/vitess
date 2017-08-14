/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package planbuilder

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

// buildInsertPlan builds the route for an INSERT statement.
func buildInsertPlan(ins *sqlparser.Insert, vschema VSchema) (*engine.Route, error) {
	table, err := vschema.Find(ins.Table)
	if err != nil {
		return nil, err
	}
	if !table.Keyspace.Sharded {
		return buildInsertUnshardedPlan(ins, table, vschema)
	}
	if ins.Action == sqlparser.ReplaceStr {
		return nil, errors.New("unsupported: REPLACE INTO with sharded schema")
	}
	return buildInsertShardedPlan(ins, table)
}

func buildInsertUnshardedPlan(ins *sqlparser.Insert, table *vindexes.Table, vschema VSchema) (*engine.Route, error) {
	eRoute := &engine.Route{
		Opcode:   engine.InsertUnsharded,
		Table:    table,
		Keyspace: table.Keyspace,
	}
	if !validateSubquerySamePlan(ins, eRoute, vschema) {
		return nil, errors.New("unsupported: sharded subquery in insert values")
	}
	var rows sqlparser.Values
	switch insertValues := ins.Rows.(type) {
	case *sqlparser.Select, *sqlparser.Union:
		if eRoute.Table.AutoIncrement != nil {
			return nil, errors.New("unsupported: auto-inc and select in insert")
		}
		eRoute.Query = generateQuery(ins)
		return eRoute, nil
	case sqlparser.Values:
		rows = insertValues
	default:
		panic(fmt.Sprintf("BUG: unexpected construct in insert: %T", insertValues))
	}
	if eRoute.Table.AutoIncrement == nil {
		eRoute.Query = generateQuery(ins)
		return eRoute, nil
	}

	// Table has auto-inc and has a VALUES clause.
	if len(ins.Columns) == 0 {
		return nil, errors.New("column list required for tables with auto-inc columns")
	}
	for _, row := range rows {
		if len(ins.Columns) != len(row) {
			return nil, errors.New("column list doesn't match values")
		}
	}
	if err := modifyForAutoinc(ins, eRoute); err != nil {
		return nil, err
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
	if ins.Ignore != "" {
		eRoute.Opcode = engine.InsertShardedIgnore
	}
	if ins.OnDup != nil {
		if isIndexChanging(sqlparser.UpdateExprs(ins.OnDup), eRoute.Table.ColumnVindexes) {
			return nil, errors.New("unsupported: DML cannot change vindex column")
		}
		eRoute.Opcode = engine.InsertShardedIgnore
	}
	if len(ins.Columns) == 0 {
		return nil, errors.New("no column list")
	}
	var rows sqlparser.Values
	switch insertValues := ins.Rows.(type) {
	case *sqlparser.Select, *sqlparser.Union:
		return nil, errors.New("unsupported: insert into select")
	case sqlparser.Values:
		rows = insertValues
		if hasSubquery(rows) {
			return nil, errors.New("unsupported: subquery in insert values")
		}
	default:
		panic(fmt.Sprintf("BUG: unexpected construct in insert: %T", insertValues))
	}
	for _, value := range rows {
		if len(ins.Columns) != len(value) {
			return nil, errors.New("column list doesn't match values")
		}
	}

	if eRoute.Table.AutoIncrement != nil {
		if err := modifyForAutoinc(ins, eRoute); err != nil {
			return nil, err
		}
	}

	for _, colVindex := range eRoute.Table.ColumnVindexes {
		pos := findOrAddColumn(ins, colVindex.Column)
		swappedValues, err := swapBindVariables(rows, pos, ":_"+colVindex.Column.CompliantName())
		if err != nil {
			return nil, err
		}
		eRoute.Values = append(eRoute.Values, swappedValues)
	}

	eRoute.Query = generateQuery(ins)
	generateInsertShardedQuery(ins, eRoute, rows)
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

// modifyForAutoinc modfies the AST and the plan to generate
// necessary autoinc values. It must be called only if eRoute.Table.AutoIncrement
// is set.
func modifyForAutoinc(ins *sqlparser.Insert, eRoute *engine.Route) error {
	pos := findOrAddColumn(ins, eRoute.Table.AutoIncrement.Column)
	autoIncValues, err := swapBindVariables(ins.Rows.(sqlparser.Values), pos, ":"+engine.SeqVarName)
	if err != nil {
		return err
	}
	eRoute.Generate = &engine.Generate{
		Keyspace: eRoute.Table.AutoIncrement.Sequence.Keyspace,
		Query:    fmt.Sprintf("select next :n values from %s", sqlparser.String(eRoute.Table.AutoIncrement.Sequence.Name)),
		Values:   autoIncValues,
	}
	return nil
}

// swapBindVariables swaps in bind variable names at the the specified
// column position in the AST values and returns the converted values back.
// Bind variable names are generated using baseName.
func swapBindVariables(rows sqlparser.Values, colNum int, baseName string) (sqltypes.PlanValue, error) {
	pv := sqltypes.PlanValue{}
	for rowNum, row := range rows {
		innerpv, err := sqlparser.NewPlanValue(row[colNum])
		if err != nil {
			return pv, fmt.Errorf("could not compute value for vindex or auto-inc column: %v", err)
		}
		pv.Values = append(pv.Values, innerpv)
		row[colNum] = sqlparser.NewValArg([]byte(baseName + strconv.Itoa(rowNum)))
	}
	return pv, nil
}

// findOrAddColumn finds the position of a column in the insert. If it's
// absent it appends it to the with NULL values and returns that position.
func findOrAddColumn(ins *sqlparser.Insert, col sqlparser.ColIdent) int {
	for i, column := range ins.Columns {
		if col.Equal(column) {
			return i
		}
	}
	ins.Columns = append(ins.Columns, col)
	rows := ins.Rows.(sqlparser.Values)
	for i := range rows {
		rows[i] = append(rows[i], &sqlparser.NullVal{})
	}
	return len(ins.Columns) - 1
}
