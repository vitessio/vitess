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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// buildInsertPlan builds the route for an INSERT statement.
func buildInsertPlan(ins *sqlparser.Insert, vschema ContextVSchema) (*engine.Insert, error) {
	pb := newPrimitiveBuilder(vschema, newJointab(sqlparser.GetBindvars(ins)))
	exprs := sqlparser.TableExprs{&sqlparser.AliasedTableExpr{Expr: ins.Table}}
	ro, err := pb.processDMLTable(exprs)
	if err != nil {
		return nil, err
	}
	// The table might have been routed to a different one.
	ins.Table = exprs[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName)
	if ro.eroute.TargetDestination != nil {
		return nil, errors.New("unsupported: INSERT with a target destination")
	}
	if !ro.vschemaTable.Keyspace.Sharded {
		if !pb.finalizeUnshardedDMLSubqueries(ins) {
			return nil, errors.New("unsupported: sharded subquery in insert values")
		}
		return buildInsertUnshardedPlan(ins, ro.vschemaTable, vschema)
	}
	if ins.Action == sqlparser.ReplaceStr {
		return nil, errors.New("unsupported: REPLACE INTO with sharded schema")
	}
	return buildInsertShardedPlan(ins, ro.vschemaTable)
}

func buildInsertUnshardedPlan(ins *sqlparser.Insert, table *vindexes.Table, vschema ContextVSchema) (*engine.Insert, error) {
	eins := engine.NewSimpleInsert(
		engine.InsertUnsharded,
		table,
		table.Keyspace,
	)
	var rows sqlparser.Values
	switch insertValues := ins.Rows.(type) {
	case *sqlparser.Select, *sqlparser.Union:
		if eins.Table.AutoIncrement != nil {
			return nil, errors.New("unsupported: auto-inc and select in insert")
		}
		eins.Query = generateQuery(ins)
		return eins, nil
	case sqlparser.Values:
		rows = insertValues
	default:
		panic(fmt.Sprintf("BUG: unexpected construct in insert: %T", insertValues))
	}
	if eins.Table.AutoIncrement == nil {
		eins.Query = generateQuery(ins)
		return eins, nil
	}

	// Table has auto-inc and has a VALUES clause.
	if len(ins.Columns) == 0 {
		if table.ColumnListAuthoritative {
			populateInsertColumnlist(ins, table)
		} else {
			return nil, errors.New("column list required for tables with auto-inc columns")
		}
	}
	for _, row := range rows {
		if len(ins.Columns) != len(row) {
			return nil, errors.New("column list doesn't match values")
		}
	}
	if err := modifyForAutoinc(ins, eins); err != nil {
		return nil, err
	}
	eins.Query = generateQuery(ins)
	return eins, nil
}

func buildInsertShardedPlan(ins *sqlparser.Insert, table *vindexes.Table) (*engine.Insert, error) {
	eins := engine.NewSimpleInsert(
		engine.InsertSharded,
		table,
		table.Keyspace,
	)
	if ins.Ignore != "" {
		eins.Opcode = engine.InsertShardedIgnore
	}
	if ins.OnDup != nil {
		if isVindexChanging(sqlparser.UpdateExprs(ins.OnDup), eins.Table.ColumnVindexes) {
			return nil, errors.New("unsupported: DML cannot change vindex column")
		}
		eins.Opcode = engine.InsertShardedIgnore
	}
	if len(ins.Columns) == 0 {
		if table.ColumnListAuthoritative {
			populateInsertColumnlist(ins, table)
		} else {
			return nil, errors.New("no column list")
		}
	}

	directives := sqlparser.ExtractCommentDirectives(ins.Comments)
	if directives.IsSet(sqlparser.DirectiveMultiShardAutocommit) {
		eins.MultiShardAutocommit = true
	}

	eins.QueryTimeout = queryTimeout(directives)

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

	if eins.Table.AutoIncrement != nil {
		if err := modifyForAutoinc(ins, eins); err != nil {
			return nil, err
		}
	}

	// Fill out the 3-d Values structure. Please see documentation of Insert.Values for details.
	routeValues := make([]sqltypes.PlanValue, len(eins.Table.ColumnVindexes))
	for vIdx, colVindex := range eins.Table.ColumnVindexes {
		routeValues[vIdx].Values = make([]sqltypes.PlanValue, len(colVindex.Columns))
		for colIdx, col := range colVindex.Columns {
			routeValues[vIdx].Values[colIdx].Values = make([]sqltypes.PlanValue, len(rows))
			colNum := findOrAddColumn(ins, col)
			// swap bind variables
			baseName := ":_" + col.CompliantName()
			for rowNum, row := range rows {
				innerpv, err := sqlparser.NewPlanValue(row[colNum])
				if err != nil {
					return nil, vterrors.Wrapf(err, "could not compute value for vindex or auto-inc column")
				}
				routeValues[vIdx].Values[colIdx].Values[rowNum] = innerpv
				row[colNum] = sqlparser.NewValArg([]byte(baseName + strconv.Itoa(rowNum)))
			}
		}
	}
	eins.VindexValues = routeValues
	eins.Query = generateQuery(ins)
	generateInsertShardedQuery(ins, eins, rows)
	return eins, nil
}

func populateInsertColumnlist(ins *sqlparser.Insert, table *vindexes.Table) {
	cols := make(sqlparser.Columns, 0, len(table.Columns))
	for _, c := range table.Columns {
		cols = append(cols, c.Name)
	}
	ins.Columns = cols
}

func generateInsertShardedQuery(node *sqlparser.Insert, eins *engine.Insert, valueTuples sqlparser.Values) {
	prefixBuf := sqlparser.NewTrackedBuffer(dmlFormatter)
	midBuf := sqlparser.NewTrackedBuffer(dmlFormatter)
	suffixBuf := sqlparser.NewTrackedBuffer(dmlFormatter)
	eins.Mid = make([]string, len(valueTuples))
	prefixBuf.Myprintf("insert %v%sinto %v%v values ",
		node.Comments, node.Ignore,
		node.Table, node.Columns)
	eins.Prefix = prefixBuf.String()
	for rowNum, val := range valueTuples {
		midBuf.Myprintf("%v", val)
		eins.Mid[rowNum] = midBuf.String()
		midBuf.Reset()
	}
	suffixBuf.Myprintf("%v", node.OnDup)
	eins.Suffix = suffixBuf.String()
}

// modifyForAutoinc modfies the AST and the plan to generate
// necessary autoinc values. It must be called only if eins.Table.AutoIncrement
// is set.
func modifyForAutoinc(ins *sqlparser.Insert, eins *engine.Insert) error {
	pos := findOrAddColumn(ins, eins.Table.AutoIncrement.Column)
	autoIncValues, err := swapBindVariables(ins.Rows.(sqlparser.Values), pos, ":"+engine.SeqVarName)
	if err != nil {
		return err
	}
	eins.Generate = &engine.Generate{
		Keyspace: eins.Table.AutoIncrement.Sequence.Keyspace,
		Query:    fmt.Sprintf("select next :n values from %s", sqlparser.String(eins.Table.AutoIncrement.Sequence.Name)),
		Values:   autoIncValues,
	}
	return nil
}

// swapBindVariables swaps in bind variable names at the specified
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

// isVindexChanging returns true if any of the update
// expressions modify a vindex column.
func isVindexChanging(setClauses sqlparser.UpdateExprs, colVindexes []*vindexes.ColumnVindex) bool {
	for _, assignment := range setClauses {
		for _, vcol := range colVindexes {
			for _, col := range vcol.Columns {
				if col.Equal(assignment.Name.Name) {
					valueExpr, isValuesFuncExpr := assignment.Expr.(*sqlparser.ValuesFuncExpr)
					if !isValuesFuncExpr {
						return true
					}
					// update on duplicate key is changing the vindex column, not supported.
					if !valueExpr.Name.Name.Equal(assignment.Name.Name) {
						return true
					}
				}
			}
		}
	}
	return false
}
