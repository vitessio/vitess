/*
Copyright 2019 The Vitess Authors.

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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// buildInsertPlan builds the route for an INSERT statement.
func buildInsertPlan(stmt sqlparser.Statement, reservedVars sqlparser.BindVars, vschema ContextVSchema) (engine.Primitive, error) {
	ins := stmt.(*sqlparser.Insert)
	pb := newPrimitiveBuilder(vschema, newJointab(reservedVars))
	exprs := sqlparser.TableExprs{&sqlparser.AliasedTableExpr{Expr: ins.Table}}
	rb, err := pb.processDMLTable(exprs, reservedVars, nil)
	if err != nil {
		return nil, err
	}
	// The table might have been routed to a different one.
	ins.Table = exprs[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName)
	if rb.eroute.TargetDestination != nil {
		return nil, errors.New("unsupported: INSERT with a target destination")
	}

	if len(pb.st.tables) != 1 {
		// Unreachable.
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "multi-table insert statement in not supported in sharded keyspace")
	}
	var vschemaTable *vindexes.Table
	for _, tval := range pb.st.tables {
		// There is only one table.
		vschemaTable = tval.vschemaTable
	}
	if !rb.eroute.Keyspace.Sharded {
		if pb.finalizeUnshardedDMLSubqueries(reservedVars, ins) {
			vschema.WarnUnshardedOnly("subqueries can't be sharded for INSERT")
		} else {
			return nil, errors.New("unsupported: sharded subquery in insert values")
		}
		return buildInsertUnshardedPlan(ins, vschemaTable)
	}
	if ins.Action == sqlparser.ReplaceAct {
		return nil, errors.New("unsupported: REPLACE INTO with sharded schema")
	}
	return buildInsertShardedPlan(ins, vschemaTable)
}

func buildInsertUnshardedPlan(ins *sqlparser.Insert, table *vindexes.Table) (engine.Primitive, error) {
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
		return nil, fmt.Errorf("BUG: unexpected construct in insert: %T", insertValues)
	}
	if eins.Table.AutoIncrement == nil {
		eins.Query = generateQuery(ins)
	} else {
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
	}

	return eins, nil
}

func buildInsertShardedPlan(ins *sqlparser.Insert, table *vindexes.Table) (engine.Primitive, error) {
	eins := engine.NewSimpleInsert(
		engine.InsertSharded,
		table,
		table.Keyspace,
	)
	if ins.Ignore {
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
		return nil, fmt.Errorf("BUG: unexpected construct in insert: %T", insertValues)
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
			for rowNum, row := range rows {
				innerpv, err := sqlparser.NewPlanValue(row[colNum])
				if err != nil {
					return nil, vterrors.Wrapf(err, "could not compute value for vindex or auto-inc column")
				}
				routeValues[vIdx].Values[colIdx].Values[rowNum] = innerpv
			}
		}
	}
	for _, colVindex := range eins.Table.ColumnVindexes {
		for _, col := range colVindex.Columns {
			colNum := findOrAddColumn(ins, col)
			for rowNum, row := range rows {
				name := ":" + engine.InsertVarName(col, rowNum)
				row[colNum] = sqlparser.NewArgument(name)
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
		node.Comments, node.Ignore.ToString(),
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
// is set. Bind variable names are generated using baseName.
func modifyForAutoinc(ins *sqlparser.Insert, eins *engine.Insert) error {
	colNum := findOrAddColumn(ins, eins.Table.AutoIncrement.Column)
	autoIncValues := sqltypes.PlanValue{}
	for rowNum, row := range ins.Rows.(sqlparser.Values) {
		// Support the DEFAULT keyword by treating it as null
		if _, ok := row[colNum].(*sqlparser.Default); ok {
			row[colNum] = &sqlparser.NullVal{}
		}
		pv, err := sqlparser.NewPlanValue(row[colNum])
		if err != nil {
			return fmt.Errorf("could not compute value for vindex or auto-inc column: %v", err)
		}
		autoIncValues.Values = append(autoIncValues.Values, pv)
		row[colNum] = sqlparser.NewArgument(":" + engine.SeqVarName + strconv.Itoa(rowNum))
	}

	eins.Generate = &engine.Generate{
		Keyspace: eins.Table.AutoIncrement.Sequence.Keyspace,
		Query:    fmt.Sprintf("select next :n values from %s", sqlparser.String(eins.Table.AutoIncrement.Sequence.Name)),
		Values:   autoIncValues,
	}
	return nil
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
