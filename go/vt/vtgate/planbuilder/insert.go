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

	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/semantics"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// buildInsertPlan builds the route for an INSERT statement.
func buildInsertPlan(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (engine.Primitive, error) {
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
	return buildInsertShardedPlan(ins, vschemaTable, reservedVars, vschema)
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

func buildInsertShardedPlan(ins *sqlparser.Insert, table *vindexes.Table, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (engine.Primitive, error) {
	rows, isRowValues := ins.Rows.(sqlparser.Values)
	if !isRowValues {
		return buildInsertSelectPlan(ins, table, reservedVars, vschema)
	}
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
	var colVindexes []*vindexes.ColumnVindex
	for _, colVindex := range eins.Table.ColumnVindexes {
		if colVindex.IgnoreInDML() {
			continue
		}
		colVindexes = append(colVindexes, colVindex)
	}
	routeValues := make([][][]evalengine.Expr, len(colVindexes))
	for vIdx, colVindex := range colVindexes {
		routeValues[vIdx] = make([][]evalengine.Expr, len(colVindex.Columns))
		for colIdx, col := range colVindex.Columns {
			routeValues[vIdx][colIdx] = make([]evalengine.Expr, len(rows))
			colNum := findOrAddColumn(ins, col)
			for rowNum, row := range rows {
				innerpv, err := evalengine.Translate(row[colNum], semantics.EmptySemTable())
				if err != nil {
					return nil, err
				}
				routeValues[vIdx][colIdx][rowNum] = innerpv
			}
		}
	}
	for _, colVindex := range colVindexes {
		for _, col := range colVindex.Columns {
			colNum := findOrAddColumn(ins, col)
			for rowNum, row := range rows {
				name := engine.InsertVarName(col, rowNum)
				row[colNum] = sqlparser.NewArgument(name)
			}
		}
	}
	eins.VindexValues = routeValues
	eins.ColVindexes = colVindexes
	eins.Query = generateQuery(ins)
	generateInsertShardedQuery(ins, eins, rows)
	return eins, nil
}

// buildInsertSelectPlan builds a s
func buildInsertSelectPlan(ins *sqlparser.Insert, table *vindexes.Table, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (engine.Primitive, error) {
	if ins.Ignore || ins.OnDup != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: insert using select with ignore/on duplicate")
	}
	if table.AutoIncrement != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: insert using select with auto-inc table")
	}
	if len(ins.Columns) == 0 {
		if !table.ColumnListAuthoritative {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "insert should contain column list or the table should have authoritative columns in vschema")
		}
		populateInsertColumnlist(ins, table)
	}

	var selectStmt sqlparser.SelectStatement
	var configuredPlanner selectPlanner
	var err error
	switch stmt := ins.Rows.(type) {
	case *sqlparser.Select:
		configuredPlanner, err = getConfiguredPlanner(vschema, buildSelectPlan, stmt, "")
		if err != nil {
			return nil, err
		}
		selectStmt = stmt
	case *sqlparser.Union:
		configuredPlanner, err = getConfiguredPlanner(vschema, buildUnionPlan, stmt, "")
		if err != nil {
			return nil, err
		}
		selectStmt = stmt
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: insert plan with %T", ins.Rows)
	}
	// validate the columns to match on insert and select
	if len(ins.Columns) < selectStmt.GetColumnCount() {
		return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValueCountOnRow, "Column count doesn't match value count at row 1")
	}
	if len(ins.Columns) > selectStmt.GetColumnCount() {
		sel := sqlparser.GetFirstSelect(selectStmt)
		var hasStarExpr bool
		for _, sExpr := range sel.SelectExprs {
			if _, hasStarExpr = sExpr.(*sqlparser.StarExpr); hasStarExpr {
				break
			}
		}
		if !hasStarExpr {
			return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValueCountOnRow, "Column count doesn't match value count at row 1")
		}
	}
	// Override the locking with `for update` to lock the rows for inserting the data.
	selectStmt.SetLock(sqlparser.ForUpdateLock)

	plan, err := configuredPlanner(selectStmt, reservedVars, vschema)
	if err != nil {
		return nil, err
	}

	eins := engine.NewSimpleInsert(
		engine.InsertSelect,
		table,
		table.Keyspace,
	)
	eins.Input = plan

	directives := sqlparser.ExtractCommentDirectives(ins.Comments)
	if directives.IsSet(sqlparser.DirectiveMultiShardAutocommit) {
		eins.MultiShardAutocommit = true
	}
	eins.QueryTimeout = queryTimeout(directives)

	// Fill out the 3-d Values structure. Please see documentation of Insert.Values for details.
	var colVindexes []*vindexes.ColumnVindex
	for _, colVindex := range eins.Table.ColumnVindexes {
		if colVindex.IgnoreInDML() {
			continue
		}
		colVindexes = append(colVindexes, colVindex)
	}
	eins.ColVindexes = colVindexes

	vv := make([][]int, len(colVindexes))
	for idx, colVindex := range colVindexes {
		for _, col := range colVindex.Columns {
			vv[idx] = append(vv[idx], findColumn(ins, col))
		}
	}
	eins.VindexValueOffset = vv
	generateInsertSelectQuery(ins, eins)
	return eins, nil
}

// findColumn returns the column index where it is placed on the insert column list.
// Otherwise, return -1 when not found.
func findColumn(ins *sqlparser.Insert, col sqlparser.ColIdent) int {
	for i, column := range ins.Columns {
		if col.Equal(column) {
			return i
		}
	}
	return -1
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

func generateInsertSelectQuery(node *sqlparser.Insert, eins *engine.Insert) {
	prefixBuf := sqlparser.NewTrackedBuffer(dmlFormatter)
	suffixBuf := sqlparser.NewTrackedBuffer(dmlFormatter)
	prefixBuf.Myprintf("insert %v%sinto %v%v ",
		node.Comments, node.Ignore.ToString(),
		node.Table, node.Columns)
	eins.Prefix = prefixBuf.String()
	suffixBuf.Myprintf("%v", node.OnDup)
	eins.Suffix = suffixBuf.String()
}

// modifyForAutoinc modfies the AST and the plan to generate
// necessary autoinc values. It must be called only if eins.Table.AutoIncrement
// is set. Bind variable names are generated using baseName.
func modifyForAutoinc(ins *sqlparser.Insert, eins *engine.Insert) error {
	colNum := findOrAddColumn(ins, eins.Table.AutoIncrement.Column)
	rows := ins.Rows.(sqlparser.Values)
	autoIncValues := make([]evalengine.Expr, 0, len(rows))
	for rowNum, row := range rows {
		// Support the DEFAULT keyword by treating it as null
		if _, ok := row[colNum].(*sqlparser.Default); ok {
			row[colNum] = &sqlparser.NullVal{}
		}

		pv, err := evalengine.Translate(row[colNum], semantics.EmptySemTable())
		if err != nil {
			return err
		}
		autoIncValues = append(autoIncValues, pv)
		row[colNum] = sqlparser.NewArgument(engine.SeqVarName + strconv.Itoa(rowNum))
	}

	eins.Generate = &engine.Generate{
		Keyspace: eins.Table.AutoIncrement.Sequence.Keyspace,
		Query:    fmt.Sprintf("select next :n values from %s", sqlparser.String(eins.Table.AutoIncrement.Sequence.Name)),
		Values:   evalengine.NewTupleExpr(autoIncValues...),
	}
	return nil
}

// findOrAddColumn finds the position of a column in the insert. If it's
// absent it appends it to the with NULL values and returns that position.
func findOrAddColumn(ins *sqlparser.Insert, col sqlparser.ColIdent) int {
	colNum := findColumn(ins, col)
	if colNum >= 0 {
		return colNum
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
