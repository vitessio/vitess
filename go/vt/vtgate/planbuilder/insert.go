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
		return buildInsertUnshardedPlan(ins, vschemaTable, reservedVars, vschema)
	}
	if ins.Action == sqlparser.ReplaceAct {
		return nil, errors.New("unsupported: REPLACE INTO with sharded schema")
	}
	return buildInsertShardedPlan(ins, vschemaTable, reservedVars, vschema)
}

func buildInsertUnshardedPlan(ins *sqlparser.Insert, table *vindexes.Table, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (engine.Primitive, error) {
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
		plan, err := subquerySelectPlan(ins, vschema, reservedVars, false)
		if err != nil {
			return nil, err
		}
		if route, ok := plan.(*engine.Route); ok && !route.Keyspace.Sharded && table.Keyspace.Name == route.Keyspace.Name {
			eins.Query = generateQuery(ins)
			return eins, nil
		}
		eins.Input = plan
		generateInsertSelectQuery(ins, eins)
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
	eins := &engine.Insert{
		Table:    table,
		Keyspace: table.Keyspace,
	}
	eins.Ignore = bool(ins.Ignore)
	if ins.OnDup != nil {
		if isVindexChanging(sqlparser.UpdateExprs(ins.OnDup), eins.Table.ColumnVindexes) {
			return nil, errors.New("unsupported: DML cannot change vindex column")
		}
		eins.Ignore = true
	}
	if len(ins.Columns) == 0 {
		if table.ColumnListAuthoritative {
			populateInsertColumnlist(ins, table)
		}
	}

	applyCommentDirectives(ins, eins)
	eins.ColVindexes = getColVindexes(eins.Table.ColumnVindexes)

	// Till here common plan building done for insert by providing values or select query.

	rows, isRowValues := ins.Rows.(sqlparser.Values)
	if !isRowValues {
		return buildInsertSelectPlan(ins, table, reservedVars, vschema, eins)
	}
	eins.Opcode = engine.InsertSharded

	for _, value := range rows {
		if len(ins.Columns) != len(value) {
			return nil, errors.New("column list doesn't match values")
		}
	}

	if err := modifyForAutoinc(ins, eins); err != nil {
		return nil, err
	}

	// Fill out the 3-d Values structure. Please see documentation of Insert.Values for details.
	colVindexes := eins.ColVindexes
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
	eins.Query = generateQuery(ins)
	generateInsertShardedQuery(ins, eins, rows)
	return eins, nil
}

// buildInsertSelectPlan builds an insert using select plan.
func buildInsertSelectPlan(ins *sqlparser.Insert, table *vindexes.Table, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, eins *engine.Insert) (engine.Primitive, error) {
	eins.Opcode = engine.InsertSelect

	// check if column list is provided if not, then vschema should be able to provide the column list.
	if len(ins.Columns) == 0 {
		if !table.ColumnListAuthoritative {
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "insert should contain column list or the table should have authoritative columns in vschema")
		}
		populateInsertColumnlist(ins, table)
	}

	// select plan will be taken as input to insert rows into the table.
	plan, err := subquerySelectPlan(ins, vschema, reservedVars, true)
	if err != nil {
		return nil, err
	}
	eins.Input = plan

	// auto-increment column is added explicility if not provided.
	if err := modifyForAutoinc(ins, eins); err != nil {
		return nil, err
	}

	// Fill out the 3-d Values structure
	eins.VindexValueOffset, err = extractColVindexOffsets(ins, eins.ColVindexes)
	if err != nil {
		return nil, err
	}

	generateInsertSelectQuery(ins, eins)
	return eins, nil
}

func subquerySelectPlan(ins *sqlparser.Insert, vschema plancontext.VSchema, reservedVars *sqlparser.ReservedVars, sharded bool) (engine.Primitive, error) {
	selectStmt, queryPlanner, err := getStatementAndPlanner(ins, vschema)
	if err != nil {
		return nil, err
	}

	// validate the columns to match on insert and select
	// for sharded insert table only
	if sharded {
		if err := checkColumnCounts(ins, selectStmt); err != nil {
			return nil, err
		}
	}

	// Override the locking with `for update` to lock the rows for inserting the data.
	selectStmt.SetLock(sqlparser.ForUpdateLock)

	return queryPlanner(selectStmt, reservedVars, vschema)
}

func getStatementAndPlanner(
	ins *sqlparser.Insert,
	vschema plancontext.VSchema,
) (selectStmt sqlparser.SelectStatement, configuredPlanner stmtPlanner, err error) {
	switch stmt := ins.Rows.(type) {
	case *sqlparser.Select:
		configuredPlanner, err = getConfiguredPlanner(vschema, buildSelectPlan, stmt, "")
		selectStmt = stmt
	case *sqlparser.Union:
		configuredPlanner, err = getConfiguredPlanner(vschema, buildUnionPlan, stmt, "")
		selectStmt = stmt
	default:
		err = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: insert plan with %T", ins.Rows)
	}

	if err != nil {
		return nil, nil, err
	}

	return selectStmt, configuredPlanner, nil
}

func checkColumnCounts(ins *sqlparser.Insert, selectStmt sqlparser.SelectStatement) error {
	if len(ins.Columns) < selectStmt.GetColumnCount() {
		return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValueCountOnRow, "Column count doesn't match value count at row 1")
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
			return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValueCountOnRow, "Column count doesn't match value count at row 1")
		}
	}
	return nil
}

func applyCommentDirectives(ins *sqlparser.Insert, eins *engine.Insert) {
	directives := ins.Comments.Directives()
	if directives.IsSet(sqlparser.DirectiveMultiShardAutocommit) {
		eins.MultiShardAutocommit = true
	}
	eins.QueryTimeout = queryTimeout(directives)
}

func getColVindexes(allColVindexes []*vindexes.ColumnVindex) (colVindexes []*vindexes.ColumnVindex) {
	for _, colVindex := range allColVindexes {
		if colVindex.IsPartialVindex() {
			continue
		}
		colVindexes = append(colVindexes, colVindex)
	}
	return
}

func extractColVindexOffsets(ins *sqlparser.Insert, colVindexes []*vindexes.ColumnVindex) ([][]int, error) {
	vv := make([][]int, len(colVindexes))
	for idx, colVindex := range colVindexes {
		for _, col := range colVindex.Columns {
			colNum := findColumn(ins, col)
			// sharding column values should be provided in the insert.
			if colNum == -1 && idx == 0 {
				return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "insert query does not have sharding column '%v' in the column list", col)
			}
			vv[idx] = append(vv[idx], colNum)
		}
	}
	return vv, nil
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

// modifyForAutoinc modifies the AST and the plan to generate necessary autoinc values.
// For row values cases, bind variable names are generated using baseName.
func modifyForAutoinc(ins *sqlparser.Insert, eins *engine.Insert) error {
	if eins.Table.AutoIncrement == nil {
		return nil
	}
	colNum := findOrAddColumn(ins, eins.Table.AutoIncrement.Column)
	eins.Generate = &engine.Generate{
		Keyspace: eins.Table.AutoIncrement.Sequence.Keyspace,
		Query:    fmt.Sprintf("select next :n values from %s", sqlparser.String(eins.Table.AutoIncrement.Sequence.Name)),
	}
	switch rows := ins.Rows.(type) {
	case sqlparser.SelectStatement:
		eins.Generate.Offset = colNum
		return nil
	case sqlparser.Values:
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
		eins.Generate.Values = evalengine.NewTupleExpr(autoIncValues...)
		return nil
	}
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: unexpected construct in insert: %T", ins.Rows)
}

// findOrAddColumn finds the position of a column in the insert. If it's
// absent it appends it to the with NULL values and returns that position.
func findOrAddColumn(ins *sqlparser.Insert, col sqlparser.ColIdent) int {
	colNum := findColumn(ins, col)
	if colNum >= 0 {
		return colNum
	}
	colOffset := len(ins.Columns)
	ins.Columns = append(ins.Columns, col)
	if rows, ok := ins.Rows.(sqlparser.Values); ok {
		for i := range rows {
			rows[i] = append(rows[i], &sqlparser.NullVal{})
		}
	}
	return colOffset
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
