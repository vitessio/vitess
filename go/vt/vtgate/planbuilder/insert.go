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
	"fmt"
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// buildInsertPlan builds the route for an INSERT statement.
func buildInsertPlan(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (*planResult, error) {
	pb := newStmtAwarePrimitiveBuilder(vschema, newJointab(reservedVars), stmt)
	ins := stmt.(*sqlparser.Insert)
	err := checkUnsupportedExpressions(ins)
	if err != nil {
		return nil, err
	}
	exprs := sqlparser.TableExprs{&sqlparser.AliasedTableExpr{Expr: ins.Table}}
	rb, err := pb.processDMLTable(exprs, reservedVars, nil)
	if err != nil {
		return nil, err
	}
	// The table might have been routed to a different one.
	ins.Table = exprs[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName)
	if rb.eroute.TargetDestination != nil {
		return nil, vterrors.VT12001("INSERT with a target destination")
	}

	if len(pb.st.tables) != 1 {
		// Unreachable.
		return nil, vterrors.VT12001("multi-table INSERT statement in a sharded keyspace")
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
		return nil, vterrors.VT12001("REPLACE INTO with sharded keyspace")
	}
	return buildInsertShardedPlan(ins, vschemaTable, reservedVars, vschema)
}

func buildInsertUnshardedPlan(ins *sqlparser.Insert, table *vindexes.Table, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (*planResult, error) {
	eins := engine.NewSimpleInsert(
		engine.InsertUnsharded,
		table,
		table.Keyspace,
	)
	applyCommentDirectives(ins, eins)

	var rows sqlparser.Values
	tc := &tableCollector{}
	tc.addVindexTable(table)
	switch insertValues := ins.Rows.(type) {
	case *sqlparser.Select, *sqlparser.Union:
		if eins.Table.AutoIncrement != nil {
			return nil, vterrors.VT12001("auto-increment and SELECT in INSERT")
		}
		plan, err := subquerySelectPlan(ins, vschema, reservedVars, false)
		if err != nil {
			return nil, err
		}
		tc.addAllTables(plan.tables)
		if route, ok := plan.primitive.(*engine.Route); ok && !route.Keyspace.Sharded && table.Keyspace.Name == route.Keyspace.Name {
			eins.Query = generateQuery(ins)
		} else {
			eins.Input = plan.primitive
			generateInsertSelectQuery(ins, eins)
		}
		return newPlanResult(eins, tc.getTables()...), nil
	case sqlparser.Values:
		rows = insertValues
	default:
		return nil, vterrors.VT13001(fmt.Sprintf("unexpected construct in INSERT: %T", insertValues))
	}
	if eins.Table.AutoIncrement == nil {
		eins.Query = generateQuery(ins)
	} else {
		// Table has auto-inc and has a VALUES clause.
		// If the column list is nil then add all the columns
		// If the column list is empty then add only the auto-inc column and this happens on calling modifyForAutoinc
		if ins.Columns == nil {
			if table.ColumnListAuthoritative {
				populateInsertColumnlist(ins, table)
			} else {
				return nil, vterrors.VT13001("column list required for tables with auto-inc columns")
			}
		}
		for _, row := range rows {
			if len(ins.Columns) != len(row) {
				return nil, vterrors.VT13001("column list does not match values")
			}
		}
		if err := modifyForAutoinc(ins, eins); err != nil {
			return nil, err
		}
		eins.Query = generateQuery(ins)
	}

	return newPlanResult(eins, tc.getTables()...), nil
}

func buildInsertShardedPlan(ins *sqlparser.Insert, table *vindexes.Table, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (*planResult, error) {
	eins := &engine.Insert{
		Table:    table,
		Keyspace: table.Keyspace,
	}
	tc := &tableCollector{}
	tc.addVindexTable(table)
	eins.Ignore = bool(ins.Ignore)
	if ins.OnDup != nil {
		if isVindexChanging(sqlparser.UpdateExprs(ins.OnDup), eins.Table.ColumnVindexes) {
			return nil, vterrors.VT12001("DML cannot update vindex column")
		}
		eins.Ignore = true
	}
	if ins.Columns == nil && table.ColumnListAuthoritative {
		populateInsertColumnlist(ins, table)
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
			return nil, vterrors.VT13001("column list does not match values")
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
				innerpv, err := evalengine.Translate(row[colNum], nil)
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
	return newPlanResult(eins, tc.getTables()...), nil
}

// buildInsertSelectPlan builds an insert using select plan.
func buildInsertSelectPlan(ins *sqlparser.Insert, table *vindexes.Table, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, eins *engine.Insert) (*planResult, error) {
	eins.Opcode = engine.InsertSelect
	tc := &tableCollector{}
	tc.addVindexTable(table)

	// check if column list is provided if not, then vschema should be able to provide the column list.
	if len(ins.Columns) == 0 {
		if !table.ColumnListAuthoritative {
			return nil, vterrors.VT09004()
		}
		populateInsertColumnlist(ins, table)
	}

	// select plan will be taken as input to insert rows into the table.
	plan, err := subquerySelectPlan(ins, vschema, reservedVars, true)
	if err != nil {
		return nil, err
	}
	tc.addAllTables(plan.tables)
	eins.Input = plan.primitive

	// When the table you are steaming data from and table you are inserting from are same.
	// Then due to locking of the index range on the table we might not be able to insert into the table.
	// Therefore, instead of streaming, this flag will ensure the records are first read and then inserted.
	if strings.Contains(plan.primitive.GetTableName(), table.Name.String()) {
		eins.ForceNonStreaming = true
	}

	// auto-increment column is added explicitly if not provided.
	if err := modifyForAutoinc(ins, eins); err != nil {
		return nil, err
	}

	// Fill out the 3-d Values structure
	eins.VindexValueOffset, err = extractColVindexOffsets(ins, eins.ColVindexes)
	if err != nil {
		return nil, err
	}

	generateInsertSelectQuery(ins, eins)
	return newPlanResult(eins, tc.getTables()...), nil
}

func subquerySelectPlan(ins *sqlparser.Insert, vschema plancontext.VSchema, reservedVars *sqlparser.ReservedVars, sharded bool) (*planResult, error) {
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
		err = vterrors.VT12001(fmt.Sprintf("INSERT plan with %T", ins.Rows))
	}

	if err != nil {
		return nil, nil, err
	}

	return selectStmt, configuredPlanner, nil
}

func checkColumnCounts(ins *sqlparser.Insert, selectStmt sqlparser.SelectStatement) error {
	if len(ins.Columns) < selectStmt.GetColumnCount() {
		return vterrors.VT03006()
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
			return vterrors.VT03006()
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
				return nil, vterrors.VT09003(col)
			}
			vv[idx] = append(vv[idx], colNum)
		}
	}
	return vv, nil
}

// findColumn returns the column index where it is placed on the insert column list.
// Otherwise, return -1 when not found.
func findColumn(ins *sqlparser.Insert, col sqlparser.IdentifierCI) int {
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

			pv, err := evalengine.Translate(row[colNum], nil)
			if err != nil {
				return err
			}
			autoIncValues = append(autoIncValues, pv)
			row[colNum] = sqlparser.NewArgument(engine.SeqVarName + strconv.Itoa(rowNum))
		}
		eins.Generate.Values = evalengine.NewTupleExpr(autoIncValues...)
		return nil
	}
	return vterrors.VT13001(fmt.Sprintf("unexpected construct in INSERT: %T", ins.Rows))
}

// findOrAddColumn finds the position of a column in the insert. If it's
// absent it appends it to the with NULL values and returns that position.
func findOrAddColumn(ins *sqlparser.Insert, col sqlparser.IdentifierCI) int {
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
