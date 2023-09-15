/*
Copyright 2023 The Vitess Authors.

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

package operators

import (
	"strconv"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// Insert represents an insert operation on a table.
type Insert struct {
	// VTable represents the target table for the insert operation.
	VTable *vindexes.Table
	// AST represents the insert statement from the SQL syntax.
	AST *sqlparser.Insert

	// AutoIncrement represents the auto-increment generator for the insert operation.
	AutoIncrement *Generate
	// Ignore specifies whether to ignore duplicate key errors during insertion.
	Ignore bool
	// ForceNonStreaming when true, select first then insert, this is to avoid locking rows by select for insert.
	ForceNonStreaming bool

	// ColVindexes are the vindexes that will use the VindexValues or VindexValueOffset
	ColVindexes []*vindexes.ColumnVindex

	// VindexValues specifies values for all the vindex columns.
	VindexValues [][][]evalengine.Expr

	// VindexValueOffset stores the offset for each column in the ColumnVindex
	// that will appear in the result set of the select query.
	VindexValueOffset [][]int

	// Insert using select query will have select plan as input operator for the insert operation.
	Input ops.Operator

	noColumns
	noPredicates
}

func (i *Insert) Inputs() []ops.Operator {
	if i.Input == nil {
		return nil
	}
	return []ops.Operator{i.Input}
}

func (i *Insert) SetInputs(inputs []ops.Operator) {
	if len(inputs) > 0 {
		i.Input = inputs[0]
	}
}

// Generate represents an auto-increment generator for the insert operation.
type Generate struct {
	// Keyspace represents the keyspace information for the table.
	Keyspace *vindexes.Keyspace
	// TableName represents the name of the table.
	TableName sqlparser.TableName

	// Values are the supplied values for the column, which
	// will be stored as a list within the expression. New
	// values will be generated based on how many were not
	// supplied (NULL).
	Values evalengine.Expr
	// Insert using Select, offset for auto increment column
	Offset int

	// added indicates whether the auto-increment column was already present in the insert column list or added.
	added bool
}

func (i *Insert) ShortDescription() string {
	return i.VTable.String()
}

func (i *Insert) GetOrdering() ([]ops.OrderBy, error) {
	return nil, nil
}

var _ ops.Operator = (*Insert)(nil)

func (i *Insert) Clone(inputs []ops.Operator) ops.Operator {
	var input ops.Operator
	if len(inputs) > 0 {
		input = inputs[0]
	}
	return &Insert{
		Input:             input,
		VTable:            i.VTable,
		AST:               i.AST,
		AutoIncrement:     i.AutoIncrement,
		Ignore:            i.Ignore,
		ForceNonStreaming: i.ForceNonStreaming,
		ColVindexes:       i.ColVindexes,
		VindexValues:      i.VindexValues,
		VindexValueOffset: i.VindexValueOffset,
	}
}

func (i *Insert) TablesUsed() []string {
	return SingleQualifiedIdentifier(i.VTable.Keyspace, i.VTable.Name)
}

func (i *Insert) Statement() sqlparser.Statement {
	return i.AST
}

func createOperatorFromInsert(ctx *plancontext.PlanningContext, ins *sqlparser.Insert) (ops.Operator, error) {
	tableInfo, qt, err := createQueryTableForDML(ctx, ins.Table, nil)
	if err != nil {
		return nil, err
	}

	vindexTable, routing, err := buildVindexTableForDML(ctx, tableInfo, qt, "insert")
	if err != nil {
		return nil, err
	}

	insOp, err := createInsertOperator(ctx, ins, vindexTable, routing)
	if err != nil {
		return nil, err
	}

	// Find the foreign key mode and for unmanaged foreign-key-mode, we don't need to do anything.
	ksMode, err := ctx.VSchema.ForeignKeyMode(vindexTable.Keyspace.Name)
	if err != nil {
		return nil, err
	}
	if ksMode != vschemapb.Keyspace_FK_MANAGED {
		return insOp, nil
	}

	parentFKsForInsert := vindexTable.ParentFKsNeedsHandling(ctx.VerifyAllFKs, ctx.ParentFKToIgnore)
	if len(parentFKsForInsert) > 0 {
		return nil, vterrors.VT12002()
	}
	if len(ins.OnDup) == 0 {
		return insOp, nil
	}

	parentFksForUpdate, childFksForUpdate := getFKRequirementsForUpdate(ctx, sqlparser.UpdateExprs(ins.OnDup), vindexTable)
	if len(parentFksForUpdate) == 0 && len(childFksForUpdate) == 0 {
		return insOp, nil
	}
	return nil, vterrors.VT12001("ON DUPLICATE KEY UPDATE with foreign keys")
}

func createInsertOperator(ctx *plancontext.PlanningContext, insStmt *sqlparser.Insert, vTbl *vindexes.Table, routing Routing) (ops.Operator, error) {
	if _, target := routing.(*TargetedRouting); target {
		return nil, vterrors.VT12001("INSERT with a target destination")
	}

	insOp := &Insert{
		VTable: vTbl,
		AST:    insStmt,
	}
	route := &Route{
		Source:  insOp,
		Routing: routing,
	}

	// Table column list is nil then add all the columns
	// If the column list is empty then add only the auto-inc column and
	// this happens on calling modifyForAutoinc
	if insStmt.Columns == nil && valuesProvided(insStmt.Rows) {
		if vTbl.ColumnListAuthoritative {
			insStmt = populateInsertColumnlist(insStmt, vTbl)
		} else {
			return nil, vterrors.VT09004()
		}
	}

	// modify column list or values for autoincrement column.
	autoIncGen, err := modifyForAutoinc(insStmt, vTbl)
	if err != nil {
		return nil, err
	}
	insOp.AutoIncrement = autoIncGen

	// set insert ignore.
	insOp.Ignore = bool(insStmt.Ignore) || insStmt.OnDup != nil

	insOp.ColVindexes = getColVindexes(insOp)
	switch rows := insStmt.Rows.(type) {
	case sqlparser.Values:
		route.Source, err = insertRowsPlan(insOp, insStmt, rows)
		if err != nil {
			return nil, err
		}
	case sqlparser.SelectStatement:
		route.Source, err = insertSelectPlan(ctx, insOp, insStmt, rows)
		if err != nil {
			return nil, err
		}
	}
	return route, nil
}

func insertSelectPlan(ctx *plancontext.PlanningContext, insOp *Insert, ins *sqlparser.Insert, sel sqlparser.SelectStatement) (*Insert, error) {
	if columnMismatch(insOp.AutoIncrement, ins, sel) {
		return nil, vterrors.VT03006()
	}

	selOp, err := PlanQuery(ctx, sel)
	if err != nil {
		return nil, err
	}

	// select plan will be taken as input to insert rows into the table.
	insOp.Input = selOp

	// When the table you are steaming data from and table you are inserting from are same.
	// Then due to locking of the index range on the table we might not be able to insert into the table.
	// Therefore, instead of streaming, this flag will ensure the records are first read and then inserted.
	insertTbl := insOp.TablesUsed()[0]
	selTables := TablesUsed(selOp)
	for _, tbl := range selTables {
		if insertTbl == tbl {
			insOp.ForceNonStreaming = true
			break
		}
	}

	if len(insOp.ColVindexes) == 0 {
		return insOp, nil
	}

	colVindexes := insOp.ColVindexes
	vv := make([][]int, len(colVindexes))
	for idx, colVindex := range colVindexes {
		for _, col := range colVindex.Columns {
			err := checkAndErrIfVindexChanging(sqlparser.UpdateExprs(ins.OnDup), col)
			if err != nil {
				return nil, err
			}

			colNum := findColumn(ins, col)
			// sharding column values should be provided in the insert.
			if colNum == -1 && idx == 0 {
				return nil, vterrors.VT09003(col)
			}
			vv[idx] = append(vv[idx], colNum)
		}
	}
	insOp.VindexValueOffset = vv
	return insOp, nil
}

func columnMismatch(gen *Generate, ins *sqlparser.Insert, sel sqlparser.SelectStatement) bool {
	origColCount := len(ins.Columns)
	if gen != nil && gen.added {
		// One column got added to the insert query ast for auto increment column.
		// adjusting it here for comparison.
		origColCount--
	}
	if origColCount < sel.GetColumnCount() {
		return true
	}
	if origColCount > sel.GetColumnCount() {
		sel := sqlparser.GetFirstSelect(sel)
		var hasStarExpr bool
		for _, sExpr := range sel.SelectExprs {
			if _, hasStarExpr = sExpr.(*sqlparser.StarExpr); hasStarExpr {
				break
			}
		}
		if !hasStarExpr {
			return true
		}
	}
	return false
}

func insertRowsPlan(insOp *Insert, ins *sqlparser.Insert, rows sqlparser.Values) (*Insert, error) {
	for _, row := range rows {
		if len(ins.Columns) != len(row) {
			return nil, vterrors.VT03006()
		}
	}

	if len(insOp.ColVindexes) == 0 {
		return insOp, nil
	}

	colVindexes := insOp.ColVindexes
	routeValues := make([][][]evalengine.Expr, len(colVindexes))
	for vIdx, colVindex := range colVindexes {
		routeValues[vIdx] = make([][]evalengine.Expr, len(colVindex.Columns))
		for colIdx, col := range colVindex.Columns {
			err := checkAndErrIfVindexChanging(sqlparser.UpdateExprs(ins.OnDup), col)
			if err != nil {
				return nil, err
			}
			routeValues[vIdx][colIdx] = make([]evalengine.Expr, len(rows))
			colNum, _ := findOrAddColumn(ins, col)
			for rowNum, row := range rows {
				innerpv, err := evalengine.Translate(row[colNum], nil)
				if err != nil {
					return nil, err
				}
				routeValues[vIdx][colIdx][rowNum] = innerpv
			}
		}
	}
	// here we are replacing the row value with the argument.
	for _, colVindex := range colVindexes {
		for _, col := range colVindex.Columns {
			colNum, _ := findOrAddColumn(ins, col)
			for rowNum, row := range rows {
				name := engine.InsertVarName(col, rowNum)
				row[colNum] = sqlparser.NewArgument(name)
			}
		}
	}
	insOp.VindexValues = routeValues
	return insOp, nil
}

func valuesProvided(rows sqlparser.InsertRows) bool {
	switch values := rows.(type) {
	case sqlparser.Values:
		return len(values) >= 0 && len(values[0]) > 0
	case sqlparser.SelectStatement:
		return true
	}
	return false
}

func getColVindexes(insOp *Insert) (colVindexes []*vindexes.ColumnVindex) {
	// For unsharded table the Column Vindex does not mean anything.
	// And therefore should be ignored.
	if !insOp.VTable.Keyspace.Sharded {
		return
	}
	for _, colVindex := range insOp.VTable.ColumnVindexes {
		if colVindex.IsPartialVindex() {
			continue
		}
		colVindexes = append(colVindexes, colVindex)
	}
	return
}

func checkAndErrIfVindexChanging(setClauses sqlparser.UpdateExprs, col sqlparser.IdentifierCI) error {
	for _, assignment := range setClauses {
		if col.Equal(assignment.Name.Name) {
			valueExpr, isValuesFuncExpr := assignment.Expr.(*sqlparser.ValuesFuncExpr)
			// update on duplicate key is changing the vindex column, not supported.
			if !isValuesFuncExpr || !valueExpr.Name.Name.Equal(assignment.Name.Name) {
				return vterrors.VT12001("DML cannot update vindex column")
			}
			return nil
		}
	}
	return nil
}

// findOrAddColumn finds the position of a column in the insert. If it's
// absent it appends it to the with NULL values.
// It returns the position of the column and also boolean representing whether it was added or already present.
func findOrAddColumn(ins *sqlparser.Insert, col sqlparser.IdentifierCI) (int, bool) {
	colNum := findColumn(ins, col)
	if colNum >= 0 {
		return colNum, false
	}
	colOffset := len(ins.Columns)
	ins.Columns = append(ins.Columns, col)
	if rows, ok := ins.Rows.(sqlparser.Values); ok {
		for i := range rows {
			rows[i] = append(rows[i], &sqlparser.NullVal{})
		}
	}
	return colOffset, true
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

func populateInsertColumnlist(ins *sqlparser.Insert, table *vindexes.Table) *sqlparser.Insert {
	cols := make(sqlparser.Columns, 0, len(table.Columns))
	for _, c := range table.Columns {
		cols = append(cols, c.Name)
	}
	ins.Columns = cols
	return ins
}

// modifyForAutoinc modifies the AST and the plan to generate necessary autoinc values.
// For row values cases, bind variable names are generated using baseName.
func modifyForAutoinc(ins *sqlparser.Insert, vTable *vindexes.Table) (*Generate, error) {
	if vTable.AutoIncrement == nil {
		return nil, nil
	}
	gen := &Generate{
		Keyspace:  vTable.AutoIncrement.Sequence.Keyspace,
		TableName: sqlparser.TableName{Name: vTable.AutoIncrement.Sequence.Name},
	}
	colNum, newColAdded := findOrAddColumn(ins, vTable.AutoIncrement.Column)
	switch rows := ins.Rows.(type) {
	case sqlparser.SelectStatement:
		gen.Offset = colNum
		gen.added = newColAdded
	case sqlparser.Values:
		autoIncValues := make([]evalengine.Expr, 0, len(rows))
		for rowNum, row := range rows {
			// Support the DEFAULT keyword by treating it as null
			if _, ok := row[colNum].(*sqlparser.Default); ok {
				row[colNum] = &sqlparser.NullVal{}
			}
			expr, err := evalengine.Translate(row[colNum], nil)
			if err != nil {
				return nil, err
			}
			autoIncValues = append(autoIncValues, expr)
			row[colNum] = sqlparser.NewArgument(engine.SeqVarName + strconv.Itoa(rowNum))
		}
		gen.Values = evalengine.NewTupleExpr(autoIncValues...)
	}
	return gen, nil
}
