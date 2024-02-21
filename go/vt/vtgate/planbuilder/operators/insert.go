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

	// ColVindexes are the vindexes that will use the VindexValues or VindexValueOffset
	ColVindexes []*vindexes.ColumnVindex

	// VindexValues specifies values for all the vindex columns.
	VindexValues [][][]evalengine.Expr

	// VindexValueOffset stores the offset for each column in the ColumnVindex
	// that will appear in the result set of the select query.
	VindexValueOffset [][]int

	noInputs
	noColumns
	noPredicates
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

func (i *Insert) GetOrdering(*plancontext.PlanningContext) []OrderBy {
	return nil
}

var _ Operator = (*Insert)(nil)

func (i *Insert) Clone([]Operator) Operator {
	return &Insert{
		VTable:            i.VTable,
		AST:               i.AST,
		AutoIncrement:     i.AutoIncrement,
		Ignore:            i.Ignore,
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

func createOperatorFromInsert(ctx *plancontext.PlanningContext, ins *sqlparser.Insert) Operator {
	tableInfo, qt := createQueryTableForDML(ctx, ins.Table, nil)

	vTbl, routing := buildVindexTableForDML(ctx, tableInfo, qt, "insert")

	deleteBeforeInsert := false
	if ins.Action == sqlparser.ReplaceAct &&
		(ctx.SemTable.ForeignKeysPresent() || vTbl.Keyspace.Sharded) &&
		(len(vTbl.PrimaryKey) > 0 || len(vTbl.UniqueKeys) > 0) {
		// this needs a delete before insert as there can be row clash which needs to be deleted first.
		ins.Action = sqlparser.InsertAct
		deleteBeforeInsert = true
	}

	insOp := checkAndCreateInsertOperator(ctx, ins, vTbl, routing)

	if !deleteBeforeInsert {
		return insOp
	}

	rows, isRows := ins.Rows.(sqlparser.Values)
	if !isRows {
		panic(vterrors.VT12001("REPLACE INTO using select statement"))
	}

	pkCompExpr := pkCompExpression(vTbl, ins, rows)
	uniqKeyCompExprs := uniqKeyCompExpressions(vTbl, ins, rows)
	whereExpr := getWhereCondExpr(append(uniqKeyCompExprs, pkCompExpr))

	delStmt := &sqlparser.Delete{
		Comments:   ins.Comments,
		TableExprs: sqlparser.TableExprs{sqlparser.CloneRefOfAliasedTableExpr(ins.Table)},
		Where:      sqlparser.NewWhere(sqlparser.WhereClause, whereExpr),
	}
	delOp := createOpFromStmt(ctx, delStmt, false, "")
	return &Sequential{Sources: []Operator{delOp, insOp}}
}

func checkAndCreateInsertOperator(ctx *plancontext.PlanningContext, ins *sqlparser.Insert, vTbl *vindexes.Table, routing Routing) Operator {
	insOp := createInsertOperator(ctx, ins, vTbl, routing)

	// Find the foreign key mode and for unmanaged foreign-key-mode, we don't need to do anything.
	ksMode, err := ctx.VSchema.ForeignKeyMode(vTbl.Keyspace.Name)
	if err != nil {
		panic(err)
	}
	if ksMode != vschemapb.Keyspace_managed {
		return insOp
	}

	parentFKs := ctx.SemTable.GetParentForeignKeysList()
	childFks := ctx.SemTable.GetChildForeignKeysList()
	if len(parentFKs) > 0 {
		panic(vterrors.VT12002())
	}
	if len(childFks) > 0 {
		if ins.Action == sqlparser.ReplaceAct {
			panic(vterrors.VT12001("REPLACE INTO with foreign keys"))
		}
		if len(ins.OnDup) > 0 {
			rows := getRowsOrError(ins)
			return createUpsertOperator(ctx, ins, insOp, rows, vTbl)
		}
	}
	return insOp
}

func getRowsOrError(ins *sqlparser.Insert) sqlparser.Values {
	if rows, ok := ins.Rows.(sqlparser.Values); ok {
		return rows
	}
	panic(vterrors.VT12001("ON DUPLICATE KEY UPDATE with foreign keys with select statement"))
}

func getWhereCondExpr(compExprs []*sqlparser.ComparisonExpr) sqlparser.Expr {
	var outputExpr sqlparser.Expr
	for _, expr := range compExprs {
		if expr == nil {
			continue
		}
		if outputExpr == nil {
			outputExpr = expr
			continue
		}
		outputExpr = &sqlparser.OrExpr{
			Left:  outputExpr,
			Right: expr,
		}
	}
	return outputExpr
}

func pkCompExpression(vTbl *vindexes.Table, ins *sqlparser.Insert, rows sqlparser.Values) *sqlparser.ComparisonExpr {
	if len(vTbl.PrimaryKey) == 0 {
		return nil
	}
	pIndexes, pColTuple := findPKIndexes(vTbl, ins)

	var pValTuple sqlparser.ValTuple
	for _, row := range rows {
		var rowTuple sqlparser.ValTuple
		for _, pIdx := range pIndexes {
			if pIdx.idx == -1 {
				rowTuple = append(rowTuple, pIdx.def)
			} else {
				rowTuple = append(rowTuple, row[pIdx.idx])
			}
		}
		pValTuple = append(pValTuple, rowTuple)
	}
	return sqlparser.NewComparisonExpr(sqlparser.InOp, pColTuple, pValTuple, nil)
}

type pComp struct {
	idx int
	def sqlparser.Expr
	col sqlparser.IdentifierCI
}

func findPKIndexes(vTbl *vindexes.Table, ins *sqlparser.Insert) (pIndexes []pComp, pColTuple sqlparser.ValTuple) {
	for _, pCol := range vTbl.PrimaryKey {
		var def sqlparser.Expr
		idx := ins.Columns.FindColumn(pCol)
		if idx == -1 {
			def = findDefault(vTbl, pCol)
			if def == nil {
				// If default value is empty, nothing to compare as it will always be false.
				return nil, nil
			}
		}
		pIndexes = append(pIndexes, pComp{idx, def, pCol})
		pColTuple = append(pColTuple, sqlparser.NewColName(pCol.String()))
	}
	return
}

func findDefault(vTbl *vindexes.Table, pCol sqlparser.IdentifierCI) sqlparser.Expr {
	for _, column := range vTbl.Columns {
		if column.Name.Equal(pCol) {
			return column.Default
		}
	}
	panic(vterrors.VT03014(pCol.String(), vTbl.Name.String()))
}

type uComp struct {
	idx int
	def sqlparser.Expr
}

func uniqKeyCompExpressions(vTbl *vindexes.Table, ins *sqlparser.Insert, rows sqlparser.Values) (comps []*sqlparser.ComparisonExpr) {
	noOfUniqKeys := len(vTbl.UniqueKeys)
	if noOfUniqKeys == 0 {
		return nil
	}

	type uIdx struct {
		Indexes [][]uComp
		uniqKey sqlparser.Exprs
	}

	allIndexes := make([]uIdx, 0, noOfUniqKeys)
	allColTuples := make([]sqlparser.ValTuple, 0, noOfUniqKeys)
	for _, uniqKey := range vTbl.UniqueKeys {
		var uIndexes [][]uComp
		var uColTuple sqlparser.ValTuple
		skipKey := false
		for _, expr := range uniqKey {
			var offsets []uComp
			offsets, skipKey = createUniqueKeyComp(ins, expr, vTbl)
			if skipKey {
				break
			}
			uIndexes = append(uIndexes, offsets)
			uColTuple = append(uColTuple, expr)
		}
		if skipKey {
			continue
		}
		allIndexes = append(allIndexes, uIdx{uIndexes, uniqKey})
		allColTuples = append(allColTuples, uColTuple)
	}

	allValTuples := make([]sqlparser.ValTuple, len(allColTuples))
	for _, row := range rows {
		for i, uk := range allIndexes {
			var rowTuple sqlparser.ValTuple
			for j, offsets := range uk.Indexes {
				colIdx := 0
				valExpr := sqlparser.CopyOnRewrite(uk.uniqKey[j], nil, func(cursor *sqlparser.CopyOnWriteCursor) {
					_, isCol := cursor.Node().(*sqlparser.ColName)
					if !isCol {
						return
					}
					if offsets[colIdx].idx == -1 {
						cursor.Replace(offsets[colIdx].def)
					} else {
						cursor.Replace(row[offsets[colIdx].idx])
					}
					colIdx++
				}, nil).(sqlparser.Expr)
				rowTuple = append(rowTuple, valExpr)
			}
			allValTuples[i] = append(allValTuples[i], rowTuple)
		}
	}

	compExprs := make([]*sqlparser.ComparisonExpr, 0, noOfUniqKeys)
	for i, valTuple := range allValTuples {
		compExprs = append(compExprs, sqlparser.NewComparisonExpr(sqlparser.InOp, allColTuples[i], valTuple, nil))
	}
	return compExprs
}

func createUniqueKeyComp(ins *sqlparser.Insert, expr sqlparser.Expr, vTbl *vindexes.Table) ([]uComp, bool) {
	col, isCol := expr.(*sqlparser.ColName)
	if isCol {
		var def sqlparser.Expr
		idx := ins.Columns.FindColumn(col.Name)
		if idx == -1 {
			def = findDefault(vTbl, col.Name)
			if def == nil {
				// default value is empty, nothing to compare as it will always be false.
				return nil, true
			}
		}
		return []uComp{{idx, def}}, false
	}
	var offsets []uComp
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		col, ok := node.(*sqlparser.ColName)
		if !ok {
			return true, nil
		}
		var def sqlparser.Expr
		idx := ins.Columns.FindColumn(col.Name)
		if idx == -1 {
			def = findDefault(vTbl, col.Name)
			// no default, replace it with null value.
			if def == nil {
				def = &sqlparser.NullVal{}
			}
		}
		offsets = append(offsets, uComp{idx, def})
		return false, nil
	}, expr)
	return offsets, false
}

func createInsertOperator(ctx *plancontext.PlanningContext, insStmt *sqlparser.Insert, vTbl *vindexes.Table, routing Routing) (op Operator) {
	if _, target := routing.(*TargetedRouting); target {
		panic(vterrors.VT09017("INSERT with a target destination is not allowed"))
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
			panic(vterrors.VT09004())
		}
	}

	// modify column list or values for autoincrement column.
	autoIncGen := modifyForAutoinc(ctx, insStmt, vTbl)
	insOp.AutoIncrement = autoIncGen

	// set insert ignore.
	insOp.Ignore = bool(insStmt.Ignore) || insStmt.OnDup != nil

	insOp.ColVindexes = getColVindexes(insOp)
	switch rows := insStmt.Rows.(type) {
	case sqlparser.Values:
		op = route
		route.Source = insertRowsPlan(ctx, insOp, insStmt, rows)
	case sqlparser.SelectStatement:
		op = insertSelectPlan(ctx, insOp, route, insStmt, rows)
	}
	if insStmt.Comments != nil {
		op = &LockAndComment{
			Source:   op,
			Comments: insStmt.Comments,
		}
	}
	return op
}

func insertSelectPlan(
	ctx *plancontext.PlanningContext,
	insOp *Insert,
	routeOp *Route,
	ins *sqlparser.Insert,
	sel sqlparser.SelectStatement,
) *InsertSelection {
	if columnMismatch(insOp.AutoIncrement, ins, sel) {
		panic(vterrors.VT03006())
	}

	selOp, err := PlanQuery(ctx, sel)
	if err != nil {
		panic(err)
	}

	// output of the select plan will be used to insert rows into the table.
	insertSelect := &InsertSelection{
		Select: &LockAndComment{
			Source: selOp,
			Lock:   sqlparser.ShareModeLock,
		},
		Insert: routeOp,
	}

	// When the table you are streaming data from and table you are inserting from are same.
	// Then due to locking of the index range on the table we might not be able to insert into the table.
	// Therefore, instead of streaming, this flag will ensure the records are first read and then inserted.
	insertTbl := insOp.TablesUsed()[0]
	selTables := TablesUsed(selOp)
	for _, tbl := range selTables {
		if insertTbl == tbl {
			insertSelect.ForceNonStreaming = true
			break
		}
	}

	if len(insOp.ColVindexes) == 0 {
		return insertSelect
	}

	colVindexes := insOp.ColVindexes
	vv := make([][]int, len(colVindexes))
	for idx, colVindex := range colVindexes {
		for _, col := range colVindex.Columns {
			checkAndErrIfVindexChanging(sqlparser.UpdateExprs(ins.OnDup), col)
			colNum := findColumn(ins, col)
			// sharding column values should be provided in the insert.
			if colNum == -1 && idx == 0 {
				panic(vterrors.VT09003(col))
			}
			vv[idx] = append(vv[idx], colNum)
		}
	}
	insOp.VindexValueOffset = vv
	return insertSelect
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

func insertRowsPlan(ctx *plancontext.PlanningContext, insOp *Insert, ins *sqlparser.Insert, rows sqlparser.Values) *Insert {
	for _, row := range rows {
		if len(ins.Columns) != len(row) {
			panic(vterrors.VT03006())
		}
	}

	if len(insOp.ColVindexes) == 0 {
		return insOp
	}

	colVindexes := insOp.ColVindexes
	routeValues := make([][][]evalengine.Expr, len(colVindexes))
	for vIdx, colVindex := range colVindexes {
		routeValues[vIdx] = make([][]evalengine.Expr, len(colVindex.Columns))
		for colIdx, col := range colVindex.Columns {
			checkAndErrIfVindexChanging(sqlparser.UpdateExprs(ins.OnDup), col)
			routeValues[vIdx][colIdx] = make([]evalengine.Expr, len(rows))
			colNum, _ := findOrAddColumn(ins, col)
			for rowNum, row := range rows {
				innerpv, err := evalengine.Translate(row[colNum], &evalengine.Config{
					ResolveType: ctx.SemTable.TypeForExpr,
					Collation:   ctx.SemTable.Collation,
					Environment: ctx.VSchema.Environment(),
				})
				if err != nil {
					panic(err)
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
	return insOp
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

func checkAndErrIfVindexChanging(setClauses sqlparser.UpdateExprs, col sqlparser.IdentifierCI) {
	for _, assignment := range setClauses {
		if col.Equal(assignment.Name.Name) {
			valueExpr, isValuesFuncExpr := assignment.Expr.(*sqlparser.ValuesFuncExpr)
			// update on duplicate key is changing the vindex column, not supported.
			if !isValuesFuncExpr || !valueExpr.Name.Name.Equal(assignment.Name.Name) {
				panic(vterrors.VT12001("DML cannot update vindex column"))
			}
			return
		}
	}
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
func modifyForAutoinc(ctx *plancontext.PlanningContext, ins *sqlparser.Insert, vTable *vindexes.Table) *Generate {
	if vTable.AutoIncrement == nil {
		return nil
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
		autoIncValues := make(sqlparser.ValTuple, 0, len(rows))
		for rowNum, row := range rows {
			if len(ins.Columns) != len(row) {
				panic(vterrors.VT03006())
			}
			// Support the DEFAULT keyword by treating it as null
			if _, ok := row[colNum].(*sqlparser.Default); ok {
				row[colNum] = &sqlparser.NullVal{}
			}
			autoIncValues = append(autoIncValues, row[colNum])
			row[colNum] = sqlparser.NewArgument(engine.SeqVarName + strconv.Itoa(rowNum))
		}
		var err error
		gen.Values, err = evalengine.Translate(autoIncValues, &evalengine.Config{
			ResolveType: ctx.SemTable.TypeForExpr,
			Collation:   ctx.SemTable.Collation,
			Environment: ctx.VSchema.Environment(),
		})
		if err != nil {
			panic(err)
		}
	}
	return gen
}
