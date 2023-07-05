/*
Copyright 2022 The Vitess Authors.

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
	"fmt"
	"strconv"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// translateQueryToOp creates an operator tree that represents the input SELECT or UNION query
func translateQueryToOp(ctx *plancontext.PlanningContext, selStmt sqlparser.Statement) (op ops.Operator, err error) {
	switch node := selStmt.(type) {
	case *sqlparser.Select:
		op, err = createOperatorFromSelect(ctx, node)
	case *sqlparser.Union:
		op, err = createOperatorFromUnion(ctx, node)
	case *sqlparser.Update:
		op, err = createOperatorFromUpdate(ctx, node)
	case *sqlparser.Delete:
		op, err = createOperatorFromDelete(ctx, node)
	case *sqlparser.Insert:
		op, err = createOperatorFromInsert(ctx, node)
	default:
		err = vterrors.VT12001(fmt.Sprintf("operator: %T", selStmt))
	}
	if err != nil {
		return nil, err
	}

	return op, nil
}

func createOperatorFromSelect(ctx *plancontext.PlanningContext, sel *sqlparser.Select) (ops.Operator, error) {
	subq, err := createSubqueryFromStatement(ctx, sel)
	if err != nil {
		return nil, err
	}
	op, err := crossJoin(ctx, sel.From)
	if err != nil {
		return nil, err
	}
	if sel.Where != nil {
		exprs := sqlparser.SplitAndExpression(nil, sel.Where.Expr)
		for _, expr := range exprs {
			sqlparser.RemoveKeyspaceFromColName(expr)
			op, err = op.AddPredicate(ctx, expr)
			if err != nil {
				return nil, err
			}
			addColumnEquality(ctx, expr)
		}
	}

	if subq != nil {
		subq.Outer = op
		op = subq
	}

	return &Horizon{
		Source: op,
		Query:  sel,
	}, nil
}

func createOperatorFromUnion(ctx *plancontext.PlanningContext, node *sqlparser.Union) (ops.Operator, error) {
	opLHS, err := translateQueryToOp(ctx, node.Left)
	if err != nil {
		return nil, err
	}

	_, isRHSUnion := node.Right.(*sqlparser.Union)
	if isRHSUnion {
		return nil, vterrors.VT12001("nesting of UNIONs on the right-hand side")
	}
	opRHS, err := translateQueryToOp(ctx, node.Right)
	if err != nil {
		return nil, err
	}

	union := &Union{
		Distinct: node.Distinct,
		Sources:  []ops.Operator{opLHS, opRHS},
	}
	return &Horizon{Source: union, Query: node}, nil
}

func createOperatorFromUpdate(ctx *plancontext.PlanningContext, updStmt *sqlparser.Update) (ops.Operator, error) {
	tableInfo, qt, err := createQueryTableForDML(ctx, updStmt.TableExprs[0], updStmt.Where)
	if err != nil {
		return nil, err
	}

	assignments := make(map[string]sqlparser.Expr)
	for _, set := range updStmt.Exprs {
		assignments[set.Name.Name.String()] = set.Expr
	}

	vindexTable, routing, err := buildVindexTableForDML(ctx, tableInfo, qt, "update")
	if err != nil {
		return nil, err
	}

	vp, cvv, ovq, err := getUpdateVindexInformation(updStmt, vindexTable, qt.ID, qt.Predicates)
	if err != nil {
		return nil, err
	}

	tr, ok := routing.(*ShardedRouting)
	if ok {
		tr.VindexPreds = vp
	}

	for _, predicate := range qt.Predicates {
		var err error
		routing, err = UpdateRoutingLogic(ctx, predicate, routing)
		if err != nil {
			return nil, err
		}
	}

	if routing.OpCode() == engine.Scatter && updStmt.Limit != nil {
		// TODO systay: we should probably check for other op code types - IN could also hit multiple shards (2022-04-07)
		return nil, vterrors.VT12001("multi shard UPDATE with LIMIT")
	}

	r := &Route{
		Source: &Update{
			QTable:              qt,
			VTable:              vindexTable,
			Assignments:         assignments,
			ChangedVindexValues: cvv,
			OwnedVindexQuery:    ovq,
			AST:                 updStmt,
		},
		Routing: routing,
	}

	subq, err := createSubqueryFromStatement(ctx, updStmt)
	if err != nil {
		return nil, err
	}
	if subq == nil {
		return r, nil
	}
	subq.Outer = r
	return subq, nil
}

func createOperatorFromDelete(ctx *plancontext.PlanningContext, deleteStmt *sqlparser.Delete) (ops.Operator, error) {
	tableInfo, qt, err := createQueryTableForDML(ctx, deleteStmt.TableExprs[0], deleteStmt.Where)
	if err != nil {
		return nil, err
	}

	vindexTable, routing, err := buildVindexTableForDML(ctx, tableInfo, qt, "delete")
	if err != nil {
		return nil, err
	}

	del := &Delete{
		QTable: qt,
		VTable: vindexTable,
		AST:    deleteStmt,
	}
	route := &Route{
		Source:  del,
		Routing: routing,
	}

	if !vindexTable.Keyspace.Sharded {
		return route, nil
	}

	primaryVindex, vindexAndPredicates, err := getVindexInformation(qt.ID, qt.Predicates, vindexTable)
	if err != nil {
		return nil, err
	}

	tr, ok := routing.(*ShardedRouting)
	if ok {
		tr.VindexPreds = vindexAndPredicates
	}

	var ovq string
	if len(vindexTable.Owned) > 0 {
		tblExpr := &sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: vindexTable.Name}, As: qt.Alias.As}
		ovq = generateOwnedVindexQuery(tblExpr, deleteStmt, vindexTable, primaryVindex.Columns)
	}

	del.OwnedVindexQuery = ovq

	for _, predicate := range qt.Predicates {
		var err error
		route.Routing, err = UpdateRoutingLogic(ctx, predicate, route.Routing)
		if err != nil {
			return nil, err
		}
	}

	if routing.OpCode() == engine.Scatter && deleteStmt.Limit != nil {
		// TODO systay: we should probably check for other op code types - IN could also hit multiple shards (2022-04-07)
		return nil, vterrors.VT12001("multi shard DELETE with LIMIT")
	}

	subq, err := createSubqueryFromStatement(ctx, deleteStmt)
	if err != nil {
		return nil, err
	}
	if subq == nil {
		return route, nil
	}
	subq.Outer = route
	return subq, nil
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

	if _, target := routing.(*TargetedRouting); target {
		return nil, vterrors.VT12001("INSERT with a target destination")
	}

	insOp := &Insert{
		VTable: vindexTable,
		AST:    ins,
	}
	route := &Route{
		Source:  insOp,
		Routing: routing,
	}

	// Table column list is nil then add all the columns
	// If the column list is empty then add only the auto-inc column and
	// this happens on calling modifyForAutoinc
	if ins.Columns == nil && valuesProvided(ins.Rows) {
		if vindexTable.ColumnListAuthoritative {
			ins = populateInsertColumnlist(ins, vindexTable)
		} else {
			return nil, vterrors.VT09004()
		}
	}

	// modify column list or values for autoincrement column.
	autoIncGen, err := modifyForAutoinc(ins, vindexTable)
	if err != nil {
		return nil, err
	}
	insOp.AutoIncrement = autoIncGen

	// set insert ignore.
	insOp.Ignore = bool(ins.Ignore) || ins.OnDup != nil

	insOp.ColVindexes = getColVindexes(insOp)
	switch rows := ins.Rows.(type) {
	case sqlparser.Values:
		route.Source, err = insertRowsPlan(insOp, ins, rows)
		if err != nil {
			return nil, err
		}
	case sqlparser.SelectStatement:
		route.Source, err = insertSelectPlan(ctx, insOp, ins, rows)
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

func getOperatorFromTableExpr(ctx *plancontext.PlanningContext, tableExpr sqlparser.TableExpr, onlyTable bool) (ops.Operator, error) {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		return getOperatorFromAliasedTableExpr(ctx, tableExpr, onlyTable)
	case *sqlparser.JoinTableExpr:
		return getOperatorFromJoinTableExpr(ctx, tableExpr)
	case *sqlparser.ParenTableExpr:
		return crossJoin(ctx, tableExpr.Exprs)
	default:
		return nil, vterrors.VT13001(fmt.Sprintf("unable to use: %T table type", tableExpr))
	}
}

func getOperatorFromJoinTableExpr(ctx *plancontext.PlanningContext, tableExpr *sqlparser.JoinTableExpr) (ops.Operator, error) {
	lhs, err := getOperatorFromTableExpr(ctx, tableExpr.LeftExpr, false)
	if err != nil {
		return nil, err
	}
	rhs, err := getOperatorFromTableExpr(ctx, tableExpr.RightExpr, false)
	if err != nil {
		return nil, err
	}

	switch tableExpr.Join {
	case sqlparser.NormalJoinType:
		return createInnerJoin(ctx, tableExpr, lhs, rhs)
	case sqlparser.LeftJoinType, sqlparser.RightJoinType:
		return createOuterJoin(tableExpr, lhs, rhs)
	default:
		return nil, vterrors.VT13001("unsupported: %s", tableExpr.Join.ToString())
	}
}

func getOperatorFromAliasedTableExpr(ctx *plancontext.PlanningContext, tableExpr *sqlparser.AliasedTableExpr, onlyTable bool) (ops.Operator, error) {
	tableID := ctx.SemTable.TableSetFor(tableExpr)
	switch tbl := tableExpr.Expr.(type) {
	case sqlparser.TableName:
		tableInfo, err := ctx.SemTable.TableInfoFor(tableID)
		if err != nil {
			return nil, err
		}

		if vt, isVindex := tableInfo.(*semantics.VindexTable); isVindex {
			solves := tableID
			return &Vindex{
				Table: VindexTable{
					TableID: tableID,
					Alias:   tableExpr,
					Table:   tbl,
					VTable:  vt.Table.GetVindexTable(),
				},
				Vindex: vt.Vindex,
				Solved: solves,
			}, nil
		}
		qg := newQueryGraph()
		isInfSchema := tableInfo.IsInfSchema()
		qt := &QueryTable{Alias: tableExpr, Table: tbl, ID: tableID, IsInfSchema: isInfSchema}
		qg.Tables = append(qg.Tables, qt)
		return qg, nil
	case *sqlparser.DerivedTable:
		inner, err := translateQueryToOp(ctx, tbl.Select)
		if err != nil {
			return nil, err
		}
		if horizon, ok := inner.(*Horizon); ok {
			inner = horizon.Source
		}

		stmt := sqlparser.CloneSelectStatement(tbl.Select)
		if onlyTable && stmt.GetLimit() == nil {
			stmt.SetOrderBy(nil)
		}
		qp, err := CreateQPFromSelectStatement(ctx, stmt)
		if err != nil {
			return nil, err
		}

		return &Horizon{
			TableId:       &tableID,
			Alias:         tableExpr.As.String(),
			Source:        inner,
			Query:         stmt,
			ColumnAliases: tableExpr.Columns,
			QP:            qp,
		}, nil
	default:
		return nil, vterrors.VT13001(fmt.Sprintf("unable to use: %T", tbl))
	}
}

func crossJoin(ctx *plancontext.PlanningContext, exprs sqlparser.TableExprs) (ops.Operator, error) {
	var output ops.Operator
	for _, tableExpr := range exprs {
		op, err := getOperatorFromTableExpr(ctx, tableExpr, len(exprs) == 1)
		if err != nil {
			return nil, err
		}
		if output == nil {
			output = op
		} else {
			output = createJoin(ctx, output, op)
		}
	}
	return output, nil
}

func createQueryTableForDML(ctx *plancontext.PlanningContext, tableExpr sqlparser.TableExpr, whereClause *sqlparser.Where) (semantics.TableInfo, *QueryTable, error) {
	alTbl, ok := tableExpr.(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, nil, vterrors.VT13001("expected AliasedTableExpr")
	}
	tblName, ok := alTbl.Expr.(sqlparser.TableName)
	if !ok {
		return nil, nil, vterrors.VT13001("expected TableName")
	}

	tableID := ctx.SemTable.TableSetFor(alTbl)
	tableInfo, err := ctx.SemTable.TableInfoFor(tableID)
	if err != nil {
		return nil, nil, err
	}

	if tableInfo.IsInfSchema() {
		return nil, nil, vterrors.VT12001("update information schema tables")
	}

	var predicates []sqlparser.Expr
	if whereClause != nil {
		predicates = sqlparser.SplitAndExpression(nil, whereClause.Expr)
	}
	qt := &QueryTable{
		ID:         tableID,
		Alias:      alTbl,
		Table:      tblName,
		Predicates: predicates,
	}
	return tableInfo, qt, nil
}

func addColumnEquality(ctx *plancontext.PlanningContext, expr sqlparser.Expr) {
	switch expr := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if expr.Operator != sqlparser.EqualOp {
			return
		}

		if left, isCol := expr.Left.(*sqlparser.ColName); isCol {
			ctx.SemTable.AddColumnEquality(left, expr.Right)
		}
		if right, isCol := expr.Right.(*sqlparser.ColName); isCol {
			ctx.SemTable.AddColumnEquality(right, expr.Left)
		}
	}
}
