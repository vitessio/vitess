// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package planbuilder

import (
	"errors"
	"fmt"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"
)

func analyzeUpdate(upd *sqlparser.Update, tables map[string]*schema.Table) (plan *Plan, err error) {
	plan = &Plan{
		PlanID:    PlanPassDML,
		FullQuery: GenerateFullQuery(upd),
	}

	tableName := sqlparser.GetTableName(upd.Table.Expr)
	if tableName.IsEmpty() {
		plan.Reason = ReasonTable
		return plan, nil
	}
	table, err := plan.setTable(tableName, tables)
	if err != nil {
		return nil, err
	}

	// Store the WHERE clause as string for the hot row protection (txserializer).
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", upd.Where)
	plan.WhereClause = buf.ParsedQuery()

	if !table.HasPrimary() {
		log.Warningf("no primary key for table %s", tableName)
		plan.Reason = ReasonTableNoIndex
		return plan, nil
	}

	plan.SecondaryPKValues, err = analyzeUpdateExpressions(upd.Exprs, table.Indexes[0])
	if err != nil {
		if err == ErrTooComplex {
			plan.Reason = ReasonPKChange
			return plan, nil
		}
		return nil, err
	}

	plan.OuterQuery = GenerateUpdateOuterQuery(upd)

	if pkValues := analyzeWhere(upd.Where, table.Indexes[0]); pkValues != nil {
		plan.PlanID = PlanDMLPK
		plan.PKValues = pkValues
		return plan, nil
	}

	plan.PlanID = PlanDMLSubquery
	plan.Subquery = GenerateUpdateSubquery(upd, table)
	return plan, nil
}

func analyzeDelete(del *sqlparser.Delete, tables map[string]*schema.Table) (plan *Plan, err error) {
	plan = &Plan{
		PlanID:    PlanPassDML,
		FullQuery: GenerateFullQuery(del),
	}

	tableName := sqlparser.GetTableName(del.Table)
	if tableName.IsEmpty() {
		plan.Reason = ReasonTable
		return plan, nil
	}
	table, err := plan.setTable(tableName, tables)
	if err != nil {
		return nil, err
	}

	// Store the WHERE clause as string for the hot row protection (txserializer).
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", del.Where)
	plan.WhereClause = buf.ParsedQuery()

	if !table.HasPrimary() {
		log.Warningf("no primary key for table %s", tableName)
		plan.Reason = ReasonTableNoIndex
		return plan, nil
	}

	plan.OuterQuery = GenerateDeleteOuterQuery(del)

	if pkValues := analyzeWhere(del.Where, table.Indexes[0]); pkValues != nil {
		plan.PlanID = PlanDMLPK
		plan.PKValues = pkValues
		return plan, nil
	}

	plan.PlanID = PlanDMLSubquery
	plan.Subquery = GenerateDeleteSubquery(del, table)
	return plan, nil
}

func analyzeSet(set *sqlparser.Set) (plan *Plan) {
	return &Plan{
		PlanID:    PlanSet,
		FullQuery: GenerateFullQuery(set),
	}
}

func analyzeUpdateExpressions(exprs sqlparser.UpdateExprs, pkIndex *schema.Index) (pkValues []interface{}, err error) {
	for _, expr := range exprs {
		index := pkIndex.FindColumn(expr.Name.Name)
		if index == -1 {
			continue
		}
		if !sqlparser.IsValue(expr.Expr) {
			return nil, ErrTooComplex
		}
		if pkValues == nil {
			pkValues = make([]interface{}, len(pkIndex.Columns))
		}
		var err error
		pkValues[index], err = sqlparser.AsInterface(expr.Expr)
		if err != nil {
			return nil, err
		}
	}
	return pkValues, nil
}

func analyzeSelect(sel *sqlparser.Select, tables map[string]*schema.Table) (plan *Plan, err error) {
	plan = &Plan{
		PlanID:     PlanPassSelect,
		FieldQuery: GenerateFieldQuery(sel),
		FullQuery:  GenerateLimitQuery(sel),
	}
	if sel.Lock != "" {
		plan.PlanID = PlanSelectLock
	}

	tableName := analyzeFrom(sel.From)
	if tableName.IsEmpty() {
		return plan, nil
	}
	table, err := plan.setTable(tableName, tables)
	if err != nil {
		return nil, err
	}

	// Check if it's a NEXT VALUE statement.
	if nextVal, ok := sel.SelectExprs[0].(sqlparser.Nextval); ok {
		if table.Type != schema.Sequence {
			return nil, fmt.Errorf("%s is not a sequence", tableName)
		}
		plan.PlanID = PlanNextval
		v, err := sqlparser.AsInterface(nextVal.Expr)
		if err != nil {
			return nil, err
		}
		plan.PKValues = []interface{}{v}
		plan.FieldQuery = nil
		plan.FullQuery = nil
	}
	return plan, nil
}

func analyzeFrom(tableExprs sqlparser.TableExprs) sqlparser.TableIdent {
	if len(tableExprs) > 1 {
		return sqlparser.NewTableIdent("")
	}
	node, ok := tableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return sqlparser.NewTableIdent("")
	}
	return sqlparser.GetTableName(node.Expr)
}

func analyzeWhere(node *sqlparser.Where, pkIndex *schema.Index) []interface{} {
	if node == nil {
		return nil
	}
	conditions := analyzeBoolean(node.Expr)
	if conditions == nil {
		return nil
	}
	return getPKValues(conditions, pkIndex)
}

func analyzeBoolean(node sqlparser.Expr) (conditions []*sqlparser.ComparisonExpr) {
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		left := analyzeBoolean(node.Left)
		right := analyzeBoolean(node.Right)
		if left == nil || right == nil {
			return nil
		}
		return append(left, right...)
	case *sqlparser.ParenExpr:
		return analyzeBoolean(node.Expr)
	case *sqlparser.ComparisonExpr:
		switch {
		case sqlparser.StringIn(
			node.Operator,
			sqlparser.EqualStr,
			sqlparser.LikeStr):
			if sqlparser.IsColName(node.Left) && sqlparser.IsValue(node.Right) {
				return []*sqlparser.ComparisonExpr{node}
			}
		case node.Operator == sqlparser.InStr:
			if sqlparser.IsColName(node.Left) && sqlparser.IsSimpleTuple(node.Right) {
				return []*sqlparser.ComparisonExpr{node}
			}
		}
	}
	return nil
}

func getPKValues(conditions []*sqlparser.ComparisonExpr, pkIndex *schema.Index) []interface{} {
	pkValues := make([]interface{}, len(pkIndex.Columns))
	inClauseSeen := false
	for _, condition := range conditions {
		if condition.Operator == sqlparser.InStr {
			if inClauseSeen {
				return nil
			}
			inClauseSeen = true
		}
		index := pkIndex.FindColumn(condition.Left.(*sqlparser.ColName).Name)
		if index == -1 {
			return nil
		}
		if pkValues[index] != nil {
			return nil
		}
		var err error
		pkValues[index], err = sqlparser.AsInterface(condition.Right)
		if err != nil {
			return nil
		}
	}
	for _, v := range pkValues {
		if v == nil {
			return nil
		}
	}
	return pkValues
}

func analyzeInsert(ins *sqlparser.Insert, tables map[string]*schema.Table) (plan *Plan, err error) {
	plan = &Plan{
		PlanID:    PlanPassDML,
		FullQuery: GenerateFullQuery(ins),
	}
	tableName := sqlparser.GetTableName(ins.Table)
	if tableName.IsEmpty() {
		plan.Reason = ReasonTable
		return plan, nil
	}
	table, err := plan.setTable(tableName, tables)
	if err != nil {
		return nil, err
	}

	if !table.HasPrimary() {
		log.Warningf("no primary key for table %s", tableName)
		plan.Reason = ReasonTableNoIndex
		return plan, nil
	}
	switch table.Type {
	case schema.NoType, schema.Sequence:
		// For now, allow sequence inserts.
		return analyzeInsertNoType(ins, plan, table)
	case schema.Message:
		return analyzeInsertMessage(ins, plan, table)
	}
	panic("unreachable")
}

func analyzeInsertNoType(ins *sqlparser.Insert, plan *Plan, table *schema.Table) (*Plan, error) {
	pkColumnNumbers := getInsertPKColumns(ins.Columns, table)

	if sel, ok := ins.Rows.(sqlparser.SelectStatement); ok {
		if ins.OnDup != nil {
			// Upserts not allowed for subqueries.
			// http://bugs.mysql.com/bug.php?id=58637
			plan.Reason = ReasonUpsert
			return plan, nil
		}
		plan.PlanID = PlanInsertSubquery
		plan.OuterQuery = GenerateInsertOuterQuery(ins)
		plan.Subquery = GenerateLimitQuery(sel)
		if len(ins.Columns) != 0 {
			for _, col := range ins.Columns {
				colIndex := table.FindColumn(col)
				if colIndex == -1 {
					return nil, fmt.Errorf("column %v not found in table %s", col, table.Name)
				}
				plan.ColumnNumbers = append(plan.ColumnNumbers, colIndex)
			}
		} else {
			// Add all columns.
			for colIndex := range table.Columns {
				plan.ColumnNumbers = append(plan.ColumnNumbers, colIndex)
			}
		}
		plan.SubqueryPKColumns = pkColumnNumbers
		return plan, nil
	}

	// If it's not a sqlparser.SelectStatement, it's Values.
	rowList := ins.Rows.(sqlparser.Values)
	pkValues, err := getInsertPKValues(pkColumnNumbers, rowList, table)
	if err != nil {
		return nil, err
	}
	if pkValues == nil {
		plan.Reason = ReasonComplexExpr
		return plan, nil
	}
	plan.PKValues = pkValues
	if ins.OnDup == nil {
		plan.PlanID = PlanInsertPK
		plan.OuterQuery = sqlparser.GenerateParsedQuery(ins)
		return plan, nil
	}
	if len(rowList) > 1 {
		// Upsert supported only for single row inserts.
		plan.Reason = ReasonUpsert
		return plan, nil
	}
	updateExprs, err := resolveUpsertUpdateValues(rowList[0], ins.Columns, ins.OnDup)
	if err != nil {
		plan.Reason = ReasonUpsertColMismatch
		return plan, nil
	}
	plan.SecondaryPKValues, err = analyzeUpdateExpressions(updateExprs, table.Indexes[0])
	if err != nil {
		plan.Reason = ReasonPKChange
		return plan, nil
	}
	plan.PlanID = PlanUpsertPK
	newins := *ins
	newins.Ignore = ""
	newins.OnDup = nil
	plan.OuterQuery = sqlparser.GenerateParsedQuery(&newins)
	upd := &sqlparser.Update{
		Comments: ins.Comments,
		Table:    &sqlparser.AliasedTableExpr{Expr: ins.Table},
		Exprs:    updateExprs,
	}
	plan.UpsertQuery = GenerateUpdateOuterQuery(upd)
	return plan, nil
}

// resolveUpsertUpdateValues walks the UpdateExprs tree for an upsert, replacing
// any VALUES(foo) expressions with the correct value from the rowList, in this
// example the value for column 'foo'
func resolveUpsertUpdateValues(rowList sqlparser.ValTuple, columns sqlparser.Columns, dup sqlparser.OnDup) (sqlparser.UpdateExprs, error) {
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		if cast, ok := node.(*sqlparser.ValuesFuncExpr); ok {
			colID := columns.FindColumn(cast.Name)
			if colID == -1 {
				return false, fmt.Errorf("Could not find column %v", cast.Name)
			}
			cast.Resolved = rowList[colID]
		}
		return true, nil
	}, dup)
	return sqlparser.UpdateExprs(dup), err
}

func analyzeInsertMessage(ins *sqlparser.Insert, plan *Plan, table *schema.Table) (*Plan, error) {
	if _, ok := ins.Rows.(sqlparser.SelectStatement); ok {
		return nil, fmt.Errorf("subquery not allowed for message table: %s", table.Name.String())
	}
	if ins.OnDup != nil {
		return nil, fmt.Errorf("'on duplicate key' construct not allowed for message table: %s", table.Name.String())
	}
	if len(ins.Columns) == 0 {
		return nil, fmt.Errorf("column list must be specified for message table insert: %s", table.Name.String())
	}

	// Sanity check first so we don't have to repeat this.
	rowList := ins.Rows.(sqlparser.Values)
	for _, row := range rowList {
		if len(row) != len(ins.Columns) {
			return nil, errors.New("column count doesn't match value count")
		}
	}

	// Perform message specific processing first, because we may be
	// adding values that address the primary key.
	timeNow := sqlparser.NewValArg([]byte(":#time_now"))

	col := sqlparser.NewColIdent("time_scheduled")
	scheduleIndex := findCol(col, ins.Columns)
	if scheduleIndex == -1 {
		scheduleIndex = addVal(ins, col, timeNow)
	}

	// time_next should be the same as time_scheduled.
	col = sqlparser.NewColIdent("time_next")
	num := findCol(col, ins.Columns)
	if num != -1 {
		return nil, fmt.Errorf("%s must not be specified for message insert", col.String())
	}
	_ = copyVal(ins, col, scheduleIndex)

	// time_created should always be now.
	col = sqlparser.NewColIdent("time_created")
	if num := findCol(col, ins.Columns); num >= 0 {
		return nil, fmt.Errorf("%s must not be specified for message insert", col.String())
	}
	_ = addVal(ins, col, timeNow)

	// epoch should always be 0.
	col = sqlparser.NewColIdent("epoch")
	if num := findCol(col, ins.Columns); num >= 0 {
		return nil, fmt.Errorf("%s must not be specified for message insert", col.String())
	}
	_ = addVal(ins, col, sqlparser.NewIntVal([]byte("0")))

	// time_acked should must not be specified.
	col = sqlparser.NewColIdent("time_acked")
	if num := findCol(col, ins.Columns); num >= 0 {
		return nil, fmt.Errorf("%s must not be specified for message insert", col.String())
	}

	col = sqlparser.NewColIdent("id")
	num = findCol(col, ins.Columns)
	if num < 0 {
		return nil, fmt.Errorf("%s must be specified for message insert", col.String())
	}

	col = sqlparser.NewColIdent("message")
	num = findCol(col, ins.Columns)
	if num < 0 {
		return nil, fmt.Errorf("%s must be specified for message insert", col.String())
	}

	pkColumnNumbers := getInsertPKColumns(ins.Columns, table)
	pkValues, err := getInsertPKValues(pkColumnNumbers, rowList, table)
	if err != nil {
		// Dead code. We've already sanity checked the row lengths.
		return nil, err
	}
	if pkValues == nil {
		// Dead code. The previous checks already catch this condition.
		plan.Reason = ReasonComplexExpr
		return plan, nil
	}
	plan.PKValues = pkValues
	plan.PlanID = PlanInsertMessage
	plan.OuterQuery = sqlparser.GenerateParsedQuery(ins)
	plan.MessageReloaderQuery = GenerateLoadMessagesQuery(ins)
	return plan, nil
}

func getInsertPKColumns(columns sqlparser.Columns, table *schema.Table) (pkColumnNumbers []int) {
	if len(columns) == 0 {
		return table.PKColumns
	}
	pkIndex := table.Indexes[0]
	pkColumnNumbers = make([]int, len(pkIndex.Columns))
	for i := range pkColumnNumbers {
		pkColumnNumbers[i] = -1
	}
	for i, column := range columns {
		index := pkIndex.FindColumn(column)
		if index == -1 {
			continue
		}
		pkColumnNumbers[index] = i
	}
	return pkColumnNumbers
}

func findCol(col sqlparser.ColIdent, cols sqlparser.Columns) int {
	for i, insCol := range cols {
		if insCol.Equal(col) {
			return i
		}
	}
	return -1
}

func addVal(ins *sqlparser.Insert, col sqlparser.ColIdent, expr sqlparser.Expr) int {
	ins.Columns = append(ins.Columns, col)
	rows := ins.Rows.(sqlparser.Values)
	for i := range rows {
		rows[i] = append(rows[i], expr)
	}
	return len(ins.Columns) - 1
}

func copyVal(ins *sqlparser.Insert, col sqlparser.ColIdent, colIndex int) int {
	ins.Columns = append(ins.Columns, col)
	rows := ins.Rows.(sqlparser.Values)
	for i := range rows {
		rows[i] = append(rows[i], rows[i][colIndex])
	}
	return len(ins.Columns) - 1
}

func getInsertPKValues(pkColumnNumbers []int, rowList sqlparser.Values, table *schema.Table) (pkValues []interface{}, err error) {
	pkValues = make([]interface{}, len(pkColumnNumbers))
	for index, columnNumber := range pkColumnNumbers {
		if columnNumber == -1 {
			pkValues[index] = table.GetPKColumn(index).Default
			continue
		}
		values := make([]interface{}, len(rowList))
		for j := 0; j < len(rowList); j++ {
			row := rowList[j]
			if columnNumber >= len(row) {
				return nil, errors.New("column count doesn't match value count")
			}
			node := row[columnNumber]
			if !sqlparser.IsNull(node) && !sqlparser.IsValue(node) {
				return nil, nil
			}
			var err error
			values[j], err = sqlparser.AsInterface(node)
			if err != nil {
				return nil, err
			}
		}
		if len(values) == 1 {
			pkValues[index] = values[0]
		} else {
			pkValues[index] = values
		}
	}
	return pkValues, nil
}
