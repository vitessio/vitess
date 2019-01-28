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
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func analyzeUpdate(upd *sqlparser.Update, tables map[string]*schema.Table) (plan *Plan, err error) {
	plan = &Plan{
		PlanID:    PlanPassDML,
		FullQuery: GenerateFullQuery(upd),
	}

	if len(upd.TableExprs) > 1 {
		plan.Reason = ReasonMultiTable
		return plan, nil
	}

	aliased, ok := upd.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		plan.Reason = ReasonMultiTable
		return plan, nil
	}

	tableName := sqlparser.GetTableName(aliased.Expr)
	if tableName.IsEmpty() {
		plan.Reason = ReasonTable
		return plan, nil
	}
	table, tableErr := plan.setTable(tableName, tables)

	// In passthrough dml mode, allow the operation even if the
	// table is unknown in the schema.
	if PassthroughDMLs {
		return plan, nil
	}

	if tableErr != nil {
		return nil, tableErr
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

	plan.OuterQuery = GenerateUpdateOuterQuery(upd, aliased, nil)

	if pkValues := analyzeWhere(upd.Where, table.Indexes[0]); pkValues != nil {
		// Also, there should be no limit clause.
		if upd.Limit == nil {
			plan.PlanID = PlanDMLPK
			plan.PKValues = pkValues
			return plan, nil
		}
	}

	plan.PlanID = PlanDMLSubquery
	plan.Subquery = GenerateUpdateSubquery(upd, table, aliased)
	return plan, nil
}

func analyzeDelete(del *sqlparser.Delete, tables map[string]*schema.Table) (plan *Plan, err error) {
	plan = &Plan{
		PlanID:    PlanPassDML,
		FullQuery: GenerateFullQuery(del),
	}

	if len(del.TableExprs) > 1 {
		plan.Reason = ReasonMultiTable
		return plan, nil
	}
	aliased, ok := del.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		plan.Reason = ReasonMultiTable
		return plan, nil
	}
	tableName := sqlparser.GetTableName(aliased.Expr)
	if tableName.IsEmpty() {
		plan.Reason = ReasonTable
		return plan, nil
	}
	table, tableErr := plan.setTable(tableName, tables)

	// In passthrough dml mode, allow the operation even if the
	// table is unknown in the schema.
	if PassthroughDMLs {
		return plan, nil
	}

	if tableErr != nil {
		return nil, tableErr
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

	plan.OuterQuery = GenerateDeleteOuterQuery(del, aliased)

	if pkValues := analyzeWhere(del.Where, table.Indexes[0]); pkValues != nil {
		// Also, there should be no limit clause.
		if del.Limit == nil {
			plan.PlanID = PlanDMLPK
			plan.PKValues = pkValues
			return plan, nil
		}
	}

	plan.PlanID = PlanDMLSubquery
	plan.Subquery = GenerateDeleteSubquery(del, table, aliased)
	return plan, nil
}

func analyzeSet(set *sqlparser.Set) (plan *Plan) {
	return &Plan{
		PlanID:    PlanSet,
		FullQuery: GenerateFullQuery(set),
	}
}

func analyzeUpdateExpressions(exprs sqlparser.UpdateExprs, pkIndex *schema.Index) (pkValues []sqltypes.PlanValue, err error) {
	for _, expr := range exprs {
		index := pkIndex.FindColumn(expr.Name.Name)
		if index == -1 {
			continue
		}
		if !sqlparser.IsValue(expr.Expr) {
			return nil, ErrTooComplex
		}
		if pkValues == nil {
			pkValues = make([]sqltypes.PlanValue, len(pkIndex.Columns))
		}
		var err error
		pkValues[index], err = sqlparser.NewPlanValue(expr.Expr)
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

	if sel.Where != nil {
		comp, ok := sel.Where.Expr.(*sqlparser.ComparisonExpr)
		if ok && comp.IsImpossible() {
			plan.PlanID = PlanSelectImpossible
			return plan, nil
		}
	}

	// Check if it's a NEXT VALUE statement.
	if nextVal, ok := sel.SelectExprs[0].(sqlparser.Nextval); ok {
		if table.Type != schema.Sequence {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%s is not a sequence", tableName)
		}
		plan.PlanID = PlanNextval
		v, err := sqlparser.NewPlanValue(nextVal.Expr)
		if err != nil {
			return nil, err
		}
		plan.PKValues = []sqltypes.PlanValue{v}
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

func analyzeWhere(node *sqlparser.Where, pkIndex *schema.Index) []sqltypes.PlanValue {
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
		case node.Operator == sqlparser.EqualStr:
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

func getPKValues(conditions []*sqlparser.ComparisonExpr, pkIndex *schema.Index) []sqltypes.PlanValue {
	pkValues := make([]sqltypes.PlanValue, len(pkIndex.Columns))
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
		if !pkValues[index].IsNull() {
			return nil
		}
		var err error
		pkValues[index], err = sqlparser.NewPlanValue(condition.Right)
		if err != nil {
			return nil
		}
	}
	for _, v := range pkValues {
		if v.IsNull() {
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

	if ins.Action == sqlparser.ReplaceStr {
		plan.Reason = ReasonReplace
		return plan, nil
	}
	tableName := sqlparser.GetTableName(ins.Table)
	if tableName.IsEmpty() {
		plan.Reason = ReasonTable
		return plan, nil
	}
	table, tableErr := plan.setTable(tableName, tables)

	// In passthrough dml mode, allow the operation even if the
	// table is unknown in the schema.
	if PassthroughDMLs {
		return plan, nil
	}

	if tableErr != nil {
		return nil, tableErr
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
	// Populate column list from schema if it wasn't specified.
	if len(ins.Columns) == 0 {
		for _, col := range table.Columns {
			ins.Columns = append(ins.Columns, col.Name)
		}
	}
	pkColumnNumbers := getInsertPKColumns(ins.Columns, table)

	if sel, ok := ins.Rows.(sqlparser.SelectStatement); ok {
		if ins.OnDup != nil {
			// Upserts not allowed for subqueries.
			// http://bugs.mysql.com/bug.php?id=58637
			plan.Reason = ReasonUpsertSubquery
			return plan, nil
		}
		plan.PlanID = PlanInsertSubquery
		plan.OuterQuery = GenerateInsertOuterQuery(ins)
		plan.Subquery = GenerateLimitQuery(sel)
		for _, col := range ins.Columns {
			colIndex := table.FindColumn(col)
			if colIndex == -1 {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "column %v not found in table %s", col, table.Name)
			}
			plan.ColumnNumbers = append(plan.ColumnNumbers, colIndex)
		}
		plan.SubqueryPKColumns = pkColumnNumbers
		return plan, nil
	}

	// If it's not a sqlparser.SelectStatement, it's Values.
	rowList := ins.Rows.(sqlparser.Values)
	for i := range rowList {
		if len(rowList[i]) == 0 {
			for _, col := range table.Columns {
				expr, err := sqlparser.ExprFromValue(col.Default)
				if err != nil {
					return nil, vterrors.Wrap(err, "could not create default row for insert without row values")
				}
				rowList[i] = append(rowList[i], expr)
			}
			continue
		}
		if len(rowList[i]) != len(ins.Columns) {
			return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "column count doesn't match value count")
		}
	}
	plan.PKValues = getInsertPKValues(pkColumnNumbers, rowList, table)
	if plan.PKValues == nil {
		plan.Reason = ReasonComplexExpr
		return plan, nil
	}

	if ins.OnDup == nil {
		plan.PlanID = PlanInsertPK
		plan.OuterQuery = sqlparser.NewParsedQuery(ins)
		return plan, nil
	}

	// Compute secondary pk values if OnDup changes them.
	var ok bool
	plan.SecondaryPKValues, ok = analyzeOnDupExpressions(ins, table.Indexes[0])
	if !ok {
		plan.Reason = ReasonPKChange
		return plan, nil
	}

	// If the table only has one unique key then it is safe to pass through
	// a simple upsert unmodified even if there are multiple rows in the
	// statement. The action is same as a regular insert except that we
	// may have to publish possible PK changes by OnDup, which would be
	// recorded in SecondaryPKValues.
	if table.UniqueIndexes() <= 1 {
		plan.PlanID = PlanInsertPK
		plan.OuterQuery = sqlparser.NewParsedQuery(ins)
		return plan, nil
	}

	// Otherwise multiple rows are unsupported
	if len(rowList) > 1 {
		plan.Reason = ReasonUpsertMultiRow
		return plan, nil
	}
	plan.PlanID = PlanUpsertPK
	newins := *ins
	newins.Ignore = ""
	newins.OnDup = nil
	plan.OuterQuery = sqlparser.NewParsedQuery(&newins)
	tableAlias := &sqlparser.AliasedTableExpr{Expr: ins.Table}
	upd := &sqlparser.Update{
		Comments:   ins.Comments,
		TableExprs: sqlparser.TableExprs{tableAlias},
		Exprs:      sqlparser.UpdateExprs(ins.OnDup),
	}

	// We need to replace 'values' expressions with the actual values they reference.
	var formatErr error
	plan.UpsertQuery = GenerateUpdateOuterQuery(upd, tableAlias, func(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
		if node, ok := node.(*sqlparser.ValuesFuncExpr); ok {
			if !node.Name.Qualifier.IsEmpty() && node.Name.Qualifier != ins.Table {
				formatErr = vterrors.Errorf(vtrpcpb.Code_NOT_FOUND,
					"could not find qualified column %v in table %v",
					sqlparser.String(node.Name), sqlparser.String(ins.Table))
				return
			}
			colnum := ins.Columns.FindColumn(node.Name.Name)
			if colnum == -1 {
				formatErr = vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "could not find column %v", node.Name)
				return
			}
			buf.Myprintf("(%v)", rowList[0][colnum])
			return
		}
		node.Format(buf)
	})
	if formatErr != nil {
		return nil, formatErr
	}
	return plan, nil
}

func analyzeInsertMessage(ins *sqlparser.Insert, plan *Plan, table *schema.Table) (*Plan, error) {
	if _, ok := ins.Rows.(sqlparser.SelectStatement); ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "subquery not allowed for message table: %s", table.Name.String())
	}
	if ins.OnDup != nil {
		// only allow 'on duplicate key' where time_scheduled and id are not referenced
		ts := sqlparser.NewColIdent("time_scheduled")
		id := sqlparser.NewColIdent("id")
		for _, updateExpr := range ins.OnDup {
			if updateExpr.Name.Name.Equal(ts) || updateExpr.Name.Name.Equal(id) {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "'on duplicate key' cannot reference time_scheduled or id for message table: %s", table.Name.String())
			}
		}
	}
	if len(ins.Columns) == 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "column list must be specified for message table insert: %s", table.Name.String())
	}

	// Sanity check first so we don't have to repeat this.
	rowList := ins.Rows.(sqlparser.Values)
	for _, row := range rowList {
		if len(row) != len(ins.Columns) {
			return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "column count doesn't match value count")
		}
	}

	// Perform message specific processing first, because we may be
	// adding values that address the primary key.
	timeNow := sqlparser.NewValArg([]byte(":#time_now"))

	col := sqlparser.NewColIdent("time_scheduled")
	scheduleIndex := ins.Columns.FindColumn(col)
	if scheduleIndex == -1 {
		scheduleIndex = addVal(ins, col, timeNow)
	}

	// time_next should be the same as time_scheduled.
	col = sqlparser.NewColIdent("time_next")
	num := ins.Columns.FindColumn(col)
	if num != -1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%s must not be specified for message insert", col.String())
	}
	_ = copyVal(ins, col, scheduleIndex)

	// time_created should always be now.
	col = sqlparser.NewColIdent("time_created")
	if num := ins.Columns.FindColumn(col); num >= 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%s must not be specified for message insert", col.String())
	}
	_ = addVal(ins, col, timeNow)

	// epoch should always be 0.
	col = sqlparser.NewColIdent("epoch")
	if num := ins.Columns.FindColumn(col); num >= 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%s must not be specified for message insert", col.String())
	}
	_ = addVal(ins, col, sqlparser.NewIntVal([]byte("0")))

	// time_acked should must not be specified.
	col = sqlparser.NewColIdent("time_acked")
	if num := ins.Columns.FindColumn(col); num >= 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%s must not be specified for message insert", col.String())
	}

	col = sqlparser.NewColIdent("id")
	num = ins.Columns.FindColumn(col)
	if num < 0 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%s must be specified for message insert", col.String())
	}

	pkColumnNumbers := getInsertPKColumns(ins.Columns, table)
	plan.PKValues = getInsertPKValues(pkColumnNumbers, rowList, table)
	if plan.PKValues == nil {
		// Dead code. The previous checks already catch this condition.
		plan.Reason = ReasonComplexExpr
		return plan, nil
	}
	plan.PlanID = PlanInsertMessage
	plan.OuterQuery = sqlparser.NewParsedQuery(ins)
	return plan, nil
}

func getInsertPKColumns(columns sqlparser.Columns, table *schema.Table) (pkColumnNumbers []int) {
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

func getInsertPKValues(pkColumnNumbers []int, rowList sqlparser.Values, table *schema.Table) []sqltypes.PlanValue {
	pkValues := make([]sqltypes.PlanValue, len(pkColumnNumbers))
	// We iterate by columns (j, i).
	for j, columnNumber := range pkColumnNumbers {
		if columnNumber == -1 {
			// No value was specified. Use the default from the schema for all rows.
			pkValues[j] = sqltypes.PlanValue{Value: table.GetPKColumn(j).Default}
			continue
		}
		var ok bool
		pkValues[j], ok = extractColumnValues(rowList, columnNumber)
		if !ok {
			return nil
		}
	}
	return pkValues
}

// analyzeOnDupExpressions analyzes the OnDup and returns the list for any pk value changes.
func analyzeOnDupExpressions(ins *sqlparser.Insert, pkIndex *schema.Index) (pkValues []sqltypes.PlanValue, ok bool) {
	rowList := ins.Rows.(sqlparser.Values)
	for _, expr := range ins.OnDup {
		index := pkIndex.FindColumn(expr.Name.Name)
		if index == -1 {
			continue
		}

		if pkValues == nil {
			pkValues = make([]sqltypes.PlanValue, len(pkIndex.Columns))
		}
		if vf, ok := expr.Expr.(*sqlparser.ValuesFuncExpr); ok {
			if !vf.Name.Qualifier.IsEmpty() && vf.Name.Qualifier != ins.Table {
				return nil, false
			}
			insertCol := ins.Columns.FindColumn(vf.Name.Name)
			if insertCol == -1 {
				return nil, false
			}
			pkValues[index], ok = extractColumnValues(rowList, insertCol)
			if !ok {
				return nil, false
			}
			continue
		}

		pkValues[index], ok = extractSingleValue(expr.Expr)
		if !ok {
			return nil, false
		}
	}
	return pkValues, true
}

// extractColumnValues extracts the values of a column into a PlanValue.
func extractColumnValues(rowList sqlparser.Values, colnum int) (sqltypes.PlanValue, bool) {
	pv := sqltypes.PlanValue{Values: make([]sqltypes.PlanValue, len(rowList))}
	for i := 0; i < len(rowList); i++ {
		var ok bool
		pv.Values[i], ok = extractSingleValue(rowList[i][colnum])
		if !ok {
			return pv, false
		}
	}
	return pv, true
}

func extractSingleValue(expr sqlparser.Expr) (sqltypes.PlanValue, bool) {
	pv := sqltypes.PlanValue{}
	if !sqlparser.IsNull(expr) && !sqlparser.IsValue(expr) {
		return pv, false
	}
	var err error
	pv, err = sqlparser.NewPlanValue(expr)
	if err != nil {
		return pv, false
	}
	return pv, true
}
