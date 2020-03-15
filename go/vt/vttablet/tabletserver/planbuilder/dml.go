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
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func analyzeUpdate(upd *sqlparser.Update, tables map[string]*schema.Table) (plan *Plan, err error) {
	plan = &Plan{
		PlanID: PlanPassDML,
		Table:  lookupTable(upd.TableExprs, tables),
	}

	// Store the WHERE clause as string for the hot row protection (txserializer).
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", upd.Where)
	plan.WhereClause = buf.ParsedQuery()

	if PassthroughDMLs || upd.Limit != nil {
		plan.FullQuery = GenerateFullQuery(upd)
		return plan, nil
	}

	plan.PlanID = PlanDMLLimit
	upd.Limit = execLimit
	plan.FullQuery = GenerateFullQuery(upd)
	upd.Limit = nil
	return plan, nil
}

func analyzeDelete(del *sqlparser.Delete, tables map[string]*schema.Table) (plan *Plan, err error) {
	plan = &Plan{
		PlanID: PlanPassDML,
		Table:  lookupTable(del.TableExprs, tables),
	}

	// Store the WHERE clause as string for the hot row protection (txserializer).
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", del.Where)
	plan.WhereClause = buf.ParsedQuery()

	if PassthroughDMLs || del.Limit != nil {
		plan.FullQuery = GenerateFullQuery(del)
		return plan, nil
	}
	plan.PlanID = PlanDMLLimit
	del.Limit = execLimit
	plan.FullQuery = GenerateFullQuery(del)
	del.Limit = nil
	return plan, nil
}

func analyzeSet(set *sqlparser.Set) (plan *Plan) {
	return &Plan{
		PlanID:    PlanSet,
		FullQuery: GenerateFullQuery(set),
	}
}

func analyzeSelect(sel *sqlparser.Select, tables map[string]*schema.Table) (plan *Plan, err error) {
	plan = &Plan{
		PlanID:     PlanPassSelect,
		Table:      lookupTable(sel.From, tables),
		FieldQuery: GenerateFieldQuery(sel),
		FullQuery:  GenerateLimitQuery(sel),
	}
	if sel.Lock != "" {
		plan.PlanID = PlanSelectLock
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
		if plan.Table == nil || plan.Table.Type != schema.Sequence {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%s is not a sequence", sqlparser.String(sel.From))
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
	plan.Table = tables[tableName.String()]
	if plan.Table == nil {
		return plan, nil
	}

	switch {
	case plan.Table.Type == schema.Message:
		// message inserts need to continue being strict, even in passthrough dml mode,
		// because field defaults are set here

	case plan.Table.IsTopic():
		plan.PlanID = PlanInsertTopic
		plan.Reason = ReasonTopic
		return plan, nil

	case PassthroughDMLs:
		// In passthrough dml mode, allow the operation even if the
		// table is unknown in the schema.
		return plan, nil
	}

	if !plan.Table.HasPrimary() {
		log.Warningf("no primary key for table %s", tableName)
		plan.Reason = ReasonTableNoIndex
		return plan, nil
	}
	switch plan.Table.Type {
	case schema.NoType, schema.Sequence:
		// For now, allow sequence inserts.
		return analyzeInsertNoType(ins, plan, plan.Table)
	case schema.Message:
		return analyzeInsertMessage(ins, plan, plan.Table)
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

	// time_acked should not be specified.
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

func lookupTable(tableExprs sqlparser.TableExprs, tables map[string]*schema.Table) *schema.Table {
	if len(tableExprs) > 1 {
		return nil
	}
	aliased, ok := tableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil
	}
	tableName := sqlparser.GetTableName(aliased.Expr)
	if tableName.IsEmpty() {
		return nil
	}
	return tables[tableName.String()]
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
