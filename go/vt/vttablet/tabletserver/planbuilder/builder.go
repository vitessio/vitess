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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func analyzeSelect(sel *sqlparser.Select, tables map[string]*schema.Table) (plan *Plan, err error) {
	plan = &Plan{
		PlanID:     PlanSelect,
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
		plan.NextCount = v
		plan.FieldQuery = nil
		plan.FullQuery = nil
	}
	return plan, nil
}

func analyzeUpdate(upd *sqlparser.Update, tables map[string]*schema.Table) (plan *Plan, err error) {
	plan = &Plan{
		PlanID: PlanUpdate,
		Table:  lookupTable(upd.TableExprs, tables),
	}

	// Store the WHERE clause as string for the hot row protection (txserializer).
	if upd.Where != nil {
		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("%v", upd.Where)
		plan.WhereClause = buf.ParsedQuery()
	}

	if PassthroughDMLs || upd.Limit != nil {
		plan.FullQuery = GenerateFullQuery(upd)
		return plan, nil
	}

	plan.PlanID = PlanUpdateLimit
	upd.Limit = execLimit
	plan.FullQuery = GenerateFullQuery(upd)
	upd.Limit = nil
	return plan, nil
}

func analyzeDelete(del *sqlparser.Delete, tables map[string]*schema.Table) (plan *Plan, err error) {
	plan = &Plan{
		PlanID: PlanDelete,
		Table:  lookupTable(del.TableExprs, tables),
	}

	// Store the WHERE clause as string for the hot row protection (txserializer).
	if del.Where != nil {
		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("%v", del.Where)
		plan.WhereClause = buf.ParsedQuery()
	}

	if PassthroughDMLs || del.Limit != nil {
		plan.FullQuery = GenerateFullQuery(del)
		return plan, nil
	}
	plan.PlanID = PlanDeleteLimit
	del.Limit = execLimit
	plan.FullQuery = GenerateFullQuery(del)
	del.Limit = nil
	return plan, nil
}

func analyzeInsert(ins *sqlparser.Insert, tables map[string]*schema.Table) (plan *Plan, err error) {
	plan = &Plan{
		PlanID:    PlanInsert,
		FullQuery: GenerateFullQuery(ins),
	}

	tableName := sqlparser.GetTableName(ins.Table)
	plan.Table = tables[tableName.String()]
	if plan.Table == nil {
		return plan, nil
	}

	switch {
	case plan.Table.Type == schema.Message:
		return analyzeInsertMessage(ins, plan, plan.Table)
	case plan.Table.IsTopic():
		plan.PlanID = PlanInsertTopic
		return plan, nil
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

	plan.PlanID = PlanInsertMessage
	plan.FullQuery = GenerateFullQuery(ins)
	return plan, nil
}

func analyzeSet(set *sqlparser.Set) (plan *Plan) {
	return &Plan{
		PlanID:    PlanSet,
		FullQuery: GenerateFullQuery(set),
	}
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
