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

	// If plan.Table==nil, it's likely a multi-table statement.
	// MySQL doesn't allow limit clauses for multi-table dmls.
	// If there's an explicity Limit, honor it.
	if PassthroughDMLs || plan.Table == nil || upd.Limit != nil {
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

	// If plan.Table==nil, it's likely a multi-table statement.
	// MySQL doesn't allow limit clauses for multi-table dmls.
	// If there's an explicity Limit, honor it.
	if PassthroughDMLs || plan.Table == nil || del.Limit != nil {
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
