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
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func analyzeSelect(sel *sqlparser.Select, tables map[string]*schema.Table) (plan *Plan, err error) {
	plan = &Plan{
		PlanID:    PlanSelect,
		FullQuery: GenerateLimitQuery(sel),
	}
	plan.Table, plan.AllTables = lookupTables(sel.From, tables)

	if sel.Where != nil {
		comp, ok := sel.Where.Expr.(*sqlparser.ComparisonExpr)
		if ok && comp.IsImpossible() {
			plan.PlanID = PlanSelectImpossible
			return plan, nil
		}
	}

	// Check if it's a NEXT VALUE statement.
	if nextVal, ok := sel.SelectExprs[0].(*sqlparser.Nextval); ok {
		if plan.Table == nil || plan.Table.Type != schema.Sequence {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%s is not a sequence", sqlparser.ToString(sel.From))
		}
		plan.PlanID = PlanNextval
		v, err := evalengine.Translate(nextVal.Expr, nil)
		if err != nil {
			return nil, err
		}
		plan.NextCount = v
		plan.FullQuery = nil
	}

	if hasLockFunc(sel) {
		plan.PlanID = PlanSelectLockFunc
		plan.NeedsReservedConn = true
	}
	return plan, nil
}

// analyzeUpdate code is almost identical to analyzeDelete.
func analyzeUpdate(upd *sqlparser.Update, tables map[string]*schema.Table) (plan *Plan, err error) {
	plan = &Plan{
		PlanID: PlanUpdate,
	}
	plan.Table, plan.AllTables = lookupTables(upd.TableExprs, tables)

	// Store the WHERE clause as string for the hot row protection (txserializer).
	if upd.Where != nil {
		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("%v", upd.Where)
		plan.WhereClause = buf.ParsedQuery()
	}

	// Situations when we pass-through:
	// PassthroughDMLs flag is set.
	// plan.Table==nil: it's likely a multi-table statement. MySQL doesn't allow limit clauses for multi-table dmls.
	// If there's an explicit Limit.
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

// analyzeDelete code is almost identical to analyzeUpdate.
func analyzeDelete(del *sqlparser.Delete, tables map[string]*schema.Table) (plan *Plan, err error) {
	plan = &Plan{
		PlanID: PlanDelete,
	}
	plan.Table, plan.AllTables = lookupTables(del.TableExprs, tables)

	if del.Where != nil {
		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("%v", del.Where)
		plan.WhereClause = buf.ParsedQuery()
	}

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

	tableName, err := ins.Table.TableName()
	if err != nil {
		return nil, err
	}
	plan.Table = tables[sqlparser.GetTableName(tableName).String()]
	return plan, nil
}

func analyzeShow(show *sqlparser.Show, dbName string) (plan *Plan, err error) {
	switch showInternal := show.Internal.(type) {
	case *sqlparser.ShowBasic:
		switch showInternal.Command {
		case sqlparser.VitessMigrations:
			return &Plan{PlanID: PlanShowMigrations, FullStmt: show}, nil
		case sqlparser.Table:
			// rewrite WHERE clause if it exists
			// `where Tables_in_Keyspace` => `where Tables_in_DbName`
			if showInternal.Filter != nil {
				showTableRewrite(showInternal, dbName)
			}
		}
		return &Plan{
			PlanID:    PlanShow,
			FullQuery: GenerateFullQuery(show),
		}, nil
	case *sqlparser.ShowCreate:
		if showInternal.Command == sqlparser.CreateDb && !sqlparser.SystemSchema(showInternal.Op.Name.String()) {
			showInternal.Op.Name = sqlparser.NewIdentifierCS(dbName)
		}
		return &Plan{
			PlanID:    PlanShow,
			FullQuery: GenerateFullQuery(show),
		}, nil
	}
	return &Plan{PlanID: PlanOtherRead}, nil
}

func showTableRewrite(show *sqlparser.ShowBasic, dbName string) {
	filter := show.Filter.Filter
	if filter == nil {
		return
	}
	_ = sqlparser.SafeRewrite(filter, nil, func(cursor *sqlparser.Cursor) bool {
		switch n := cursor.Node().(type) {
		case *sqlparser.ColName:
			if n.Qualifier.IsEmpty() && strings.HasPrefix(n.Name.Lowered(), "tables_in_") {
				cursor.Replace(sqlparser.NewColName("Tables_in_" + dbName))
			}
		}
		return true
	})
}

func analyzeSet(set *sqlparser.Set) (plan *Plan) {
	return &Plan{
		PlanID:            PlanSet,
		FullQuery:         GenerateFullQuery(set),
		NeedsReservedConn: true,
	}
}

func lookupTables(tableExprs sqlparser.TableExprs, tables map[string]*schema.Table) (singleTable *schema.Table, allTables []*schema.Table) {
	for _, tableExpr := range tableExprs {
		if t := lookupSingleTable(tableExpr, tables); t != nil {
			allTables = append(allTables, t)
		}
	}
	if len(allTables) == 1 {
		singleTable = allTables[0]
	}
	return singleTable, allTables
}

func lookupSingleTable(tableExpr sqlparser.TableExpr, tables map[string]*schema.Table) *schema.Table {
	aliased, ok := tableExpr.(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil
	}
	tableName := sqlparser.GetTableName(aliased.Expr)
	if tableName.IsEmpty() {
		return nil
	}
	return tables[tableName.String()]
}

func analyzeDDL(stmt sqlparser.DDLStatement) (*Plan, error) {
	// DDLs and some other statements below don't get fully parsed.
	// We have to use the original query at the time of execution.
	// We are in the process of changing this
	var fullQuery *sqlparser.ParsedQuery
	// If the query is fully parsed, then use the ast and store the fullQuery
	if stmt.IsFullyParsed() {
		fullQuery = GenerateFullQuery(stmt)
	}
	return &Plan{PlanID: PlanDDL, FullQuery: fullQuery, FullStmt: stmt, NeedsReservedConn: stmt.IsTemporary()}, nil
}
