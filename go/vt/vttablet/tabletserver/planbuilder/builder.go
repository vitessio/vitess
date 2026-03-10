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
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func analyzeUnion(stmt *sqlparser.Union, noRowslimit bool) *Plan {
	if noRowslimit {
		return &Plan{PlanID: PlanSelect, FullQuery: GenerateFullQuery(stmt)}
	}
	return &Plan{PlanID: PlanSelect, FullQuery: GenerateLimitQuery(stmt)}
}

// buildBaseSelectPlan performs shared SELECT analysis: impossible WHERE, nextval, lock func detection.
// It is used by both Build (via analyzeSelect) and BuildStreaming.
func buildBaseSelectPlan(env *vtenv.Environment, sel *sqlparser.Select, tables map[string]*schema.Table) (*Plan, error) {
	plan := &Plan{
		PlanID:    PlanSelect,
		FullQuery: GenerateFullQuery(sel),
	}
	plan.Table = lookupTables(sel.From, tables)

	if sel.Where != nil {
		if comp, ok := sel.Where.Expr.(*sqlparser.ComparisonExpr); ok && comp.IsImpossible() {
			plan.PlanID = PlanSelectImpossible
			return plan, nil
		}
	}

	// Check if it's a NEXT VALUE statement.
	if nextVal, ok := sel.GetColumns()[0].(*sqlparser.Nextval); ok {
		if plan.Table == nil || plan.Table.Type != schema.Sequence {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%s is not a sequence", sqlparser.ToString(sel.From))
		}
		plan.PlanID = PlanNextval
		v, err := evalengine.Translate(nextVal.Expr, &evalengine.Config{
			Environment: env,
			Collation:   env.CollationEnv().DefaultConnectionCharset(),
		})
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

func analyzeSelect(env *vtenv.Environment, sel *sqlparser.Select, tables map[string]*schema.Table, noRowsLimit bool) (*Plan, error) {
	plan, err := buildBaseSelectPlan(env, sel, tables)
	if err != nil {
		return nil, err
	}

	// Apply LIMIT and noRowsLimit overrides for non-streaming Build().
	// PlanSelectImpossible and PlanSelectLockFunc take precedence over PlanSelectNoLimit.
	// PlanNextval doesn't use a FullQuery, so no LIMIT is needed.
	switch plan.PlanID {
	case PlanSelect:
		if noRowsLimit {
			plan.PlanID = PlanSelectNoLimit
		} else {
			plan.FullQuery = GenerateLimitQuery(sel)
		}
	case PlanSelectImpossible, PlanSelectLockFunc:
		if !noRowsLimit {
			plan.FullQuery = GenerateLimitQuery(sel)
		}
	}

	return plan, nil
}

// buildBaseUpdatePlan builds the core UPDATE plan without safety LIMIT.
// It is used by both Build (via analyzeUpdate) and BuildStreaming.
func buildBaseUpdatePlan(upd *sqlparser.Update, tables map[string]*schema.Table) *Plan {
	plan := &Plan{
		PlanID:    PlanUpdate,
		FullQuery: GenerateFullQuery(upd),
	}
	plan.Table = lookupTables(upd.TableExprs, tables)

	// Store the WHERE clause as string for the hot row protection (txserializer).
	if upd.Where != nil {
		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("%v", upd.Where)
		plan.WhereClause = buf.ParsedQuery()
	}

	return plan
}

// analyzeUpdate builds an UPDATE plan with safety LIMIT for non-streaming Build().
func analyzeUpdate(upd *sqlparser.Update, tables map[string]*schema.Table) (*Plan, error) {
	plan := buildBaseUpdatePlan(upd, tables)

	// Situations when we pass-through:
	// PassthroughDMLs flag is set.
	// plan.Table==nil: it's likely a multi-table statement. MySQL doesn't allow limit clauses for multi-table dmls.
	// If there's an explicit Limit.
	if PassthroughDMLs || plan.Table == nil || upd.Limit != nil {
		return plan, nil
	}

	plan.PlanID = PlanUpdateLimit
	upd.Limit = execLimit
	plan.FullQuery = GenerateFullQuery(upd)
	upd.Limit = nil
	return plan, nil
}

// buildBaseDeletePlan builds the core DELETE plan without safety LIMIT.
// It is used by both Build (via analyzeDelete) and BuildStreaming.
func buildBaseDeletePlan(del *sqlparser.Delete, tables map[string]*schema.Table) *Plan {
	plan := &Plan{
		PlanID:    PlanDelete,
		FullQuery: GenerateFullQuery(del),
	}
	plan.Table = lookupTables(del.TableExprs, tables)

	if del.Where != nil {
		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("%v", del.Where)
		plan.WhereClause = buf.ParsedQuery()
	}

	return plan
}

// analyzeDelete builds a DELETE plan with safety LIMIT for non-streaming Build().
func analyzeDelete(del *sqlparser.Delete, tables map[string]*schema.Table) (*Plan, error) {
	plan := buildBaseDeletePlan(del, tables)

	if PassthroughDMLs || plan.Table == nil || del.Limit != nil {
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

	plan.Table = lookupTables(sqlparser.TableExprs{ins.Table}, tables)
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

func lookupTables(tableExprs sqlparser.TableExprs, tables map[string]*schema.Table) (singleTable *schema.Table) {
	for _, tableExpr := range tableExprs {
		if t := lookupSingleTable(tableExpr, tables); t != nil {
			if singleTable != nil {
				return nil
			}
			singleTable = t
		}
	}
	return singleTable
}

func lookupAllTables(stmt sqlparser.Statement, tables map[string]*schema.Table) (allTables []*schema.Table) {
	tablesUsed := sqlparser.ExtractAllTables(stmt)
	for _, tbl := range tablesUsed {
		if t := tables[tbl]; t != nil {
			allTables = append(allTables, t)
		}
	}
	return allTables
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

func analyzeFlush(stmt *sqlparser.Flush, tables map[string]*schema.Table) (*Plan, error) {
	plan := &Plan{PlanID: PlanFlush, FullQuery: GenerateFullQuery(stmt)}

	for _, tbl := range stmt.TableNames {
		if schemaTbl, ok := tables[tbl.Name.String()]; ok {
			if plan.Table != nil {
				// If there are multiple tables, we empty out the table field.
				plan.Table = nil
				break
			}
			plan.Table = schemaTbl
		}
	}

	if stmt.WithLock {
		plan.NeedsReservedConn = true
	}
	return plan, nil
}
