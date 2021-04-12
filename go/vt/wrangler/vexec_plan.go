/*
Copyright 2020 The Vitess Authors.

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

package wrangler

import (
	"context"
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/olekukonko/tablewriter"
)

// vexecPlan contains the final query to be sent to the tablets
type vexecPlan struct {
	opcode      int
	parsedQuery *sqlparser.ParsedQuery
}

// vexecPlannerParams controls how some queries/columns are handled
type vexecPlannerParams struct {
	dbNameColumn         string
	workflowColumn       string
	immutableColumnNames []string
	updatableColumnNames []string
	updateTemplates      []string
	insertTemplates      []string
}

// vexecPlanner generates and executes a plan
type vexecPlanner interface {
	params() *vexecPlannerParams
	exec(ctx context.Context, masterAlias *topodatapb.TabletAlias, query string) (*querypb.QueryResult, error)
	dryRun(ctx context.Context) error
}

// vreplicationPlanner is a vexecPlanner implementation, specific to _vt.vreplication table
type vreplicationPlanner struct {
	vx *vexec
	d  *vexecPlannerParams
}

func newVReplicationPlanner(vx *vexec) vexecPlanner {
	return &vreplicationPlanner{
		vx: vx,
		d: &vexecPlannerParams{
			dbNameColumn:         "db_name",
			workflowColumn:       "workflow",
			immutableColumnNames: []string{"id"},
			updatableColumnNames: []string{},
		},
	}
}
func (p vreplicationPlanner) params() *vexecPlannerParams { return p.d }
func (p vreplicationPlanner) exec(
	ctx context.Context, masterAlias *topodatapb.TabletAlias, query string,
) (*querypb.QueryResult, error) {
	qr, err := p.vx.wr.VReplicationExec(ctx, masterAlias, query)
	if err != nil {
		return nil, err
	}
	if qr.RowsAffected == 0 {
		log.Infof("no matching streams found for workflow %s, tablet %s, query %s", p.vx.workflow, masterAlias, query)
	}
	return qr, nil
}
func (p vreplicationPlanner) dryRun(ctx context.Context) error {
	rsr, err := p.vx.wr.getStreams(p.vx.ctx, p.vx.workflow, p.vx.keyspace)
	if err != nil {
		return err
	}

	p.vx.wr.Logger().Printf("Query: %s\nwill be run on the following streams in keyspace %s for workflow %s:\n\n",
		p.vx.plannedQuery, p.vx.keyspace, p.vx.workflow)
	tableString := &strings.Builder{}
	table := tablewriter.NewWriter(tableString)
	table.SetHeader([]string{"Tablet", "ID", "BinLogSource", "State", "DBName", "Current GTID"})
	for _, master := range p.vx.masters {
		key := fmt.Sprintf("%s/%s", master.Shard, master.AliasString())
		for _, stream := range rsr.ShardStatuses[key].MasterReplicationStatuses {
			table.Append([]string{key, fmt.Sprintf("%d", stream.ID), stream.Bls.String(), stream.State, stream.DBName, stream.Pos})
		}
	}
	table.SetAutoMergeCellsByColumnIndex([]int{0})
	table.SetRowLine(true)
	table.Render()
	p.vx.wr.Logger().Printf(tableString.String())
	p.vx.wr.Logger().Printf("\n\n")

	return nil
}

// schemaMigrationsPlanner is a vexecPlanner implementation, specific to _vt.schema_migrations table
type schemaMigrationsPlanner struct {
	vx *vexec
	d  *vexecPlannerParams
}

func newSchemaMigrationsPlanner(vx *vexec) vexecPlanner {
	return &schemaMigrationsPlanner{
		vx: vx,
		d: &vexecPlannerParams{
			dbNameColumn:   "mysql_schema",
			workflowColumn: "migration_uuid",
			updateTemplates: []string{
				`update _vt.schema_migrations set migration_status='val1'`,
				`update _vt.schema_migrations set migration_status='val1' where migration_uuid='val2'`,
				`update _vt.schema_migrations set migration_status='val1' where migration_uuid='val2' and shard='val3'`,
			},
			insertTemplates: []string{
				`INSERT IGNORE INTO _vt.schema_migrations (
					migration_uuid,
					keyspace,
					shard,
					mysql_schema,
					mysql_table,
					migration_statement,
					strategy,
					options,
					ddl_action,
					requested_timestamp,
					migration_context,
					migration_status
				) VALUES (
					'val', 'val', 'val', 'val', 'val', 'val', 'val', 'val', 'val', FROM_UNIXTIME(0), 'val', 'val'
				)`,
			},
		},
	}
}
func (p schemaMigrationsPlanner) params() *vexecPlannerParams { return p.d }
func (p schemaMigrationsPlanner) exec(ctx context.Context, masterAlias *topodatapb.TabletAlias, query string) (*querypb.QueryResult, error) {
	qr, err := p.vx.wr.GenericVExec(ctx, masterAlias, query, p.vx.workflow, p.vx.keyspace)
	if err != nil {
		return nil, err
	}
	return qr, nil
}
func (p schemaMigrationsPlanner) dryRun(ctx context.Context) error { return nil }

// make sure these planners implement vexecPlanner interface
var _ vexecPlanner = vreplicationPlanner{}
var _ vexecPlanner = schemaMigrationsPlanner{}

const (
	updateQuery = iota
	deleteQuery
	insertQuery
	selectQuery
)

// extractTableName returns the qualified table name (e.g. "_vt.schema_migrations") from a SELECT/DELETE/UPDATE statement
func extractTableName(stmt sqlparser.Statement) (string, error) {
	switch stmt := stmt.(type) {
	case *sqlparser.Update:
		return sqlparser.String(stmt.TableExprs), nil
	case *sqlparser.Delete:
		return sqlparser.String(stmt.TableExprs), nil
	case *sqlparser.Insert:
		return sqlparser.String(stmt.Table), nil
	case *sqlparser.Select:
		return sqlparser.String(stmt.From), nil
	}
	return "", fmt.Errorf("query not supported by vexec: %+v", sqlparser.String(stmt))
}

// qualifiedTableName qualifies a table with "_vt." schema
func qualifiedTableName(tableName string) string {
	return fmt.Sprintf("%s.%s", vexecTableQualifier, tableName)
}

// getPlanner returns a specific planner appropriate for the queried table
func (vx *vexec) getPlanner(ctx context.Context) error {
	switch vx.tableName {
	case qualifiedTableName(schema.SchemaMigrationsTableName):
		vx.planner = newSchemaMigrationsPlanner(vx)
	case qualifiedTableName(vreplicationTableName):
		vx.planner = newVReplicationPlanner(vx)
	default:
		return fmt.Errorf("table not supported by vexec: %v", vx.tableName)
	}
	return nil
}

// buildPlan builds an execution plan. More specifically, it generates the query which is then sent to
// relevant vttablet servers
func (vx *vexec) buildPlan(ctx context.Context) (plan *vexecPlan, err error) {
	switch stmt := vx.stmt.(type) {
	case *sqlparser.Update:
		plan, err = vx.buildUpdatePlan(ctx, vx.planner, stmt)
	case *sqlparser.Delete:
		plan, err = vx.buildDeletePlan(ctx, vx.planner, stmt)
	case *sqlparser.Insert:
		plan, err = vx.buildInsertPlan(ctx, vx.planner, stmt)
	case *sqlparser.Select:
		plan, err = vx.buildSelectPlan(ctx, vx.planner, stmt)
	default:
		return nil, fmt.Errorf("query not supported by vexec: %s", sqlparser.String(stmt))
	}
	return plan, err
}

// analyzeWhereEqualsColumns identifies column names in a WHERE clause that have a comparison expression
func (vx *vexec) analyzeWhereEqualsColumns(where *sqlparser.Where) []string {
	var cols []string
	if where == nil {
		return cols
	}
	exprs := sqlparser.SplitAndExpression(nil, where.Expr)
	for _, expr := range exprs {
		switch expr := expr.(type) {
		case *sqlparser.ComparisonExpr:
			qualifiedName, ok := expr.Left.(*sqlparser.ColName)
			if ok {
				cols = append(cols, qualifiedName.Name.String())
			}
		}
	}
	return cols
}

// addDefaultWheres modifies the query to add, if appropriate, the workflow and DB-name column modifiers
func (vx *vexec) addDefaultWheres(planner vexecPlanner, where *sqlparser.Where) *sqlparser.Where {
	cols := vx.analyzeWhereEqualsColumns(where)
	var hasDBName, hasWorkflow bool
	plannerParams := planner.params()
	for _, col := range cols {
		if col == plannerParams.dbNameColumn {
			hasDBName = true
		} else if col == plannerParams.workflowColumn {
			hasWorkflow = true
		}
	}
	newWhere := where
	if !hasDBName {
		expr := &sqlparser.ComparisonExpr{
			Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent(plannerParams.dbNameColumn)},
			Operator: sqlparser.EqualOp,
			Right:    sqlparser.NewStrLiteral(vx.masters[0].DbName()),
		}
		if newWhere == nil {
			newWhere = &sqlparser.Where{
				Type: sqlparser.WhereClause,
				Expr: expr,
			}
		} else {
			newWhere.Expr = &sqlparser.AndExpr{
				Left:  newWhere.Expr,
				Right: expr,
			}
		}
	}
	if !hasWorkflow && vx.workflow != "" {
		expr := &sqlparser.ComparisonExpr{
			Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent(plannerParams.workflowColumn)},
			Operator: sqlparser.EqualOp,
			Right:    sqlparser.NewStrLiteral(vx.workflow),
		}
		newWhere.Expr = &sqlparser.AndExpr{
			Left:  newWhere.Expr,
			Right: expr,
		}
	}
	return newWhere
}

// buildUpdatePlan builds a plan for an UPDATE query
func (vx *vexec) buildUpdatePlan(ctx context.Context, planner vexecPlanner, upd *sqlparser.Update) (*vexecPlan, error) {
	if upd.OrderBy != nil || upd.Limit != nil {
		return nil, fmt.Errorf("unsupported construct: %v", sqlparser.String(upd))
	}
	plannerParams := planner.params()
	if immutableColumnNames := plannerParams.immutableColumnNames; len(immutableColumnNames) > 0 {
		// we must never allow changing an immutable column
		for _, expr := range upd.Exprs {
			for _, immutableColName := range plannerParams.immutableColumnNames {
				if expr.Name.Name.EqualString(immutableColName) {
					return nil, fmt.Errorf("%s cannot be changed: %v", immutableColName, sqlparser.String(expr))
				}
			}
		}
	}
	if updatableColumnNames := plannerParams.updatableColumnNames; len(updatableColumnNames) > 0 {
		// if updatableColumnNames is non empty, then we must only accept changes to columns listed there
		for _, expr := range upd.Exprs {
			isUpdatable := false
			for _, updatableColName := range updatableColumnNames {
				if expr.Name.Name.EqualString(updatableColName) {
					isUpdatable = true
				}
			}
			if !isUpdatable {
				return nil, fmt.Errorf("%+v cannot be changed: %v", expr.Name.Name, sqlparser.String(expr))
			}
		}
	}
	if templates := plannerParams.updateTemplates; len(templates) > 0 {
		match, err := sqlparser.QueryMatchesTemplates(vx.query, templates)
		if err != nil {
			return nil, err
		}
		if !match {
			return nil, fmt.Errorf("Query must match one of these templates: %s", strings.Join(templates, "; "))
		}
	}
	upd.Where = vx.addDefaultWheres(planner, upd.Where)

	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", upd)

	return &vexecPlan{
		opcode:      updateQuery,
		parsedQuery: buf.ParsedQuery(),
	}, nil
}

// buildUpdatePlan builds a plan for a DELETE query
func (vx *vexec) buildDeletePlan(ctx context.Context, planner vexecPlanner, del *sqlparser.Delete) (*vexecPlan, error) {
	if del.Targets != nil {
		return nil, fmt.Errorf("unsupported construct: %v", sqlparser.String(del))
	}
	if del.Partitions != nil {
		return nil, fmt.Errorf("unsupported construct: %v", sqlparser.String(del))
	}
	if del.OrderBy != nil || del.Limit != nil {
		return nil, fmt.Errorf("unsupported construct: %v", sqlparser.String(del))
	}

	del.Where = vx.addDefaultWheres(planner, del.Where)

	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", del)

	return &vexecPlan{
		opcode:      deleteQuery,
		parsedQuery: buf.ParsedQuery(),
	}, nil
}

// buildInsertPlan builds a plan for a INSERT query
func (vx *vexec) buildInsertPlan(ctx context.Context, planner vexecPlanner, ins *sqlparser.Insert) (*vexecPlan, error) {
	plannerParams := planner.params()
	templates := plannerParams.insertTemplates
	if len(templates) == 0 {
		// at this time INSERT is only supported if an insert template exists
		// Remove this conditional if there's any new case for INSERT
		return nil, fmt.Errorf("query not supported by vexec: %s", sqlparser.String(ins))
	}
	if len(templates) > 0 {
		match, err := sqlparser.QueryMatchesTemplates(vx.query, templates)
		if err != nil {
			return nil, err
		}
		if !match {
			return nil, fmt.Errorf("Query must match one of these templates: %s", strings.Join(templates, "; "))
		}
	}

	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", ins)

	return &vexecPlan{
		opcode:      insertQuery,
		parsedQuery: buf.ParsedQuery(),
	}, nil
}

// buildUpdatePlan builds a plan for a SELECT query
func (vx *vexec) buildSelectPlan(ctx context.Context, planner vexecPlanner, sel *sqlparser.Select) (*vexecPlan, error) {
	sel.Where = vx.addDefaultWheres(planner, sel.Where)
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", sel)

	return &vexecPlan{
		opcode:      selectQuery,
		parsedQuery: buf.ParsedQuery(),
	}, nil
}
