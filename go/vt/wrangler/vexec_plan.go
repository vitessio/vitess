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
	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/olekukonko/tablewriter"
)

type vexecPlan struct {
	query       string
	opcode      int
	parsedQuery *sqlparser.ParsedQuery
	planner     vexecPlanner
}

type vexecPlanner interface {
	dbNameColumn() (colName string)
	workflowColumn() (colName string)
	immutableColumnNames() (colNames []string)
	updatableColumnNames() (colNames []string)
	exec(ctx context.Context, masterAlias *topodatapb.TabletAlias, query string) (*querypb.QueryResult, error)
	dryRun() error
}

type vreplicationPlanner struct{ vx *vexec }

func (p vreplicationPlanner) dbNameColumn() (colName string)            { return "db_name" }
func (p vreplicationPlanner) workflowColumn() (colName string)          { return "workflow" }
func (p vreplicationPlanner) immutableColumnNames() (colNames []string) { return []string{"id"} }
func (p vreplicationPlanner) updatableColumnNames() (colNames []string) { return []string{} }
func (p vreplicationPlanner) exec(ctx context.Context, masterAlias *topodatapb.TabletAlias, query string) (*querypb.QueryResult, error) {
	qr, err := p.vx.wr.VReplicationExec(ctx, masterAlias, query)
	if err != nil {
		return nil, err
	}
	if qr.RowsAffected == 0 {
		log.Infof("no matching streams found for workflow %s, tablet %s, query %s", p.vx.workflow, masterAlias, query)
	}
	return qr, nil
}
func (p vreplicationPlanner) dryRun() error {
	rsr, err := p.vx.wr.getStreams(p.vx.ctx, p.vx.workflow, p.vx.keyspace)
	if err != nil {
		return err
	}

	p.vx.wr.Logger().Printf("Query: %s\nwill be run on the following streams in keyspace %s for workflow %s:\n\n",
		p.vx.plan.parsedQuery.Query, p.vx.keyspace, p.vx.workflow)
	tableString := &strings.Builder{}
	table := tablewriter.NewWriter(tableString)
	table.SetHeader([]string{"Tablet", "ID", "BinLogSource", "State", "DBName", "Current GTID", "MaxReplicationLag"})
	for _, master := range p.vx.masters {
		key := fmt.Sprintf("%s/%s", master.Shard, master.AliasString())
		for _, stream := range rsr.ShardStatuses[key].MasterReplicationStatuses {
			table.Append([]string{key, fmt.Sprintf("%d", stream.ID), stream.Bls.String(), stream.State, stream.DBName, stream.Pos, fmt.Sprintf("%d", stream.MaxReplicationLag)})
		}
	}
	table.SetAutoMergeCellsByColumnIndex([]int{0})
	table.SetRowLine(true)
	table.Render()
	p.vx.wr.Logger().Printf(tableString.String())
	p.vx.wr.Logger().Printf("\n\n")

	return nil
}

type schemaMigrationsPlanner struct{ vx *vexec }

func (p schemaMigrationsPlanner) dbNameColumn() (colName string)            { return "mysql_schema" }
func (p schemaMigrationsPlanner) workflowColumn() (colName string)          { return "migration_uuid" }
func (p schemaMigrationsPlanner) immutableColumnNames() (colNames []string) { return []string{""} }
func (p schemaMigrationsPlanner) updatableColumnNames() (colNames []string) {
	return []string{"migration_status"}
}
func (p schemaMigrationsPlanner) exec(ctx context.Context, masterAlias *topodatapb.TabletAlias, query string) (*querypb.QueryResult, error) {
	qr, err := p.vx.wr.GenericVExec(ctx, masterAlias, query)
	if err != nil {
		return nil, err
	}
	if qr.RowsAffected == 0 {
		return nil, fmt.Errorf("\nno matching migrations found for workflow %s, tablet %s, query %s", p.vx.workflow, masterAlias, query)
	}
	return qr, nil
}
func (p schemaMigrationsPlanner) dryRun() error { return nil }

var _ vexecPlanner = vreplicationPlanner{}
var _ vexecPlanner = schemaMigrationsPlanner{}

const (
	updateQuery = iota
	deleteQuery
	selectQuery
)

func extractTableName(stmt sqlparser.Statement) (string, error) {
	switch stmt := stmt.(type) {
	case *sqlparser.Update:
		return sqlparser.String(stmt.TableExprs), nil
	case *sqlparser.Delete:
		return sqlparser.String(stmt.TableExprs), nil
	case *sqlparser.Select:
		return sqlparser.String(stmt.From), nil
	}
	return "", fmt.Errorf("query not supported by vexec: %+v", sqlparser.String(stmt))
}

func (vx *vexec) buildVExecPlan() (*vexecPlan, error) {
	stmt, err := sqlparser.Parse(vx.query)
	if err != nil {
		return nil, err
	}

	tableName, err := extractTableName(stmt)
	if err != nil {
		return nil, err
	}

	var planner vexecPlanner
	switch tableName {
	case schemaMigrationsTableName:
		planner = schemaMigrationsPlanner{vx: vx}
	case vreplicationTableName:
		planner = vreplicationPlanner{vx: vx}
	default:
		return nil, fmt.Errorf("table not supported by vexec: %v", tableName)
	}

	var plan *vexecPlan
	switch stmt := stmt.(type) {
	case *sqlparser.Update:
		plan, err = vx.buildUpdatePlan(planner, stmt)
	case *sqlparser.Delete:
		plan, err = vx.buildDeletePlan(planner, stmt)
	case *sqlparser.Select:
		plan, err = vx.buildSelectPlan(planner, stmt)
	default:
		return nil, fmt.Errorf("query not supported by vexec: %s", sqlparser.String(stmt))
	}

	if err != nil {
		return nil, err
	}
	plan.planner = planner
	plan.query = vx.query
	vx.plan = plan
	return plan, nil
}

func splitAndExpression(filters []sqlparser.Expr, node sqlparser.Expr) []sqlparser.Expr {
	if node == nil {
		return filters
	}
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		filters = splitAndExpression(filters, node.Left)
		return splitAndExpression(filters, node.Right)
	}
	return append(filters, node)
}

func (vx *vexec) analyzeWhere(where *sqlparser.Where) []string {
	var cols []string
	if where == nil {
		return cols
	}
	exprs := splitAndExpression(nil, where.Expr)
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

func (vx *vexec) addDefaultWheres(planner vexecPlanner, where *sqlparser.Where) *sqlparser.Where {
	cols := vx.analyzeWhere(where)
	var hasDBName, hasWorkflow bool
	for _, col := range cols {
		if col == planner.dbNameColumn() {
			hasDBName = true
		} else if col == planner.workflowColumn() {
			hasWorkflow = true
		}
	}
	newWhere := where
	if !hasDBName {
		expr := &sqlparser.ComparisonExpr{
			Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent(planner.dbNameColumn())},
			Operator: sqlparser.EqualStr,
			Right:    sqlparser.NewStrLiteral([]byte(vx.masters[0].DbName())),
		}
		if newWhere == nil {
			newWhere = &sqlparser.Where{
				Type: sqlparser.WhereStr,
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
			Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent(planner.workflowColumn())},
			Operator: sqlparser.EqualStr,
			Right:    sqlparser.NewStrLiteral([]byte(vx.workflow)),
		}
		newWhere.Expr = &sqlparser.AndExpr{
			Left:  newWhere.Expr,
			Right: expr,
		}
	}
	return newWhere
}

func (vx *vexec) buildUpdatePlan(planner vexecPlanner, upd *sqlparser.Update) (*vexecPlan, error) {
	if upd.OrderBy != nil || upd.Limit != nil {
		return nil, fmt.Errorf("unsupported construct: %v", sqlparser.String(upd))
	}
	for _, expr := range upd.Exprs {
		for _, immutableColName := range planner.immutableColumnNames() {
			if expr.Name.Name.EqualString(immutableColName) {
				return nil, fmt.Errorf("%s cannot be changed: %v", immutableColName, sqlparser.String(expr))
			}
		}
	}
	if updatableColumnNames := planner.updatableColumnNames(); len(updatableColumnNames) > 0 {
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

	upd.Where = vx.addDefaultWheres(planner, upd.Where)

	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", upd)

	return &vexecPlan{
		opcode:      updateQuery,
		parsedQuery: buf.ParsedQuery(),
	}, nil
}

func (vx *vexec) buildDeletePlan(planner vexecPlanner, del *sqlparser.Delete) (*vexecPlan, error) {
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

func (vx *vexec) buildSelectPlan(planner vexecPlanner, sel *sqlparser.Select) (*vexecPlan, error) {
	sel.Where = vx.addDefaultWheres(planner, sel.Where)
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", sel)

	return &vexecPlan{
		opcode:      selectQuery,
		parsedQuery: buf.ParsedQuery(),
	}, nil
}
