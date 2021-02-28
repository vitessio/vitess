/*
Copyright 2021 The Vitess Authors.

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

package vexec

import (
	"context"
	"fmt"
	"sync"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

type QueryPlanner interface {
	PlanQuery(stmt sqlparser.Statement) (*QueryPlan, error)
	QueryParams() QueryParams

	DBName() string
	WorkflowName() string
}

type QueryParams struct {
	DBNameColumn   string
	WorkflowColumn string
}

type VReplicationQueryPlanner struct {
	tmc tmclient.TabletManagerClient

	dbname   string
	workflow string
}

func NewVReplicationQueryPlanner(tmc tmclient.TabletManagerClient, workflow string, dbname string) *VReplicationQueryPlanner {
	return &VReplicationQueryPlanner{
		tmc:      tmc,
		dbname:   dbname,
		workflow: workflow,
	}
}

func (planner *VReplicationQueryPlanner) DBName() string {
	return planner.dbname
}

func (planner *VReplicationQueryPlanner) PlanQuery(stmt sqlparser.Statement) (plan *QueryPlan, err error) {
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		plan, err = planner.planSelect(stmt)
	case *sqlparser.Insert:
		err = ErrUnsupportedQuery
	case *sqlparser.Update:
		plan, err = planner.planUpdate(stmt)
	case *sqlparser.Delete:
		plan, err = planner.planDelete(stmt)
	default:
		err = ErrUnsupportedQuery
	}

	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, sqlparser.String(stmt))
	}

	return plan, nil
}

func (planner *VReplicationQueryPlanner) QueryParams() QueryParams {
	return QueryParams{
		DBNameColumn:   "db_name",
		WorkflowColumn: "workflow",
	}
}

func (planner *VReplicationQueryPlanner) WorkflowName() string {
	return planner.workflow
}

func (planner *VReplicationQueryPlanner) planDelete(del *sqlparser.Delete) (*QueryPlan, error) {
	if del.Targets != nil {
		return nil, fmt.Errorf(
			"%w: DELETE must not have explicit targets (have: %v): %v",
			ErrUnsupportedQueryConstruct,
			del.Targets,
			sqlparser.String(del),
		)
	}

	if del.Partitions != nil {
		return nil, fmt.Errorf(
			"%w: DELETE must not have explicit partitions (have: %v): %v",
			ErrUnsupportedQueryConstruct,
			del.Partitions,
			sqlparser.String(del),
		)
	}

	if del.OrderBy != nil || del.Limit != nil {
		return nil, fmt.Errorf(
			"%w: DELETE must not have explicit ordering (have: %v) or limit clauses (have: %v): %v",
			ErrUnsupportedQueryConstruct,
			del.OrderBy,
			del.Limit,
			sqlparser.String(del),
		)
	}

	del.Where = addDefaultWheres(planner, del.Where)

	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", del)

	return &QueryPlan{
		ParsedQuery: buf.ParsedQuery(),
		workflow:    planner.workflow,
		tmc:         planner.tmc,
	}, nil
}

func (planner *VReplicationQueryPlanner) planSelect(sel *sqlparser.Select) (*QueryPlan, error) {
	sel.Where = addDefaultWheres(planner, sel.Where)

	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", sel)

	return &QueryPlan{
		ParsedQuery: buf.ParsedQuery(),
		workflow:    planner.workflow,
		tmc:         planner.tmc,
	}, nil
}

func (planner *VReplicationQueryPlanner) planUpdate(upd *sqlparser.Update) (*QueryPlan, error) {
	if upd.OrderBy != nil || upd.Limit != nil {
		return nil, fmt.Errorf(
			"%w: UPDATE must not have explicit ordering (have: %v) or limit clauses (have: %v): %v",
			ErrUnsupportedQueryConstruct,
			upd.OrderBy,
			upd.Limit,
			sqlparser.String(upd),
		)
	}

	// For updates on the _vt.vreplication table, we ban updates to the `id`
	// column, and allow updates to all other columns.
	for _, expr := range upd.Exprs {
		if expr.Name.Name.EqualString("id") {
			return nil, fmt.Errorf(
				"%w %+v: %v",
				ErrCannotUpdateImmutableColumn,
				expr.Name.Name,
				sqlparser.String(expr),
			)
		}
	}

	upd.Where = addDefaultWheres(planner, upd.Where)

	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", upd)

	return &QueryPlan{
		ParsedQuery: buf.ParsedQuery(),
		workflow:    planner.workflow,
		tmc:         planner.tmc,
	}, nil
}

type QueryPlan struct {
	ParsedQuery *sqlparser.ParsedQuery

	workflow string
	tmc      tmclient.TabletManagerClient
}

func (qp *QueryPlan) Execute(ctx context.Context, target *topo.TabletInfo) (qr *querypb.QueryResult, err error) {
	if qp.ParsedQuery == nil {
		return nil, fmt.Errorf("%w: call PlanQuery on a query planner first", ErrUnpreparedQuery)
	}

	targetAliasStr := target.AliasString()

	log.Infof("Running %v on %v", qp.ParsedQuery.Query, targetAliasStr)
	defer func() {
		if err != nil {
			log.Warningf("Result on %v: %v", targetAliasStr, err)

			return
		}

		log.Infof("Result on %v: %v", targetAliasStr, qr)
	}()

	qr, err = qp.tmc.VReplicationExec(ctx, target.Tablet, qp.ParsedQuery.Query)
	if err != nil {
		return nil, err
	}

	if qr.RowsAffected == 0 {
		log.Infof("no matching streams found for workflows %s, tablet %s, query %s", qp.workflow, targetAliasStr, qp.ParsedQuery.Query)
	}

	return qr, nil
}

func (qp *QueryPlan) ExecuteScatter(ctx context.Context, targets ...*topo.TabletInfo) (map[*topo.TabletInfo]*querypb.QueryResult, error) {
	var (
		m       sync.Mutex
		wg      sync.WaitGroup
		rec     concurrency.AllErrorRecorder
		results = make(map[*topo.TabletInfo]*querypb.QueryResult, len(targets))
	)

	for _, target := range targets {
		wg.Add(1)

		go func(ctx context.Context, target *topo.TabletInfo) {
			defer wg.Done()

			qr, err := qp.Execute(ctx, target)
			if err != nil {
				rec.RecordError(err)

				return
			}

			m.Lock()
			defer m.Unlock()

			results[target] = qr
		}(ctx, target)
	}

	wg.Wait()

	return results, rec.AggrError(vterrors.Aggregate)
}

func addDefaultWheres(planner QueryPlanner, where *sqlparser.Where) *sqlparser.Where {
	cols := extractWhereComparisonColumns(where)

	params := planner.QueryParams()
	hasDBNameCol := false
	hasWorkflowCol := false

	for _, col := range cols {
		switch col {
		case params.DBNameColumn:
			hasDBNameCol = true
		case params.WorkflowColumn:
			hasWorkflowCol = true
		}
	}

	newWhere := where

	if !hasDBNameCol {
		expr := &sqlparser.ComparisonExpr{
			Left: &sqlparser.ColName{
				Name: sqlparser.NewColIdent(params.DBNameColumn),
			},
			Operator: sqlparser.EqualOp,
			Right:    sqlparser.NewStrLiteral([]byte(planner.DBName())),
		}

		switch newWhere {
		case nil:
			newWhere = &sqlparser.Where{
				Type: sqlparser.WhereClause,
				Expr: expr,
			}
		default:
			newWhere.Expr = &sqlparser.AndExpr{
				Left:  newWhere.Expr,
				Right: expr,
			}
		}
	}

	if !hasWorkflowCol && planner.WorkflowName() != "" {
		expr := &sqlparser.ComparisonExpr{
			Left: &sqlparser.ColName{
				Name: sqlparser.NewColIdent(params.WorkflowColumn),
			},
			Operator: sqlparser.EqualOp,
			Right:    sqlparser.NewStrLiteral([]byte(planner.WorkflowName())),
		}

		newWhere.Expr = &sqlparser.AndExpr{
			Left:  newWhere.Expr,
			Right: expr,
		}
	}

	return newWhere
}

// extractWhereComparisonColumns extracts the column names used in AND-ed
// comparison expressions in a where clause, given the following assumptions:
// - (1) The column name is always the left-hand side of the comparison.
// - (2) There are no compound expressions within the where clause involving OR.
func extractWhereComparisonColumns(where *sqlparser.Where) []string {
	if where == nil {
		return nil
	}

	exprs := sqlparser.SplitAndExpression(nil, where.Expr)
	cols := make([]string, 0, len(exprs))

	for _, expr := range exprs {
		switch expr := expr.(type) {
		case *sqlparser.ComparisonExpr:
			if qualifiedName, ok := expr.Left.(*sqlparser.ColName); ok {
				cols = append(cols, qualifiedName.Name.String())
			}
		}
	}

	return cols
}
