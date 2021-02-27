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
	"errors"
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
	case *sqlparser.Insert, *sqlparser.Update, *sqlparser.Delete:
		plan, err = nil, fmt.Errorf("%w: bother @ajm188, this is still WIP", ErrUnsupportedQuery)
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

func (planner *VReplicationQueryPlanner) planSelect(sel *sqlparser.Select) (*QueryPlan, error) {
	sel.Where = addDefaultWheres(planner, sel.Where)

	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", sel)

	return &QueryPlan{
		ParsedQuery: buf.ParsedQuery(),
		tmc:         planner.tmc,
	}, nil
}

type QueryPlan struct {
	ParsedQuery *sqlparser.ParsedQuery

	tmc tmclient.TabletManagerClient
}

func (qp *QueryPlan) Execute(ctx context.Context, target *topo.TabletInfo) (qr *querypb.QueryResult, err error) {
	if qp.ParsedQuery == nil {
		return nil, errors.New("TODO: some error about executing a query before it was planned")
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
		log.Infof("no matching streams found for workflows %s, tablet %s, query %s", "TODO: pass through workflow", targetAliasStr, qp.ParsedQuery.Query)
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
