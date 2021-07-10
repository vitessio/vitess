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
	"errors"
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

var ( // Query planning errors.
	// ErrCannotUpdateImmutableColumn is returned when attempting to plan a
	// query that updates a column that should be treated as immutable.
	ErrCannotUpdateImmutableColumn = errors.New("cannot update immutable column")
	// ErrUnsupportedQueryConstruct is returned when a particular query
	// construct is unsupported by a QueryPlanner, despite the more general kind
	// of query being supported.
	//
	// For example, VReplication supports DELETEs, but does not support DELETEs
	// with LIMIT clauses, so planning a "DELETE ... LIMIT" will return
	// ErrUnsupportedQueryConstruct rather than a "CREATE TABLE", which would
	// return an ErrUnsupportedQuery.
	ErrUnsupportedQueryConstruct = errors.New("unsupported query construct")
)

var ( // Query execution errors.
	// ErrUnpreparedQuery is returned when attempting to execute an unprepared
	// QueryPlan.
	ErrUnpreparedQuery = errors.New("attempted to execute unprepared query")
)

// QueryPlanner defines the interface that VExec uses to build QueryPlans for
// various vexec workflows. A given vexec table, which is to say a table in the
// "_vt" database, will have at most one QueryPlanner implementation, which is
// responsible for defining both what queries are supported for that table, as
// well as how to build plans for those queries.
//
// VReplicationQueryPlanner is a good example implementation to refer to.
type QueryPlanner interface {
	// (NOTE:@ajm188) I don't think this method fits on the query planner. To
	// me, especially given that it's only implemented by the vrep query planner
	// in the old implementation (the schema migration query planner no-ops this
	// method), this fits better on our workflow.Manager struct, probably as a
	// method called something like "VReplicationExec(ctx, query, Options{DryRun: true})"
	// DryRun(ctx context.Context) error

	// PlanQuery constructs and returns a QueryPlan for a given statement. The
	// resulting QueryPlan is suitable for repeated, concurrent use.
	PlanQuery(stmt sqlparser.Statement) (*QueryPlan, error)
	// QueryParams returns a struct of column parameters the QueryPlanner uses.
	// It is used primarily to abstract the adding of default WHERE clauses to
	// queries by a private function of this package, and may be removed from
	// the interface later.
	QueryParams() QueryParams
}

// QueryParams is a struct that QueryPlanner implementations can provide to
// control the addition of default WHERE clauses to their queries.
type QueryParams struct {
	// DBName is the value that the column referred to by DBNameColumn should
	// equal in a WHERE clause, if set.
	DBName string
	// DBNameColumn is the name of the column that DBName should equal in a
	// WHERE clause, if set.
	DBNameColumn string
	// Workflow is the value that the column referred to by WorkflowColumn
	// should equal in a WHERE clause, if set.
	Workflow string
	// WorkflowColumn is the name of the column that Workflow should equal in a
	// WHERE clause, if set.
	WorkflowColumn string
}

// VReplicationQueryPlanner implements the QueryPlanner interface for queries on
// the _vt.vreplication table.
type VReplicationQueryPlanner struct {
	tmc tmclient.TabletManagerClient

	dbname   string
	workflow string
}

// NewVReplicationQueryPlanner returns a new VReplicationQueryPlanner. It is
// valid to pass empty strings for both the dbname and workflow parameters.
func NewVReplicationQueryPlanner(tmc tmclient.TabletManagerClient, workflow string, dbname string) *VReplicationQueryPlanner {
	return &VReplicationQueryPlanner{
		tmc:      tmc,
		dbname:   dbname,
		workflow: workflow,
	}
}

// PlanQuery is part of the QueryPlanner interface.
//
// For vreplication query planners, only SELECT, UPDATE, and DELETE queries are
// supported.
//
// For UPDATE queries, ORDER BY and LIMIT clauses are not supported. Attempting
// to update vreplication.id is an error.
//
// For DELETE queries, USING, PARTITION, ORDER BY, and LIMIT clauses are not
// supported.
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

// QueryParams is part of the QueryPlanner interface. A VReplicationQueryPlanner
// will attach the following WHERE clauses iff (a) DBName, Workflow are set,
// respectively, and (b) db_name and workflow do not appear in the original
// query's WHERE clause:
//
// 		WHERE (db_name = {{ .DBName }} AND)? (workflow = {{ .Workflow }} AND)? {{ .OriginalWhere }}
func (planner *VReplicationQueryPlanner) QueryParams() QueryParams {
	return QueryParams{
		DBName:         planner.dbname,
		DBNameColumn:   "db_name",
		Workflow:       planner.workflow,
		WorkflowColumn: "workflow",
	}
}

func (planner *VReplicationQueryPlanner) planDelete(del *sqlparser.Delete) (*QueryPlan, error) {
	if del.Targets != nil {
		return nil, fmt.Errorf(
			"%w: DELETE must not have USING clause (have: %v): %v",
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
			Right:    sqlparser.NewStrLiteral(params.DBName),
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

	if !hasWorkflowCol && params.Workflow != "" {
		expr := &sqlparser.ComparisonExpr{
			Left: &sqlparser.ColName{
				Name: sqlparser.NewColIdent(params.WorkflowColumn),
			},
			Operator: sqlparser.EqualOp,
			Right:    sqlparser.NewStrLiteral(params.Workflow),
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
