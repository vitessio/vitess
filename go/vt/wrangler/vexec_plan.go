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
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
)

type vexecPlan struct {
	query       string
	opcode      int
	parsedQuery *sqlparser.ParsedQuery
}

const (
	updateQuery = iota
	deleteQuery
	selectQuery
)

func (vx *vexec) buildVExecPlan() (*vexecPlan, error) {
	stmt, err := sqlparser.Parse(vx.query)
	if err != nil {
		return nil, err
	}
	var plan *vexecPlan
	switch stmt := stmt.(type) {
	case *sqlparser.Update:
		plan, err = vx.buildUpdatePlan(stmt)
	case *sqlparser.Delete:
		plan, err = vx.buildDeletePlan(stmt)
	case *sqlparser.Select:
		plan, err = vx.buildSelectPlan(stmt)
	default:
		return nil, fmt.Errorf("query not supported by vexec: %s", sqlparser.String(stmt))
	}

	if err != nil {
		return nil, err
	}
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

func (vx *vexec) addDefaultWheres(where *sqlparser.Where) *sqlparser.Where {
	cols := vx.analyzeWhere(where)
	var hasDBName, hasWorkflow bool
	for _, col := range cols {
		if col == "db_name" {
			hasDBName = true
		} else if col == "workflow" {
			hasWorkflow = true
		}
	}
	newWhere := where
	if !hasDBName {
		expr := &sqlparser.ComparisonExpr{
			Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("db_name")},
			Operator: sqlparser.EqualStr,
			Right:    sqlparser.NewStrVal([]byte(vx.masters[0].DbName())),
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
			Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("workflow")},
			Operator: sqlparser.EqualStr,
			Right:    sqlparser.NewStrVal([]byte(vx.workflow)),
		}
		newWhere.Expr = &sqlparser.AndExpr{
			Left:  newWhere.Expr,
			Right: expr,
		}
	}
	return newWhere
}

func (vx *vexec) buildUpdatePlan(upd *sqlparser.Update) (*vexecPlan, error) {
	switch sqlparser.String(upd.TableExprs) {
	case vreplicationTableName:
		// no-op
	default:
		return nil, fmt.Errorf("vexec does not support: %v", sqlparser.String(upd.TableExprs))
	}
	if upd.OrderBy != nil || upd.Limit != nil {
		return nil, fmt.Errorf("unsupported construct: %v", sqlparser.String(upd))
	}
	for _, expr := range upd.Exprs {
		if expr.Name.Name.EqualString("id") {
			return nil, fmt.Errorf("id cannot be changed: %v", sqlparser.String(expr))
		}
	}

	upd.Where = vx.addDefaultWheres(upd.Where)

	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", upd)

	return &vexecPlan{
		opcode:      updateQuery,
		parsedQuery: buf.ParsedQuery(),
	}, nil
}

func (vx *vexec) buildDeletePlan(del *sqlparser.Delete) (*vexecPlan, error) {
	switch sqlparser.String(del.TableExprs) {
	case vreplicationTableName:
		// no-op
	default:
		return nil, fmt.Errorf("invalid table name: %v", sqlparser.String(del.TableExprs))
	}
	if del.Targets != nil {
		return nil, fmt.Errorf("unsupported construct: %v", sqlparser.String(del))
	}
	if del.Partitions != nil {
		return nil, fmt.Errorf("unsupported construct: %v", sqlparser.String(del))
	}
	if del.OrderBy != nil || del.Limit != nil {
		return nil, fmt.Errorf("unsupported construct: %v", sqlparser.String(del))
	}

	del.Where = vx.addDefaultWheres(del.Where)

	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", del)

	return &vexecPlan{
		opcode:      deleteQuery,
		parsedQuery: buf.ParsedQuery(),
	}, nil
}

func (vx *vexec) buildSelectPlan(sel *sqlparser.Select) (*vexecPlan, error) {
	switch sqlparser.String(sel.From) {
	case vreplicationTableName:
		// no-op
	default:
		return nil, fmt.Errorf("invalid table name: %v", sqlparser.String(sel.From))
	}
	sel.Where = vx.addDefaultWheres(sel.Where)
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", sel)

	return &vexecPlan{
		opcode:      selectQuery,
		parsedQuery: buf.ParsedQuery(),
	}, nil
}
