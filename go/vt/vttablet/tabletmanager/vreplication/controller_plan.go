/*
Copyright 2018 The Vitess Authors.

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

package vreplication

import (
	"fmt"
	"strconv"

	"vitess.io/vitess/go/vt/sqlparser"
)

// controllerPlan is the plan for vreplication control statements.
type controllerPlan struct {
	opcode int
	query  string
	// delCopySate is set for deletes.
	delCopyState string
	id           int
}

const (
	insertQuery = iota
	updateQuery
	deleteQuery
	selectQuery
	reshardingJournalQuery
)

// buildControllerPlan parses the input query and returns an appropriate plan.
func buildControllerPlan(query string) (*controllerPlan, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	switch stmt := stmt.(type) {
	case *sqlparser.Insert:
		return buildInsertPlan(stmt)
	case *sqlparser.Update:
		return buildUpdatePlan(stmt)
	case *sqlparser.Delete:
		return buildDeletePlan(stmt)
	case *sqlparser.Select:
		return buildSelectPlan(stmt)
	default:
		return nil, fmt.Errorf("unsupported construct: %s", sqlparser.String(stmt))
	}
}

func buildInsertPlan(ins *sqlparser.Insert) (*controllerPlan, error) {
	switch sqlparser.String(ins.Table) {
	case reshardingJournalTableName:
		return &controllerPlan{
			opcode: reshardingJournalQuery,
			query:  sqlparser.String(ins),
		}, nil
	case vreplicationTableName:
		// no-op
	default:
		return nil, fmt.Errorf("invalid table name: %v", sqlparser.String(ins.Table))
	}
	if ins.Action != sqlparser.InsertStr {
		return nil, fmt.Errorf("unsupported construct: %v", sqlparser.String(ins))
	}
	if ins.Ignore != "" {
		return nil, fmt.Errorf("unsupported construct: %v", sqlparser.String(ins))
	}
	if ins.Partitions != nil {
		return nil, fmt.Errorf("unsupported construct: %v", sqlparser.String(ins))
	}
	if ins.OnDup != nil {
		return nil, fmt.Errorf("unsupported construct: %v", sqlparser.String(ins))
	}
	rows, ok := ins.Rows.(sqlparser.Values)
	if !ok {
		return nil, fmt.Errorf("unsupported construct: %v", sqlparser.String(ins))
	}
	if len(rows) != 1 {
		return nil, fmt.Errorf("unsupported construct: %v", sqlparser.String(ins))
	}
	row := rows[0]
	idPos := 0
	if len(ins.Columns) != 0 {
		if len(ins.Columns) != len(row) {
			return nil, fmt.Errorf("malformed statement: %v", sqlparser.String(ins))
		}
		idPos = -1
		for i, col := range ins.Columns {
			if col.EqualString("id") {
				idPos = i
				break
			}
		}
	}
	if idPos >= 0 {
		if _, ok := row[idPos].(*sqlparser.NullVal); !ok {
			return nil, fmt.Errorf("id should not have a value: %v", sqlparser.String(ins))
		}
	}
	return &controllerPlan{
		opcode: insertQuery,
		query:  sqlparser.String(ins),
	}, nil
}

func buildUpdatePlan(upd *sqlparser.Update) (*controllerPlan, error) {
	switch sqlparser.String(upd.TableExprs) {
	case reshardingJournalTableName:
		return &controllerPlan{
			opcode: reshardingJournalQuery,
			query:  sqlparser.String(upd),
		}, nil
	case vreplicationTableName:
		// no-op
	default:
		return nil, fmt.Errorf("invalid table name: %v", sqlparser.String(upd.TableExprs))
	}
	if upd.OrderBy != nil || upd.Limit != nil {
		return nil, fmt.Errorf("unsupported construct: %v", sqlparser.String(upd))
	}
	for _, expr := range upd.Exprs {
		if expr.Name.Name.EqualString("id") {
			return nil, fmt.Errorf("id cannot be changed: %v", sqlparser.String(expr))
		}
	}

	id, err := extractID(upd.Where)
	if err != nil {
		return nil, err
	}

	return &controllerPlan{
		opcode: updateQuery,
		query:  sqlparser.String(upd),
		id:     id,
	}, nil
}

func buildDeletePlan(del *sqlparser.Delete) (*controllerPlan, error) {
	switch sqlparser.String(del.TableExprs) {
	case reshardingJournalTableName:
		return &controllerPlan{
			opcode: reshardingJournalQuery,
			query:  sqlparser.String(del),
		}, nil
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

	id, err := extractID(del.Where)
	if err != nil {
		return nil, err
	}

	return &controllerPlan{
		opcode:       deleteQuery,
		query:        sqlparser.String(del),
		delCopyState: fmt.Sprintf("delete from %s where vrepl_id = %d", copySateTableName, id),
		id:           id,
	}, nil
}

func buildSelectPlan(sel *sqlparser.Select) (*controllerPlan, error) {
	switch sqlparser.String(sel.From) {
	case vreplicationTableName, reshardingJournalTableName, copySateTableName:
		return &controllerPlan{
			opcode: selectQuery,
			query:  sqlparser.String(sel),
		}, nil
	default:
		return nil, fmt.Errorf("invalid table name: %v", sqlparser.String(sel.From))
	}
}

func extractID(where *sqlparser.Where) (int, error) {
	if where == nil {
		return 0, fmt.Errorf("invalid where clause:%v", sqlparser.String(where))
	}
	comp, ok := where.Expr.(*sqlparser.ComparisonExpr)
	if !ok {
		return 0, fmt.Errorf("invalid where clause:%v", sqlparser.String(where))
	}
	if sqlparser.String(comp.Left) != "id" {
		return 0, fmt.Errorf("invalid where clause:%v", sqlparser.String(where))
	}
	if comp.Operator != sqlparser.EqualStr {
		return 0, fmt.Errorf("invalid where clause:%v", sqlparser.String(where))
	}

	id, err := strconv.Atoi(sqlparser.String(comp.Right))
	if err != nil {
		return 0, fmt.Errorf("invalid where clause:%v", sqlparser.String(where))
	}
	return id, nil
}
