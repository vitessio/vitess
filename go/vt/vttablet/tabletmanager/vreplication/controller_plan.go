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

	"vitess.io/vitess/go/vt/sqlparser"
)

// controllerPlan is the plan for vreplication control statements.
type controllerPlan struct {
	query  string
	opcode int

	// numInserts is set for insertQuery.
	numInserts int

	// selector and applier are set for updateQuery and deleteQuery.
	selector string
	applier  *sqlparser.ParsedQuery

	// delCopyState is set of deletes.
	delCopyState *sqlparser.ParsedQuery
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
	var plan *controllerPlan
	switch stmt := stmt.(type) {
	case *sqlparser.Insert:
		plan, err = buildInsertPlan(stmt)
	case *sqlparser.Update:
		plan, err = buildUpdatePlan(stmt)
	case *sqlparser.Delete:
		plan, err = buildDeletePlan(stmt)
	case *sqlparser.Select:
		plan, err = buildSelectPlan(stmt)
	default:
		return nil, fmt.Errorf("unsupported construct: %s", sqlparser.String(stmt))
	}
	if err != nil {
		return nil, err
	}
	plan.query = query
	return plan, nil
}

func buildInsertPlan(ins *sqlparser.Insert) (*controllerPlan, error) {
	switch sqlparser.String(ins.Table) {
	case reshardingJournalTableName:
		return &controllerPlan{
			opcode: reshardingJournalQuery,
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
	idPos := 0
	if len(ins.Columns) != 0 {
		idPos = -1
		for i, col := range ins.Columns {
			if col.EqualString("id") {
				idPos = i
				break
			}
		}
	}
	if idPos >= 0 {
		for _, row := range rows {
			if idPos >= len(row) {
				return nil, fmt.Errorf("malformed statement: %v", sqlparser.String(ins))
			}
			if _, ok := row[idPos].(*sqlparser.NullVal); !ok {
				return nil, fmt.Errorf("id should not have a value: %v", sqlparser.String(ins))
			}
		}
	}
	return &controllerPlan{
		opcode:     insertQuery,
		numInserts: len(rows),
	}, nil
}

func buildUpdatePlan(upd *sqlparser.Update) (*controllerPlan, error) {
	switch sqlparser.String(upd.TableExprs) {
	case reshardingJournalTableName:
		return &controllerPlan{
			opcode: reshardingJournalQuery,
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

	buf1 := sqlparser.NewTrackedBuffer(nil)
	buf1.Myprintf("select id from %s%v", vreplicationTableName, upd.Where)
	upd.Where = &sqlparser.Where{
		Type: sqlparser.WhereStr,
		Expr: &sqlparser.ComparisonExpr{
			Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("id")},
			Operator: sqlparser.InStr,
			Right:    sqlparser.ListArg("::ids"),
		},
	}

	buf2 := sqlparser.NewTrackedBuffer(nil)
	buf2.Myprintf("%v", upd)

	return &controllerPlan{
		opcode:   updateQuery,
		selector: buf1.String(),
		applier:  buf2.ParsedQuery(),
	}, nil
}

func buildDeletePlan(del *sqlparser.Delete) (*controllerPlan, error) {
	switch sqlparser.String(del.TableExprs) {
	case reshardingJournalTableName:
		return &controllerPlan{
			opcode: reshardingJournalQuery,
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

	buf1 := sqlparser.NewTrackedBuffer(nil)
	buf1.Myprintf("select id from %s%v", vreplicationTableName, del.Where)
	del.Where = &sqlparser.Where{
		Type: sqlparser.WhereStr,
		Expr: &sqlparser.ComparisonExpr{
			Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("id")},
			Operator: sqlparser.InStr,
			Right:    sqlparser.ListArg("::ids"),
		},
	}

	buf2 := sqlparser.NewTrackedBuffer(nil)
	buf2.Myprintf("%v", del)

	copyStateWhere := &sqlparser.Where{
		Type: sqlparser.WhereStr,
		Expr: &sqlparser.ComparisonExpr{
			Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("vrepl_id")},
			Operator: sqlparser.InStr,
			Right:    sqlparser.ListArg("::ids"),
		},
	}
	buf3 := sqlparser.NewTrackedBuffer(nil)
	buf3.Myprintf("delete from %s%v", copyStateTableName, copyStateWhere)

	return &controllerPlan{
		opcode:       deleteQuery,
		selector:     buf1.String(),
		applier:      buf2.ParsedQuery(),
		delCopyState: buf3.ParsedQuery(),
	}, nil
}

func buildSelectPlan(sel *sqlparser.Select) (*controllerPlan, error) {
	switch sqlparser.String(sel.From) {
	case vreplicationTableName, reshardingJournalTableName, copyStateTableName:
		return &controllerPlan{
			opcode: selectQuery,
		}, nil
	default:
		return nil, fmt.Errorf("invalid table name: %v", sqlparser.String(sel.From))
	}
}
