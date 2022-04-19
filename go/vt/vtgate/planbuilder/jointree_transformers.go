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

package planbuilder

import (
	"sort"
	"strings"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/vterrors"
)

func transformToLogicalPlan(tree joinTree, semTable *semantics.SemTable) (logicalPlan, error) {
	switch n := tree.(type) {
	case *routePlan:
		return transformRoutePlan(n)

	case *joinPlan:
		return transformJoinPlan(n, semTable)
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unknown type encountered: %T", tree)
}

func transformJoinPlan(n *joinPlan, semTable *semantics.SemTable) (logicalPlan, error) {
	lhs, err := transformToLogicalPlan(n.lhs, semTable)
	if err != nil {
		return nil, err
	}
	rhs, err := transformToLogicalPlan(n.rhs, semTable)
	if err != nil {
		return nil, err
	}
	opCode := engine.InnerJoin
	if n.outer {
		opCode = engine.LeftJoin
	}
	return &joinGen4{
		Left:   lhs,
		Right:  rhs,
		Cols:   n.columns,
		Vars:   n.vars,
		Opcode: opCode,
	}, nil
}

func transformRoutePlan(n *routePlan) (*route, error) {
	var tablesForSelect sqlparser.TableExprs
	tableNameMap := map[string]interface{}{}

	sort.Sort(n.tables)
	for _, t := range n.tables {
		tableExpr, err := relToTableExpr(t)
		if err != nil {
			return nil, err
		}
		tablesForSelect = append(tablesForSelect, tableExpr)
		for _, name := range t.tableNames() {
			tableNameMap[name] = nil
		}
	}

	for _, predicate := range n.vindexPredicates {
		switch predicate := predicate.(type) {
		case *sqlparser.ComparisonExpr:
			if predicate.Operator == sqlparser.InOp {
				switch predicate.Left.(type) {
				case *sqlparser.ColName:
					predicate.Right = sqlparser.ListArg(engine.ListVarName)
				}
			}
		}
	}

	for _, leftJoin := range n.leftJoins {
		var lft sqlparser.TableExpr
		if len(tablesForSelect) == 1 {
			lft = tablesForSelect[0]
		} else {
			lft = &sqlparser.ParenTableExpr{Exprs: tablesForSelect}
		}

		rightExpr, err := relToTableExpr(leftJoin.right)
		if err != nil {
			return nil, err
		}
		joinExpr := &sqlparser.JoinTableExpr{
			Join: sqlparser.LeftJoinType,
			Condition: sqlparser.JoinCondition{
				On: leftJoin.pred,
			},
			RightExpr: rightExpr,
			LeftExpr:  lft,
		}
		tablesForSelect = sqlparser.TableExprs{joinExpr}
		// todo: add table
		// tableNameMap[sqlparser.String()] = nil
	}

	predicates := n.Predicates()
	var where *sqlparser.Where
	if predicates != nil {
		where = &sqlparser.Where{Expr: predicates, Type: sqlparser.WhereClause}
	}

	var singleColumn vindexes.SingleColumn
	if n.vindex != nil {
		singleColumn = n.vindex.(vindexes.SingleColumn)
	}

	var expressions sqlparser.SelectExprs
	for _, col := range n.columns {
		expressions = append(expressions, &sqlparser.AliasedExpr{
			Expr: col,
			As:   sqlparser.ColIdent{},
		})
	}

	var tableNames []string
	for name := range tableNameMap {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	return &route{
		eroute: &engine.Route{
			Opcode:    n.routeOpCode,
			TableName: strings.Join(tableNames, ", "),
			Keyspace:  n.keyspace,
			Vindex:    singleColumn,
			Values:    n.vindexValues,
		},
		Select: &sqlparser.Select{
			SelectExprs: expressions,
			From:        tablesForSelect,
			Where:       where,
		},
		tables: n.solved,
	}, nil
}

func relToTableExpr(t relation) (sqlparser.TableExpr, error) {
	switch t := t.(type) {
	case *routeTable:
		return &sqlparser.AliasedTableExpr{
			Expr: sqlparser.TableName{
				Name: t.vtable.Name,
			},
			Partitions: nil,
			As:         t.qtable.Alias.As,
			Hints:      nil,
		}, nil
	case parenTables:
		tables := sqlparser.TableExprs{}
		for _, t := range t {
			tableExpr, err := relToTableExpr(t)
			if err != nil {
				return nil, err
			}
			tables = append(tables, tableExpr)
		}
		return &sqlparser.ParenTableExpr{Exprs: tables}, nil
	case *leJoin:
		lExpr, err := relToTableExpr(t.lhs)
		if err != nil {
			return nil, err
		}
		rExpr, err := relToTableExpr(t.rhs)
		if err != nil {
			return nil, err
		}
		return &sqlparser.JoinTableExpr{
			LeftExpr:  lExpr,
			Join:      sqlparser.NormalJoinType,
			RightExpr: rExpr,
			Condition: sqlparser.JoinCondition{
				On: t.pred,
			},
		}, nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unknown relation type: %T", t)
	}
}
