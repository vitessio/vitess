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

	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func transformToLogicalPlan(tree joinTree, semTable *semantics.SemTable) (logicalPlan, error) {
	switch n := tree.(type) {
	case *routePlan:
		return transformRoutePlan(n)

	case *joinPlan:
		return transformJoinPlan(n, semTable)
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: unknown type encountered: %T", tree)
}

func transformJoinPlan(n *joinPlan, semTable *semantics.SemTable) (*join2, error) {
	lhsColList := extractColumnsNeededFromLHS(n, semTable, n.lhs.solves())

	var lhsColExpr []*sqlparser.AliasedExpr
	for _, col := range lhsColList {
		lhsColExpr = append(lhsColExpr, &sqlparser.AliasedExpr{
			Expr: col,
		})
	}

	lhs, err := transformToLogicalPlan(n.lhs, semTable)
	if err != nil {
		return nil, err
	}
	offset, err := pushProjection(lhsColExpr, lhs, semTable)
	if err != nil {
		return nil, err
	}

	vars := map[string]int{}

	for _, col := range lhsColList {
		vars[col.CompliantName("")] = offset
		offset++
	}

	rhs, err := transformToLogicalPlan(n.rhs, semTable)
	if err != nil {
		return nil, err
	}

	err = pushPredicate(n.predicates, rhs, semTable)
	if err != nil {
		return nil, err
	}

	return &join2{
		Left:  lhs,
		Right: rhs,
		Vars:  vars,
	}, nil
}

func transformRoutePlan(n *routePlan) (*route, error) {
	var tablesForSelect sqlparser.TableExprs
	tableNameMap := map[string]interface{}{}

	sort.Sort(n.tables)
	for _, t := range n.tables {
		alias := sqlparser.AliasedTableExpr{
			Expr: sqlparser.TableName{
				Name: t.vtable.Name,
			},
			Partitions: nil,
			As:         t.qtable.alias.As,
			Hints:      nil,
		}
		tablesForSelect = append(tablesForSelect, &alias)
		tableNameMap[sqlparser.String(t.qtable.table.Name)] = nil
	}

	predicates := n.Predicates()
	var where *sqlparser.Where
	if predicates != nil {
		where = &sqlparser.Where{Expr: predicates, Type: sqlparser.WhereClause}
	}
	var values []sqltypes.PlanValue
	if len(n.conditions) == 1 {
		value, err := sqlparser.NewPlanValue(n.conditions[0].(*sqlparser.ComparisonExpr).Right)
		if err != nil {
			return nil, err
		}
		values = []sqltypes.PlanValue{value}
	}
	var singleColumn vindexes.SingleColumn
	if n.vindex != nil {
		singleColumn = n.vindex.(vindexes.SingleColumn)
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
			Values:    values,
		},
		Select: &sqlparser.Select{
			From:  tablesForSelect,
			Where: where,
		},
		solvedTables: n.solved,
	}, nil
}

func extractColumnsNeededFromLHS(n *joinPlan, semTable *semantics.SemTable, lhsSolves semantics.TableSet) []*sqlparser.ColName {
	lhsColMap := map[*sqlparser.ColName]sqlparser.Argument{}
	for _, predicate := range n.predicates {
		sqlparser.Rewrite(predicate, func(cursor *sqlparser.Cursor) bool {
			switch node := cursor.Node().(type) {
			case *sqlparser.ColName:
				if semTable.Dependencies(node).IsSolvedBy(lhsSolves) {
					arg := sqlparser.NewArgument([]byte(":" + node.CompliantName("")))
					lhsColMap[node] = arg
					cursor.Replace(arg)
				}
			}
			return true
		}, nil)
	}

	var lhsColList []*sqlparser.ColName
	for col := range lhsColMap {
		lhsColList = append(lhsColList, col)
	}
	return lhsColList
}
