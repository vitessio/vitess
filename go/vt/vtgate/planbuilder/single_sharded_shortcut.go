/*
Copyright 2022 The Vitess Authors.

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

	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func unshardedShortcut(ctx *plancontext.PlanningContext, stmt sqlparser.SelectStatement, ks *vindexes.Keyspace) (logicalPlan, error) {
	// this method is used when the query we are handling has all tables in the same unsharded keyspace
	sqlparser.Rewrite(stmt, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case sqlparser.SelectExpr:
			removeKeyspaceFromSelectExpr(node)
		case sqlparser.TableName:
			cursor.Replace(sqlparser.TableName{
				Name: node.Name,
			})
		}
		return true
	}, nil)

	tableNames, err := getTableNames(ctx.SemTable)
	if err != nil {
		return nil, err
	}
	plan := &routeGen4{
		eroute: &engine.Route{
			RoutingParameters: &engine.RoutingParameters{
				Opcode:   engine.Unsharded,
				Keyspace: ks,
			},
			TableName: strings.Join(tableNames, ", "),
		},
		Select: stmt,
	}

	if err := plan.WireupGen4(ctx); err != nil {
		return nil, err
	}
	return plan, nil
}

func getTableNames(semTable *semantics.SemTable) ([]string, error) {
	tableNameMap := map[string]any{}

	for _, tableInfo := range semTable.Tables {
		tblObj := tableInfo.GetVindexTable()
		if tblObj == nil {
			// probably a derived table
			continue
		}
		var name string
		if tableInfo.IsInfSchema() {
			name = "tableName"
		} else {
			name = sqlparser.String(tblObj.Name)
		}
		tableNameMap[name] = nil
	}

	var tableNames []string
	for name := range tableNameMap {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)
	return tableNames, nil
}
