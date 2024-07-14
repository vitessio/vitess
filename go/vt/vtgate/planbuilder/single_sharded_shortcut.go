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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var emptyIdentifier = sqlparser.NewIdentifierCS("")

func selectUnshardedShortcut(ctx *plancontext.PlanningContext, stmt sqlparser.SelectStatement, ks *vindexes.Keyspace) (engine.Primitive, []sqlparser.TableName, error) {
	ctx.AddNodeTransformer(removeKeyspacesTransformer())

	tableNames, err := getTableNames(ctx.SemTable)
	if err != nil {
		return nil, nil, err
	}
	eroute := &engine.Route{
		RoutingParameters: &engine.RoutingParameters{
			Opcode:   engine.Unsharded,
			Keyspace: ks,
		},
		TableName: strings.Join(escapedTableNames(tableNames), ", "),
	}

	prim, err := WireupRoute(ctx, eroute, stmt)
	if err != nil {
		return nil, nil, err
	}
	return prim, operators.QualifiedTableNames(ks, tableNames), nil
}

func escapedTableNames(tableNames []sqlparser.TableName) []string {
	escaped := make([]string, len(tableNames))
	for i, tableName := range tableNames {
		escaped[i] = sqlparser.String(tableName)
	}
	return escaped
}

func getTableNames(semTable *semantics.SemTable) ([]sqlparser.TableName, error) {
	tableNameMap := make(map[string]sqlparser.TableName)

	for _, tableInfo := range semTable.Tables {
		tblObj := tableInfo.GetVindexTable()
		if tblObj == nil {
			// probably a derived table
			continue
		}
		if tableInfo.IsInfSchema() {
			tableNameMap["tableName"] = sqlparser.TableName{
				Name: sqlparser.NewIdentifierCS("tableName"),
			}
		} else {
			tableNameMap[sqlparser.String(tblObj.Name)] = sqlparser.TableName{
				Name: tblObj.Name,
			}
		}
	}
	var keys []string
	for k := range tableNameMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var tableNames []sqlparser.TableName
	for _, k := range keys {
		tableNames = append(tableNames, tableNameMap[k])
	}
	return tableNames, nil
}

// removeKeyspacesTransformer returns a NodeTransformer that produces
// transformed SQLNode stripped of keyspace information.
func removeKeyspacesTransformer() sqlparser.NodeTransformer {
	var inAliasedExpr bool
	var inSelectExpr bool

	return func(node sqlparser.SQLNode, done func(node sqlparser.SQLNode)) {
		switch node := node.(type) {
		case *sqlparser.AliasedExpr:
			inAliasedExpr = true
			done(node)
			inAliasedExpr = false
		case *sqlparser.ColName:
			if inSelectExpr && inAliasedExpr {
				if !node.Qualifier.Qualifier.IsEmpty() {
					node := sqlparser.Clone(node)
					node.Qualifier.Qualifier = emptyIdentifier
				}
			}
			done(node)
		case sqlparser.SelectExpr:
			inSelectExpr = true
			done(node)
			inSelectExpr = false
		case sqlparser.TableName:
			if !node.Qualifier.IsEmpty() {
				node = sqlparser.Clone(node)
				node.Qualifier = emptyIdentifier
			}
			done(node)
		default:
			done(node)
		}
	}
}
