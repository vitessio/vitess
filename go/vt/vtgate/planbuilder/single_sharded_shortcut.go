package planbuilder

import (
	"sort"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func unshardedShortcut(stmt sqlparser.SelectStatement, ks *vindexes.Keyspace, semTable *semantics.SemTable) (logicalPlan, error) {
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

	tableNames, err := getTableNames(semTable)
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
	if err := plan.WireupGen4(semTable); err != nil {
		return nil, err
	}
	return plan, nil
}

func getTableNames(semTable *semantics.SemTable) ([]string, error) {
	tableNameMap := map[string]interface{}{}

	for _, tableInfo := range semTable.Tables {
		tblObj := tableInfo.GetVindexTable()
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
