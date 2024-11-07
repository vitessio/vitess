/*
Copyright 2019 The Vitess Authors.

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
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/tableacl"
)

// Permission associates the required access permission
// for each table.
type Permission struct {
	TableName string
	Role      tableacl.Role
}

// BuildPermissions builds the list of required permissions for all the
// tables referenced in a query.
func BuildPermissions(stmt sqlparser.Statement) []Permission {
	var permissions []Permission
	// All Statement types myst be covered here.
	switch node := stmt.(type) {
	case *sqlparser.Union, *sqlparser.Select:
		permissions = buildSubqueryPermissions(node, tableacl.READER, permissions)
	case *sqlparser.Insert:
		permissions = buildTableExprPermissions(node.Table, tableacl.WRITER, nil, permissions)
		permissions = buildSubqueryPermissions(node, tableacl.READER, permissions)
	case *sqlparser.Update:
		permissions = buildTableExprsPermissions(node.TableExprs, tableacl.WRITER, nil, permissions)
		permissions = buildSubqueryPermissions(node, tableacl.READER, permissions)
	case *sqlparser.Delete:
		permissions = buildTableExprsPermissions(node.TableExprs, tableacl.WRITER, nil, permissions)
		permissions = buildSubqueryPermissions(node, tableacl.READER, permissions)
	case sqlparser.DDLStatement:
		for _, t := range node.AffectedTables() {
			permissions = buildTableNamePermissions(t, tableacl.ADMIN, nil, permissions)
		}
	case
		*sqlparser.AlterMigration,
		*sqlparser.RevertMigration,
		*sqlparser.ShowMigrationLogs,
		*sqlparser.ShowThrottledApps,
		*sqlparser.ShowThrottlerStatus:
		permissions = []Permission{} // TODO(shlomi) what are the correct permissions here? Table is unknown
	case *sqlparser.Flush:
		for _, t := range node.TableNames {
			permissions = buildTableNamePermissions(t, tableacl.ADMIN, nil, permissions)
		}
	case *sqlparser.Analyze:
		permissions = buildTableNamePermissions(node.Table, tableacl.WRITER, nil, permissions)
	case *sqlparser.OtherAdmin, *sqlparser.CallProc, *sqlparser.Begin, *sqlparser.Commit, *sqlparser.Rollback,
		*sqlparser.Load, *sqlparser.Savepoint, *sqlparser.Release, *sqlparser.SRollback, *sqlparser.Set, *sqlparser.Show, sqlparser.Explain,
		*sqlparser.UnlockTables:
		// no op
	default:
		panic(fmt.Errorf("BUG: unexpected statement type: %T", node))
	}
	return permissions
}

func buildSubqueryPermissions(stmt sqlparser.Statement, role tableacl.Role, permissions []Permission) []Permission {
	var cteScopes [][]sqlparser.IdentifierCS
	sqlparser.Rewrite(stmt, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.Select:
			if node.With != nil {
				cteScopes = append(cteScopes, gatherCTEs(node.With))
			}
			var ctes []sqlparser.IdentifierCS
			for _, cteScope := range cteScopes {
				ctes = append(ctes, cteScope...)
			}
			permissions = buildTableExprsPermissions(node.From, role, ctes, permissions)
		case *sqlparser.Delete:
			if node.With != nil {
				cteScopes = append(cteScopes, gatherCTEs(node.With))
			}
		case *sqlparser.Update:
			if node.With != nil {
				cteScopes = append(cteScopes, gatherCTEs(node.With))
			}
		case *sqlparser.Union:
			if node.With != nil {
				cteScopes = append(cteScopes, gatherCTEs(node.With))
			}
		}
		return true
	}, func(cursor *sqlparser.Cursor) bool {
		// When we encounter a With expression coming up, we should remove
		// the last value from the cte scopes to ensure we none of the outer
		// elements of the query see this table name.
		_, isWith := cursor.Node().(*sqlparser.With)
		if isWith {
			cteScopes = cteScopes[:len(cteScopes)-1]
		}
		return true
	})
	return permissions
}

// gatherCTEs gathers the CTEs from the WITH clause.
func gatherCTEs(with *sqlparser.With) []sqlparser.IdentifierCS {
	var ctes []sqlparser.IdentifierCS
	for _, cte := range with.CTEs {
		ctes = append(ctes, cte.ID)
	}
	return ctes
}

func buildTableExprsPermissions(node []sqlparser.TableExpr, role tableacl.Role, ctes []sqlparser.IdentifierCS, permissions []Permission) []Permission {
	for _, node := range node {
		permissions = buildTableExprPermissions(node, role, ctes, permissions)
	}
	return permissions
}

func buildTableExprPermissions(node sqlparser.TableExpr, role tableacl.Role, ctes []sqlparser.IdentifierCS, permissions []Permission) []Permission {
	switch node := node.(type) {
	case *sqlparser.AliasedTableExpr:
		// An AliasedTableExpr can also be a derived table, but we should skip them here
		// because the buildSubQueryPermissions walker will catch them and extract
		// the corresponding table names.
		if tblName, ok := node.Expr.(sqlparser.TableName); ok {
			permissions = buildTableNamePermissions(tblName, role, ctes, permissions)
		}
	case *sqlparser.ParenTableExpr:
		permissions = buildTableExprsPermissions(node.Exprs, role, ctes, permissions)
	case *sqlparser.JoinTableExpr:
		permissions = buildTableExprPermissions(node.LeftExpr, role, ctes, permissions)
		permissions = buildTableExprPermissions(node.RightExpr, role, ctes, permissions)
	}
	return permissions
}

func buildTableNamePermissions(node sqlparser.TableName, role tableacl.Role, ctes []sqlparser.IdentifierCS, permissions []Permission) []Permission {
	tableName := node.Name.String()
	// Check whether this table is a cte or not.
	// If the table name is qualified, then it cannot be a cte.
	if node.Qualifier.IsEmpty() {
		for _, cte := range ctes {
			if cte.String() == tableName {
				return permissions
			}
		}
	}
	permissions = append(permissions, Permission{
		TableName: tableName,
		Role:      role,
	})
	return permissions
}
