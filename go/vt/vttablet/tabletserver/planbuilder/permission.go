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
	case *sqlparser.Select:
		role := tableacl.READER
		if _, ok := node.GetColumns()[0].(*sqlparser.Nextval); ok {
			role = tableacl.WRITER
		}
		permissions = buildSubqueryPermissions(node, role, permissions)
	case *sqlparser.Union:
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
	return buildSubqueryPermissionsInScope(stmt, role, nil, permissions)
}

// cteScope is the set of CTE names one query block brings into scope. with is
// the clause that declared them, or nil for names inherited from an enclosing
// query.
type cteScope struct {
	with  *sqlparser.With
	names []sqlparser.IdentifierCS
}

// buildSubqueryPermissionsInScope walks stmt and collects the permissions for
// every real table it references. outerCTEs are the CTE names that are already
// in scope from an enclosing query (used when recursing into a CTE body).
//
// A statement's WITH clause is pushed when the walker enters the statement and
// popped when it leaves, so the whole query block, including derived tables
// and subqueries walked after the WITH clause itself, sees the CTE names. The
// CTE bodies are walked separately with the narrower scope that is legal
// inside a body.
func buildSubqueryPermissionsInScope(stmt sqlparser.Statement, role tableacl.Role, outerCTEs []sqlparser.IdentifierCS, permissions []Permission) []Permission {
	var cteScopes []cteScope
	if len(outerCTEs) > 0 {
		cteScopes = append(cteScopes, cteScope{names: outerCTEs})
	}
	push := func(with *sqlparser.With) {
		if with != nil {
			cteScopes = append(cteScopes, cteScope{with: with, names: gatherCTEs(with)})
		}
	}
	pop := func(with *sqlparser.With) {
		if with != nil && len(cteScopes) > 0 && cteScopes[len(cteScopes)-1].with == with {
			cteScopes = cteScopes[:len(cteScopes)-1]
		}
	}
	sqlparser.Rewrite(stmt, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.Select:
			push(node.With)
			var ctes []sqlparser.IdentifierCS
			for _, scope := range cteScopes {
				ctes = append(ctes, scope.names...)
			}
			permissions = buildTableExprsPermissions(node.From, role, ctes, permissions)
		case *sqlparser.Delete:
			push(node.With)
		case *sqlparser.Update:
			push(node.With)
		case *sqlparser.Union:
			push(node.With)
		case *sqlparser.ValuesStatement:
			push(node.With)
		case *sqlparser.With:
			// A CTE body has a narrower view than the query block that
			// declares it: a non-recursive CTE is not visible inside its own
			// definition (there the name is the real base table), and later
			// siblings are never visible. Walk each body with exactly the CTEs
			// that are legal there: everything in scope from enclosing query
			// blocks, the earlier siblings, and the CTE itself only when the
			// clause is RECURSIVE. Skip the walker's own descent so the bodies
			// are not also walked with the consumer's view.
			var outer []sqlparser.IdentifierCS
			for _, scope := range cteScopes {
				if scope.with != node {
					outer = append(outer, scope.names...)
				}
			}
			for i, cte := range node.CTEs {
				bodyScope := append([]sqlparser.IdentifierCS(nil), outer...)
				for _, sibling := range node.CTEs[:i] {
					bodyScope = append(bodyScope, sibling.ID)
				}
				if node.Recursive {
					bodyScope = append(bodyScope, cte.ID)
				}
				permissions = buildSubqueryPermissionsInScope(cte.Subquery, role, bodyScope, permissions)
			}
			return false
		}
		return true
	}, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.Select:
			pop(node.With)
		case *sqlparser.Delete:
			pop(node.With)
		case *sqlparser.Update:
			pop(node.With)
		case *sqlparser.Union:
			pop(node.With)
		case *sqlparser.ValuesStatement:
			pop(node.With)
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
