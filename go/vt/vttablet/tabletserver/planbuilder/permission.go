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
		permissions = buildTableNamePermissions(node.Table, tableacl.WRITER, permissions)
		permissions = buildSubqueryPermissions(node, tableacl.READER, permissions)
	case *sqlparser.Update:
		permissions = buildTableExprsPermissions(node.TableExprs, tableacl.WRITER, permissions)
		permissions = buildSubqueryPermissions(node, tableacl.READER, permissions)
	case *sqlparser.Delete:
		permissions = buildTableExprsPermissions(node.TableExprs, tableacl.WRITER, permissions)
		permissions = buildSubqueryPermissions(node, tableacl.READER, permissions)
	case *sqlparser.Set, *sqlparser.Show, *sqlparser.OtherRead:
		// no-op
	case *sqlparser.DDL:
		for _, t := range node.AffectedTables() {
			permissions = buildTableNamePermissions(t, tableacl.ADMIN, permissions)
		}
	case *sqlparser.OtherAdmin:
		// no op
	case *sqlparser.Begin, *sqlparser.Commit, *sqlparser.Rollback:
		// no op
	default:
		panic(fmt.Errorf("BUG: unexpected statement type: %T", node))
	}
	return permissions
}

func buildSubqueryPermissions(stmt sqlparser.Statement, role tableacl.Role, permissions []Permission) []Permission {
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node := node.(type) {
		case *sqlparser.Select:
			permissions = buildTableExprsPermissions(node.From, role, permissions)
		case sqlparser.TableExprs:
			return false, nil
		}
		return true, nil
	}, stmt)
	return permissions
}

func buildTableExprsPermissions(node sqlparser.TableExprs, role tableacl.Role, permissions []Permission) []Permission {
	for _, node := range node {
		permissions = buildTableExprPermissions(node, role, permissions)
	}
	return permissions
}

func buildTableExprPermissions(node sqlparser.TableExpr, role tableacl.Role, permissions []Permission) []Permission {
	switch node := node.(type) {
	case *sqlparser.AliasedTableExpr:
		// An AliasedTableExpr can also be a subquery, but we should skip them here
		// because the buildSubQueryPermissions walker will catch them and extract
		// the corresponding table names.
		switch node := node.Expr.(type) {
		case sqlparser.TableName:
			permissions = buildTableNamePermissions(node, role, permissions)
		case *sqlparser.Subquery:
			permissions = buildSubqueryPermissions(node.Select, role, permissions)
		}
	case *sqlparser.ParenTableExpr:
		permissions = buildTableExprsPermissions(node.Exprs, role, permissions)
	case *sqlparser.JoinTableExpr:
		permissions = buildTableExprPermissions(node.LeftExpr, role, permissions)
		permissions = buildTableExprPermissions(node.RightExpr, role, permissions)
	}
	return permissions
}

func buildTableNamePermissions(node sqlparser.TableName, role tableacl.Role, permissions []Permission) []Permission {
	permissions = append(permissions, Permission{
		TableName: node.Name.String(),
		Role:      role,
	})
	return permissions
}
