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
	"slices"

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
		if _, ok := node.GetColumns()[0].(*sqlparser.Nextval); ok {
			// A NEXT VALUE plan allocates from the sequence named in FROM,
			// which the planner resolves against the real schema; any WITH
			// clause or subquery is never executed. Require WRITER on the
			// sequence itself, ignoring CTE names that may shadow it.
			permissions = buildTableExprsPermissions(node.From, tableacl.WRITER, nil, permissions)
			break
		}
		permissions = buildSubqueryPermissions(node, tableacl.READER, permissions)
	case *sqlparser.Union:
		permissions = buildSubqueryPermissions(node, tableacl.READER, permissions)
	case *sqlparser.Insert:
		permissions = buildTableExprPermissions(node.Table, tableacl.WRITER, nil, permissions)
		permissions = buildSubqueryPermissions(node, tableacl.READER, permissions)
	case *sqlparser.Update:
		// A CTE joined into the statement is a read source, never a write
		// target, so the statement's own CTE names carry no WRITER permission.
		permissions = buildTableExprsPermissions(node.TableExprs, tableacl.WRITER, gatherCTEs(node.With), permissions)
		permissions = buildSubqueryPermissions(node, tableacl.READER, permissions)
	case *sqlparser.Delete:
		permissions = buildTableExprsPermissions(node.TableExprs, tableacl.WRITER, gatherCTEs(node.With), permissions)
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
			permissions = buildTableExprsPermissions(node.From, role, visibleCTEs(cteScopes), permissions)
		case *sqlparser.Delete:
			push(node.With)
		case *sqlparser.Update:
			push(node.With)
		case *sqlparser.Union:
			if node.With != nil && unionArmDeclaresCTEs(node) {
				permissions = buildUnionWithCTEArmsPermissions(node, role, visibleCTEs(cteScopes), permissions)
				return false
			}
			push(node.With)
		case *sqlparser.ValuesStatement:
			push(node.With)
		case *sqlparser.With:
			// The enclosing statement pushed this clause's names for its own
			// query block; the bodies see everything but that scope. Skip the
			// walker's own descent so the bodies are not also walked with the
			// consumer's view.
			var outer []sqlparser.IdentifierCS
			for _, scope := range cteScopes {
				if scope.with != node {
					outer = append(outer, scope.names...)
				}
			}
			permissions = buildCTEBodiesPermissions(node, role, outer, permissions)
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

// visibleCTEs flattens the scope stack into the CTE names visible to the
// current query block.
func visibleCTEs(scopes []cteScope) []sqlparser.IdentifierCS {
	var ctes []sqlparser.IdentifierCS
	for _, scope := range scopes {
		ctes = append(ctes, scope.names...)
	}
	return ctes
}

// buildCTEBodiesPermissions walks the bodies of a WITH clause. A CTE body has
// a narrower view than the query block that declares the clause: a
// non-recursive CTE is not visible inside its own definition (there the name
// is the real base table), and later siblings are never visible. Each body is
// walked with exactly the CTEs that are legal there: outer, the names in scope
// from enclosing query blocks, the earlier siblings, and the CTE itself only
// when the clause is RECURSIVE.
func buildCTEBodiesPermissions(with *sqlparser.With, role tableacl.Role, outer []sqlparser.IdentifierCS, permissions []Permission) []Permission {
	for i, cte := range with.CTEs {
		bodyScope := append([]sqlparser.IdentifierCS(nil), outer...)
		for _, sibling := range with.CTEs[:i] {
			bodyScope = append(bodyScope, sibling.ID)
		}
		if with.Recursive {
			bodyScope = append(bodyScope, cte.ID)
		}
		permissions = buildSubqueryPermissionsInScope(cte.Subquery, role, bodyScope, permissions)
	}
	return permissions
}

// buildUnionWithCTEArmsPermissions walks a union that carries a leading WITH
// clause and whose arms include one that declares a WITH of its own. MySQL
// resolves the leading CTEs in the arms before that one, and from that arm
// onward a reference to one of their names is the real table instead. Walk the
// leading CTE bodies with the enclosing scopes, then each arm in order with
// the enclosing scopes plus, until the first arm with its own WITH, the
// leading CTEs.
func buildUnionWithCTEArmsPermissions(union *sqlparser.Union, role tableacl.Role, outer []sqlparser.IdentifierCS, permissions []Permission) []Permission {
	permissions = buildCTEBodiesPermissions(union.With, role, outer, permissions)
	withLeading := append(append([]sqlparser.IdentifierCS(nil), outer...), gatherCTEs(union.With)...)
	leadingVisible := true
	for _, arm := range unionArms(union) {
		if statementDeclaresCTEs(arm) {
			leadingVisible = false
		}
		scope := outer
		if leadingVisible {
			scope = withLeading
		}
		permissions = buildSubqueryPermissionsInScope(arm, role, scope, permissions)
	}
	return permissions
}

// unionArms lists the statements joined by UNION in a chain, left to right,
// looking through nested unions that carry no WITH clause of their own. A
// nested union with a WITH clause is a parenthesized arm and stays one arm.
func unionArms(union *sqlparser.Union) []sqlparser.TableStatement {
	var arms []sqlparser.TableStatement
	for _, arm := range []sqlparser.TableStatement{union.Left, union.Right} {
		if nested, ok := arm.(*sqlparser.Union); ok && nested.With == nil {
			arms = append(arms, unionArms(nested)...)
			continue
		}
		arms = append(arms, arm)
	}
	return arms
}

// unionArmDeclaresCTEs reports whether any arm of the union chain carries a
// WITH clause of its own. Derived tables and subqueries inside an arm are
// separate query blocks and are not inspected.
func unionArmDeclaresCTEs(union *sqlparser.Union) bool {
	return slices.ContainsFunc(unionArms(union), statementDeclaresCTEs)
}

// statementDeclaresCTEs reports whether a union arm carries a WITH clause.
func statementDeclaresCTEs(stmt sqlparser.TableStatement) bool {
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		return stmt.With != nil
	case *sqlparser.ValuesStatement:
		return stmt.With != nil
	case *sqlparser.Union:
		return stmt.With != nil
	}
	return false
}

// gatherCTEs gathers the CTEs from the WITH clause, nil when there is none.
func gatherCTEs(with *sqlparser.With) []sqlparser.IdentifierCS {
	if with == nil {
		return nil
	}
	var ctes []sqlparser.IdentifierCS
	if len(with.CTEs) > 0 {
		ctes = make([]sqlparser.IdentifierCS, 0, len(with.CTEs))
	}
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
