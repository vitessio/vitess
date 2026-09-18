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

type (
	// Permission associates the required access permission
	// for each table.
	Permission struct {
		TableName string
		Role      tableacl.Role
	}

	// cteScope is the set of CTE names one query block brings into scope.
	// with is the clause that declared them, or nil for names inherited from
	// an enclosing query.
	cteScope struct {
		with  *sqlparser.With
		names []sqlparser.IdentifierCS
	}
)

// BuildPermissions builds the list of required permissions for all the
// tables referenced in a query. tablesUndetermined reports a statement whose
// tables the parser discards, so that no permission could be derived for it;
// the executor fails closed on such a statement under strict table ACL.
func BuildPermissions(stmt sqlparser.Statement) (permissions []Permission, tablesUndetermined bool) {
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
	case *sqlparser.OtherAdmin, *sqlparser.CallProc, *sqlparser.Load:
		// The parser discards the tables these statements touch: DO's
		// expressions and the tables REPAIR and OPTIMIZE name (OtherAdmin), a
		// procedure body (CALL), and LOAD DATA's target table (a write, with
		// vt_app holding the FILE privilege by default). No permission can be
		// derived, so the statement is flagged and the executor denies it
		// under strict table ACL rather than skip the check. A new statement
		// type the parser leaves opaque belongs here, not in the arm below.
		// This flags whole statements only. A stored function is invoked
		// inside an expression rather than CALLed, so `select f()` is checked
		// on the tables it names and what f's body touches is not.
		tablesUndetermined = true
	case *sqlparser.Begin, *sqlparser.Commit, *sqlparser.Rollback,
		*sqlparser.Savepoint, *sqlparser.Release, *sqlparser.SRollback, *sqlparser.Set, *sqlparser.Show, sqlparser.Explain,
		*sqlparser.UnlockTables:
		// no op
	default:
		panic(fmt.Errorf("BUG: unexpected statement type: %T", node))
	}
	return permissions, tablesUndetermined
}

func buildSubqueryPermissions(stmt sqlparser.Statement, role tableacl.Role, permissions []Permission) []Permission {
	return buildSubqueryPermissionsInScope(stmt, role, nil, permissions)
}

// buildSubqueryPermissionsInScope walks node and collects the permissions for
// every real table it references. outerCTEs are the CTE names that are already
// in scope from an enclosing query (used when recursing into a CTE body).
//
// A statement's WITH clause is pushed when the walker enters the statement and
// popped when it leaves, so the whole query block, including derived tables
// and subqueries walked after the WITH clause itself, sees the CTE names. The
// CTE bodies are walked separately with the narrower scope that is legal
// inside a body.
func buildSubqueryPermissionsInScope(node sqlparser.SQLNode, role tableacl.Role, outerCTEs []sqlparser.IdentifierCS, permissions []Permission) []Permission {
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
	sqlparser.Rewrite(node, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.Select:
			push(node.With)
			permissions = buildTableExprsPermissions(node.From, role, visibleCTEs(cteScopes), permissions)
		case *sqlparser.Delete:
			push(node.With)
		case *sqlparser.Update:
			push(node.With)
		case *sqlparser.Union:
			permissions, _ = buildUnionPermissions(node, role, visibleCTEs(cteScopes), permissions)
			return false
		case *sqlparser.Insert:
			permissions = buildInsertPermissions(node, role, visibleCTEs(cteScopes), permissions)
			return false
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

// buildUnionPermissions walks a union the way MySQL resolves CTE names in
// it, which the generated walker cannot express, and returns the CTE names in
// scope after its last arm:
//
//   - The arms see the enclosing scopes and the union's leading CTEs, until
//     the first parenthesized arm that declares a WITH of its own. From that
//     arm onward the WITH's names replace the leading CTEs, for the later
//     arms and for the union's own ORDER BY, LIMIT and INTO; a later arm
//     with its own WITH replaces them again.
//   - A nested union without a WITH is transparent: its arms continue the
//     chain, and its own ORDER BY, LIMIT and INTO see what its last arm saw.
//   - A parenthesized nested union with a WITH is one arm, and leaves behind
//     whatever its own chain ended with.
//
// Observed on MySQL 8.0, 8.4 and 9.
func buildUnionPermissions(union *sqlparser.Union, role tableacl.Role, outer []sqlparser.IdentifierCS, permissions []Permission) ([]Permission, []sqlparser.IdentifierCS) {
	if union.With != nil {
		permissions = buildCTEBodiesPermissions(union.With, role, outer, permissions)
	}
	scope := append(append([]sqlparser.IdentifierCS(nil), outer...), gatherCTEs(union.With)...)
	permissions, scope = buildUnionArmsPermissions(union, role, outer, scope, permissions)
	return buildUnionTrailingPermissions(union, role, scope, permissions), scope
}

// buildUnionArmsPermissions walks the arms of a union chain left to right,
// starting from scope, and returns the scope in effect after the last arm.
func buildUnionArmsPermissions(union *sqlparser.Union, role tableacl.Role, outer, scope []sqlparser.IdentifierCS, permissions []Permission) ([]Permission, []sqlparser.IdentifierCS) {
	for _, arm := range []sqlparser.TableStatement{union.Left, union.Right} {
		switch arm := arm.(type) {
		case *sqlparser.Union:
			if arm.With == nil {
				permissions, scope = buildUnionArmsPermissions(arm, role, outer, scope, permissions)
				permissions = buildUnionTrailingPermissions(arm, role, scope, permissions)
				continue
			}
			permissions, scope = buildUnionPermissions(arm, role, outer, permissions)
		case *sqlparser.Select:
			if arm.With == nil {
				permissions = buildSubqueryPermissionsInScope(arm, role, scope, permissions)
				continue
			}
			// The arm resolves its own WITH itself.
			permissions = buildSubqueryPermissionsInScope(arm, role, outer, permissions)
			scope = append(append([]sqlparser.IdentifierCS(nil), outer...), gatherCTEs(arm.With)...)
		case *sqlparser.ValuesStatement:
			if arm.With == nil {
				permissions = buildSubqueryPermissionsInScope(arm, role, scope, permissions)
				continue
			}
			permissions = buildSubqueryPermissionsInScope(arm, role, outer, permissions)
			scope = append(append([]sqlparser.IdentifierCS(nil), outer...), gatherCTEs(arm.With)...)
		default:
			permissions = buildSubqueryPermissionsInScope(arm, role, scope, permissions)
		}
	}
	return permissions, scope
}

// buildInsertPermissions walks an INSERT's rows, then its ON DUPLICATE KEY
// UPDATE clause with the CTE names the rows' last arm saw: a SELECT's own
// WITH, or what a union chain ended with.
func buildInsertPermissions(ins *sqlparser.Insert, role tableacl.Role, outer []sqlparser.IdentifierCS, permissions []Permission) []Permission {
	scope := outer
	switch rows := ins.Rows.(type) {
	case *sqlparser.Union:
		permissions, scope = buildUnionPermissions(rows, role, outer, permissions)
	case *sqlparser.Select:
		permissions = buildSubqueryPermissionsInScope(rows, role, outer, permissions)
		scope = append(append([]sqlparser.IdentifierCS(nil), outer...), gatherCTEs(rows.With)...)
	case *sqlparser.ValuesStatement:
		permissions = buildSubqueryPermissionsInScope(rows, role, outer, permissions)
		scope = append(append([]sqlparser.IdentifierCS(nil), outer...), gatherCTEs(rows.With)...)
	default:
		permissions = buildSubqueryPermissionsInScope(rows, role, outer, permissions)
	}
	return buildSubqueryPermissionsInScope(ins.OnDup, role, scope, permissions)
}

// buildUnionTrailingPermissions walks a union's ORDER BY, LIMIT and INTO.
func buildUnionTrailingPermissions(union *sqlparser.Union, role tableacl.Role, scope []sqlparser.IdentifierCS, permissions []Permission) []Permission {
	permissions = buildSubqueryPermissionsInScope(union.OrderBy, role, scope, permissions)
	if union.Limit != nil {
		permissions = buildSubqueryPermissionsInScope(union.Limit, role, scope, permissions)
	}
	if union.Into != nil {
		permissions = buildSubqueryPermissionsInScope(union.Into, role, scope, permissions)
	}
	return permissions
}

// gatherCTEs gathers the CTEs from the WITH clause, nil when there is none.
func gatherCTEs(with *sqlparser.With) []sqlparser.IdentifierCS {
	if with == nil {
		return nil
	}
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
