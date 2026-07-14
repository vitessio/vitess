/*
Copyright 2026 The Vitess Authors.

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
	"strings"

	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	// dmlTable is a table in a DML statement's FROM clause, keyed by the name
	// that SET column qualifiers and DELETE targets use to reference it.
	dmlTable struct {
		key  string
		name string
	}
)

// internalTableModificationError returns the planner error for a statement
// that targets a Vitess internal table.
func internalTableModificationError(tableName string) error {
	return vterrors.VT09033(tableName)
}

// isInternalOperationTableName reports whether tableName is reserved for
// internal Vitess operations.
func isInternalOperationTableName(tableName string) bool {
	return schema.IsInternalOperationTableName(strings.ToLower(tableName))
}

// rejectInternalTableDML returns an error when the supplied DML statement
// would modify a Vitess internal operation table. Reading an internal table,
// for example in a join, is allowed. semTable carries schema knowledge when
// the caller has run semantic analysis; nil means unqualified SET columns in
// a multi-table UPDATE cannot be attributed and are passed through unchecked.
func rejectInternalTableDML(stmt sqlparser.Statement, semTable *semantics.SemTable) error {
	switch stmt := stmt.(type) {
	case *sqlparser.Insert:
		return rejectInternalDMLTables(dmlTables(stmt.Table))
	case *sqlparser.Update:
		return rejectInternalTableUpdate(stmt, semTable)
	case *sqlparser.Delete:
		return rejectInternalTableDelete(stmt)
	default:
		return nil
	}
}

// rejectInternalTableUpdate returns an error when an UPDATE assigns a column
// of a Vitess internal operation table. An UPDATE modifies only the tables
// named in its SET clause.
func rejectInternalTableUpdate(stmt *sqlparser.Update, semTable *semantics.SemTable) error {
	tables := dmlTables(stmt.TableExprs...)

	// A single-table UPDATE modifies its one table no matter how the SET
	// columns are written, so qualifiers do not need to be inspected.
	if len(tables) == 1 {
		return rejectInternalDMLTables(tables)
	}

	for _, updateExpr := range stmt.Exprs {
		qualifier := strings.ToLower(updateExpr.Name.Qualifier.Name.String())
		if qualifier == "" {
			// An unqualified column belongs to whichever table defines it.
			// When schema tracking has made the other tables authoritative,
			// the column can be attributed; otherwise it stays unresolved
			// and is passed through unchecked.
			name, ok := updateColumnTable(semTable, updateExpr.Name)
			if ok && isInternalOperationTableName(name) {
				return internalTableModificationError(name)
			}
			continue
		}

		for _, table := range tables {
			if table.key == qualifier && isInternalOperationTableName(table.name) {
				return internalTableModificationError(table.name)
			}
		}
	}

	return nil
}

// updateColumnTable resolves the table an unqualified SET column belongs to.
// Schema knowledge is consulted directly rather than the analyzer's column
// bindings because the single-unsharded-keyspace shortcut skips binding. A
// table can own the column unless schema tracking has made its column list
// authoritative and the column is absent. The resolution is conclusive only
// when exactly one table can own the column.
func updateColumnTable(semTable *semantics.SemTable, col *sqlparser.ColName) (string, bool) {
	if semTable == nil {
		return "", false
	}

	owner := ""
	found := false
	for _, table := range semTable.Tables {
		if tableExcludesColumn(table, col) {
			continue
		}

		if found {
			return "", false
		}
		found = true

		// A table without a vschema entry, such as a derived table, can own
		// the column but cannot be an internal table, so it resolves to an
		// empty name rather than to its alias.
		if vtbl := table.GetVindexTable(); vtbl != nil {
			owner = vtbl.Name.String()
		}
	}

	return owner, found
}

// tableExcludesColumn reports whether table is known not to have col. Only a
// table with an authoritative column list can rule the column out.
func tableExcludesColumn(table semantics.TableInfo, col *sqlparser.ColName) bool {
	vtbl := table.GetVindexTable()
	if vtbl == nil || !vtbl.ColumnListAuthoritative {
		return false
	}

	for _, column := range vtbl.Columns {
		if col.Name.Equal(column.Name) {
			return false
		}
	}

	return true
}

// rejectInternalTableDelete returns an error when a DELETE removes rows from
// a Vitess internal operation table. A multi-table DELETE removes rows only
// from its target tables, so internal tables that are merely joined are
// allowed.
func rejectInternalTableDelete(stmt *sqlparser.Delete) error {
	tables := dmlTables(stmt.TableExprs...)

	// A single-table DELETE has no targets and removes rows from its one
	// FROM table.
	if len(stmt.Targets) == 0 {
		return rejectInternalDMLTables(tables)
	}

	for _, target := range stmt.Targets {
		// A target names a FROM table by its alias when it has one, so it is
		// resolved to the real table name before being checked. A target that
		// matches no FROM entry is checked under its own name.
		name := target.Name.String()
		key := strings.ToLower(name)

		for _, table := range tables {
			if table.key == key {
				name = table.name
				break
			}
		}

		if isInternalOperationTableName(name) {
			return internalTableModificationError(name)
		}
	}

	return nil
}

// dmlTables lists the real tables under the supplied FROM clause expressions
// in declaration order.
func dmlTables(tableExprs ...sqlparser.TableExpr) []dmlTable {
	var tables []dmlTable
	for _, tableExpr := range tableExprs {
		switch tableExpr := tableExpr.(type) {
		case *sqlparser.AliasedTableExpr:
			// Anything other than a plain table name, such as a derived
			// table, cannot be an internal table.
			tableName, ok := tableExpr.Expr.(sqlparser.TableName)
			if !ok {
				continue
			}

			key := tableExpr.As.String()
			if key == "" {
				key = tableName.Name.String()
			}

			tables = append(tables, dmlTable{key: strings.ToLower(key), name: tableName.Name.String()})
		case *sqlparser.JoinTableExpr:
			tables = append(tables, dmlTables(tableExpr.LeftExpr, tableExpr.RightExpr)...)
		case *sqlparser.ParenTableExpr:
			tables = append(tables, dmlTables(tableExpr.Exprs...)...)
		}
	}

	return tables
}

// rejectInternalDMLTables returns an error when any of tables is a Vitess
// internal operation table.
func rejectInternalDMLTables(tables []dmlTable) error {
	name, found := firstInternalDMLTable(tables)
	if !found {
		return nil
	}

	return internalTableModificationError(name)
}

// firstInternalDMLTable returns the first Vitess internal operation table in
// tables.
func firstInternalDMLTable(tables []dmlTable) (string, bool) {
	for _, table := range tables {
		if isInternalOperationTableName(table.name) {
			return table.name, true
		}
	}

	return "", false
}

// rejectInternalTableDDL returns an error when the supplied DDL statement
// targets a Vitess internal operation table.
func rejectInternalTableDDL(stmt sqlparser.DDLStatement) error {
	switch stmt.(type) {
	// Procedure names live outside the table namespace, so an
	// internal-shaped procedure name does not target an internal table.
	case *sqlparser.CreateProcedure, *sqlparser.DropProcedure:
		return nil
	}

	for _, tableName := range stmt.AffectedTables() {
		if err := rejectInternalTableName(tableName); err != nil {
			return err
		}
	}

	return nil
}

// rejectInternalTableName returns an error for a Vitess internal operation
// table name.
func rejectInternalTableName(tableName sqlparser.TableName) error {
	name := tableName.Name.String()
	if !isInternalOperationTableName(name) {
		return nil
	}

	return internalTableModificationError(name)
}
