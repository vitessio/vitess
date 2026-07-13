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
// for example in a join, is allowed.
func rejectInternalTableDML(stmt sqlparser.Statement) error {
	switch stmt := stmt.(type) {
	case *sqlparser.Insert:
		return rejectInternalDMLTables(dmlTables(stmt.Table))
	case *sqlparser.Update:
		return rejectInternalTableUpdate(stmt)
	case *sqlparser.Delete:
		return rejectInternalTableDelete(stmt)
	default:
		return nil
	}
}

// rejectInternalTableUpdate returns an error when an UPDATE assigns a column
// of a Vitess internal operation table. An UPDATE modifies only the tables
// named in its SET clause. An unqualified column in a multi-table UPDATE
// cannot be attributed to a table without schema knowledge, so it is passed
// through unchecked.
func rejectInternalTableUpdate(stmt *sqlparser.Update) error {
	tables := dmlTables(stmt.TableExprs...)

	// A single-table UPDATE modifies its one table no matter how the SET
	// columns are written, so qualifiers do not need to be inspected.
	if len(tables) == 1 {
		return rejectInternalDMLTables(tables)
	}

	for _, updateExpr := range stmt.Exprs {
		// An unqualified column belongs to whichever table defines it, which
		// only the backend schema knows, so it is passed through unchecked.
		qualifier := strings.ToLower(updateExpr.Name.Qualifier.Name.String())
		if qualifier == "" {
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
