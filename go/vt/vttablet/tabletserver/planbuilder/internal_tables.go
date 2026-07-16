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

	vtschema "vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

// dmlTable is a table in a DML statement's FROM clause, keyed by the name
// that SET column qualifiers and DELETE targets use to reference it.
type dmlTable struct {
	key  string
	name string
}

// internalTableModificationError returns the plan error for a statement that
// targets a Vitess internal table.
func internalTableModificationError(tableName string) error {
	return vterrors.VT09033(tableName)
}

// isInternalOperationTableName reports whether tableName uses the Vitess
// internal table name format.
func isInternalOperationTableName(tableName string) bool {
	isInternal, _, _, _, _ := vtschema.AnalyzeInternalTableName(strings.ToLower(tableName))
	return isInternal
}

// rejectInternalTableWrites returns an error when statement modifies a Vitess
// internal operation table. Reading an internal table, for example in a join,
// is allowed. A nil parser leaves PREPARE literals inside stored procedure
// bodies unchecked.
func rejectInternalTableWrites(statement sqlparser.Statement, tables map[string]*schema.Table, parser *sqlparser.Parser) error {
	switch stmt := statement.(type) {
	case *sqlparser.Insert:
		return rejectInternalDMLTables(dmlTables(stmt.Table))
	case *sqlparser.Update:
		return rejectInternalTableUpdate(stmt, tables)
	case *sqlparser.Delete:
		return rejectInternalTableDelete(stmt)
	case sqlparser.DDLStatement:
		return rejectInternalTableDDL(stmt, tables, parser)
	}
	return nil
}

// rejectInternalTableUpdate returns an error when an UPDATE assigns a column
// of a Vitess internal operation table. An UPDATE modifies only the tables
// named in its SET clause.
func rejectInternalTableUpdate(stmt *sqlparser.Update, schemaTables map[string]*schema.Table) error {
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
			// The tablet schema attributes it when it resolves to exactly
			// one table, otherwise it is passed through unchecked.
			name, ok := updateColumnTable(tables, schemaTables, updateExpr.Name)
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
// The resolution is conclusive only when exactly one FROM table can own the
// column.
func updateColumnTable(tables []dmlTable, schemaTables map[string]*schema.Table, col *sqlparser.ColName) (string, bool) {
	owner := ""
	found := false
	for _, table := range tables {
		if schemaTable := schemaTables[table.name]; schemaTable != nil && schemaTable.FindColumn(col.Name) == -1 {
			continue
		}

		if found {
			return "", false
		}
		found = true
		owner = table.name
	}

	return owner, found
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
	for _, table := range tables {
		if isInternalOperationTableName(table.name) {
			return internalTableModificationError(table.name)
		}
	}

	return nil
}

// rejectInternalTableDDL returns an error when the supplied DDL statement
// targets a Vitess internal operation table.
func rejectInternalTableDDL(stmt sqlparser.DDLStatement, tables map[string]*schema.Table, parser *sqlparser.Parser) error {
	// Procedure names live outside the table namespace, so an
	// internal-shaped procedure name does not target an internal table.
	switch stmt := stmt.(type) {
	case *sqlparser.CreateProcedure:
		return rejectInternalTableCreateProcedure(stmt, tables, parser)
	case *sqlparser.DropProcedure:
		return nil
	}

	for _, tableName := range stmt.AffectedTables() {
		name := tableName.Name.String()
		if isInternalOperationTableName(name) {
			return internalTableModificationError(name)
		}
	}

	alterTable, ok := stmt.(*sqlparser.AlterTable)
	if !ok {
		return nil
	}

	// AffectedTables does not include the WITH TABLE target, but EXCHANGE
	// PARTITION swaps data with that table.
	partitionSpec := alterTable.PartitionSpec
	if partitionSpec == nil || partitionSpec.Action != sqlparser.ExchangeAction {
		return nil
	}

	name := partitionSpec.TableName.Name.String()
	if isInternalOperationTableName(name) {
		return internalTableModificationError(name)
	}

	return nil
}

// rejectInternalTableCreateProcedure returns an error when a stored procedure
// body contains a statement that would modify a Vitess internal operation
// table.
func rejectInternalTableCreateProcedure(stmt *sqlparser.CreateProcedure, tables map[string]*schema.Table, parser *sqlparser.Parser) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node := node.(type) {
		case *sqlparser.Insert:
			return false, rejectInternalTableWrites(node, tables, parser)
		case *sqlparser.Update:
			return false, rejectInternalTableWrites(node, tables, parser)
		case *sqlparser.Delete:
			return false, rejectInternalTableWrites(node, tables, parser)
		case sqlparser.DDLStatement:
			return false, rejectInternalTableWrites(node, tables, parser)
		case *sqlparser.PrepareStmt:
			return false, rejectInternalTablePrepare(node, tables, parser)
		}
		return true, nil
	}, stmt.Body)
}

// rejectInternalTablePrepare returns an error when a PREPARE statement inside
// a stored procedure would modify a Vitess internal operation table. Dynamic
// and unparseable statements are passed through unchecked.
func rejectInternalTablePrepare(stmt *sqlparser.PrepareStmt, tables map[string]*schema.Table, parser *sqlparser.Parser) error {
	if parser == nil {
		return nil
	}

	literal, ok := stmt.Statement.(*sqlparser.Literal)
	if !ok {
		return nil
	}

	preparedStmt, err := parser.Parse(literal.Val)
	if err != nil {
		return nil
	}

	return rejectInternalTableWrites(preparedStmt, tables, parser)
}
