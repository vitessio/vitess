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
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

// internalTableModificationError returns the planner error for a statement
// that targets a Vitess internal table.
func internalTableModificationError(tableName string) error {
	return vterrors.VT09033(tableName)
}

// rejectInternalTableDML returns an error when the supplied DML statement
// targets a Vitess internal operation table.
func rejectInternalTableDML(stmt sqlparser.Statement) error {
	switch stmt := stmt.(type) {
	case *sqlparser.Insert:
		return rejectInternalTableDMLTableExpr(stmt.Table)
	case *sqlparser.Update:
		for _, tableExpr := range stmt.TableExprs {
			if err := rejectInternalTableDMLTableExpr(tableExpr); err != nil {
				return err
			}
		}
	case *sqlparser.Delete:
		for _, tableExpr := range stmt.TableExprs {
			if err := rejectInternalTableDMLTableExpr(tableExpr); err != nil {
				return err
			}
		}
	default:
		return nil
	}

	return nil
}

// rejectInternalTableDMLTableExpr returns an error when the supplied table
// expression references a Vitess internal table.
func rejectInternalTableDMLTableExpr(tableExpr sqlparser.TableExpr) error {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		tableName, ok := tableExpr.Expr.(sqlparser.TableName)
		if !ok {
			return nil
		}

		if !schema.IsInternalOperationTableName(tableName.Name.String()) {
			return nil
		}

		return internalTableModificationError(tableName.Name.String())
	case *sqlparser.JoinTableExpr:
		if err := rejectInternalTableDMLTableExpr(tableExpr.LeftExpr); err != nil {
			return err
		}

		return rejectInternalTableDMLTableExpr(tableExpr.RightExpr)
	case *sqlparser.ParenTableExpr:
		for _, nestedExpr := range tableExpr.Exprs {
			if err := rejectInternalTableDMLTableExpr(nestedExpr); err != nil {
				return err
			}
		}
	}

	return nil
}

// rejectInternalTableDDL returns an error when the supplied DDL statement
// targets a Vitess internal operation table.
func rejectInternalTableDDL(stmt sqlparser.DDLStatement) error {
	for _, tableName := range stmt.AffectedTables() {
		if !schema.IsInternalOperationTableName(tableName.Name.String()) {
			continue
		}

		return internalTableModificationError(tableName.Name.String())
	}

	return nil
}
