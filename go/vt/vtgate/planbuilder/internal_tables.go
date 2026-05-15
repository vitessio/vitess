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

// rejectInternalTableLoad returns an error when a LOAD DATA statement targets a
// Vitess internal operation table.
func rejectInternalTableLoad(query string, parser *sqlparser.Parser) error {
	return rejectInternalTableLoadStatements(query, parser, true)
}

// rejectInternalTableLoadStatements scans LOAD DATA statements and returns an
// error when any target a Vitess internal operation table.
func rejectInternalTableLoadStatements(query string, parser *sqlparser.Parser, stopAfterFirstLoad bool) error {
	tokenizer := parser.NewStringTokenizer(query)

	for {
		token, _ := nextNonCommentToken(tokenizer)

		switch token {
		case sqlparser.LEX_ERROR, 0:
			return nil
		case sqlparser.LOAD:
			token, _ = nextNonCommentToken(tokenizer)
			if token != sqlparser.DATA {
				continue
			}

			tableName, ok := loadDataTableName(tokenizer)
			if ok && isInternalOperationTableName(tableName) {
				return internalTableModificationError(tableName)
			}

			if stopAfterFirstLoad {
				return nil
			}
		}
	}
}

// loadDataTableName returns the target table named by a LOAD DATA statement.
func loadDataTableName(tokenizer *sqlparser.Tokenizer) (string, bool) {
	for {
		token, _ := nextNonCommentToken(tokenizer)

		switch token {
		case sqlparser.LEX_ERROR, 0, ';':
			return "", false
		case sqlparser.INTO:
			token, _ = nextNonCommentToken(tokenizer)
			if token != sqlparser.TABLE {
				continue
			}

			tableName, ok := loadDataTableIdentifier(tokenizer)
			skipLoadDataStatement(tokenizer)

			return tableName, ok
		}
	}
}

// loadDataTableIdentifier reads the table identifier after LOAD DATA ... INTO
// TABLE.
func loadDataTableIdentifier(tokenizer *sqlparser.Tokenizer) (string, bool) {
	token, value := nextNonCommentToken(tokenizer)
	tableName, ok := loadDataIdentifier(token, value)
	if !ok {
		return "", false
	}

	token, _ = nextNonCommentToken(tokenizer)
	if token != '.' {
		return tableName, true
	}

	token, value = nextNonCommentToken(tokenizer)
	return loadDataIdentifier(token, value)
}

// loadDataIdentifier converts a tokenizer token into a table identifier.
func loadDataIdentifier(token int, value string) (string, bool) {
	if token == sqlparser.ID {
		return value, true
	}

	tableName := sqlparser.KeywordString(token)
	if tableName == "" {
		return "", false
	}

	return tableName, true
}

// skipLoadDataStatement consumes the rest of a LOAD DATA statement after its
// target table has already been read.
func skipLoadDataStatement(tokenizer *sqlparser.Tokenizer) {
	for {
		token, _ := nextNonCommentToken(tokenizer)
		if token == sqlparser.LEX_ERROR || token == 0 || token == ';' {
			return
		}
	}
}

// nextNonCommentToken skips SQL comments when scanning a statement manually.
func nextNonCommentToken(tokenizer *sqlparser.Tokenizer) (int, string) {
	for {
		token, value := tokenizer.Scan()
		if token != sqlparser.COMMENT {
			return token, value
		}
	}
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

		if !isInternalOperationTableName(tableName.Name.String()) {
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
func rejectInternalTableDDL(stmt sqlparser.DDLStatement, query string, parser *sqlparser.Parser) error {
	switch stmt := stmt.(type) {
	case *sqlparser.CreateProcedure:
		// Procedure names live outside the table namespace, so only the body
		// can modify an internal operation table.
		return rejectInternalTableCreateProcedure(stmt, query, parser)
	case *sqlparser.DropProcedure:
		return nil
	}

	for _, tableName := range stmt.AffectedTables() {
		if err := rejectInternalTableName(tableName); err != nil {
			return err
		}
	}

	switch stmt := stmt.(type) {
	case *sqlparser.AlterTable:
		return rejectInternalTableExchangePartition(stmt)
	default:
		return nil
	}
}

// rejectInternalTableExchangePartition rejects ALTER TABLE ... EXCHANGE PARTITION
// when the exchanged table is a Vitess internal operation table.
func rejectInternalTableExchangePartition(alterTable *sqlparser.AlterTable) error {
	partitionSpec := alterTable.PartitionSpec
	if partitionSpec == nil || partitionSpec.Action != sqlparser.ExchangeAction {
		return nil
	}

	return rejectInternalTableName(partitionSpec.TableName)
}

// rejectInternalTableCreateProcedure returns an error when a stored procedure body
// contains a statement that would modify a Vitess internal operation table.
func rejectInternalTableCreateProcedure(
	stmt *sqlparser.CreateProcedure,
	query string,
	parser *sqlparser.Parser,
) error {
	loadChecked := false

	return sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node := node.(type) {
		case *sqlparser.Insert:
			return false, rejectInternalTableDML(node)
		case *sqlparser.Update:
			return false, rejectInternalTableDML(node)
		case *sqlparser.Delete:
			return false, rejectInternalTableDML(node)
		case sqlparser.DDLStatement:
			return false, rejectInternalTableDDL(node, query, parser)
		case *sqlparser.Load:
			if loadChecked {
				return false, nil
			}

			loadChecked = true

			return false, rejectInternalTableLoadStatements(query, parser, false)
		default:
			return true, nil
		}
	}, stmt.Body)
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
