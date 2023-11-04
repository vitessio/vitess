/*
Copyright 2022 The Vitess Authors.

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

package semantics

import (
	"fmt"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	unsupportedError interface {
		error
		unsupported()
	}

	bugError interface {
		error
		bug()
	}
)

func eprintf(e error, format string, args ...any) string {
	switch e.(type) {
	case unsupportedError:
		format = "VT12001: unsupported: " + format
	case bugError:
		format = "VT13001: [BUG] " + format
	}
	return fmt.Sprintf(format, args...)
}

// Specific error implementations follow

// UnionColumnsDoNotMatchError
type UnionColumnsDoNotMatchError struct {
	FirstProj  int
	SecondProj int
}

func (e *UnionColumnsDoNotMatchError) ErrorState() vterrors.State {
	return vterrors.WrongNumberOfColumnsInSelect
}

func (e *UnionColumnsDoNotMatchError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_FAILED_PRECONDITION
}

func (e *UnionColumnsDoNotMatchError) Error() string {
	return eprintf(e, "The used SELECT statements have a different number of columns: %v, %v", e.FirstProj, e.SecondProj)
}

// UnsupportedMultiTablesInUpdateError
type UnsupportedMultiTablesInUpdateError struct {
	ExprCount int
	NotAlias  bool
}

func (e *UnsupportedMultiTablesInUpdateError) Error() string {
	switch {
	case e.NotAlias:
		return eprintf(e, "unaliased multiple tables in update")
	default:
		return eprintf(e, "multiple (%d) tables in update", e.ExprCount)
	}
}

func (e *UnsupportedMultiTablesInUpdateError) unsupported() {}

// UnsupportedNaturalJoinError
type UnsupportedNaturalJoinError struct {
	JoinExpr *sqlparser.JoinTableExpr
}

func (e *UnsupportedNaturalJoinError) Error() string {
	return eprintf(e, "%s", e.JoinExpr.Join.ToString())
}

func (e *UnsupportedNaturalJoinError) unsupported() {}

// UnionWithSQLCalcFoundRowsError
type UnionWithSQLCalcFoundRowsError struct {
}

func (e *UnionWithSQLCalcFoundRowsError) Error() string {
	return eprintf(e, "SQL_CALC_FOUND_ROWS not supported with union")
}

func (e *UnionWithSQLCalcFoundRowsError) unsupported() {}

// TableNotUpdatableError
type TableNotUpdatableError struct {
	Table string
}

func (e *TableNotUpdatableError) Error() string {
	return eprintf(e, "The target table %s of the UPDATE is not updatable", e.Table)
}

func (e *TableNotUpdatableError) ErrorState() vterrors.State {
	return vterrors.NonUpdateableTable
}

func (e *TableNotUpdatableError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

// SQLCalcFoundRowsUsageError
type SQLCalcFoundRowsUsageError struct {
}

func (e *SQLCalcFoundRowsUsageError) Error() string {
	return eprintf(e, "Incorrect usage/placement of 'SQL_CALC_FOUND_ROWS'")
}

func (e *SQLCalcFoundRowsUsageError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

// CantUseOptionHereError
type CantUseOptionHereError struct {
	Msg string
}

func (e *CantUseOptionHereError) Error() string {
	return eprintf(e, "Incorrect usage/placement of '%s'", e.Msg)
}

func (e *CantUseOptionHereError) ErrorState() vterrors.State {
	return vterrors.CantUseOptionHere
}

func (e *CantUseOptionHereError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

// MissingInVSchemaError
type MissingInVSchemaError struct {
	Table TableInfo
}

func (e *MissingInVSchemaError) Error() string {
	tableName, _ := e.Table.Name()
	return eprintf(e, "Table information is not provided in vschema for table `%s`", sqlparser.String(tableName))
}

func (e *MissingInVSchemaError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

// NotSequenceTableError
type NotSequenceTableError struct {
	Table string
}

func (e *NotSequenceTableError) Error() string {
	return eprintf(e, "NEXT used on a non-sequence table `%s`", e.Table)
}

func (e *NotSequenceTableError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

// NextWithMultipleTablesError
type NextWithMultipleTablesError struct {
	CountTables int
}

func (e *NextWithMultipleTablesError) Error() string {
	return eprintf(e, "Next statement should not contain multiple tables: found %d tables", e.CountTables)
}

func (e *NextWithMultipleTablesError) bug() {}

// LockOnlyWithDualError
type LockOnlyWithDualError struct {
	Node *sqlparser.LockingFunc
}

func (e *LockOnlyWithDualError) Error() string {
	return eprintf(e, "%v allowed only with dual", sqlparser.String(e.Node))
}

func (e *LockOnlyWithDualError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_UNIMPLEMENTED
}

// QualifiedOrderInUnionError
type QualifiedOrderInUnionError struct {
	Table string
}

func (e *QualifiedOrderInUnionError) Error() string {
	return eprintf(e, "Table `%s` from one of the SELECTs cannot be used in global ORDER clause", e.Table)
}

// JSONTablesError
type JSONTablesError struct {
	Table string
}

func (e *JSONTablesError) Error() string {
	return eprintf(e, "json_table expressions")
}

func (e *JSONTablesError) unsupported() {}

// BuggyError is used for checking conditions that should never occur
type BuggyError struct {
	Msg string
}

func (e *BuggyError) Error() string {
	return eprintf(e, e.Msg)
}

func (e *BuggyError) bug() {}

// ColumnNotFoundError
type ColumnNotFoundError struct {
	Column *sqlparser.ColName
	Table  *sqlparser.TableName
}

func (e *ColumnNotFoundError) Error() string {
	if e.Table == nil {
		return eprintf(e, "column '%s' not found", sqlparser.String(e.Column))
	}
	return eprintf(e, "column '%s' not found in table '%s'", sqlparser.String(e.Column), sqlparser.String(e.Table))
}

func (e *ColumnNotFoundError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

func (e *ColumnNotFoundError) ErrorState() vterrors.State {
	return vterrors.BadFieldError
}

// AmbiguousColumnError
type AmbiguousColumnError struct {
	Column string
}

func (e *AmbiguousColumnError) Error() string {
	return eprintf(e, "Column '%s' in field list is ambiguous", e.Column)
}

func (e *AmbiguousColumnError) ErrorState() vterrors.State {
	return vterrors.BadFieldError
}

func (e *AmbiguousColumnError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

type UnsupportedConstruct struct {
	errString string
}

func (e *UnsupportedConstruct) unsupported() {}

func (e *UnsupportedConstruct) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_UNIMPLEMENTED
}

func (e *UnsupportedConstruct) Error() string {
	return eprintf(e, e.errString)
}
