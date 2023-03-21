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
	ErrType int

	identifiableError interface {
		id() string
	}

	typedError interface {
		typ() ErrType
	}
)

const (
	UndefinedErrorCode = 0

	UndefinedErrorType ErrType = iota
	UnsupportedErrorType
	BugErrorType
)

func printf(e error, msg string, args ...any) string {
	format := msg

	if eId, ok := e.(identifiableError); ok {
		format = fmt.Sprintf("%s: %s", eId.id(), format)
	}

	if eTyp, ok := e.(typedError); ok {
		switch eTyp.typ() {
		case UnsupportedErrorType:
			format = "VT12001: unsupported: " + format
		case BugErrorType:
			format = "VT13001: [BUG] " + format
		}
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
	return printf(e, "The used SELECT statements have a different number of columns: %v, %v", e.FirstProj, e.SecondProj)
}

// UnsupportedMultiTablesInUpdateError
type UnsupportedMultiTablesInUpdateError struct {
	ExprCount int
	NotAlias  bool
}

func (e *UnsupportedMultiTablesInUpdateError) Error() string {
	switch {
	case e.NotAlias:
		return printf(e, "unaliased multiple tables in update")
	default:
		return printf(e, "multiple (%d) tables in update", e.ExprCount)
	}
}

func (e *UnsupportedMultiTablesInUpdateError) typ() ErrType {
	return UnsupportedErrorType
}

// UnsupportedNaturalJoinError
type UnsupportedNaturalJoinError struct {
	JoinExpr *sqlparser.JoinTableExpr
}

func (e *UnsupportedNaturalJoinError) Error() string {
	return printf(e, "%s", e.JoinExpr.Join.ToString())
}

func (e *UnsupportedNaturalJoinError) typ() ErrType {
	return UnsupportedErrorType
}

// UnionWithSQLCalcFoundRowsError
type UnionWithSQLCalcFoundRowsError struct {
}

func (e *UnionWithSQLCalcFoundRowsError) Error() string {
	return printf(e, "SQL_CALC_FOUND_ROWS not supported with union")
}

func (e *UnionWithSQLCalcFoundRowsError) typ() ErrType {
	return UnsupportedErrorType
}

// TableNotUpdatableError
type TableNotUpdatableError struct {
	Table string
}

func (e *TableNotUpdatableError) Error() string {
	return printf(e, "The target table %s of the UPDATE is not updatable", e.Table)
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
	return printf(e, "Incorrect usage/placement of 'SQL_CALC_FOUND_ROWS'")
}

func (e *SQLCalcFoundRowsUsageError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

// CantUseOptionHereError
type CantUseOptionHereError struct {
	Msg string
}

func (e *CantUseOptionHereError) Error() string {
	return printf(e, "Incorrect usage/placement of '%s'", e.Msg)
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
	return printf(e, "Table information is not provided in vschema for table `%s`", sqlparser.String(tableName))
}

func (e *MissingInVSchemaError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

// NotSequenceTableError
type NotSequenceTableError struct {
	Table string
}

func (e *NotSequenceTableError) Error() string {
	return printf(e, "NEXT used on a non-sequence table `%s`", e.Table)
}

func (e *NotSequenceTableError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}

// NextWithMultipleTablesError
type NextWithMultipleTablesError struct {
	CountTables int
}

func (e *NextWithMultipleTablesError) Error() string {
	return printf(e, "Next statement should not contain multiple tables: found %d tables", e.CountTables)
}

func (e *NextWithMultipleTablesError) typ() ErrType {
	return BugErrorType
}

// LockOnlyWithDualError
type LockOnlyWithDualError struct {
	Node *sqlparser.LockingFunc
}

func (e *LockOnlyWithDualError) Error() string {
	return printf(e, "%v allowed only with dual", sqlparser.String(e.Node))
}

func (e *LockOnlyWithDualError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_UNIMPLEMENTED
}

// QualifiedOrderInUnionError
type QualifiedOrderInUnionError struct {
	Table string
}

func (e *QualifiedOrderInUnionError) Error() string {
	return printf(e, "Table `%s` from one of the SELECTs cannot be used in global ORDER clause", e.Table)
}

// JSONTablesError
type JSONTablesError struct {
	Table string
}

func (e *JSONTablesError) Error() string {
	return printf(e, "json_table expressions")
}

func (e *JSONTablesError) typ() ErrType {
	return UnsupportedErrorType
}

// BuggyError is used for checking conditions that should never occur
type BuggyError struct {
	Msg string
}

func (e *BuggyError) Error() string {
	return printf(e, e.Msg)
}

func (e *BuggyError) typ() ErrType {
	return BugErrorType
}

// ColumnNotFoundError
type ColumnNotFoundError struct {
	Column *sqlparser.ColName
}

func (e *ColumnNotFoundError) Error() string {
	return printf(e, "symbol %s not found", sqlparser.String(e.Column))
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
	return printf(e, "Column '%s' in field list is ambiguous", e.Column)
}

func (e *AmbiguousColumnError) ErrorState() vterrors.State {
	return vterrors.BadFieldError
}

func (e *AmbiguousColumnError) ErrorCode() vtrpcpb.Code {
	return vtrpcpb.Code_INVALID_ARGUMENT
}
