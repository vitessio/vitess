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
	// Error should be implemented by all errors in this package that arise from a semantic problem
	Error interface {
		Error() string
		Info() *ErrorInfo
	}
	// ErrorInfo provides additional information about a semantic error
	ErrorInfo struct {
		code  vtrpcpb.Code
		state vterrors.State
		typ   ErrType
		id    string
	}
)

const (
	UndefinedErrorCode = 0

	UndefinedErrorType ErrType = iota
	UnsupportedErrorType
	BugErrorType
)

func printf(e Error, msg string, args ...any) string {
	format := msg

	if e.Info().id != "" {
		format = fmt.Sprintf("%s: %s", e.Info().id, format)
	}

	switch e.Info().typ {
	case UnsupportedErrorType:
		format = "VT12001: unsupported: " + format
	case BugErrorType:
		format = "VT13001: [BUG] " + format
	}
	return fmt.Sprintf(format, args...)
}

func (c *ErrorInfo) ErrorCode() vtrpcpb.Code {
	if c.code == UndefinedErrorCode {
		return vtrpcpb.Code_UNKNOWN
	}

	switch c.typ {
	case UnsupportedErrorType:
		return vtrpcpb.Code_UNIMPLEMENTED
	case BugErrorType:
		return vtrpcpb.Code_INTERNAL
	default:
		return c.code
	}
}

func (c *ErrorInfo) State() vterrors.State {
	return c.state
}

func (c *ErrorInfo) ErrorType() ErrType {
	return c.typ
}

func (c *ErrorInfo) Id() string {
	return c.id
}

// Specific error implementations follow

// UnionColumnsDoNotMatchError
type UnionColumnsDoNotMatchError struct {
	FirstProj  int
	SecondProj int
}

var _ Error = (*UnionColumnsDoNotMatchError)(nil)

func (e *UnionColumnsDoNotMatchError) Error() string {
	return printf(e, "The used SELECT statements have a different number of columns: %v, %v", e.FirstProj, e.SecondProj)
}

func (e *UnionColumnsDoNotMatchError) Info() *ErrorInfo {
	return &ErrorInfo{code: vtrpcpb.Code_FAILED_PRECONDITION, state: vterrors.WrongNumberOfColumnsInSelect}
}

// UnsupportedMultiTablesInUpdateError
type UnsupportedMultiTablesInUpdateError struct {
	ExprCount int
	NotAlias  bool
}

var _ Error = (*UnsupportedMultiTablesInUpdateError)(nil)

func (e *UnsupportedMultiTablesInUpdateError) Error() string {
	switch {
	case e.NotAlias:
		return printf(e, "unaliased multiple tables in update")
	default:
		return printf(e, "multiple (%d) tables in update", e.ExprCount)
	}
}

func (e *UnsupportedMultiTablesInUpdateError) Info() *ErrorInfo {
	return &ErrorInfo{typ: UnsupportedErrorType}
}

// UnsupportedNaturalJoinError
type UnsupportedNaturalJoinError struct {
	JoinExpr *sqlparser.JoinTableExpr
}

var _ Error = (*UnsupportedNaturalJoinError)(nil)

func (e *UnsupportedNaturalJoinError) Error() string {
	return printf(e, "%s", e.JoinExpr.Join.ToString())
}

func (e *UnsupportedNaturalJoinError) Info() *ErrorInfo {
	return &ErrorInfo{typ: UnsupportedErrorType}
}

// UnionWithSQLCalcFoundRowsError
type UnionWithSQLCalcFoundRowsError struct {
}

var _ Error = (*UnionWithSQLCalcFoundRowsError)(nil)

func (e *UnionWithSQLCalcFoundRowsError) Error() string {
	return printf(e, "SQL_CALC_FOUND_ROWS not supported with union")
}

func (e *UnionWithSQLCalcFoundRowsError) Info() *ErrorInfo {
	return &ErrorInfo{typ: UnsupportedErrorType}
}

// TableNotUpdatableError
type TableNotUpdatableError struct {
	Table string
}

var _ Error = (*TableNotUpdatableError)(nil)

func (e *TableNotUpdatableError) Error() string {
	return printf(e, "The target table %s of the UPDATE is not updatable", e.Table)
}

func (e *TableNotUpdatableError) Info() *ErrorInfo {
	return &ErrorInfo{state: vterrors.NonUpdateableTable, code: vtrpcpb.Code_INVALID_ARGUMENT}
}

// SQLCalcFoundRowsUsageError
type SQLCalcFoundRowsUsageError struct {
}

var _ Error = (*SQLCalcFoundRowsUsageError)(nil)

func (e *SQLCalcFoundRowsUsageError) Error() string {
	return printf(e, "Incorrect usage/placement of 'SQL_CALC_FOUND_ROWS'")
}

func (e *SQLCalcFoundRowsUsageError) Info() *ErrorInfo {
	return &ErrorInfo{code: vtrpcpb.Code_INVALID_ARGUMENT}
}

// CantUseOptionHereError
type CantUseOptionHereError struct {
	Msg string
}

var _ Error = (*CantUseOptionHereError)(nil)

func (e *CantUseOptionHereError) Error() string {
	return printf(e, "Incorrect usage/placement of '%s'", e.Msg)
}

func (e *CantUseOptionHereError) Info() *ErrorInfo {
	return &ErrorInfo{state: vterrors.CantUseOptionHere, code: vtrpcpb.Code_INVALID_ARGUMENT}
}

// MissingInVSchemaError
type MissingInVSchemaError struct {
	Table TableInfo
}

var _ Error = (*MissingInVSchemaError)(nil)

func (e *MissingInVSchemaError) Error() string {
	tableName, _ := e.Table.Name()
	return printf(e, "Table information is not provided in vschema for table `%s`", sqlparser.String(tableName))
}

func (e *MissingInVSchemaError) Info() *ErrorInfo {
	return &ErrorInfo{code: vtrpcpb.Code_INVALID_ARGUMENT}
}

// NotSequenceTableError
type NotSequenceTableError struct {
	Table string
}

var _ Error = (*NotSequenceTableError)(nil)

func (e *NotSequenceTableError) Error() string {
	return printf(e, "NEXT used on a non-sequence table `%s`", e.Table)
}

func (e *NotSequenceTableError) Info() *ErrorInfo {
	return &ErrorInfo{code: vtrpcpb.Code_INVALID_ARGUMENT}
}

// NextWithMultipleTablesError
type NextWithMultipleTablesError struct {
	CountTables int
}

var _ Error = (*NextWithMultipleTablesError)(nil)

func (e *NextWithMultipleTablesError) Error() string {
	return printf(e, "Next statement should not contain multiple tables: found %d tables", e.CountTables)
}

func (e *NextWithMultipleTablesError) Info() *ErrorInfo {
	return &ErrorInfo{typ: BugErrorType}
}

// LockOnlyWithDualError
type LockOnlyWithDualError struct {
	Node *sqlparser.LockingFunc
}

var _ Error = (*LockOnlyWithDualError)(nil)

func (e *LockOnlyWithDualError) Error() string {
	return printf(e, "%v allowed only with dual", sqlparser.String(e.Node))
}

func (e *LockOnlyWithDualError) Info() *ErrorInfo {
	return &ErrorInfo{code: vtrpcpb.Code_UNIMPLEMENTED}
}

// QualifiedOrderInUnionError
type QualifiedOrderInUnionError struct {
	Table string
}

var _ Error = (*QualifiedOrderInUnionError)(nil)

func (e *QualifiedOrderInUnionError) Error() string {
	return printf(e, "Table `%s` from one of the SELECTs cannot be used in global ORDER clause", e.Table)
}

func (e *QualifiedOrderInUnionError) Info() *ErrorInfo {
	return &ErrorInfo{}
}

// JSONTablesError
type JSONTablesError struct {
	Table string
}

var _ Error = (*JSONTablesError)(nil)

func (e *JSONTablesError) Error() string {
	return printf(e, "json_table expressions")
}

func (e *JSONTablesError) Info() *ErrorInfo {
	return &ErrorInfo{typ: UnsupportedErrorType}
}

// BuggyError is used for checking conditions that should never occur
type BuggyError struct {
	Msg string
}

var _ Error = (*BuggyError)(nil)

func (e *BuggyError) Error() string {
	return printf(e, e.Msg)
}

func (e *BuggyError) Info() *ErrorInfo {
	return &ErrorInfo{typ: BugErrorType}
}

// ColumnNotFoundError
type ColumnNotFoundError struct {
	Column *sqlparser.ColName
}

var _ Error = (*ColumnNotFoundError)(nil)

func (e *ColumnNotFoundError) Error() string {
	return printf(e, "symbol %s not found", sqlparser.String(e.Column))
}

func (e *ColumnNotFoundError) Info() *ErrorInfo {
	return &ErrorInfo{state: vterrors.BadFieldError, code: vtrpcpb.Code_INVALID_ARGUMENT}
}

// AmbiguousColumnError
type AmbiguousColumnError struct {
	Column string
}

var _ Error = (*AmbiguousColumnError)(nil)

func (e *AmbiguousColumnError) Error() string {
	return printf(e, "Column '%s' in field list is ambiguous", e.Column)
}

func (e *AmbiguousColumnError) Info() *ErrorInfo {
	return &ErrorInfo{state: vterrors.BadFieldError, code: vtrpcpb.Code_INVALID_ARGUMENT}
}
