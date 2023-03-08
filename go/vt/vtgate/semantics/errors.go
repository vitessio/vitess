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

const (
	UndefinedErrorCode = 0
)

type ErrType int

const (
	UndefinedErrorType ErrType = iota
	UnsupportedErrorType
	BugErrorType
)

func printf(e SemanticsError, msg string, args ...any) string {
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

// SemanticsError should be implemented by all errors in this package that arise from a semantic problem
type SemanticsError interface {
	Error() string
	Info() *SemanticsErrorInfo
}

// SemanticsErrorInfo provides additional information about a semantic error
type SemanticsErrorInfo struct {
	code  vtrpcpb.Code
	state vterrors.State
	typ   ErrType
	id    string
}

func (c *SemanticsErrorInfo) ErrorCode() vtrpcpb.Code {
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

func (c *SemanticsErrorInfo) State() vterrors.State {
	return c.state
}

func (c *SemanticsErrorInfo) ErrorType() ErrType {
	return c.typ
}

func (c *SemanticsErrorInfo) Id() string {
	return c.id
}

// Specific error implementations follow

// UnionColumnsDoNotMatchError
type UnionColumnsDoNotMatchError struct {
	FirstProj  int
	SecondProj int
}

func (e *UnionColumnsDoNotMatchError) Error() string {
	return printf(e, "The used SELECT statements have a different number of columns: %v, %v", e.FirstProj, e.SecondProj)
}

func (e *UnionColumnsDoNotMatchError) Info() *SemanticsErrorInfo {
	return &SemanticsErrorInfo{code: vtrpcpb.Code_FAILED_PRECONDITION, state: vterrors.WrongNumberOfColumnsInSelect}
}

// TODO: untested
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

func (e *UnsupportedMultiTablesInUpdateError) Info() *SemanticsErrorInfo {
	return &SemanticsErrorInfo{typ: UnsupportedErrorType}
}

// UnsupportedNaturalJoinError
type UnsupportedNaturalJoinError struct {
	Join string
}

func (e *UnsupportedNaturalJoinError) Error() string {
	return printf(e, "%s", e.Join)
}

func (e *UnsupportedNaturalJoinError) Info() *SemanticsErrorInfo {
	return &SemanticsErrorInfo{typ: UnsupportedErrorType}
}

// UnionWithSQLCalcFoundRowsError
type UnionWithSQLCalcFoundRowsError struct {
}

func (e *UnionWithSQLCalcFoundRowsError) Error() string {
	return printf(e, "SQL_CALC_FOUND_ROWS not supported with union")
}

func (e *UnionWithSQLCalcFoundRowsError) Info() *SemanticsErrorInfo {
	return &SemanticsErrorInfo{typ: UnsupportedErrorType}
}

// TODO: untested
// TableNotUpdatableError
type TableNotUpdatableError struct {
	Table string
}

func (e *TableNotUpdatableError) Error() string {
	return printf(e, "The target table %s of the UPDATE is not updatable", e.Table)
}

func (e *TableNotUpdatableError) Info() *SemanticsErrorInfo {
	return &SemanticsErrorInfo{state: vterrors.NonUpdateableTable, code: vtrpcpb.Code_INVALID_ARGUMENT}
}

// SQLCalcFoundRowsUsageError
type SQLCalcFoundRowsUsageError struct {
}

func (e *SQLCalcFoundRowsUsageError) Error() string {
	return printf(e, "Incorrect usage/placement of 'SQL_CALC_FOUND_ROWS'")
}

func (e *SQLCalcFoundRowsUsageError) Info() *SemanticsErrorInfo {
	return &SemanticsErrorInfo{code: vtrpcpb.Code_INVALID_ARGUMENT}
}

// TODO: untested
// CantUseOptionHereError
type CantUseOptionHereError struct {
	Msg string
}

func (e *CantUseOptionHereError) Error() string {
	return printf(e, "Incorrect usage/placement of '%s'", e.Msg)
}

func (e *CantUseOptionHereError) Info() *SemanticsErrorInfo {
	return &SemanticsErrorInfo{state: vterrors.CantUseOptionHere, code: vtrpcpb.Code_INVALID_ARGUMENT}
}

// TODO: untested
// MissingInVSchemaError
type MissingInVSchemaError struct {
}

func (e *MissingInVSchemaError) Error() string {
	return printf(e, "Table information is not provided in vschema")
}

func (e *MissingInVSchemaError) Info() *SemanticsErrorInfo {
	return &SemanticsErrorInfo{code: vtrpcpb.Code_INVALID_ARGUMENT}
}

// TODO: untested
// NotSequenceTableError
type NotSequenceTableError struct {
	Table string
}

func (e *NotSequenceTableError) Error() string {
	return printf(e, "NEXT used on a non-sequence table `%s`", e.Table)
}

func (e *NotSequenceTableError) Info() *SemanticsErrorInfo {
	return &SemanticsErrorInfo{code: vtrpcpb.Code_INVALID_ARGUMENT}
}

// TODO: untested
// NextWithMultipleTablesError
type NextWithMultipleTablesError struct {
	CountTables int
}

func (e *NextWithMultipleTablesError) Error() string {
	return printf(e, "Next statement should not contain multiple tables: found %d tables", e.CountTables)
}

func (e *NextWithMultipleTablesError) Info() *SemanticsErrorInfo {
	return &SemanticsErrorInfo{typ: BugErrorType}
}

// LockOnlyWithDualError
type LockOnlyWithDualError struct {
	Node *sqlparser.LockingFunc
}

func (e *LockOnlyWithDualError) Error() string {
	return printf(e, "%v allowed only with dual", sqlparser.String(e.Node))
}

func (e *LockOnlyWithDualError) Info() *SemanticsErrorInfo {
	return &SemanticsErrorInfo{code: vtrpcpb.Code_UNIMPLEMENTED}
}

// QualifiedOrderInUnionError
type QualifiedOrderInUnionError struct {
	Table string
}

func (e *QualifiedOrderInUnionError) Error() string {
	return printf(e, "Table `%s` from one of the SELECTs cannot be used in global ORDER clause", e.Table)
}

func (e *QualifiedOrderInUnionError) Info() *SemanticsErrorInfo {
	return &SemanticsErrorInfo{}
}

// JSONTablesError
type JSONTablesError struct {
	Table string
}

func (e *JSONTablesError) Error() string {
	return printf(e, "json_table expressions")
}

func (e *JSONTablesError) Info() *SemanticsErrorInfo {
	return &SemanticsErrorInfo{typ: UnsupportedErrorType}
}

// TODO: untested
// BuggyError
type BuggyError struct {
	Msg string
}

func (e *BuggyError) Error() string {
	return printf(e, e.Msg)
}

func (e *BuggyError) Info() *SemanticsErrorInfo {
	return &SemanticsErrorInfo{typ: BugErrorType}
}

// ColumnNotFoundError
type ColumnNotFoundError struct {
	Column string
}

func (e *ColumnNotFoundError) Error() string {
	return printf(e, "symbol %s not found", e.Column)
}

func (e *ColumnNotFoundError) Info() *SemanticsErrorInfo {
	return &SemanticsErrorInfo{state: vterrors.BadFieldError, code: vtrpcpb.Code_INVALID_ARGUMENT}
}

// AmbiguousColumnError
type AmbiguousColumnError struct {
	Column string
}

func (e *AmbiguousColumnError) Error() string {
	return printf(e, "Column '%s' in field list is ambiguous", e.Column)
}

func (e *AmbiguousColumnError) Info() *SemanticsErrorInfo {
	return &SemanticsErrorInfo{state: vterrors.BadFieldError, code: vtrpcpb.Code_INVALID_ARGUMENT}
}
