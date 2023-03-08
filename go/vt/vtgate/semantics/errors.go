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
	ErrorCode int

	Error struct {
		Code ErrorCode
		args []any
	}
	ErrType int

	info struct {
		format string
		state  vterrors.State
		code   vtrpcpb.Code
		id     string
		typ    ErrType
	}
)

const (
	UndefinedErrorCode = 0
)

const (
	UndefinedErrorType ErrType = iota
	UnsupportedErrorType
	BugErrorType
)

const (
	deprecatedUnionColumnsDoNotMatch ErrorCode = iota
)

func NewError(code ErrorCode, args ...any) *Error {
	return &Error{
		Code: code,
		args: args,
	}
}

var errors = map[ErrorCode]info{}

func (n *Error) Error() string {
	f, ok := errors[n.Code]
	if !ok {
		return "unknown error"
	}

	format := f.format

	if f.id != "" {
		format = fmt.Sprintf("%s: %s", f.id, format)
	}

	switch f.typ {
	case UnsupportedErrorType:
		format = "VT12001: unsupported: " + format
	case BugErrorType:
		format = "VT13001: [BUG] " + format
	}

	var args []any
	for _, arg := range n.args {
		ast, isAST := arg.(sqlparser.SQLNode)
		if isAST {
			args = append(args, sqlparser.String(ast))
		} else {
			args = append(args, arg)
		}
	}

	return fmt.Sprintf(format, args...)
}

func (n *Error) ErrorState() vterrors.State {
	f, ok := errors[n.Code]
	if !ok {
		return vterrors.Undefined
	}

	return f.state
}

func (n *Error) ErrorCode() vtrpcpb.Code {
	f, ok := errors[n.Code]
	if !ok {
		return vtrpcpb.Code_UNKNOWN
	}

	switch f.typ {
	case UnsupportedErrorType:
		return vtrpcpb.Code_UNIMPLEMENTED
	case BugErrorType:
		return vtrpcpb.Code_INTERNAL
	default:
		return f.code
	}
}

func printf(e SemanticsError, msg string, args ...any) string {
	format := msg

	if e.Classify().Id != "" {
		format = fmt.Sprintf("%s: %s", e.Classify().Id, format)
	}

	switch e.Classify().Typ {
	case UnsupportedErrorType:
		format = "VT12001: unsupported: " + format
	case BugErrorType:
		format = "VT13001: [BUG] " + format
	}
	return fmt.Sprintf(format, args...)
}

type SemanticsError interface {
	Error() string
	Classify() *SemanticsErrorClassification
}

type SemanticsErrorClassification struct {
	Code  int
	State vterrors.State
	Typ   ErrType
	Id    string
}

// UnionColumnsDoNotMatchError
type UnionColumnsDoNotMatchError struct {
	FirstProj  int
	SecondProj int
}

func (e *UnionColumnsDoNotMatchError) Error() string {
	return printf(e, "The used SELECT statements have a different number of columns: %v, %v", e.FirstProj, e.SecondProj)
}

func (e *UnionColumnsDoNotMatchError) Classify() *SemanticsErrorClassification {
	return &SemanticsErrorClassification{Code: int(vtrpcpb.Code_FAILED_PRECONDITION), State: vterrors.WrongNumberOfColumnsInSelect}
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

func (e *UnsupportedMultiTablesInUpdateError) Classify() *SemanticsErrorClassification {
	return &SemanticsErrorClassification{Typ: UnsupportedErrorType}
}

// UnsupportedNaturalJoinError
type UnsupportedNaturalJoinError struct {
	Join string
}

func (e *UnsupportedNaturalJoinError) Error() string {
	return printf(e, "", e.Join)
}

func (e *UnsupportedNaturalJoinError) Classify() *SemanticsErrorClassification {
	return &SemanticsErrorClassification{Typ: UnsupportedErrorType}
}

// UnionWithSQLCalcFoundRowsError
type UnionWithSQLCalcFoundRowsError struct {
}

func (e *UnionWithSQLCalcFoundRowsError) Error() string {
	return printf(e, "SQL_CALC_FOUND_ROWS not supported with union")
}

func (e *UnionWithSQLCalcFoundRowsError) Classify() *SemanticsErrorClassification {
	return &SemanticsErrorClassification{Typ: UnsupportedErrorType}
}

// TODO: untested
// TableNotUpdatableError
type TableNotUpdatableError struct {
	Table string
}

func (e *TableNotUpdatableError) Error() string {
	return printf(e, "The target table %s of the UPDATE is not updatable", e.Table)
}

func (e *TableNotUpdatableError) Classify() *SemanticsErrorClassification {
	return &SemanticsErrorClassification{State: vterrors.NonUpdateableTable, Code: int(vtrpcpb.Code_INVALID_ARGUMENT)}
}

// SQLCalcFoundRowsUsageError
type SQLCalcFoundRowsUsageError struct {
}

func (e *SQLCalcFoundRowsUsageError) Error() string {
	return printf(e, "Incorrect usage/placement of 'SQL_CALC_FOUND_ROWS'")
}

func (e *SQLCalcFoundRowsUsageError) Classify() *SemanticsErrorClassification {
	return &SemanticsErrorClassification{Code: int(vtrpcpb.Code_INVALID_ARGUMENT)}
}

// TODO: untested
// CantUseOptionHereError
type CantUseOptionHereError struct {
	Msg string
}

func (e *CantUseOptionHereError) Error() string {
	return printf(e, "Incorrect usage/placement of '%s'", e.Msg)
}

func (e *CantUseOptionHereError) Classify() *SemanticsErrorClassification {
	return &SemanticsErrorClassification{State: vterrors.CantUseOptionHere, Code: int(vtrpcpb.Code_INVALID_ARGUMENT)}
}

// TODO: untested
// MissingInVSchemaError
type MissingInVSchemaError struct {
}

func (e *MissingInVSchemaError) Error() string {
	return printf(e, "Table information is not provided in vschema")
}

func (e *MissingInVSchemaError) Classify() *SemanticsErrorClassification {
	return &SemanticsErrorClassification{Code: int(vtrpcpb.Code_INVALID_ARGUMENT)}
}

// TODO: untested
// NotSequenceTableError
type NotSequenceTableError struct {
	Table string
}

func (e *NotSequenceTableError) Error() string {
	return printf(e, "NEXT used on a non-sequence table %s", e.Table)
}

func (e *NotSequenceTableError) Classify() *SemanticsErrorClassification {
	return &SemanticsErrorClassification{Code: int(vtrpcpb.Code_INVALID_ARGUMENT)}
}

// TODO: untested
// NextWithMultipleTablesError
type NextWithMultipleTablesError struct {
	CountTables int
}

func (e *NextWithMultipleTablesError) Error() string {
	return printf(e, "Next statement should not contain multiple tables: found %d tables", e.CountTables)
}

func (e *NextWithMultipleTablesError) Classify() *SemanticsErrorClassification {
	return &SemanticsErrorClassification{Typ: BugErrorType}
}

// LockOnlyWithDualError
type LockOnlyWithDualError struct {
	Node *sqlparser.LockingFunc
}

func (e *LockOnlyWithDualError) Error() string {
	return printf(e, "%v allowed only with dual", sqlparser.String(e.Node))
}

func (e *LockOnlyWithDualError) Classify() *SemanticsErrorClassification {
	return &SemanticsErrorClassification{Code: int(vtrpcpb.Code_UNIMPLEMENTED)}
}

// QualifiedOrderInUnionError
type QualifiedOrderInUnionError struct {
	Table string
}

func (e *QualifiedOrderInUnionError) Error() string {
	return printf(e, "Table %s from one of the SELECTs cannot be used in global ORDER clause", e.Table)
}

func (e *QualifiedOrderInUnionError) Classify() *SemanticsErrorClassification {
	return &SemanticsErrorClassification{}
}

// JSONTablesError
type JSONTablesError struct {
	Table string
}

func (e *JSONTablesError) Error() string {
	return printf(e, "json_table expressions")
}

func (e *JSONTablesError) Classify() *SemanticsErrorClassification {
	return &SemanticsErrorClassification{Typ: UnsupportedErrorType}
}

// TODO: untested
// BuggyError
type BuggyError struct {
	Msg string
}

func (e *BuggyError) Error() string {
	return printf(e, e.Msg)
}

func (e *BuggyError) Classify() *SemanticsErrorClassification {
	return &SemanticsErrorClassification{Typ: BugErrorType}
}

// ColumnNotFoundError
type ColumnNotFoundError struct {
	Column string
}

func (e *ColumnNotFoundError) Error() string {
	return printf(e, "symbol %s not found", e.Column)
}

func (e *ColumnNotFoundError) Classify() *SemanticsErrorClassification {
	return &SemanticsErrorClassification{State: vterrors.BadFieldError, Code: int(vtrpcpb.Code_INVALID_ARGUMENT)}
}

// AmbiguousColumnError
type AmbiguousColumnError struct {
	Column string
}

func (e *AmbiguousColumnError) Error() string {
	return printf(e, "Column '%s' in field list is ambiguous", e.Column)
}

func (e *AmbiguousColumnError) Classify() *SemanticsErrorClassification {
	return &SemanticsErrorClassification{State: vterrors.BadFieldError, Code: int(vtrpcpb.Code_INVALID_ARGUMENT)}
}
