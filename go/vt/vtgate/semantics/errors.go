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
	TableNotUpdatable
	SQLCalcFoundRowsUsage
	CantUseOptionHere
	MissingInVSchema
	NotSequenceTable
	NextWithMultipleTables
	LockOnlyWithDual
	QualifiedOrderInUnion
	JSONTables
	Buggy
	ColumnNotFound
	AmbiguousColumn
)

func NewError(code ErrorCode, args ...any) *Error {
	return &Error{
		Code: code,
		args: args,
	}
}

var errors = map[ErrorCode]info{
	TableNotUpdatable: {
		format: "The target table %s of the UPDATE is not updatable",
		state:  vterrors.NonUpdateableTable,
		code:   vtrpcpb.Code_INVALID_ARGUMENT,
	},
	SQLCalcFoundRowsUsage: {
		format: "Incorrect usage/placement of 'SQL_CALC_FOUND_ROWS'",
		code:   vtrpcpb.Code_INVALID_ARGUMENT,
	},
	CantUseOptionHere: {
		format: "Incorrect usage/placement of '%s'",
		state:  vterrors.CantUseOptionHere,
		code:   vtrpcpb.Code_INVALID_ARGUMENT,
	},
	MissingInVSchema: {
		format: "Table information is not provided in vschema",
		code:   vtrpcpb.Code_INVALID_ARGUMENT,
	},
	NotSequenceTable: {
		format: "NEXT used on a non-sequence table",
		code:   vtrpcpb.Code_INVALID_ARGUMENT,
	},
	NextWithMultipleTables: {
		format: "Next statement should not contain multiple tables",
		typ:    BugErrorType,
	},
	LockOnlyWithDual: {
		format: "%v allowed only with dual",
		code:   vtrpcpb.Code_UNIMPLEMENTED,
	},
	QualifiedOrderInUnion: {
		format: "Table %s from one of the SELECTs cannot be used in global ORDER clause",
	},
	JSONTables: {
		format: "json_table expressions",
		typ:    UnsupportedErrorType,
	},
	Buggy: {
		format: "%s",
		typ:    BugErrorType,
	},
	ColumnNotFound: {
		format: "symbol %s not found",
		state:  vterrors.BadFieldError,
		code:   vtrpcpb.Code_INVALID_ARGUMENT,
	},
	AmbiguousColumn: {
		format: "Column '%s' in field list is ambiguous",
		state:  vterrors.BadFieldError,
		code:   vtrpcpb.Code_INVALID_ARGUMENT,
	},
}

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

// TODO: missing test
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

// UnionWithSQLCalcFoundRows
type UnionWithSQLCalcFoundRowsError struct {
}

func (e *UnionWithSQLCalcFoundRowsError) Error() string {
	return printf(e, "SQL_CALC_FOUND_ROWS not supported with union")
}

func (e *UnionWithSQLCalcFoundRowsError) Classify() *SemanticsErrorClassification {
	return &SemanticsErrorClassification{Typ: UnsupportedErrorType}
}
