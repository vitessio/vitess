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
	Other ErrType = iota
	Unsupported
	Bug
)

const (
	UnionColumnsDoNotMatch ErrorCode = iota
	UnsupportedMultiTablesInUpdate
	UnsupportedNaturalJoin
	TableNotUpdatable
	UnionWithSQLCalcFoundRows
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
	UnionColumnsDoNotMatch: {
		format: "The used SELECT statements have a different number of columns",
		state:  vterrors.WrongNumberOfColumnsInSelect,
		code:   vtrpcpb.Code_FAILED_PRECONDITION,
	},
	UnsupportedMultiTablesInUpdate: {
		format: "multiple tables in update",
		typ:    Unsupported,
	},
	TableNotUpdatable: {
		format: "The target table %s of the UPDATE is not updatable",
		state:  vterrors.NonUpdateableTable,
		code:   vtrpcpb.Code_INVALID_ARGUMENT,
	},
	UnsupportedNaturalJoin: {
		format: "%s",
		typ:    Unsupported,
	},
	UnionWithSQLCalcFoundRows: {
		format: "SQL_CALC_FOUND_ROWS not supported with union",
		typ:    Unsupported,
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
		typ:    Bug,
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
		typ:    Unsupported,
	},
	Buggy: {
		format: "%s",
		typ:    Bug,
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
	case Unsupported:
		format = "VT12001: unsupported: " + format
	case Bug:
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
	case Unsupported:
		return vtrpcpb.Code_UNIMPLEMENTED
	case Bug:
		return vtrpcpb.Code_INTERNAL
	default:
		return f.code
	}
}
