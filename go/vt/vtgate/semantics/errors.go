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
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	ErrorCode int

	Error struct {
		Code ErrorCode
		args []any
	}

	info struct {
		format      string
		state       vterrors.State
		code        vtrpcpb.Code
		id          string
		unsupported bool
	}
)

const (
	UnionColumnsDoNotMatch ErrorCode = iota
	UnsupportedMultiTablesInUpdate
	UnsupportedNaturalJoin
)

func NewError(code ErrorCode, args ...any) error {
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
		format:      "multiple tables in update",
		unsupported: true,
	},
	UnsupportedNaturalJoin: {
		format:      "%s",
		unsupported: true,
	},
}

func (n *Error) Error() string {
	var format string
	f, ok := errors[n.Code]
	if !ok {
		return "unknown error"
	}
	format = f.format
	if f.id != "" {
		format = fmt.Sprintf("%s: %s", f.id, format)
	} else if f.unsupported {
		format = "VT12001: unsupported: " + format
	}

	sprintf := fmt.Sprintf(format, n.args...)
	return sprintf
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

	if f.unsupported {
		return vtrpcpb.Code_UNIMPLEMENTED
	}

	return f.code
}
