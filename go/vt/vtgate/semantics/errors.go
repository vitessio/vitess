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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	ErrorCode int

	Error struct {
		Code ErrorCode
	}

	info struct {
		format string
		state  vterrors.State
		code   vtrpcpb.Code
	}
)

var errors = map[ErrorCode]info{
	UnionColumnsDoNotMatch: {
		format: "The used SELECT statements have a different number of columns",
		state:  vterrors.WrongNumberOfColumnsInSelect,
		code:   vtrpcpb.Code_FAILED_PRECONDITION,
	},
}

const (
	UnionColumnsDoNotMatch ErrorCode = iota
)

func (n *Error) Error() string {
	f, ok := errors[n.Code]
	if !ok {
		return "unknown error"
	}

	return f.format
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

	return f.code
}
