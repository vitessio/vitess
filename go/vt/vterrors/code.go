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

package vterrors

import (
	"fmt"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	VT03001 = errorWithState("VT03001", vtrpcpb.Code_INVALID_ARGUMENT, SyntaxError, "aggregate functions take a single argument '%s'", "The planner accepts aggregate functions that take a single argument only.")
	VT03002 = errorWithState("VT03002", vtrpcpb.Code_INVALID_ARGUMENT, ForbidSchemaChange, "Changing schema from '%s' to '%s' is not allowed", "aa")
	VT03003 = errorWithState("VT03003", vtrpcpb.Code_INVALID_ARGUMENT, UnknownTable, "Unknown table '%s' in MULTI DELETE", "aa")
	VT03004 = errorWithState("VT03004", vtrpcpb.Code_INVALID_ARGUMENT, NonUpdateableTable, "The target table %s of the DELETE is not updatable", "aa")

	VT05001 = errorWithState("VT05001", vtrpcpb.Code_NOT_FOUND, DbDropExists, "Can't drop database '%s'; database doesn't exists", "aa")
	VT05002 = errorWithState("VT05002", vtrpcpb.Code_NOT_FOUND, BadDb, "Can't alter database '%s'; unknown database", "aa")

	VT06001 = errorWithState("VT06001", vtrpcpb.Code_ALREADY_EXISTS, DbCreateExists, "Can't create database '%s'; database exists", "aa")

	VT12001 = errorWithoutState("VT12001", vtrpcpb.Code_UNIMPLEMENTED, "unsupported: %s", "aa")

	// VT13001 General Error
	VT13001 = errorWithoutState("VT13001", vtrpcpb.Code_INTERNAL, "[BUG] %s", "aa")

	// VT13002 Test
	VT13002 = errorWithoutState("VT13002", vtrpcpb.Code_INTERNAL, "[BUG] %s", "aa")

	Errors = []func(args ...any) *OurError{
		VT03001,
		VT03002,
		VT03003,
		VT03004,
		VT05001,
		VT05002,
		VT06001,
		VT12001,
		VT13001,
		VT13002,
	}
)

type OurError struct {
	Err         error
	Description string
	ID          string
	State       State
}

func (o *OurError) Error() string {
	return o.Err.Error()
}

var _ error = (*OurError)(nil)

func errorWithoutState(id string, code vtrpcpb.Code, short, long string) func(args ...any) *OurError {
	return func(args ...any) *OurError {
		if len(args) != 0 {
			short = fmt.Sprintf(short, args...)
		}

		return &OurError{
			Err:         New(code, id+": "+short),
			Description: long,
			ID:          id,
		}
	}
}

func errorWithState(id string, code vtrpcpb.Code, state State, short, long string) func(args ...any) *OurError {
	return func(args ...any) *OurError {
		return &OurError{
			Err:         NewErrorf(code, state, id+": "+short, args...),
			Description: long,
			ID:          id,
			State:       state,
		}
	}
}
