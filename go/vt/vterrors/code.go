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
	VT03001 = c("VT03001", vtrpcpb.Code_INVALID_ARGUMENT, SyntaxError, "aggregate functions take a single argument '%s'", "The planner accepts aggregate functions that take a single argument only.")
	VT12001 = b("VT12001", vtrpcpb.Code_UNIMPLEMENTED, "unsupported: cross-shard query with aggregates", "Vitess currently is not able to split this aggregation into multiple routes.")

	Errors = []func(args ...any) *OurError{
		VT03001,
		VT12001,
	}
)

type OurError struct {
	Err      error
	Describe string
	ID       string
	State    State
}

func (o *OurError) Error() string {
	return o.Err.Error()
}

var _ error = (*OurError)(nil)

func b(id string, code vtrpcpb.Code, short, long string) func(args ...any) *OurError {
	return func(args ...any) *OurError {
		if len(args) != 0 {
			short = fmt.Sprintf(short, args...)
		}

		return &OurError{
			Err:      New(code, short),
			Describe: long,
			ID:       id,
		}
	}
}

func c(id string, code vtrpcpb.Code, state State, short, long string) func(args ...any) *OurError {
	return func(args ...any) *OurError {
		return &OurError{
			Err:      NewErrorf(code, state, short, args...),
			Describe: long,
			ID:       id,
			State:    state,
		}
	}
}
