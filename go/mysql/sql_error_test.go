/*
Copyright 2020 The Vitess Authors.

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

package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"github.com/stretchr/testify/assert"
)

func TestDumuxResourceExhaustedErrors(t *testing.T) {
	type testCase struct {
		msg  string
		want int
	}

	cases := []testCase{
		{"misc", ERTooManyUserConnections},
		{"grpc: received message larger than max (99282 vs. 1234): trailer", ERNetPacketTooLarge},
		{"grpc: received message larger than max (1234 vs. 1234)", ERNetPacketTooLarge},
		{"header: grpc: received message larger than max (1234 vs. 1234)", ERNetPacketTooLarge},
		// This should be explicitly handled by returning ERNetPacketTooLarge from the execturo directly
		// and therefore shouldn't need to be teased out of another error.
		{"in-memory row count exceeded allowed limit of 13", ERTooManyUserConnections},
	}

	for _, c := range cases {
		got := demuxResourceExhaustedErrors(c.msg)
		assert.Equalf(t, c.want, got, c.msg)
	}
}

func TestNewSQLErrorFromError(t *testing.T) {
	var tCases = []struct {
		err error
		num int
		ss  string
	}{
		{
			err: vterrors.Errorf(vtrpc.Code_OK, "ok"),
			num: ERUnknownError,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_CANCELED, "cancelled"),
			num: ERQueryInterrupted,
			ss:  SSQueryInterrupted,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_UNKNOWN, "unknown"),
			num: ERUnknownError,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "invalid argument"),
			num: ERUnknownError,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_DEADLINE_EXCEEDED, "deadline exceeded"),
			num: ERQueryInterrupted,
			ss:  SSQueryInterrupted,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_NOT_FOUND, "code not found"),
			num: ERUnknownError,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_ALREADY_EXISTS, "already exists"),
			num: ERUnknownError,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_PERMISSION_DENIED, "permission denied"),
			num: ERAccessDeniedError,
			ss:  SSAccessDeniedError,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_UNAUTHENTICATED, "unauthenticated"),
			num: ERAccessDeniedError,
			ss:  SSAccessDeniedError,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_RESOURCE_EXHAUSTED, "resource exhausted"),
			num: ERTooManyUserConnections,
			ss:  SSClientError,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "failed precondition"),
			num: ERUnknownError,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_ABORTED, "aborted"),
			num: ERQueryInterrupted,
			ss:  SSQueryInterrupted,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_OUT_OF_RANGE, "out of range"),
			num: ERUnknownError,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "unimplemented"),
			num: ERNotSupportedYet,
			ss:  SSClientError,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_INTERNAL, "internal"),
			num: ERInternalError,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_UNAVAILABLE, "unavailable"),
			num: ERUnknownError,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_DATA_LOSS, "data loss"),
			num: ERUnknownError,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.NewErrorf(vtrpc.Code_ALREADY_EXISTS, vterrors.DbCreateExists, "create db exists"),
			num: ERDbCreateExists,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.NewErrorf(vtrpc.Code_FAILED_PRECONDITION, vterrors.NoDB, "no db selected"),
			num: ERNoDb,
			ss:  SSNoDB,
		},
	}

	for _, tc := range tCases {
		t.Run(tc.err.Error(), func(t *testing.T) {
			err := NewSQLErrorFromError(tc.err)
			sErr, ok := err.(*SQLError)
			require.True(t, ok)
			assert.Equal(t, tc.num, sErr.Number())
			assert.Equal(t, tc.ss, sErr.SQLState())
		})
	}
}
