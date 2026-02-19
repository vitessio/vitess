/*
Copyright 2024 The Vitess Authors.

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

package vtgate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/sqlerror"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"
)

func TestParseTransactionModeString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected vtgatepb.TransactionMode
		wantErr  bool
	}{
		{name: "single", input: "single", expected: vtgatepb.TransactionMode_SINGLE},
		{name: "multi", input: "multi", expected: vtgatepb.TransactionMode_MULTI},
		{name: "twopc", input: "twopc", expected: vtgatepb.TransactionMode_TWOPC},
		{name: "TWOPC_upper", input: "TWOPC", expected: vtgatepb.TransactionMode_TWOPC},
		{name: "unspecified", input: "unspecified", expected: vtgatepb.TransactionMode_UNSPECIFIED},
		{name: "UNSPECIFIED_upper", input: "UNSPECIFIED", expected: vtgatepb.TransactionMode_UNSPECIFIED},
		{name: "empty_string", input: "", wantErr: true},
		{name: "garbage", input: "invalid", wantErr: true},
		{name: "spaces_around_valid", input: " twopc ", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseTransactionModeString(tc.input)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, got)
			}
		})
	}
}

func TestSetTransactionModeLimitWithUnspecifiedLimit(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)

	executor.vConfig.TransactionModeLimit = func() vtgatepb.TransactionMode {
		return vtgatepb.TransactionMode_UNSPECIFIED
	}

	for _, mode := range []string{"single", "multi", "twopc"} {
		t.Run(mode, func(t *testing.T) {
			session := econtext.NewSafeSession(&vtgatepb.Session{Autocommit: true})
			_, err := executorExecSession(ctx, executor, session, "set transaction_mode = '"+mode+"'", nil)
			require.NoError(t, err, "limit=UNSPECIFIED should not reject any mode")
		})
	}
}

func TestSetTransactionModeLimitErrorCode(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)
	executor.vConfig.TransactionModeLimit = func() vtgatepb.TransactionMode {
		return vtgatepb.TransactionMode_SINGLE
	}

	session := econtext.NewSafeSession(&vtgatepb.Session{Autocommit: true})
	_, err := executorExecSession(ctx, executor, session, "set transaction_mode = 'multi'", nil)
	require.Error(t, err)

	converted := sqlerror.NewSQLErrorFromError(err)
	sqlErr, ok := converted.(*sqlerror.SQLError)
	require.True(t, ok, "error should be convertible to *sqlerror.SQLError")
	assert.Equal(t, sqlerror.ERWrongValueForVar, sqlErr.Number(),
		"limit violation must return MySQL error code 1231 (ERWrongValueForVar)")
}

func TestCompoundSetWithLimitFailure(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)
	executor.vConfig.TransactionModeLimit = func() vtgatepb.TransactionMode {
		return vtgatepb.TransactionMode_MULTI
	}

	t.Run("earlier_ops_take_effect_before_failure", func(t *testing.T) {
		session := econtext.NewSafeSession(&vtgatepb.Session{})
		_, err := executorExecSession(ctx, executor, session,
			"set autocommit = 1, transaction_mode = 'twopc'", nil)
		require.Error(t, err)
		require.ErrorContains(t, err, "exceeds vtgate limit")
		assert.True(t, session.Session.Autocommit, "autocommit should be set despite later failure")
	})

	t.Run("later_ops_skipped_after_failure", func(t *testing.T) {
		session := econtext.NewSafeSession(&vtgatepb.Session{})
		_, err := executorExecSession(ctx, executor, session,
			"set transaction_mode = 'twopc', autocommit = 1", nil)
		require.Error(t, err)
		assert.False(t, session.Session.Autocommit, "autocommit must NOT be set when prior op fails")
	})
}

func TestSelectTransactionModeRoundtrip(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)
	executor.vConfig.TransactionModeLimit = func() vtgatepb.TransactionMode {
		return vtgatepb.TransactionMode_TWOPC
	}

	t.Run("set_then_select", func(t *testing.T) {
		session := econtext.NewSafeSession(&vtgatepb.Session{Autocommit: true})
		_, err := executorExecSession(ctx, executor, session, "set transaction_mode = 'single'", nil)
		require.NoError(t, err)

		qr, err := executorExecSession(ctx, executor, session, "select @@transaction_mode", nil)
		require.NoError(t, err)
		require.Len(t, qr.Rows, 1)
		assert.Equal(t, "SINGLE", qr.Rows[0][0].ToString())
	})

	t.Run("unspecified_shows_default_not_literal", func(t *testing.T) {
		session := econtext.NewSafeSession(&vtgatepb.Session{Autocommit: true})
		_, err := executorExecSession(ctx, executor, session, "set transaction_mode = 'unspecified'", nil)
		require.NoError(t, err)

		qr, err := executorExecSession(ctx, executor, session, "select @@transaction_mode", nil)
		require.NoError(t, err)
		require.Len(t, qr.Rows, 1)
		assert.NotEqual(t, "UNSPECIFIED", qr.Rows[0][0].ToString(),
			"SELECT @@transaction_mode should show resolved default, not 'UNSPECIFIED'")
	})

	t.Run("fresh_session_shows_default", func(t *testing.T) {
		session := econtext.NewSafeSession(&vtgatepb.Session{Autocommit: true})
		qr, err := executorExecSession(ctx, executor, session, "select @@transaction_mode", nil)
		require.NoError(t, err)
		require.Len(t, qr.Rows, 1)
		assert.NotEqual(t, "UNSPECIFIED", qr.Rows[0][0].ToString())
	})
}

func TestSequentialSetWithDynamicLimitChange(t *testing.T) {
	executor, _, _, _, ctx := createExecutorEnv(t)

	executor.vConfig.TransactionModeLimit = func() vtgatepb.TransactionMode {
		return vtgatepb.TransactionMode_TWOPC
	}
	session := econtext.NewSafeSession(&vtgatepb.Session{Autocommit: true})
	_, err := executorExecSession(ctx, executor, session, "set transaction_mode = 'twopc'", nil)
	require.NoError(t, err)

	executor.vConfig.TransactionModeLimit = func() vtgatepb.TransactionMode {
		return vtgatepb.TransactionMode_SINGLE
	}

	session2 := econtext.NewSafeSession(&vtgatepb.Session{Autocommit: true})
	_, err = executorExecSession(ctx, executor, session2, "set transaction_mode = 'twopc'", nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "exceeds vtgate limit")
}
