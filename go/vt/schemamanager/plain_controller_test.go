/*
Copyright 2019 The Vitess Authors.

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

package schemamanager

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func TestPlainController(t *testing.T) {
	sql := "CREATE TABLE test_table (pk int)"
	controller := NewPlainController([]string{sql}, "test_keyspace")
	ctx := t.Context()
	err := controller.Open(ctx)
	require.NoError(t, err)

	keyspace := controller.Keyspace()
	require.Equal(t, "test_keyspace", keyspace)

	sqls, err := controller.Read(ctx)
	require.NoError(t, err)
	require.Len(t, sqls, 1, "controller should only get one sql")
	require.Equal(t, sql, sqls[0])

	defer controller.Close()
	err = controller.OnReadSuccess(ctx)
	require.NoError(t, err)

	errReadFail := errors.New("read fail")
	err = controller.OnReadFail(ctx, errReadFail)
	require.ErrorIs(t, err, errReadFail)

	err = controller.OnValidationSuccess(ctx)
	require.NoError(t, err)

	errValidationFail := errors.New("validation fail")
	err = controller.OnValidationFail(ctx, errValidationFail)
	require.ErrorIs(t, err, errValidationFail)

	err = controller.OnExecutorComplete(ctx, &ExecuteResult{})
	require.NoError(t, err)
}

// TestPlainControllerLogs checks that the schema change SQL is logged only
// under result, and that a validation error is logged with its code.
func TestPlainControllerLogs(t *testing.T) {
	sql := "ALTER TABLE t ADD COLUMN c varchar(10) DEFAULT 'leaked-value'"
	controller := NewPlainController([]string{sql}, "test_keyspace")
	ctx := t.Context()
	logs := logtest.Capture(t, slog.LevelInfo)

	err := controller.OnValidationFail(ctx, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "failed to parse sql"))
	require.Error(t, err)
	record := logs.Record(t, "Failed to validate SQL statements")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "failed to parse sql", record["error"])
	assert.Equal(t, "INVALID_ARGUMENT", record["error_code"])

	require.NoError(t, controller.OnExecutorComplete(ctx, &ExecuteResult{Sqls: []string{sql}}))
	record = logs.Record(t, "Executor finished")
	assert.Equal(t, "INFO", record["level"])
	assert.Contains(t, record["result"], "leaked-value")
	for _, record := range logs.Records(t) {
		assert.NotContains(t, record["msg"], "leaked-value")
	}
}
