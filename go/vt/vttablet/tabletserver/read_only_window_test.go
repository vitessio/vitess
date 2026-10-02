/*
Copyright 2026 The Vitess Authors.

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

package tabletserver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TestWriteRefusedDuringGroupReplicationBootstrapIsRetried reproduces the writes that failed in
// TestGroupReplicationLifecycle while MigrateReplicationMode bootstrapped the group on the serving
// semi-sync primary. MySQL was super_read_only for a few milliseconds and refused a write with
// errno 1290, which the tablet reports as CLUSTER_EVENT: vtgate buffered it, waited 30s for a
// failover that never came since the same primary kept serving, and failed it. Or Group
// Replication's before_commit hook refused a commit already under way with errno 3100, which vtgate
// failed right away. During the bootstrap, the tablet now retries such a write once MySQL is the
// writable primary of its group.
func TestWriteRefusedDuringGroupReplicationBootstrapIsRetried(t *testing.T) {
	const (
		query      = "insert into test_table(a) values(1)"
		mysqlQuery = "insert into test_table(a) values (1)"
	)
	tests := []struct {
		name    string
		refusal *sqlerror.SQLError
		// code is the code with which the tablet reports the refusal outside of a bootstrap.
		code vtrpcpb.Code
	}{{
		name: "read-only",
		refusal: sqlerror.NewSQLError(sqlerror.EROptionPreventsStatement, sqlerror.SSUnknownSQLState,
			"The MySQL server is running with the --read-only option so it cannot execute this statement"),
		code: vtrpcpb.Code_CLUSTER_EVENT,
	}, {
		name: "before_commit hook",
		refusal: sqlerror.NewSQLError(sqlerror.ERRunHookError, sqlerror.SSUnknownSQLState,
			"Error on observer while running replication hook 'before_commit'."),
		code: vtrpcpb.Code_UNKNOWN,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			db := setUpQueryExecutorTest(t)
			t.Cleanup(db.Close)
			tsv := newTestTabletServer(ctx, noFlags, db)
			t.Cleanup(tsv.StopService)
			db.AddRejectedQuery(mysqlQuery, tt.refusal)

			// Outside of a bootstrap, the refusal is reported as before.
			_, err := newTestQueryExecutor(ctx, tsv, query, 0).Execute()
			require.Error(t, err)
			assert.Equal(t, tt.code, convertErrorCode(err))
			assert.Equal(t, 1, db.GetQueryCalledNum(mysqlQuery))

			// The tablet manager makes MySQL bootstrap the group: a refused write waits for the
			// bootstrap to end.
			tsv.SetGroupReplicationBootstrapInProgress(true)
			type outcome struct {
				result *sqltypes.Result
				err    error
			}
			done := make(chan outcome, 1)
			go func() {
				result, err := newTestQueryExecutor(ctx, tsv, query, 0).Execute()
				done <- outcome{result, err}
			}()
			require.Eventually(t, func() bool { return db.GetQueryCalledNum(mysqlQuery) == 2 }, 30*time.Second, time.Millisecond)
			select {
			case <-done:
				require.FailNow(t, "the write must wait for the bootstrap to end")
			default:
			}

			// MySQL is the writable primary of its group, and the bootstrap ends: the write is
			// retried.
			db.DeleteRejectedQuery(mysqlQuery)
			db.AddQuery(mysqlQuery, &sqltypes.Result{RowsAffected: 1})
			tsv.SetGroupReplicationBootstrapInProgress(false)
			var got outcome
			select {
			case got = <-done:
			case <-time.After(30 * time.Second):
				require.FailNow(t, "the write was not retried")
			}
			require.NoError(t, got.err)
			assert.EqualValues(t, 1, got.result.RowsAffected)
			assert.Equal(t, 1, db.GetQueryCalledNum(mysqlQuery), "the write was retried once")

			// A statement of a client's transaction is not retried: the client owns the
			// transaction.
			db.AddRejectedQuery(mysqlQuery, tt.refusal)
			tsv.SetGroupReplicationBootstrapInProgress(true)
			t.Cleanup(func() { tsv.SetGroupReplicationBootstrapInProgress(false) })
			txID := newTransaction(tsv, nil)
			start := time.Now()
			_, err = newTestQueryExecutor(ctx, tsv, query, txID).Execute()
			require.Error(t, err)
			assert.Less(t, time.Since(start), readOnlyWindowMaxWait, "a statement of a client's transaction does not wait")
			_, err = tsv.Rollback(ctx, tsv.sm.Target(), txID)
			require.NoError(t, err)
		})
	}
}

// TestReadOnlyWindowGrace checks that a refusal that reaches the query executor shortly after the
// window ended is still retried, and an unrelated error never is.
func TestReadOnlyWindowGrace(t *testing.T) {
	var w readOnlyWindow
	readOnly := sqlerror.NewSQLError(sqlerror.EROptionPreventsStatement, sqlerror.SSUnknownSQLState, "read-only")
	assert.False(t, w.waitToRetry(t.Context(), readOnly), "no window")
	w.set(true)
	w.set(false)
	assert.True(t, w.waitToRetry(t.Context(), readOnly), "the window ended a moment ago")
	assert.True(t, w.waitToRetry(t.Context(), sqlerror.NewSQLError(sqlerror.ERRunHookError, sqlerror.SSUnknownSQLState, "before_commit")))
	assert.False(t, w.waitToRetry(t.Context(), sqlerror.NewSQLError(sqlerror.ERDupEntry, sqlerror.SSUnknownSQLState, "duplicate")))
}
