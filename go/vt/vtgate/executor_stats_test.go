/*
Copyright 2025 The Vitess Authors.

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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"
)

// TestQueryExecutionsByTable_OnError verifies that queryExecutionsByTable counters
// are incremented on successful execution but NOT incremented on execution failure.
func TestQueryExecutionsByTable_OnError(t *testing.T) {
	executor, sbc1, _, _, ctx := createExecutorEnv(t)

	// Set up successful result first
	sbc1.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1")})

	// Get initial counter values
	initialCounts := getCurrentQueryExecutionsByTableCounts()

	// Execute a query successfully first
	session := econtext.NewSafeSession(&vtgatepb.Session{TargetString: KsTestSharded})
	result, err := executorExecSession(ctx, executor, session, "select id from user where id = 1", nil)

	// Verify successful execution
	require.NoError(t, err, "Expected query execution to succeed")
	assert.NotNil(t, result, "Expected valid result")

	// Get counter values after successful execution
	successCounts := getCurrentQueryExecutionsByTableCounts()

	// Check the specific counter that should be incremented for SELECT on user table
	selectUserKey := "SELECT.TestExecutor_user"
	initialUserCount := initialCounts[selectUserKey]
	successUserCount := successCounts[selectUserKey]

	// Verify counter was incremented on successful execution
	assert.Equal(t, initialUserCount+1, successUserCount,
		"queryExecutionsByTable counter should be incremented on successful execution")

	// Now set up the sandbox connection to return an error on next execution
	sbc1.MustFailCodes[vtrpcpb.Code_INTERNAL] = 1

	// Execute the same query again, but this time it should fail
	_, err = executorExecSession(ctx, executor, session, "select id from user where id = 1", nil)

	// Verify that the execution failed
	require.Error(t, err, "Expected query execution to fail")

	// Get counter values after failed execution
	finalCounts := getCurrentQueryExecutionsByTableCounts()

	// Verify that queryExecutionsByTable counter was NOT incremented on execution error
	// The counter should remain the same as after the successful execution
	finalUserCount := finalCounts[selectUserKey]
	assert.Equal(t, successUserCount, finalUserCount,
		"queryExecutionsByTable counter should not be incremented on execution error")
}

// TestQueryExecutions_StreamOnError verifies that the streaming path records the
// per-plan QueryExecutions counter even when a row-returning query fails, while
// leaving the per-table QueryExecutionsByTable counter untouched — matching the
// buffered Execute path.
func TestQueryExecutions_StreamOnError(t *testing.T) {
	executor, sbc1, _, _, ctx := createExecutorEnv(t)

	sbc1.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1")})

	initialExec := getTotalQueryExecutionsCount()
	initialByTable := getTotalQueryExecutionsByTableCount()

	// Make the next execution fail.
	sbc1.MustFailCodes[vtrpcpb.Code_INTERNAL] = 1

	session := econtext.NewSafeSession(&vtgatepb.Session{TargetString: KsTestSharded})
	err := executor.StreamExecute(ctx, nil, "TestQueryExecutions_StreamOnError", session,
		"select id from user where id = 1", nil, false, func(*sqltypes.Result) error { return nil })
	require.Error(t, err, "Expected streamed query execution to fail")

	assert.Equal(t, initialExec+1, getTotalQueryExecutionsCount(),
		"per-plan QueryExecutions must be recorded even when a streamed row-returning query fails")
	assert.Equal(t, initialByTable, getTotalQueryExecutionsByTableCount(),
		"per-table QueryExecutionsByTable must not be incremented on a failed streamed query")
}

// TestQueryExecutions_StreamOnFatalTxError verifies that a row-returning streamed
// query failing with a fatal transaction error (VT15001) inside a transaction
// still records the QueryExecutions counter, matching the buffered Execute path
// which records stats before its rollback handling.
func TestQueryExecutions_StreamOnFatalTxError(t *testing.T) {
	executor, sbc1, _, _, ctx := createExecutorEnv(t)

	initialExec := getTotalQueryExecutionsCount()
	initialByTable := getTotalQueryExecutionsByTableCount()

	// Fail the next execution with a fatal transaction error, which makes the
	// streaming path roll back the transaction before returning. The code must
	// not be ABORTED or RESOURCE_EXHAUSTED: those make the scatter conn roll
	// back the transaction itself, taking the session out of the transaction
	// before the executor's fatal-tx handling can run.
	sbc1.EphemeralShardErr = vterrors.VT15001(vtrpcpb.Code_INTERNAL, "fatal tx error")

	session := econtext.NewSafeSession(&vtgatepb.Session{TargetString: KsTestSharded, InTransaction: true})
	err := executor.StreamExecute(ctx, nil, "TestQueryExecutions_StreamOnFatalTxError", session,
		"select id from user where id = 1", nil, false, func(*sqltypes.Result) error { return nil })
	require.ErrorContains(t, err, "VT15001")
	require.True(t, session.IsErrorUntilRollback(),
		"expected the fatal-tx rollback handling to have run for this error")

	assert.Equal(t, initialExec+1, getTotalQueryExecutionsCount(),
		"QueryExecutions must be recorded when a streamed row-returning query fails with a fatal tx error")
	assert.Equal(t, initialByTable, getTotalQueryExecutionsByTableCount(),
		"per-table QueryExecutionsByTable must not be incremented on a failed streamed query")
}

// TestQueryExecutions_StreamOnFinalSendError verifies that a streamed query that
// executes successfully but fails to deliver the final result to the client still
// records the QueryExecutions counter, as an error, like a mid-stream send failure.
func TestQueryExecutions_StreamOnFinalSendError(t *testing.T) {
	executor, _, _, sbclookup, ctx := createExecutorEnv(t)

	sbclookup.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1")})

	initialExec := getTotalQueryExecutionsCount()
	initialByTable := getTotalQueryExecutionsByTableCount()

	// The fields-only metadata result is sent mid-stream and succeeds; the rows
	// only go out with the final send, so failing on them exercises the
	// final-send error path.
	sendErr := errors.New("failed to send final result to the client")
	session := econtext.NewSafeSession(&vtgatepb.Session{TargetString: KsTestUnsharded})
	err := executor.StreamExecute(ctx, nil, "TestQueryExecutions_StreamOnFinalSendError", session,
		"select id from main1", nil, false, func(qr *sqltypes.Result) error {
			if len(qr.Rows) > 0 {
				return sendErr
			}
			return nil
		})
	require.ErrorContains(t, err, "failed to send final result to the client")

	assert.Equal(t, initialExec+1, getTotalQueryExecutionsCount(),
		"QueryExecutions must be recorded when the final send to the client fails")
	assert.Equal(t, initialByTable, getTotalQueryExecutionsByTableCount(),
		"per-table QueryExecutionsByTable must not be incremented when the final send fails")
}

func TestSlowQueriesCounter(t *testing.T) {
	executor, sbc1, _, _, ctx := createExecutorEnv(t)

	oldThreshold := slowQueryThreshold
	slowQueryThreshold = time.Hour
	t.Cleanup(func() {
		slowQueryThreshold = oldThreshold
		sbc1.ExecDelayResponse = 0
	})

	sbc1.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1"),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1"),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1"),
	})

	session := econtext.NewSafeSession(&vtgatepb.Session{TargetString: KsTestSharded})
	initialCount := getTotalSlowQueryCount()

	_, err := executorExecSession(ctx, executor, session, "select id from user where id = 1", nil)
	require.NoError(t, err)
	assert.Equal(t, initialCount, getTotalSlowQueryCount(), "fast query should not increment slow query count")

	sbc1.ExecDelayResponse = 20 * time.Millisecond
	slowQueryThreshold = 5 * time.Millisecond
	_, err = executorExecSession(ctx, executor, session, "select id from user where id = 1", nil)
	require.NoError(t, err)
	assert.Equal(t, initialCount+1, getTotalSlowQueryCount(), "slow query should increment slow query count")

	sbc1.ExecDelayResponse = 20 * time.Millisecond
	slowQueryThreshold = 0
	_, err = executorExecSession(ctx, executor, session, "select id from user where id = 1", nil)
	require.NoError(t, err)
	assert.Equal(t, initialCount+1, getTotalSlowQueryCount(), "disabled slow query threshold should not increment slow query count")
}

// getCurrentQueryExecutionsByTableCounts returns the current values of all queryExecutionsByTable counters
func getCurrentQueryExecutionsByTableCounts() map[string]int64 {
	// queryExecutionsByTable is a global variable, so we can use its Counts() method
	// to get all counter values. The keys are already formatted as "query.table"
	return queryExecutionsByTable.Counts()
}

func getTotalQueryExecutionsCount() int64 {
	var total int64
	for _, count := range queryExecutions.Counts() {
		total += count
	}
	return total
}

func getTotalQueryExecutionsByTableCount() int64 {
	var total int64
	for _, count := range queryExecutionsByTable.Counts() {
		total += count
	}
	return total
}

func getTotalSlowQueryCount() int64 {
	var total int64
	for _, count := range slowQueries.Counts() {
		total += count
	}
	return total
}
