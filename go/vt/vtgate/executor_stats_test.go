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
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/sqltypes"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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
	assert.NoError(t, err, "Expected query execution to succeed")
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
	assert.Error(t, err, "Expected query execution to fail")

	// Get counter values after failed execution
	finalCounts := getCurrentQueryExecutionsByTableCounts()

	// Verify that queryExecutionsByTable counter was NOT incremented on execution error
	// The counter should remain the same as after the successful execution
	finalUserCount := finalCounts[selectUserKey]
	assert.Equal(t, successUserCount, finalUserCount,
		"queryExecutionsByTable counter should not be incremented on execution error")
}

// getCurrentQueryExecutionsByTableCounts returns the current values of all queryExecutionsByTable counters
func getCurrentQueryExecutionsByTableCounts() map[string]int64 {
	// queryExecutionsByTable is a global variable, so we can use its Counts() method
	// to get all counter values. The keys are already formatted as "query.table"
	return queryExecutionsByTable.Counts()
}
