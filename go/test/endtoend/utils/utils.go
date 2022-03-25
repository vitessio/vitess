/*
Copyright 2021 The Vitess Authors.

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

package utils

import (
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"vitess.io/vitess/go/test/utils"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
)

func AssertMatches(t *testing.T, conn *mysql.Conn, query, expected string) {
	t.Helper()
	qr := Exec(t, conn, query)
	got := fmt.Sprintf("%v", qr.Rows)
	diff := cmp.Diff(expected, got)
	if diff != "" {
		t.Errorf("Query: %s (-want +got):\n%s\nGot:%s", query, diff, got)
	}
}

func AssertContainsError(t *testing.T, conn *mysql.Conn, query, expected string) {
	t.Helper()
	_, err := ExecAllowError(t, conn, query)
	require.Error(t, err)
	assert.Contains(t, err.Error(), expected, "actual error: %s", err.Error())
}

func AssertMatchesNoOrder(t *testing.T, conn *mysql.Conn, query, expected string) {
	t.Helper()
	qr := Exec(t, conn, query)
	actual := fmt.Sprintf("%v", qr.Rows)
	assert.Equal(t, utils.SortString(expected), utils.SortString(actual), "for query: [%s] expected \n%s \nbut actual \n%s", query, expected, actual)
}

func AssertIsEmpty(t *testing.T, conn *mysql.Conn, query string) {
	t.Helper()
	qr := Exec(t, conn, query)
	assert.Empty(t, qr.Rows, "for query: "+query)
}

func AssertFoundRowsValue(t *testing.T, conn *mysql.Conn, query, workload string, count int) {
	Exec(t, conn, query)
	qr := Exec(t, conn, "select found_rows()")
	got := fmt.Sprintf("%v", qr.Rows)
	want := fmt.Sprintf(`[[UINT64(%d)]]`, count)
	assert.Equalf(t, want, got, "Workload: %s\nQuery:%s\n", workload, query)
}

func AssertSingleRowIsReturned(t *testing.T, conn *mysql.Conn, predicate string, expectedKs string) {
	t.Run(predicate, func(t *testing.T) {
		qr, err := conn.ExecuteFetch("SELECT distinct table_schema FROM information_schema.tables WHERE "+predicate, 1000, true)
		require.NoError(t, err)
		assert.Equal(t, 1, len(qr.Rows), "did not get enough rows back")
		assert.Equal(t, expectedKs, qr.Rows[0][0].ToString())
	})
}

func AssertResultIsEmpty(t *testing.T, conn *mysql.Conn, pre string) {
	t.Run(pre, func(t *testing.T) {
		qr, err := conn.ExecuteFetch("SELECT distinct table_schema FROM information_schema.tables WHERE "+pre, 1000, true)
		require.NoError(t, err)
		assert.Empty(t, qr.Rows)
	})
}

func Exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err, "for query: "+query)
	return qr
}

func ExecAllowError(t *testing.T, conn *mysql.Conn, query string) (*sqltypes.Result, error) {
	t.Helper()
	return conn.ExecuteFetch(query, 1000, true)
}

// SkipIfBinaryIsBelowVersion skips the given test if the binary's major version is below majorVersion.
func SkipIfBinaryIsBelowVersion(t *testing.T, majorVersion int, binary string) {
	version, err := cluster.GetMajorVersion(binary)
	if err != nil {
		return
	}
	if version < majorVersion {
		t.Skip("Current version of ", binary, ": v", version, ", expected version >= v", majorVersion)
	}
}

// AssertMatchesWithTimeout asserts that the given query produces the expected result.
// The query will be executed every 'r' duration until it matches the expected result.
// If after 'd' duration we still did not find the expected result, the test will be marked as failed.
func AssertMatchesWithTimeout(t *testing.T, conn *mysql.Conn, query, expected string, r time.Duration, d time.Duration, failureMsg string) {
	t.Helper()
	timeout := time.After(d)
	diff := "actual and expectation does not match"
	for len(diff) > 0 {
		select {
		case <-timeout:
			require.Fail(t, failureMsg, diff)
		case <-time.After(r):
			qr := Exec(t, conn, query)
			diff = cmp.Diff(expected,
				fmt.Sprintf("%v", qr.Rows))
		}

	}
}
