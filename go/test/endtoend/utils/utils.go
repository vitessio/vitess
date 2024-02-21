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
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

// AssertContains ensures the given query result contains the expected results.
func AssertContains(t testing.TB, conn *mysql.Conn, query, expected string) {
	t.Helper()
	qr := Exec(t, conn, query)
	got := fmt.Sprintf("%v", qr.Rows)
	assert.Contains(t, got, expected, "Query: %s", query)
}

// AssertMatches ensures the given query produces the expected results.
func AssertMatches(t testing.TB, conn *mysql.Conn, query, expected string) {
	t.Helper()
	qr := Exec(t, conn, query)
	got := fmt.Sprintf("%v", qr.Rows)
	diff := cmp.Diff(expected, got)
	if diff != "" {
		t.Errorf("Query: %s (-want +got):\n%s\nGot:%s", query, diff, got)
	}
}

// AssertMatchesContains ensures the given query produces the given substring.
func AssertMatchesContains(t testing.TB, conn *mysql.Conn, query string, substrings ...string) {
	t.Helper()
	qr := Exec(t, conn, query)
	got := fmt.Sprintf("%v", qr.Rows)
	for _, substring := range substrings {
		if !strings.Contains(got, substring) {
			t.Errorf("Query: %s Got:\n%s\nLooking for substring:%s", query, got, substring)
		}
	}
}

// AssertMatchesNotContains ensures the given query's output doesn't have the given substring.
func AssertMatchesNotContains(t testing.TB, conn *mysql.Conn, query string, substrings ...string) {
	t.Helper()
	qr := Exec(t, conn, query)
	got := fmt.Sprintf("%v", qr.Rows)
	for _, substring := range substrings {
		if strings.Contains(got, substring) {
			t.Errorf("Query: %s Got:\n%s\nFound substring:%s", query, got, substring)
		}
	}
}

// AssertMatchesAny ensures the given query produces any one of the expected results.
func AssertMatchesAny(t testing.TB, conn *mysql.Conn, query string, expected ...string) {
	t.Helper()
	qr := Exec(t, conn, query)
	got := fmt.Sprintf("%v", qr.Rows)
	for _, e := range expected {
		diff := cmp.Diff(e, got)
		if diff == "" {
			return
		}
	}
	t.Errorf("Query: %s (-want +got):\n%v\nGot:%s", query, expected, got)
}

// AssertMatchesCompareMySQL executes the given query on both Vitess and MySQL and make sure
// they have the same result set. The result set of Vitess is then matched with the given expectation.
func AssertMatchesCompareMySQL(t *testing.T, vtConn, mysqlConn *mysql.Conn, query, expected string) {
	t.Helper()
	qr := ExecCompareMySQL(t, vtConn, mysqlConn, query)
	got := fmt.Sprintf("%v", qr.Rows)
	diff := cmp.Diff(expected, got)
	if diff != "" {
		t.Errorf("Query: %s (-want +got):\n%s\nGot:%s", query, diff, got)
	}
}

// AssertContainsError ensures that the given query returns a certain error.
func AssertContainsError(t *testing.T, conn *mysql.Conn, query, expected string) {
	t.Helper()
	_, err := ExecAllowError(t, conn, query)
	require.Error(t, err)
	assert.Contains(t, err.Error(), expected, "actual error: %s", err.Error())
}

// AssertMatchesNoOrder executes the given query and makes sure it matches the given `expected` string.
// The order applied to the results or expectation is ignored. They are both re-sorted.
func AssertMatchesNoOrder(t *testing.T, conn *mysql.Conn, query, expected string) {
	t.Helper()
	qr := Exec(t, conn, query)
	if err := sqltypes.RowsEqualsStr(expected, qr.Rows); err != nil {
		t.Errorf("for query: [%s] %v", query, err)
	}
}

// AssertIsEmpty ensures that the given query returns 0 row.
func AssertIsEmpty(t *testing.T, conn *mysql.Conn, query string) {
	t.Helper()
	qr := Exec(t, conn, query)
	assert.Empty(t, qr.Rows, "for query: "+query)
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

// Exec executes the given query using the given connection. The results are returned.
// The test fails if the query produces an error.
func Exec(t testing.TB, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err, "for query: "+query)
	return qr
}

// ExecMulti executes the given (potential multi) queries using the given connection.
// The test fails if any of the queries produces an error
func ExecMulti(t testing.TB, conn *mysql.Conn, query string) error {
	t.Helper()
	err := conn.ExecuteFetchMultiDrain(query)
	require.NoError(t, err, "for query: "+query)
	return err
}

// ExecCompareMySQL executes the given query against both Vitess and MySQL and compares
// the two result set. If there is a mismatch, the difference will be printed and the
// test will fail. If the query produces an error in either Vitess or MySQL, the test
// will be marked as failed.
// The result set of Vitess is returned to the caller.
func ExecCompareMySQL(t *testing.T, vtConn, mysqlConn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	vtQr, err := vtConn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err, "[Vitess Error] for query: "+query)

	mysqlQr, err := mysqlConn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err, "[MySQL Error] for query: "+query)
	compareVitessAndMySQLResults(t, query, vtConn, vtQr, mysqlQr, false)
	return vtQr
}

// ExecAllowError executes the given query without failing the test if it produces
// an error. The error is returned to the client, along with the result set.
func ExecAllowError(t testing.TB, conn *mysql.Conn, query string) (*sqltypes.Result, error) {
	t.Helper()
	return conn.ExecuteFetch(query, 1000, true)
}

// ExecWithRowCount is similar to ExecAllowError with max row count provided.
func ExecWithRowCount(t testing.TB, conn *mysql.Conn, query string, rowCount int) (*sqltypes.Result, error) {
	t.Helper()
	return conn.ExecuteFetch(query, rowCount, true)
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

// BinaryIsAtLeastAtVersion returns true if this binary is at or above the required version
func BinaryIsAtLeastAtVersion(majorVersion int, binary string) bool {
	version, err := cluster.GetMajorVersion(binary)
	if err != nil {
		return false
	}
	return version >= majorVersion

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
			qr, err := ExecAllowError(t, conn, query)
			if err != nil {
				diff = err.Error()
				break
			}
			diff = cmp.Diff(expected,
				fmt.Sprintf("%v", qr.Rows))
		}

	}
}

// WaitForAuthoritative waits for a table to become authoritative
func WaitForAuthoritative(t *testing.T, ks, tbl string, readVSchema func() (*interface{}, error)) error {
	timeout := time.After(60 * time.Second)
	for {
		select {
		case <-timeout:
			return fmt.Errorf("schema tracking didn't mark table t2 as authoritative until timeout")
		default:
			res, err := readVSchema()
			require.NoError(t, err, res)
			t2Map := getTableT2Map(res, ks, tbl)
			authoritative, fieldPresent := t2Map["column_list_authoritative"]
			if !fieldPresent {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			authoritativeBool, isBool := authoritative.(bool)
			if !isBool || !authoritativeBool {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return nil
		}
	}
}

// WaitForKsError waits for the ks error field to be populated and returns it.
func WaitForKsError(t *testing.T, vtgateProcess cluster.VtgateProcess, ks string) string {
	var errString string
	WaitForVschemaCondition(t, vtgateProcess, ks, func(t *testing.T, keyspace map[string]interface{}) bool {
		ksErr, fieldPresent := keyspace["error"]
		if !fieldPresent {
			return false
		}
		var ok bool
		errString, ok = ksErr.(string)
		return ok
	})
	return errString
}

// WaitForVschemaCondition waits for the condition to be true
func WaitForVschemaCondition(t *testing.T, vtgateProcess cluster.VtgateProcess, ks string, conditionMet func(t *testing.T, keyspace map[string]interface{}) bool) {
	timeout := time.After(60 * time.Second)
	for {
		select {
		case <-timeout:
			t.Fatalf("schema tracking did not met the condition within the time for keyspace: %s", ks)
		default:
			res, err := vtgateProcess.ReadVSchema()
			require.NoError(t, err, res)
			kss := convertToMap(*res)["keyspaces"]
			ksMap := convertToMap(convertToMap(kss)[ks])
			if conditionMet(t, ksMap) {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// WaitForTableDeletions waits for a table to be deleted
func WaitForTableDeletions(ctx context.Context, t *testing.T, vtgateProcess cluster.VtgateProcess, ks, tbl string) {
	WaitForVschemaCondition(t, vtgateProcess, ks, func(t *testing.T, keyspace map[string]interface{}) bool {
		tablesMap := keyspace["tables"]
		_, isPresent := convertToMap(tablesMap)[tbl]
		return !isPresent
	})
}

// WaitForColumn waits for a table's column to be present
func WaitForColumn(t testing.TB, vtgateProcess cluster.VtgateProcess, ks, tbl, col string) error {
	timeout := time.After(60 * time.Second)
	for {
		select {
		case <-timeout:
			return fmt.Errorf("schema tracking did not find column '%s' in table '%s'", col, tbl)
		default:
			res, err := vtgateProcess.ReadVSchema()
			require.NoError(t, err, res)
			t2Map := getTableT2Map(res, ks, tbl)
			authoritative, fieldPresent := t2Map["column_list_authoritative"]
			if !fieldPresent {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			authoritativeBool, isBool := authoritative.(bool)
			if !isBool || !authoritativeBool {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			colMap, exists := t2Map["columns"]
			if !exists {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			colList, isSlice := colMap.([]interface{})
			if !isSlice {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			for _, c := range colList {
				colDef, isMap := c.(map[string]interface{})
				if !isMap {
					break
				}
				if colName, exists := colDef["name"]; exists && colName == col {
					return nil
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func getTableT2Map(res *interface{}, ks, tbl string) map[string]interface{} {
	step1 := convertToMap(*res)["keyspaces"]
	step2 := convertToMap(step1)[ks]
	step3 := convertToMap(step2)["tables"]
	tblMap := convertToMap(step3)[tbl]
	return convertToMap(tblMap)
}

func convertToMap(input interface{}) map[string]interface{} {
	output, ok := input.(map[string]interface{})
	if !ok {
		return make(map[string]interface{})
	}
	return output
}

func GetInitDBSQL(initDBSQL string, updatedPasswords string, oldAlterTableMode string) (string, error) {
	// Since password update is DML we need to insert it before we disable
	// super_read_only therefore doing the split below.
	splitString := strings.Split(initDBSQL, "# {{custom_sql}}")
	if len(splitString) != 2 {
		return "", fmt.Errorf("missing `# {{custom_sql}}` in init_db.sql file")
	}
	var builder strings.Builder
	builder.WriteString(splitString[0])
	builder.WriteString(updatedPasswords)

	// https://github.com/vitessio/vitess/issues/8315
	if oldAlterTableMode != "" {
		builder.WriteString(oldAlterTableMode)
	}
	builder.WriteString(splitString[1])

	return builder.String(), nil
}

// TimeoutAction performs the action within the given timeout limit.
// If the timeout is reached, the test is failed with errMsg.
// If action returns false, the timeout loop continues, if it returns true, the function succeeds.
func TimeoutAction(t *testing.T, timeout time.Duration, errMsg string, action func() bool) {
	deadline := time.After(timeout)
	ok := false
	for !ok {
		select {
		case <-deadline:
			t.Error(errMsg)
			return
		case <-time.After(1 * time.Second):
			ok = action()
		}
	}
}

// RunSQLs is used to run a list of SQL statements on the given tablet
func RunSQLs(t *testing.T, sqls []string, tablet *cluster.Vttablet, db string) error {
	// Get Connection
	tabletParams := getMysqlConnParam(tablet, db)
	var timeoutDuration = time.Duration(5 * len(sqls))
	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration*time.Second)
	defer cancel()
	conn, err := mysql.Connect(ctx, &tabletParams)
	require.Nil(t, err)
	defer conn.Close()

	// Run SQLs
	for _, sql := range sqls {
		if _, err := execute(t, conn, sql); err != nil {
			return err
		}
	}
	return nil
}

// RunSQL is used to run a SQL statement on the given tablet
func RunSQL(t *testing.T, sql string, tablet *cluster.Vttablet, db string) (*sqltypes.Result, error) {
	// Get Connection
	tabletParams := getMysqlConnParam(tablet, db)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := mysql.Connect(ctx, &tabletParams)
	require.Nil(t, err)
	defer conn.Close()

	// RunSQL
	return execute(t, conn, sql)
}

// GetMySQLConn gets a MySQL connection for the given tablet
func GetMySQLConn(tablet *cluster.Vttablet, db string) (*mysql.Conn, error) {
	tabletParams := getMysqlConnParam(tablet, db)
	return mysql.Connect(context.Background(), &tabletParams)
}

func execute(t *testing.T, conn *mysql.Conn, query string) (*sqltypes.Result, error) {
	t.Helper()
	return conn.ExecuteFetch(query, 1000, true)
}

func getMysqlConnParam(tablet *cluster.Vttablet, db string) mysql.ConnParams {
	connParams := mysql.ConnParams{
		Uname:      "vt_dba",
		UnixSocket: path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("/vt_%010d/mysql.sock", tablet.TabletUID)),
	}
	if db != "" {
		connParams.DbName = db
	}
	return connParams
}
