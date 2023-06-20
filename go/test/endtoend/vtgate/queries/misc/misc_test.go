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

package misc

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		tables := []string{"t1"}
		for _, table := range tables {
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
		}
	}

	deleteAll()

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
		cluster.PanicHandler(t)
	}
}

func TestBitVals(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(id1, id2) values (0,0)")

	mcmp.AssertMatches(`select b'1001', 0x9, B'010011011010'`, `[[VARBINARY("\t") VARBINARY("\t") VARBINARY("\x04\xda")]]`)
	mcmp.AssertMatches(`select b'1001', 0x9, B'010011011010' from t1`, `[[VARBINARY("\t") VARBINARY("\t") VARBINARY("\x04\xda")]]`)
	mcmp.AssertMatchesNoCompare(`select 1 + b'1001', 2 + 0x9, 3 + B'010011011010'`, `[[INT64(10) UINT64(11) INT64(1245)]]`, `[[UINT64(10) UINT64(11) UINT64(1245)]]`)
	mcmp.AssertMatchesNoCompare(`select 1 + b'1001', 2 + 0x9, 3 + B'010011011010' from t1`, `[[INT64(10) UINT64(11) INT64(1245)]]`, `[[UINT64(10) UINT64(11) UINT64(1245)]]`)
}

func TestHexVals(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(id1, id2) values (0,0)")

	mcmp.AssertMatches(`select x'09', 0x9`, `[[VARBINARY("\t") VARBINARY("\t")]]`)
	mcmp.AssertMatches(`select X'09', 0x9 from t1`, `[[VARBINARY("\t") VARBINARY("\t")]]`)
	mcmp.AssertMatches(`select 1 + x'09', 2 + 0x9`, `[[UINT64(10) UINT64(11)]]`)
	mcmp.AssertMatches(`select 1 + X'09', 2 + 0x9 from t1`, `[[UINT64(10) UINT64(11)]]`)
}

func TestDateTimeTimestampVals(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.AssertMatches(`select date'2022-08-03'`, `[[DATE("2022-08-03")]]`)
	mcmp.AssertMatches(`select time'12:34:56'`, `[[TIME("12:34:56")]]`)
	mcmp.AssertMatches(`select timestamp'2012-12-31 11:30:45'`, `[[DATETIME("2012-12-31 11:30:45")]]`)
}

func TestInvalidDateTimeTimestampVals(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	_, err := mcmp.ExecAllowAndCompareError(`select date'2022'`)
	require.Error(t, err)
	_, err = mcmp.ExecAllowAndCompareError(`select time'12:34:56:78'`)
	require.Error(t, err)
	_, err = mcmp.ExecAllowAndCompareError(`select timestamp'2022'`)
	require.Error(t, err)
}

// TestIntervalWithMathFunctions tests that the Interval keyword can be used with math functions.
func TestIntervalWithMathFunctions(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// Set the time zone explicitly to UTC, otherwise the output of FROM_UNIXTIME is going to be dependent
	// on the time zone of the system.
	mcmp.Exec("SET time_zone = '+00:00'")
	mcmp.AssertMatches("select '2020-01-01' + interval month(DATE_SUB(FROM_UNIXTIME(1234), interval 1 month))-1 month", `[[CHAR("2020-12-01")]]`)
	mcmp.AssertMatches("select DATE_ADD(MIN(FROM_UNIXTIME(1673444922)),interval -DAYOFWEEK(MIN(FROM_UNIXTIME(1673444922)))+1 DAY)", `[[DATETIME("2023-01-08 13:48:42")]]`)
}

// TestCast tests the queries that contain the cast function.
func TestCast(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.AssertMatches("select cast('2023-01-07 12:34:56' as date) limit 1", `[[DATE("2023-01-07")]]`)
	mcmp.AssertMatches("select cast('2023-01-07 12:34:56' as date)", `[[DATE("2023-01-07")]]`)
	mcmp.AssertMatches("select cast('3.2' as float)", `[[FLOAT32(3.2)]]`)
	mcmp.AssertMatches("select cast('3.2' as double)", `[[FLOAT64(3.2)]]`)
	mcmp.AssertMatches("select cast('3.2' as unsigned)", `[[UINT64(3)]]`)
}

// This test ensures that we support PREPARE statement with 65530 parameters.
// It opens a MySQL connection using the go-mysql driver and execute a select query
// it then checks the result contains the proper rows and that it's not failing.
func TestHighNumberOfParams(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(id1) values (0), (1), (2), (3), (4)")

	paramCount := 65530

	// create the value and argument slices used to build the prepare stmt
	var vals []any
	var params []string
	for i := 0; i < paramCount; i++ {
		vals = append(vals, strconv.Itoa(i))
		params = append(params, "?")
	}

	// connect to the vitess cluster
	db, err := sql.Open("mysql", fmt.Sprintf("@tcp(%s:%v)/%s", vtParams.Host, vtParams.Port, vtParams.DbName))
	require.NoError(t, err)

	// run the query
	r, err := db.Query(fmt.Sprintf("SELECT /*vt+ QUERY_TIMEOUT_MS=10000 */ id1 FROM t1 WHERE id1 in (%s) ORDER BY id1 ASC", strings.Join(params, ", ")), vals...)
	require.NoError(t, err)

	// check the results we got, we should get 5 rows with each: 0, 1, 2, 3, 4
	// count is the row number we are currently visiting, also correspond to the
	// column value we expect.
	count := 0
	for r.Next() {
		j := -1
		err := r.Scan(&j)
		require.NoError(t, err)
		require.Equal(t, j, count)
		count++
	}
	require.Equal(t, 5, count)
}
