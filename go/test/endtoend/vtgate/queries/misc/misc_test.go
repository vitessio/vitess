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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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

func TestQueryTimeoutWithDual(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	_, err := utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ PLANNER=gen4 */ sleep(0.04) from dual")
	assert.NoError(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ PLANNER=gen4 */ sleep(0.24) from dual")
	assert.Error(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "set @@session.query_timeout=20")
	require.NoError(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ PLANNER=gen4 */ sleep(0.04) from dual")
	assert.Error(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ PLANNER=gen4 */ sleep(0.01) from dual")
	assert.NoError(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ PLANNER=gen4 QUERY_TIMEOUT_MS=500 */ sleep(0.24) from dual")
	assert.NoError(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ PLANNER=gen4 QUERY_TIMEOUT_MS=10 */ sleep(0.04) from dual")
	assert.Error(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ PLANNER=gen4 QUERY_TIMEOUT_MS=10 */ sleep(0.001) from dual")
	assert.NoError(t, err)
}

func TestQueryTimeoutWithTables(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// unsharded
	utils.Exec(t, mcmp.VtConn, "insert /*vt+ QUERY_TIMEOUT_MS=1000 */ into uks.unsharded(id1) values (1),(2),(3),(4),(5)")
	for i := 0; i < 12; i++ {
		utils.Exec(t, mcmp.VtConn, "insert /*vt+ QUERY_TIMEOUT_MS=1000 */ into uks.unsharded(id1) select id1+5 from uks.unsharded")
	}

	utils.Exec(t, mcmp.VtConn, "select count(*) from uks.unsharded where id1 > 31")
	utils.Exec(t, mcmp.VtConn, "select /*vt+ PLANNER=gen4 QUERY_TIMEOUT_MS=100 */ count(*) from uks.unsharded where id1 > 31")

	// the query usually takes more than 5ms to return. So this should fail.
	_, err := utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ PLANNER=gen4 QUERY_TIMEOUT_MS=1 */ count(*) from uks.unsharded where id1 > 31")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")
	assert.Contains(t, err.Error(), "(errno 1317) (sqlstate 70100)")

	// sharded
	for i := 0; i < 300000; i += 1000 {
		var str strings.Builder
		for j := 1; j <= 1000; j++ {
			if j == 1 {
				str.WriteString(fmt.Sprintf("(%d)", i*1000+j))
				continue
			}
			str.WriteString(fmt.Sprintf(",(%d)", i*1000+j))
		}
		utils.Exec(t, mcmp.VtConn, fmt.Sprintf("insert /*vt+ QUERY_TIMEOUT_MS=1000 */ into t1(id1) values %s", str.String()))
	}
	// too much data added in the loop, do drop and recreate the table.
	defer func() {
		mcmp.Exec("drop table t1")
		mcmp.Exec(schemaSQL)
	}()

	utils.Exec(t, mcmp.VtConn, "select count(*) from t1 where id1 > 31")
	utils.Exec(t, mcmp.VtConn, "select /*vt+ PLANNER=gen4 QUERY_TIMEOUT_MS=100 */ count(*) from t1 where id1 > 31")

	// the query usually takes more than 5ms to return. So this should fail.
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ PLANNER=gen4 QUERY_TIMEOUT_MS=1 */ count(*) from t1 where id1 > 31")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")
	assert.Contains(t, err.Error(), "(errno 1317) (sqlstate 70100)")
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

func TestOuterJoinWithPredicate(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// This test uses a predicate on the outer side.
	// These can't be pushed down to MySQL and have
	// to be evaluated on the vtgate, so we are checking
	// that evalengine handles the predicate correctly

	mcmp.Exec("insert into t1(id1, id2) values (0,0), (1,10), (2,20), (3,30), (4,40)")

	mcmp.AssertMatchesNoOrder("select A.id1, B.id2 from t1 as A left join t1 as B on A.id1*10 = B.id2 WHERE B.id2 BETWEEN 20 AND 30",
		`[[INT64(2) INT64(20)] [INT64(3) INT64(30)]]`)
	mcmp.AssertMatchesNoOrder("select A.id1, B.id2 from t1 as A left join t1 as B on A.id1*10 = B.id2 WHERE B.id2 NOT BETWEEN 20 AND 30",
		`[[INT64(0) INT64(0)] [INT64(1) INT64(10)] [INT64(4) INT64(40)]]`)
}
