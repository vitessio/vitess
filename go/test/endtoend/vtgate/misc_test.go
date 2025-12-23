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

package vtgate

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestInsertOnDuplicateKey(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "insert into t11(id, sharding_key, col1, col2, col3) values(1, 2, 'a', 1, 2)")
	utils.Exec(t, conn, "insert into t11(id, sharding_key, col1, col2, col3) values(1, 2, 'a', 1, 2) on duplicate key update id=10;")
	utils.AssertMatches(t, conn, "select id, sharding_key from t11 where id=10", "[[INT64(10) INT64(2)]]")
}

func TestInsertNeg(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "insert ignore into t10(id, sharding_key, col1, col2, col3) values(10, 20, 'a', 1, 2), (20, -20, 'b', 3, 4), (30, -40, 'c', 6, 7), (40, 60, 'd', 4, 10)")
	utils.Exec(t, conn, "insert ignore into t10(id, sharding_key, col1, col2, col3) values(1, 2, 'a', 1, 2), (2, -2, 'b', -3, 4), (3, -4, 'c', 6, -7), (4, 6, 'd', 4, -10)")
}

func TestSelectNull(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into t5_null_vindex(id, idx) values(1, 'a'), (2, 'b'), (3, null)")
	utils.Exec(t, conn, "commit")

	utils.AssertMatches(t, conn, "select id, idx from t5_null_vindex order by id", "[[INT64(1) VARCHAR(\"a\")] [INT64(2) VARCHAR(\"b\")] [INT64(3) NULL]]")
	utils.AssertIsEmpty(t, conn, "select id, idx from t5_null_vindex where idx = null")
	utils.AssertMatches(t, conn, "select id, idx from t5_null_vindex where idx is null", "[[INT64(3) NULL]]")
	utils.AssertMatches(t, conn, "select id, idx from t5_null_vindex where idx <=> null", "[[INT64(3) NULL]]")
	utils.AssertMatches(t, conn, "select id, idx from t5_null_vindex where idx is not null order by id", "[[INT64(1) VARCHAR(\"a\")] [INT64(2) VARCHAR(\"b\")]]")
	utils.AssertIsEmpty(t, conn, "select id, idx from t5_null_vindex where id IN (null)")
	utils.AssertMatches(t, conn, "select id, idx from t5_null_vindex where id IN (1,2,null) order by id", "[[INT64(1) VARCHAR(\"a\")] [INT64(2) VARCHAR(\"b\")]]")
	utils.AssertIsEmpty(t, conn, "select id, idx from t5_null_vindex where id NOT IN (1,null) order by id")
	utils.AssertMatches(t, conn, "select id, idx from t5_null_vindex where id NOT IN (1,3)", "[[INT64(2) VARCHAR(\"b\")]]")
}

func TestDoStatement(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "do 1")
	utils.Exec(t, conn, "do 'a', 1+2,database()")
}

func TestShowColumns(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	expected80 := `[[VARCHAR("id") BLOB("bigint") VARCHAR("NO") BINARY("PRI") NULL VARCHAR("")] [VARCHAR("idx") BLOB("varchar(50)") VARCHAR("YES") BINARY("") NULL VARCHAR("")]]`
	expected57 := `[[VARCHAR("id") TEXT("bigint(20)") VARCHAR("NO") VARCHAR("PRI") NULL VARCHAR("")] [VARCHAR("idx") TEXT("varchar(50)") VARCHAR("YES") VARCHAR("") NULL VARCHAR("")]]`
	utils.AssertMatchesAny(t, conn, "show columns from `t5_null_vindex` in `ks`", expected80, expected57)
	utils.AssertMatchesAny(t, conn, "SHOW COLUMNS from `t5_null_vindex` in `ks`", expected80, expected57)
	utils.AssertMatchesAny(t, conn, "SHOW columns FROM `t5_null_vindex` in `ks`", expected80, expected57)
	utils.AssertMatchesAny(t, conn, "SHOW columns FROM `t5_null_vindex` where Field = 'id'",
		`[[VARCHAR("id") BLOB("bigint") VARCHAR("NO") BINARY("PRI") NULL VARCHAR("")]]`,
		`[[VARCHAR("id") TEXT("bigint(20)") VARCHAR("NO") VARCHAR("PRI") NULL VARCHAR("")]]`)
}

func TestShowTables(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	qr := utils.Exec(t, conn, "show tables")
	assert.Equal(t, "Tables_in_ks", qr.Fields[0].Name)

	// no error on executing `show tables` on system schema
	utils.Exec(t, conn, `use mysql`)
	utils.Exec(t, conn, "show tables")
	utils.Exec(t, conn, "show tables from information_schema")
}

func TestCastConvert(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.AssertMatches(t, conn, `SELECT CAST("test" AS CHAR(60))`, `[[VARCHAR("test")]]`)
}

func TestCompositeIN(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "insert into t1(id1, id2) values(1, 2), (4, 5)")

	// Just check for correct results. Plan generation is tested in unit tests.
	utils.AssertMatches(t, conn, "select id1 from t1 where (id1, id2) in ((1, 2))", "[[INT64(1)]]")
}

func TestSavepointInTx(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "savepoint a")
	utils.Exec(t, conn, "start transaction")
	utils.Exec(t, conn, "savepoint b")
	utils.Exec(t, conn, "rollback to b")
	utils.Exec(t, conn, "release savepoint b")
	utils.Exec(t, conn, "savepoint b")
	utils.Exec(t, conn, "insert into t1(id1, id2) values(1,1)") // -80
	utils.Exec(t, conn, "savepoint c")
	utils.Exec(t, conn, "insert into t1(id1, id2) values(4,4)") // 80-
	utils.Exec(t, conn, "savepoint d")
	utils.Exec(t, conn, "insert into t1(id1, id2) values(2,2)") // -80
	utils.Exec(t, conn, "savepoint e")

	// Validate all the data.
	utils.Exec(t, conn, "use `ks:-80`")
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)] [INT64(2)]]`)
	utils.Exec(t, conn, "use `ks:80-`")
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(4)]]`)
	utils.Exec(t, conn, "use ks")
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)] [INT64(2)] [INT64(4)]]`)

	_, err := conn.ExecuteFetch("rollback work to savepoint a", 1000, true)
	require.Error(t, err)

	utils.Exec(t, conn, "release savepoint d")

	_, err = conn.ExecuteFetch("rollback to d", 1000, true)
	require.Error(t, err)
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)] [INT64(2)] [INT64(4)]]`)

	utils.Exec(t, conn, "rollback to c")
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)]]`)

	utils.Exec(t, conn, "insert into t1(id1, id2) values(2,2),(3,3),(4,4)")
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)]]`)

	utils.Exec(t, conn, "rollback to b")
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1", `[]`)

	utils.Exec(t, conn, "commit")
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1", `[]`)

	utils.Exec(t, conn, "start transaction")

	utils.Exec(t, conn, "insert into t1(id1, id2) values(2,2),(3,3),(4,4)")
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(2)] [INT64(3)] [INT64(4)]]`)

	// After previous commit all the savepoints are cleared.
	_, err = conn.ExecuteFetch("rollback to b", 1000, true)
	require.Error(t, err)

	utils.Exec(t, conn, "rollback")
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1", `[]`)
}

func TestSavepointOutsideTx(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "savepoint a")
	utils.Exec(t, conn, "savepoint b")

	_, err := conn.ExecuteFetch("rollback to b", 1, true)
	require.Error(t, err)
	_, err = conn.ExecuteFetch("release savepoint a", 1, true)
	require.Error(t, err)
}

func TestSavepointAdditionalCase(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "start transaction")
	utils.Exec(t, conn, "savepoint a")
	utils.Exec(t, conn, "insert into t1(id1, id2) values(1,1)")             // -80
	utils.Exec(t, conn, "insert into t1(id1, id2) values(2,2),(3,3),(4,4)") // -80 & 80-
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)]]`)

	utils.Exec(t, conn, "rollback to a")
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1", `[]`)

	utils.Exec(t, conn, "commit")
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1", `[]`)

	utils.Exec(t, conn, "start transaction")
	utils.Exec(t, conn, "insert into t1(id1, id2) values(1,1)") // -80
	utils.Exec(t, conn, "savepoint a")
	utils.Exec(t, conn, "insert into t1(id1, id2) values(2,2),(3,3)") // -80
	utils.Exec(t, conn, "insert into t1(id1, id2) values(4,4)")       // 80-
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)]]`)

	utils.Exec(t, conn, "rollback to a")
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)]]`)

	utils.Exec(t, conn, "rollback")
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1", `[]`)
}

func TestExplainPassthrough(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	result := utils.Exec(t, conn, "explain select * from t1")
	got := fmt.Sprintf("%v", result.Rows)
	require.Contains(t, got, "SIMPLE") // there is a lot more coming from mysql,
	// but we are trying to make the test less fragile

	result = utils.Exec(t, conn, "explain ks.t1")
	require.EqualValues(t, 2, len(result.Rows))
}

func TestXXHash(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "insert into t7_xxhash(uid, phone, msg) values('u-1', 1, 'message')")
	utils.AssertMatches(t, conn, "select uid, phone, msg from t7_xxhash where phone = 1", `[[VARCHAR("u-1") INT64(1) VARCHAR("message")]]`)
	utils.AssertMatches(t, conn, "select phone, keyspace_id from t7_xxhash_idx", `[[INT64(1) VARBINARY("\x1cU^f\xbfyE^")]]`)
	utils.Exec(t, conn, "update t7_xxhash set phone = 2 where uid = 'u-1'")
	utils.AssertMatches(t, conn, "select uid, phone, msg from t7_xxhash where phone = 1", `[]`)
	utils.AssertMatches(t, conn, "select uid, phone, msg from t7_xxhash where phone = 2", `[[VARCHAR("u-1") INT64(2) VARCHAR("message")]]`)
	utils.AssertMatches(t, conn, "select phone, keyspace_id from t7_xxhash_idx", `[[INT64(2) VARBINARY("\x1cU^f\xbfyE^")]]`)
	utils.Exec(t, conn, "delete from t7_xxhash where uid = 'u-1'")
	utils.AssertMatches(t, conn, "select uid, phone, msg from t7_xxhash where uid = 'u-1'", `[]`)
	utils.AssertMatches(t, conn, "select phone, keyspace_id from t7_xxhash_idx", `[]`)
}

func TestShowTablesWithWhereClause(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.AssertMatchesAny(t, conn, "show tables from ks where Tables_in_ks='t6'", `[[VARBINARY("t6")]]`, `[[VARCHAR("t6")]]`)
	utils.Exec(t, conn, "begin")
	utils.AssertMatchesAny(t, conn, "show tables from ks where Tables_in_ks='t3'", `[[VARBINARY("t3")]]`, `[[VARCHAR("t3")]]`)
}

func TestOffsetAndLimitWithOLAP(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1 limit 3 offset 2", "[[INT64(3)] [INT64(4)] [INT64(5)]]")
	utils.Exec(t, conn, "set workload='olap'")
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1 limit 3 offset 2", "[[INT64(3)] [INT64(4)] [INT64(5)]]")
}

func TestSwitchBetweenOlapAndOltp(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.AssertMatches(t, conn, "select @@workload", `[[VARCHAR("OLTP")]]`)

	utils.Exec(t, conn, "set workload='olap'")

	utils.AssertMatches(t, conn, "select @@workload", `[[VARCHAR("OLAP")]]`)

	utils.Exec(t, conn, "set workload='oltp'")

	utils.AssertMatches(t, conn, "select @@workload", `[[VARCHAR("OLTP")]]`)
}

func TestFoundRowsOnDualQueries(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "select 42")
	utils.AssertMatches(t, conn, "select found_rows()", "[[INT64(1)]]")
}

func TestUseStmtInOLAP(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	queries := []string{"set workload='olap'", "use `ks:80-`", "use `ks:-80`"}
	for i, q := range queries {
		t.Run(fmt.Sprintf("%d-%s", i, q), func(t *testing.T) {
			utils.Exec(t, conn, q)
		})
	}
}

func TestInsertStmtInOLAP(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, `set workload='olap'`)
	utils.Exec(t, conn, `insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)`)
	utils.AssertMatches(t, conn, `select id1 from t1 order by id1`, `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)] [INT64(5)]]`)
}

func TestCreateIndex(t *testing.T) {
	conn, closer := start(t)
	defer closer()
	// Test that create index with the correct table name works
	utils.Exec(t, conn, `create index i1 on t1 (id1)`)
	// Test routing rules for create index.
	utils.Exec(t, conn, `create index i2 on ks.t1000 (id1)`)
}

func TestVersions(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	qr := utils.Exec(t, conn, `select @@version`)
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), "vitess")

	qr = utils.Exec(t, conn, `select @@version_comment`)
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), "Git revision")
}

func TestFlush(t *testing.T) {
	conn, closer := start(t)
	defer closer()
	utils.Exec(t, conn, "flush local tables t1, t2")
}

// TestFlushLock tests that ftwrl and unlock tables should unblock other session connections to execute the query.
func TestFlushLock(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	// replica: fail it
	utils.Exec(t, conn, "use @replica")
	_, err := utils.ExecAllowError(t, conn, "flush tables ks.t1, ks.t2 with read lock")
	require.ErrorContains(t, err, "VT09012: FLUSH statement with REPLICA tablet not allowed")

	// primary: should work
	utils.Exec(t, conn, "use @primary")
	utils.Exec(t, conn, "flush tables ks.t1, ks.t2 with read lock")

	var cnt atomic.Int32
	go func() {
		ctx := context.Background()
		conn2, err := mysql.Connect(ctx, &vtParams)
		require.NoError(t, err)
		defer conn2.Close()

		cnt.Add(1)
		utils.Exec(t, conn2, "select * from ks.t1 for update")
		cnt.Add(1)
	}()
	for cnt.Load() == 0 {
	}
	// added sleep to let the query execute inside the go routine, which should be blocked.
	time.Sleep(1 * time.Second)
	require.EqualValues(t, 1, cnt.Load())

	// unlock it
	utils.Exec(t, conn, "unlock tables")

	// now wait for go routine to complete.
	timeout := time.After(3 * time.Second)
	for cnt.Load() != 2 {
		select {
		case <-timeout:
			t.Fatalf("test timeout waiting for select query to complete")
		default:
		}
	}
}

func TestShowVariables(t *testing.T) {
	conn, closer := start(t)
	defer closer()
	res := utils.Exec(t, conn, "show variables like \"%version%\";")
	found := false
	for _, row := range res.Rows {
		if row[0].ToString() == "version" {
			assert.Contains(t, row[1].ToString(), "vitess")
			found = true
		}
	}
	require.True(t, found, "Expected a row for version in show query")
}

func TestShowVGtid(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	query := "show global vgtid_executed from ks"
	qr := utils.Exec(t, conn, query)
	require.Equal(t, 1, len(qr.Rows))
	require.Equal(t, 2, len(qr.Rows[0]))

	utils.Exec(t, conn, `insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)`)
	qr2 := utils.Exec(t, conn, query)
	require.Equal(t, 1, len(qr2.Rows))
	require.Equal(t, 2, len(qr2.Rows[0]))

	require.Equal(t, qr.Rows[0][0], qr2.Rows[0][0], "keyspace should be same")
	require.NotEqual(t, qr.Rows[0][1].ToString(), qr2.Rows[0][1].ToString(), "vgtid should have changed")
}

func TestShowGtid(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	query := "show global gtid_executed from ks"
	qr := utils.Exec(t, conn, query)
	require.Equal(t, 2, len(qr.Rows))

	res := make(map[string]string, 2)
	for _, row := range qr.Rows {
		require.Equal(t, KeyspaceName, row[0].ToString())
		res[row[2].ToString()] = row[1].ToString()
	}

	utils.Exec(t, conn, `insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)`)
	qr2 := utils.Exec(t, conn, query)
	require.Equal(t, 2, len(qr2.Rows))

	for _, row := range qr2.Rows {
		require.Equal(t, KeyspaceName, row[0].ToString())
		gtid, exists := res[row[2].ToString()]
		require.True(t, exists, "gtid not cached for row: %v", row)
		require.NotEqual(t, gtid, row[1].ToString())
	}
}

func TestDeleteAlias(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "delete t1 from t1 where id1 = 1")
	utils.Exec(t, conn, "delete t.* from t1 t where t.id1 = 1")
}

func TestFunctionInDefault(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	// set the sql mode ALLOW_INVALID_DATES
	utils.Exec(t, conn, `SET sql_mode = 'ALLOW_INVALID_DATES'`)

	// test that default expression works for columns.
	utils.Exec(t, conn, `create table function_default (x varchar(25) DEFAULT (TRIM(" check ")))`)
	utils.Exec(t, conn, "drop table function_default")

	// verify that current_timestamp and it's aliases work as default values
	utils.Exec(t, conn, `create table function_default (
ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
dt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
ts2 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
dt2 DATETIME DEFAULT CURRENT_TIMESTAMP,
ts3 TIMESTAMP DEFAULT 0,
dt3 DATETIME DEFAULT 0,
ts4 TIMESTAMP DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP,
dt4 DATETIME DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP,
ts5 TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
ts6 TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP,
dt5 DATETIME ON UPDATE CURRENT_TIMESTAMP,
dt6 DATETIME NOT NULL ON UPDATE CURRENT_TIMESTAMP,
ts7 TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
ts8 TIMESTAMP DEFAULT NOW(),
ts9 TIMESTAMP DEFAULT LOCALTIMESTAMP,
ts10 TIMESTAMP DEFAULT LOCALTIME,
ts11 TIMESTAMP DEFAULT LOCALTIMESTAMP(),
ts12 TIMESTAMP DEFAULT LOCALTIME()
)`)
	utils.Exec(t, conn, "drop table function_default")

	utils.Exec(t, conn, `create table function_default (ts TIMESTAMP DEFAULT (UTC_TIMESTAMP))`)
	utils.Exec(t, conn, "drop table function_default")

	utils.Exec(t, conn, `create table function_default (x varchar(25) DEFAULT "check")`)
	utils.Exec(t, conn, "drop table function_default")
}

func TestRenameFieldsOnOLAP(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	_ = utils.Exec(t, conn, "set workload = olap")

	qr := utils.Exec(t, conn, "show tables")
	require.Equal(t, 1, len(qr.Fields))
	assert.Equal(t, `Tables_in_ks`, qr.Fields[0].Name)
	_ = utils.Exec(t, conn, "use mysql")
	qr = utils.Exec(t, conn, "select @@workload")
	assert.Equal(t, `[[VARCHAR("OLAP")]]`, fmt.Sprintf("%v", qr.Rows))
}

func TestSelectEqualUniqueOuterJoinRightPredicate(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "insert into t1(id1, id2) values (0,10),(1,9),(2,8),(3,7),(4,6),(5,5)")
	utils.Exec(t, conn, "insert into t2(id3, id4) values (0,20),(1,19),(2,18),(3,17),(4,16),(5,15)")
	utils.AssertMatches(t, conn, `SELECT id3 FROM t1 LEFT JOIN t2 ON t1.id1 = t2.id3 WHERE t2.id3 = 10`, `[]`)
}

func TestSQLSelectLimit(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "insert into t7_xxhash(uid, msg) values(1, 'a'), (2, 'b'), (3, null), (4, 'a'), (5, 'a'), (6, 'b')")

	for _, workload := range []string{"olap", "oltp"} {
		utils.Exec(t, conn, "set workload = "+workload)
		utils.Exec(t, conn, "set sql_select_limit = 2")
		utils.AssertMatches(t, conn, "select uid, msg from t7_xxhash order by uid", `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("2") VARCHAR("b")]]`)
		utils.AssertMatches(t, conn, "(select uid, msg from t7_xxhash order by uid)", `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("2") VARCHAR("b")]]`)
		utils.AssertMatches(t, conn, "select uid, msg from t7_xxhash order by uid limit 4", `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("2") VARCHAR("b")] [VARCHAR("3") NULL] [VARCHAR("4") VARCHAR("a")]]`)

		// Don't LIMIT subqueries
		utils.AssertMatches(t, conn, "select count(*) from (select uid, msg from t7_xxhash order by uid) as subquery", `[[INT64(6)]]`)
		utils.AssertMatches(t, conn, "select count(*) from (select 1 union all select 2 union all select 3) as subquery", `[[INT64(3)]]`)

		utils.AssertMatches(t, conn, "select 1 union all select 2 union all select 3", `[[INT64(1)] [INT64(2)]]`)

		/*
			planner does not support query with order by in union query. without order by the results are not deterministic for testing purpose
			utils.AssertMatches(t, conn, "select uid, msg from t7_xxhash union all select uid, msg from t7_xxhash order by uid", ``)
			utils.AssertMatches(t, conn, "select uid, msg from t7_xxhash union all select uid, msg from t7_xxhash order by uid limit 3", ``)
		*/

		//	without order by the results are not deterministic for testing purpose. Checking row count only.
		qr := utils.Exec(t, conn, "select /*vt+ PLANNER=gen4 */ uid, msg from t7_xxhash union all select uid, msg from t7_xxhash")
		assert.Equal(t, 2, len(qr.Rows))

		qr = utils.Exec(t, conn, "select /*vt+ PLANNER=gen4 */ uid, msg from t7_xxhash union all select uid, msg from t7_xxhash limit 3")
		assert.Equal(t, 3, len(qr.Rows))
	}
}

func TestSQLSelectLimitWithPlanCache(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "insert into t7_xxhash(uid, msg) values(1, 'a'), (2, 'b'), (3, null)")

	tcases := []struct {
		limit int
		out   string
	}{{
		limit: -1,
		out:   `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("2") VARCHAR("b")] [VARCHAR("3") NULL]]`,
	}, {
		limit: 1,
		out:   `[[VARCHAR("1") VARCHAR("a")]]`,
	}, {
		limit: 2,
		out:   `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("2") VARCHAR("b")]]`,
	}, {
		limit: 3,
		out:   `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("2") VARCHAR("b")] [VARCHAR("3") NULL]]`,
	}, {
		limit: 4,
		out:   `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("2") VARCHAR("b")] [VARCHAR("3") NULL]]`,
	}}
	for _, workload := range []string{"olap", "oltp"} {
		utils.Exec(t, conn, "set workload = "+workload)
		for _, tcase := range tcases {
			utils.Exec(t, conn, fmt.Sprintf("set sql_select_limit = %d", tcase.limit))
			utils.AssertMatches(t, conn, "select uid, msg from t7_xxhash order by uid", tcase.out)
		}
	}
}

func TestSavepointInReservedConn(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "set session sql_mode = ''")
	utils.Exec(t, conn, "BEGIN")
	utils.Exec(t, conn, "SAVEPOINT sp_1")
	utils.Exec(t, conn, "insert into t7_xxhash(uid, msg) values(1, 'a')")
	utils.Exec(t, conn, "RELEASE SAVEPOINT sp_1")
	utils.Exec(t, conn, "ROLLBACK")

	utils.Exec(t, conn, "set session sql_mode = ''")
	utils.Exec(t, conn, "BEGIN")
	utils.Exec(t, conn, "SAVEPOINT sp_1")
	utils.Exec(t, conn, "RELEASE SAVEPOINT sp_1")
	utils.Exec(t, conn, "SAVEPOINT sp_2")
	utils.Exec(t, conn, "insert into t7_xxhash(uid, msg) values(2, 'a')")
	utils.Exec(t, conn, "RELEASE SAVEPOINT sp_2")
	utils.Exec(t, conn, "COMMIT")
	utils.AssertMatches(t, conn, "select uid from t7_xxhash", `[[VARCHAR("2")]]`)
}

func TestUnionWithManyInfSchemaQueries(t *testing.T) {
	// trying to reproduce the problems in https://github.com/vitessio/vitess/issues/9139
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, `SELECT /*vt+ PLANNER=gen4 */ 
                    TABLE_SCHEMA,
                    TABLE_NAME
                FROM
                    INFORMATION_SCHEMA.TABLES
                WHERE
                    TABLE_SCHEMA = 'ionescu'
                    AND
                    TABLE_NAME = 'company_invite_code'
                 UNION 
                SELECT
                    TABLE_SCHEMA,
                    TABLE_NAME
                FROM
                    INFORMATION_SCHEMA.TABLES
                WHERE
                    TABLE_SCHEMA = 'ionescu'
                    AND
                    TABLE_NAME = 'site_role'
                 UNION 
                SELECT
                    TABLE_SCHEMA,
                    TABLE_NAME
                FROM
                    INFORMATION_SCHEMA.TABLES
                WHERE
                    TABLE_SCHEMA = 'ionescu'
                    AND
                    TABLE_NAME = 'item'
                 UNION 
                SELECT
                    TABLE_SCHEMA,
                    TABLE_NAME
                FROM
                    INFORMATION_SCHEMA.TABLES
                WHERE
                    TABLE_SCHEMA = 'ionescu'
                    AND
                    TABLE_NAME = 'site_item_urgent'
                 UNION 
                SELECT
                    TABLE_SCHEMA,
                    TABLE_NAME
                FROM
                    INFORMATION_SCHEMA.TABLES
                WHERE
                    TABLE_SCHEMA = 'ionescu'
                    AND
                    TABLE_NAME = 'site_item_event'
                 UNION 
                SELECT
                    TABLE_SCHEMA,
                    TABLE_NAME
                FROM
                    INFORMATION_SCHEMA.TABLES
                WHERE
                    TABLE_SCHEMA = 'ionescu'
                    AND
                    TABLE_NAME = 'site_item'
                 UNION 
                SELECT
                    TABLE_SCHEMA,
                    TABLE_NAME
                FROM
                    INFORMATION_SCHEMA.TABLES
                WHERE
                    TABLE_SCHEMA = 'ionescu'
                    AND
                    TABLE_NAME = 'site'
                 UNION 
                SELECT
                    TABLE_SCHEMA,
                    TABLE_NAME
                FROM
                    INFORMATION_SCHEMA.TABLES
                WHERE
                    TABLE_SCHEMA = 'ionescu'
                    AND
                    TABLE_NAME = 'company'
                 UNION 
                SELECT
                    TABLE_SCHEMA,
                    TABLE_NAME
                FROM
                    INFORMATION_SCHEMA.TABLES
                WHERE
                    TABLE_SCHEMA = 'ionescu'
                    AND
                    TABLE_NAME = 'user_company'
                 UNION 
                SELECT
                    TABLE_SCHEMA,
                    TABLE_NAME
                FROM
                    INFORMATION_SCHEMA.TABLES
                WHERE
                    TABLE_SCHEMA = 'ionescu'
                    AND
                    TABLE_NAME = 'user'`)
}

func TestTransactionsInStreamingMode(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "set workload = olap")
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into t1(id1, id2) values (1,2)")
	utils.AssertMatches(t, conn, "select id1, id2 from t1", `[[INT64(1) INT64(2)]]`)
	utils.Exec(t, conn, "commit")
	utils.AssertMatches(t, conn, "select id1, id2 from t1", `[[INT64(1) INT64(2)]]`)

	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into t1(id1, id2) values (2,3)")
	utils.AssertMatches(t, conn, "select id1, id2 from t1 where id1 = 2", `[[INT64(2) INT64(3)]]`)
	utils.Exec(t, conn, "rollback")
	utils.AssertMatches(t, conn, "select id1, id2 from t1 where id1 = 2", `[]`)
}

func TestCharsetIntro(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "insert into t4 (id1,id2) values (666, _binary'abc')")
	utils.Exec(t, conn, "update t4 set id2 = _latin1'xyz' where id1 = 666")
	utils.Exec(t, conn, "delete from t4 where id2 = _utf8'xyz'")
	qr := utils.Exec(t, conn, "select id1 from t4 where id2 = _utf8mb4'xyz'")
	require.EqualValues(t, 0, qr.RowsAffected)
}

func TestFilterAfterLeftJoin(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "insert into t1 (id1,id2) values (1, 10)")
	utils.Exec(t, conn, "insert into t1 (id1,id2) values (2, 3)")
	utils.Exec(t, conn, "insert into t1 (id1,id2) values (3, 2)")

	query := "select /*vt+ PLANNER=gen4 */ A.id1, A.id2 from t1 as A left join t1 as B on A.id1 = B.id2 WHERE B.id1 IS NULL"
	utils.AssertMatches(t, conn, query, `[[INT64(1) INT64(10)]]`)
}

func TestFilterWithINAfterLeftJoin(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "insert into t1 (id1,id2) values (1, 10)")
	utils.Exec(t, conn, "insert into t1 (id1,id2) values (2, 3)")
	utils.Exec(t, conn, "insert into t1 (id1,id2) values (3, 2)")
	utils.Exec(t, conn, "insert into t1 (id1,id2) values (4, 5)")

	query := "select a.id1, b.id3 from t1 as a left outer join t2 as b on a.id2 = b.id4 WHERE a.id2 = 10 AND (b.id3 IS NULL OR b.id3 IN (1))"
	utils.AssertMatches(t, conn, query, `[[INT64(1) NULL]]`)

	utils.Exec(t, conn, "insert into t2 (id3,id4) values (1, 10)")

	query = "select a.id1, b.id3 from t1 as a left outer join t2 as b on a.id2 = b.id4 WHERE a.id2 = 10 AND (b.id3 IS NULL OR b.id3 IN (1))"
	utils.AssertMatches(t, conn, query, `[[INT64(1) INT64(1)]]`)

	query = "select a.id1, b.id3 from t1 as a left outer join t2 as b on a.id2 = b.id4 WHERE a.id2 = 10 AND (b.id3 IS NULL OR (b.id3, b.id4) IN ((1, 10)))"
	utils.AssertMatches(t, conn, query, `[[INT64(1) INT64(1)]]`)
}

func TestDescribeVindex(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	_, err := conn.ExecuteFetch("describe hash", 1000, false)
	require.Error(t, err)
	mysqlErr := err.(*sqlerror.SQLError)
	assert.Equal(t, sqlerror.ERUnknownTable, mysqlErr.Num)
	assert.Equal(t, "42S02", mysqlErr.State)
	assert.ErrorContains(t, mysqlErr, "VT05004: table 'hash' does not exist")
}

func TestEmptyQuery(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.AssertContainsError(t, conn, "", "Query was empty")
	utils.AssertContainsError(t, conn, ";", "Query was empty")
	utils.AssertIsEmpty(t, conn, "-- this is a comment")
}

// TestJoinWithMergedRouteWithPredicate checks the issue found in https://github.com/vitessio/vitess/issues/10713
func TestJoinWithMergedRouteWithPredicate(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "insert into t1 (id1,id2) values (1, 13)")
	utils.Exec(t, conn, "insert into t2 (id3,id4) values (5, 10), (15, 20)")
	utils.Exec(t, conn, "insert into t3 (id5,id6,id7) values (13, 5, 8)")

	utils.AssertMatches(t, conn, "select t3.id7, t2.id3, t3.id6 from t1 join t3 on t1.id2 = t3.id5 join t2 on t3.id6 = t2.id3 where t1.id2 = 13", `[[INT64(8) INT64(5) INT64(5)]]`)
}

func TestRowCountExceed(t *testing.T) {
	conn, _ := start(t)
	defer func() {
		// needs special delete logic as it exceeds row count.
		for i := 50; i <= 300; i += 50 {
			utils.Exec(t, conn, fmt.Sprintf("delete from t1 where id1 < %d", i))
		}
		conn.Close()
	}()

	for i := 0; i < 250; i++ {
		utils.Exec(t, conn, fmt.Sprintf("insert into t1 (id1, id2) values (%d, %d)", i, i+1))
	}

	utils.AssertContainsError(t, conn, "select id1 from t1 where id1 < 1000", `Row count exceeded 100`)
}

func TestDDLTargeted(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "use `ks/-80`")
	utils.Exec(t, conn, `begin`)
	utils.Exec(t, conn, `create table ddl_targeted (id bigint primary key)`)
	// implicit commit on ddl would have closed the open transaction
	// so this execution should happen as autocommit.
	utils.Exec(t, conn, `insert into ddl_targeted (id) values (1)`)
	// this will have not impact and the row would have inserted.
	utils.Exec(t, conn, `rollback`)
	// validating the row
	utils.AssertMatches(t, conn, `select id from ddl_targeted`, `[[INT64(1)]]`)
}

// TestDynamicConfig tests the dynamic configurations.
func TestDynamicConfig(t *testing.T) {
	t.Run("DiscoveryLowReplicationLag", func(t *testing.T) {
		// Test initial config value
		err := clusterInstance.VtgateProcess.WaitForConfig(`"discovery_low_replication_lag":30000000000`)
		require.NoError(t, err)
		defer func() {
			// Restore default back.
			clusterInstance.VtgateProcess.Config.DiscoveryLowReplicationLag = "30s"
			err = clusterInstance.VtgateProcess.RewriteConfiguration()
			require.NoError(t, err)
		}()
		clusterInstance.VtgateProcess.Config.DiscoveryLowReplicationLag = "15s"
		err = clusterInstance.VtgateProcess.RewriteConfiguration()
		require.NoError(t, err)
		// Test final config value.
		err = clusterInstance.VtgateProcess.WaitForConfig(`"discovery_low_replication_lag":"15s"`)
		require.NoError(t, err)
	})

	t.Run("DiscoveryHighReplicationLag", func(t *testing.T) {
		// Test initial config value
		err := clusterInstance.VtgateProcess.WaitForConfig(`"discovery_high_replication_lag":7200000000000`)
		require.NoError(t, err)
		defer func() {
			// Restore default back.
			clusterInstance.VtgateProcess.Config.DiscoveryHighReplicationLag = "2h"
			err = clusterInstance.VtgateProcess.RewriteConfiguration()
			require.NoError(t, err)
		}()
		clusterInstance.VtgateProcess.Config.DiscoveryHighReplicationLag = "1h"
		err = clusterInstance.VtgateProcess.RewriteConfiguration()
		require.NoError(t, err)
		// Test final config value.
		err = clusterInstance.VtgateProcess.WaitForConfig(`"discovery_high_replication_lag":"1h"`)
		require.NoError(t, err)
	})

	t.Run("DiscoveryMinServingVttablets", func(t *testing.T) {
		// Test initial config value
		err := clusterInstance.VtgateProcess.WaitForConfig(`"discovery_min_number_serving_vttablets":2`)
		require.NoError(t, err)
		defer func() {
			// Restore default back.
			clusterInstance.VtgateProcess.Config.DiscoveryMinServingVttablets = "2"
			err = clusterInstance.VtgateProcess.RewriteConfiguration()
			require.NoError(t, err)
		}()
		clusterInstance.VtgateProcess.Config.DiscoveryMinServingVttablets = "1"
		err = clusterInstance.VtgateProcess.RewriteConfiguration()
		require.NoError(t, err)
		// Test final config value.
		err = clusterInstance.VtgateProcess.WaitForConfig(`"discovery_min_number_serving_vttablets":"1"`)
		require.NoError(t, err)
	})

	t.Run("DiscoveryLegacyReplicationLagAlgo", func(t *testing.T) {
		// Test initial config value
		err := clusterInstance.VtgateProcess.WaitForConfig(`"discovery_legacy_replication_lag_algorithm":""`)
		require.NoError(t, err)
		defer func() {
			// Restore default back.
			clusterInstance.VtgateProcess.Config.DiscoveryLegacyReplicationLagAlgo = "true"
			err = clusterInstance.VtgateProcess.RewriteConfiguration()
			require.NoError(t, err)
		}()
		clusterInstance.VtgateProcess.Config.DiscoveryLegacyReplicationLagAlgo = "false"
		err = clusterInstance.VtgateProcess.RewriteConfiguration()
		require.NoError(t, err)
		// Test final config value.
		err = clusterInstance.VtgateProcess.WaitForConfig(`"discovery_legacy_replication_lag_algorithm":"false"`)
		require.NoError(t, err)
	})
}

func TestLookupErrorMetric(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	oldErrCount := getVtgateApiErrorCounts(t)

	utils.Exec(t, conn, `insert into t1 values (1,1)`)
	_, err := utils.ExecAllowError(t, conn, `insert into t1 values (2,1)`)
	require.ErrorContains(t, err, `(errno 1062) (sqlstate 23000)`)

	newErrCount := getVtgateApiErrorCounts(t)
	require.EqualValues(t, oldErrCount+1, newErrCount)
}

func getVtgateApiErrorCounts(t *testing.T) float64 {
	apiErr := getVar(t, "VtgateApiErrorCounts")
	if apiErr == nil {
		return 0
	}
	mapErrors := apiErr.(map[string]interface{})
	val, exists := mapErrors["Execute.ks.primary.ALREADY_EXISTS"]
	if exists {
		return val.(float64)
	}
	return 0
}

func getVar(t *testing.T, key string) interface{} {
	vars := clusterInstance.VtgateProcess.GetVars()
	require.NotNil(t, vars)

	val, exists := vars[key]
	if !exists {
		return nil
	}
	return val
}

// TestQueryProcessedMetric verifies that query metrics are correctly published.
func TestQueryProcessedMetric(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	tcases := []struct {
		sql          string
		queryMetric  string
		tableMetrics []string
		shards       int
	}{{
		sql:          "select id1, id2 from t1",
		queryMetric:  "SELECT.Scatter.PRIMARY",
		shards:       2,
		tableMetrics: []string{"SELECT.ks_t1"},
	}, {
		sql:          "update t1 set id2 = 2 where id1 = 1",
		queryMetric:  "UPDATE.MultiShard.PRIMARY",
		shards:       2,
		tableMetrics: []string{"UPDATE.ks_t1"},
	}, {
		sql:          "delete from t1 where id1 in (1)",
		queryMetric:  "DELETE.MultiShard.PRIMARY",
		shards:       2,
		tableMetrics: []string{"DELETE.ks_t1"},
	}, {
		sql:         "show tables",
		queryMetric: "SHOW.Passthrough.PRIMARY",
		shards:      1,
	}, {
		sql:         "savepoint a",
		queryMetric: "SAVEPOINT.Transaction.PRIMARY",
	}, {
		sql:         "rollback",
		queryMetric: "ROLLBACK.Transaction.PRIMARY",
	}, {
		sql:         "set @x=3",
		queryMetric: "SET.Local.PRIMARY",
	}, {
		sql:         "set sql_mode=''",
		queryMetric: "SET.MultiShard.PRIMARY",
		shards:      1,
	}, {
		sql:         "set @@vitess_metadata.k1='v1'",
		queryMetric: "SET.Topology.PRIMARY",
	}, {
		sql:          "select 1 from t1 a, t1 b",
		queryMetric:  "SELECT.Join.PRIMARY",
		shards:       3,
		tableMetrics: []string{"SELECT.ks_t1"},
	}, {
		sql:          "select count(*) from t1 a, t1 b",
		queryMetric:  "SELECT.Complex.PRIMARY",
		shards:       6,
		tableMetrics: []string{"SELECT.ks_t1"},
	}, {
		sql:          "select 1 from t1, t2, t3, t4 where t1.id1 = t2.id3 and t2.id3 = t3.id6 and t3.id6 = t4.id1 and t3.id7 = 5",
		queryMetric:  "SELECT.Lookup.PRIMARY",
		shards:       2,
		tableMetrics: []string{"SELECT.ks_t1", "SELECT.ks_t2", "SELECT.ks_t3", "SELECT.ks_t4"},
	}}

	initialQP := getQPMetric(t, "QueryExecutions")
	initialQR := getQPMetric(t, "QueryRoutes")
	initialQT := getQPMetric(t, "QueryExecutionsByTable")
	for _, tc := range tcases {
		t.Run(tc.sql, func(t *testing.T) {
			utils.Exec(t, conn, tc.sql)
			updatedQP := getQPMetric(t, "QueryExecutions")
			updatedQR := getQPMetric(t, "QueryRoutes")
			updatedQT := getQPMetric(t, "QueryExecutionsByTable")
			assert.EqualValuesf(t, 1, getValue(updatedQP, tc.queryMetric)-getValue(initialQP, tc.queryMetric), "queryExecutions metric: %s", tc.queryMetric)
			assert.EqualValuesf(t, tc.shards, getValue(updatedQR, tc.queryMetric)-getValue(initialQR, tc.queryMetric), "queryRoutes metric: %s", tc.queryMetric)
			for _, metric := range tc.tableMetrics {
				assert.EqualValuesf(t, 1, getValue(updatedQT, metric)-getValue(initialQT, metric), "queryExecutionsByTable metric: %s", metric)
			}
			initialQP, initialQR, initialQT = updatedQP, updatedQR, updatedQT
		})
	}
}

// TestQueryProcessedMetric verifies that query metrics are correctly published.
func TestMetricForExplain(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	initialQP := getQPMetric(t, "QueryExecutions")
	initialQT := getQPMetric(t, "QueryExecutionsByTable")
	t.Run("explain t1", func(t *testing.T) {
		utils.Exec(t, conn, "explain t1")
		updatedQP := getQPMetric(t, "QueryExecutions")
		updatedQT := getQPMetric(t, "QueryExecutionsByTable")
		assert.EqualValuesf(t, 1, getValue(updatedQP, "EXPLAIN.Passthrough.PRIMARY")-getValue(initialQP, "EXPLAIN.Passthrough.PRIMARY"), "queryExecutions metric: %s", "explain")
		assert.EqualValuesf(t, 1, getValue(updatedQT, "EXPLAIN.ks_t1")-getValue(initialQT, "EXPLAIN.ks_t1"), "queryExecutionsByTable metric: %s", "asdasd")
	})

	t.Run("explain `select id1, id2 from t1`", func(t *testing.T) {
		utils.ExecAllowError(t, conn, "explain `select id1, id2 from t1`")
		updatedQP := getQPMetric(t, "QueryExecutions")
		updatedQT := getQPMetric(t, "QueryExecutionsByTable")
		assert.EqualValuesf(t, 1, getValue(updatedQP, "EXPLAIN.Passthrough.PRIMARY")-getValue(initialQP, "EXPLAIN.Passthrough.PRIMARY"), "queryExecutions metric: %s", "explain")
		assert.EqualValuesf(t, 1, getValue(updatedQT, "EXPLAIN.ks_t1")-getValue(initialQT, "EXPLAIN.ks_t1"), "queryExecutionsByTable metric: %s", "asdasd")
	})

	t.Run("explain select id1, id2 from t1", func(t *testing.T) {
		utils.Exec(t, conn, "explain select id1, id2 from t1")
		updatedQP := getQPMetric(t, "QueryExecutions")
		updatedQT := getQPMetric(t, "QueryExecutionsByTable")
		assert.EqualValuesf(t, 2, getValue(updatedQP, "EXPLAIN.Passthrough.PRIMARY")-getValue(initialQP, "EXPLAIN.Passthrough.PRIMARY"), "queryExecutions metric: %s", "explain")
		assert.EqualValuesf(t, 2, getValue(updatedQT, "EXPLAIN.ks_t1")-getValue(initialQT, "EXPLAIN.ks_t1"), "queryExecutionsByTable metric: %s", "asdasd")
	})
}

func getQPMetric(t *testing.T, metric string) map[string]any {
	t.Helper()

	vars := clusterInstance.VtgateProcess.GetVars()
	require.NotNil(t, vars)

	qpVars, exists := vars[metric]
	if !exists {
		return nil
	}

	qpMap, ok := qpVars.(map[string]any)
	require.True(t, ok, "query queryMetric vars is not a map")

	return qpMap
}

func getValue(m map[string]any, key string) float64 {
	if m == nil {
		return 0
	}
	val, exists := m[key]
	if !exists {
		return 0
	}
	f, ok := val.(float64)
	if !ok {
		return 0
	}
	return f
}
