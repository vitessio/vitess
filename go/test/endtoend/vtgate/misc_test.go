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
	"fmt"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	query := "show tables;"
	qr := utils.Exec(t, conn, query)

	assert.Equal(t, "Tables_in_ks", qr.Fields[0].Name)
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

func TestCreateView(t *testing.T) {
	// The test wont work since we cant change the vschema without reloading the vtgate.
	t.Skip()
	conn, closer := start(t)
	defer closer()
	// Test that create view works and the output is as expected
	utils.Exec(t, conn, `create view v1 as select * from t1`)
	utils.Exec(t, conn, `insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)`)
	// This wont work, since ALTER VSCHEMA ADD TABLE is only supported for unsharded keyspaces
	utils.Exec(t, conn, "alter vschema add table v1")
	utils.AssertMatches(t, conn, "select * from v1", `[[INT64(1) INT64(1)] [INT64(2) INT64(2)] [INT64(3) INT64(3)] [INT64(4) INT64(4)] [INT64(5) INT64(5)]]`)
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

	utils.Exec(t, conn, `create table function_default (ts TIMESTAMP DEFAULT UTC_TIMESTAMP)`)
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
	assert.Equal(t, `Tables_in_ks`, fmt.Sprintf("%v", qr.Fields[0].Name))
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
		utils.Exec(t, conn, fmt.Sprintf("set workload = %s", workload))
		utils.Exec(t, conn, "set sql_select_limit = 2")
		utils.AssertMatches(t, conn, "select uid, msg from t7_xxhash order by uid", `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("2") VARCHAR("b")]]`)
		utils.AssertMatches(t, conn, "(select uid, msg from t7_xxhash order by uid)", `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("2") VARCHAR("b")]]`)
		utils.AssertMatches(t, conn, "select uid, msg from t7_xxhash order by uid limit 4", `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("2") VARCHAR("b")] [VARCHAR("3") NULL] [VARCHAR("4") VARCHAR("a")]]`)
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
		utils.Exec(t, conn, fmt.Sprintf("set workload = %s", workload))
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

func TestDescribeVindex(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	_, err := conn.ExecuteFetch("describe hash", 1000, false)
	require.Error(t, err)
	mysqlErr := err.(*mysql.SQLError)
	assert.Equal(t, mysql.ERNoSuchTable, mysqlErr.Num)
	assert.Equal(t, "42S02", mysqlErr.State)
	assert.Contains(t, mysqlErr.Message, "NotFound desc")
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
	conn, closer := start(t)
	defer closer()

	for i := 0; i < 250; i++ {
		utils.Exec(t, conn, fmt.Sprintf("insert into t1 (id1, id2) values (%d, %d)", i, i+1))
	}

	utils.AssertContainsError(t, conn, "select id1 from t1 where id1 < 1000", `Row count exceeded 100`)
}
