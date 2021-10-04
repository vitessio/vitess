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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtgate/utils"
)

func TestSelectNull(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into t5_null_vindex(id, idx) values(1, 'a'), (2, 'b'), (3, null)")
	utils.Exec(t, conn, "commit")

	utils.AssertMatches(t, conn, "select id, idx from t5_null_vindex order by id", "[[INT64(1) VARCHAR(\"a\")] [INT64(2) VARCHAR(\"b\")] [INT64(3) NULL]]")
	utils.AssertIsEmpty(t, conn, "select id, idx from t5_null_vindex where idx = null")
	utils.AssertMatches(t, conn, "select id, idx from t5_null_vindex where idx is null", "[[INT64(3) NULL]]")
	utils.AssertMatches(t, conn, "select id, idx from t5_null_vindex where idx is not null order by id", "[[INT64(1) VARCHAR(\"a\")] [INT64(2) VARCHAR(\"b\")]]")
	utils.AssertIsEmpty(t, conn, "select id, idx from t5_null_vindex where id IN (null)")
	utils.AssertMatches(t, conn, "select id, idx from t5_null_vindex where id IN (1,2,null) order by id", "[[INT64(1) VARCHAR(\"a\")] [INT64(2) VARCHAR(\"b\")]]")
	utils.AssertIsEmpty(t, conn, "select id, idx from t5_null_vindex where id NOT IN (1,null) order by id")
	utils.AssertMatches(t, conn, "select id, idx from t5_null_vindex where id NOT IN (1,3)", "[[INT64(2) VARCHAR(\"b\")]]")

	utils.Exec(t, conn, "delete from t5_null_vindex")
}

func TestDoStatement(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "do 1")
	utils.Exec(t, conn, "do 'a', 1+2,database()")
}

func TestShowColumns(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	expected := `[[VARCHAR("id") TEXT("bigint(20)") VARCHAR("NO") VARCHAR("PRI") NULL VARCHAR("")] [VARCHAR("idx") TEXT("varchar(50)") VARCHAR("YES") VARCHAR("") NULL VARCHAR("")]]`
	utils.AssertMatches(t, conn, "show columns from `t5_null_vindex` in `ks`", expected)
	utils.AssertMatches(t, conn, "SHOW COLUMNS from `t5_null_vindex` in `ks`", expected)
	utils.AssertMatches(t, conn, "SHOW columns FROM `t5_null_vindex` in `ks`", expected)
}

func TestShowTables(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	query := "show tables;"
	qr := utils.Exec(t, conn, query)

	assert.Equal(t, "information_schema", qr.Fields[0].Database)
	assert.Equal(t, "Tables_in_ks", qr.Fields[0].Name)
}

func TestCastConvert(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.AssertMatches(t, conn, `SELECT CAST("test" AS CHAR(60))`, `[[VARCHAR("test")]]`)
}

func TestCompositeIN(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// clean up before & after
	utils.Exec(t, conn, "delete from t1")
	defer utils.Exec(t, conn, "delete from t1")

	utils.Exec(t, conn, "insert into t1(id1, id2) values(1, 2), (4, 5)")

	// Just check for correct results. Plan generation is tested in unit tests.
	utils.AssertMatches(t, conn, "select id1 from t1 where (id1, id2) in ((1, 2))", "[[INT64(1)]]")
}

func TestSavepointInTx(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

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
	utils.Exec(t, conn, "use")
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)] [INT64(2)] [INT64(4)]]`)

	_, err = conn.ExecuteFetch("rollback work to savepoint a", 1000, true)
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
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "savepoint a")
	utils.Exec(t, conn, "savepoint b")

	_, err = conn.ExecuteFetch("rollback to b", 1, true)
	require.Error(t, err)
	_, err = conn.ExecuteFetch("release savepoint a", 1, true)
	require.Error(t, err)
}

func TestSavepointAdditionalCase(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

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
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	result := utils.Exec(t, conn, "explain select * from t1")
	got := fmt.Sprintf("%v", result.Rows)
	require.Contains(t, got, "SIMPLE") // there is a lot more coming from mysql,
	// but we are trying to make the test less fragile

	result = utils.Exec(t, conn, "explain ks.t1")
	require.EqualValues(t, 2, len(result.Rows))
}

func TestXXHash(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

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
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	utils.AssertMatches(t, conn, "show tables from ks where Tables_in_ks='t6'", `[[VARCHAR("t6")]]`)
	utils.Exec(t, conn, "begin")
	utils.AssertMatches(t, conn, "show tables from ks where Tables_in_ks='t3'", `[[VARCHAR("t3")]]`)
}

func TestOffsetAndLimitWithOLAP(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	defer utils.Exec(t, conn, "set workload=oltp;delete from t1")

	utils.Exec(t, conn, "insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1 limit 3 offset 2", "[[INT64(3)] [INT64(4)] [INT64(5)]]")
	utils.Exec(t, conn, "set workload='olap'")
	utils.AssertMatches(t, conn, "select id1 from t1 order by id1 limit 3 offset 2", "[[INT64(3)] [INT64(4)] [INT64(5)]]")
}

func TestSwitchBetweenOlapAndOltp(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.AssertMatches(t, conn, "select @@workload", `[[VARBINARY("OLTP")]]`)

	utils.Exec(t, conn, "set workload='olap'")

	utils.AssertMatches(t, conn, "select @@workload", `[[VARBINARY("OLAP")]]`)

	utils.Exec(t, conn, "set workload='oltp'")

	utils.AssertMatches(t, conn, "select @@workload", `[[VARBINARY("OLTP")]]`)
}

func TestFoundRowsOnDualQueries(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "select 42")
	utils.AssertMatches(t, conn, "select found_rows()", "[[UINT64(1)]]")
}

func TestUseStmtInOLAP(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	queries := []string{"set workload='olap'", "use `ks:80-`", "use `ks:-80`"}
	for i, q := range queries {
		t.Run(fmt.Sprintf("%d-%s", i, q), func(t *testing.T) {
			utils.Exec(t, conn, q)
			require.NoError(t, err)
		})
	}
}

func TestInsertStmtInOLAP(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, `set workload='olap'`)
	_, err = conn.ExecuteFetch(`insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)`, 1000, true)
	require.Error(t, err)
	utils.AssertMatches(t, conn, `select id1 from t1 order by id1`, `[]`)
}

func TestCreateIndex(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	// Test that create index with the correct table name works
	_, err = conn.ExecuteFetch(`create index i1 on t1 (id1)`, 1000, true)
	require.NoError(t, err)
	// Test routing rules for create index.
	_, err = conn.ExecuteFetch(`create index i2 on ks.t1000 (id1)`, 1000, true)
	require.NoError(t, err)
}

func TestCreateView(t *testing.T) {
	// The test wont work since we cant change the vschema without reloading the vtgate.
	t.Skip()
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	defer utils.Exec(t, conn, `delete from t1`)
	// Test that create view works and the output is as expected
	utils.Exec(t, conn, `create view v1 as select * from t1`)
	utils.Exec(t, conn, `insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)`)
	// This wont work, since ALTER VSCHEMA ADD TABLE is only supported for unsharded keyspaces
	utils.Exec(t, conn, "alter vschema add table v1")
	utils.AssertMatches(t, conn, "select * from v1", `[[INT64(1) INT64(1)] [INT64(2) INT64(2)] [INT64(3) INT64(3)] [INT64(4) INT64(4)] [INT64(5) INT64(5)]]`)
}

func TestVersions(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	qr := utils.Exec(t, conn, `select @@version`)
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), "vitess")

	qr = utils.Exec(t, conn, `select @@version_comment`)
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), "Git revision")
}

func TestFlush(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	utils.Exec(t, conn, "flush local tables t1, t2")
}

func TestShowVariables(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
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
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	query := "show global vgtid_executed from ks"
	qr := utils.Exec(t, conn, query)
	require.Equal(t, 1, len(qr.Rows))
	require.Equal(t, 2, len(qr.Rows[0]))

	defer utils.Exec(t, conn, `delete from t1`)
	utils.Exec(t, conn, `insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)`)
	qr2 := utils.Exec(t, conn, query)
	require.Equal(t, 1, len(qr2.Rows))
	require.Equal(t, 2, len(qr2.Rows[0]))

	require.Equal(t, qr.Rows[0][0], qr2.Rows[0][0], "keyspace should be same")
	require.NotEqual(t, qr.Rows[0][1].ToString(), qr2.Rows[0][1].ToString(), "vgtid should have changed")
}

func TestShowGtid(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	query := "show global gtid_executed from ks"
	qr := utils.Exec(t, conn, query)
	require.Equal(t, 2, len(qr.Rows))

	res := make(map[string]string, 2)
	for _, row := range qr.Rows {
		require.Equal(t, KeyspaceName, row[0].ToString())
		res[row[2].ToString()] = row[1].ToString()
	}

	defer utils.Exec(t, conn, `delete from t1`)
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
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "delete t1 from t1 where id1 = 1")
	utils.Exec(t, conn, "delete t.* from t1 t where t.id1 = 1")
}

func TestFunctionInDefault(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// set the sql mode ALLOW_INVALID_DATES
	utils.Exec(t, conn, `SET sql_mode = 'ALLOW_INVALID_DATES'`)

	_, err = conn.ExecuteFetch(`create table function_default (x varchar(25) DEFAULT (TRIM(" check ")))`, 1000, true)
	// this query fails because mysql57 does not support functions in default clause
	require.Error(t, err)

	// verify that currenet_timestamp and it's aliases work as default values
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

	_, err = conn.ExecuteFetch(`create table function_default (ts TIMESTAMP DEFAULT UTC_TIMESTAMP)`, 1000, true)
	// this query fails because utc_timestamp is not supported in default clause
	require.Error(t, err)

	utils.Exec(t, conn, `create table function_default (x varchar(25) DEFAULT "check")`)
	utils.Exec(t, conn, "drop table function_default")
}

func TestRenameFieldsOnOLAP(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	_ = utils.Exec(t, conn, "set workload = olap")
	defer func() {
		utils.Exec(t, conn, "set workload = oltp")
	}()

	qr := utils.Exec(t, conn, "show tables")
	require.Equal(t, 1, len(qr.Fields))
	assert.Equal(t, `Tables_in_ks`, fmt.Sprintf("%v", qr.Fields[0].Name))
	_ = utils.Exec(t, conn, "use mysql")
	qr = utils.Exec(t, conn, "select @@workload")
	assert.Equal(t, `[[VARBINARY("OLAP")]]`, fmt.Sprintf("%v", qr.Rows))
}

func TestSelectEqualUniqueOuterJoinRightPredicate(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, `delete from t1`)
	utils.Exec(t, conn, `delete from t2`)
	defer func() {
		utils.Exec(t, conn, `delete from t1`)
		utils.Exec(t, conn, `delete from t2`)
	}()
	utils.Exec(t, conn, "insert into t1(id1, id2) values (0,10),(1,9),(2,8),(3,7),(4,6),(5,5)")
	utils.Exec(t, conn, "insert into t2(id3, id4) values (0,20),(1,19),(2,18),(3,17),(4,16),(5,15)")
	utils.AssertMatches(t, conn, `SELECT id3 FROM t1 LEFT JOIN t2 ON t1.id1 = t2.id3 WHERE t2.id3 = 10`, `[]`)
}
