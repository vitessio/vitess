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
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/test/vitesst"
)

func TestInsertOnDuplicateKey(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "insert into t11(id, sharding_key, col1, col2, col3) values(1, 2, 'a', 1, 2)")
	vitesst.Exec(t, conn, "insert into t11(id, sharding_key, col1, col2, col3) values(1, 2, 'a', 1, 2) on duplicate key update id=10;")
	vitesst.AssertMatches(t, conn, "select id, sharding_key from t11 where id=10", "[[INT64(10) INT64(2)]]")
}

func TestInsertNeg(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "insert ignore into t10(id, sharding_key, col1, col2, col3) values(10, 20, 'a', 1, 2), (20, -20, 'b', 3, 4), (30, -40, 'c', 6, 7), (40, 60, 'd', 4, 10)")
	vitesst.Exec(t, conn, "insert ignore into t10(id, sharding_key, col1, col2, col3) values(1, 2, 'a', 1, 2), (2, -2, 'b', -3, 4), (3, -4, 'c', 6, -7), (4, 6, 'd', 4, -10)")
}

func TestSelectNull(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into t5_null_vindex(id, idx) values(1, 'a'), (2, 'b'), (3, null)")
	vitesst.Exec(t, conn, "commit")

	vitesst.AssertMatches(t, conn, "select id, idx from t5_null_vindex order by id", "[[INT64(1) VARCHAR(\"a\")] [INT64(2) VARCHAR(\"b\")] [INT64(3) NULL]]")
	vitesst.AssertIsEmpty(t, conn, "select id, idx from t5_null_vindex where idx = null")
	vitesst.AssertMatches(t, conn, "select id, idx from t5_null_vindex where idx is null", "[[INT64(3) NULL]]")
	vitesst.AssertMatches(t, conn, "select id, idx from t5_null_vindex where idx <=> null", "[[INT64(3) NULL]]")
	vitesst.AssertMatches(t, conn, "select id, idx from t5_null_vindex where idx is not null order by id", "[[INT64(1) VARCHAR(\"a\")] [INT64(2) VARCHAR(\"b\")]]")
	vitesst.AssertIsEmpty(t, conn, "select id, idx from t5_null_vindex where id IN (null)")
	vitesst.AssertMatches(t, conn, "select id, idx from t5_null_vindex where id IN (1,2,null) order by id", "[[INT64(1) VARCHAR(\"a\")] [INT64(2) VARCHAR(\"b\")]]")
	vitesst.AssertIsEmpty(t, conn, "select id, idx from t5_null_vindex where id NOT IN (1,null) order by id")
	vitesst.AssertMatches(t, conn, "select id, idx from t5_null_vindex where id NOT IN (1,3)", "[[INT64(2) VARCHAR(\"b\")]]")
}

func TestDoStatement(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "do 1")
	vitesst.Exec(t, conn, "do 'a', 1+2,database()")
}

func TestShowColumns(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	expected80 := `[[VARCHAR("id") BLOB("bigint") VARCHAR("NO") BINARY("PRI") NULL VARCHAR("")] [VARCHAR("idx") BLOB("varchar(50)") VARCHAR("YES") BINARY("") NULL VARCHAR("")]]`
	expected57 := `[[VARCHAR("id") TEXT("bigint(20)") VARCHAR("NO") VARCHAR("PRI") NULL VARCHAR("")] [VARCHAR("idx") TEXT("varchar(50)") VARCHAR("YES") VARCHAR("") NULL VARCHAR("")]]`
	vitesst.AssertMatchesAny(t, conn, "show columns from `t5_null_vindex` in `ks`", expected80, expected57)
	vitesst.AssertMatchesAny(t, conn, "SHOW COLUMNS from `t5_null_vindex` in `ks`", expected80, expected57)
	vitesst.AssertMatchesAny(t, conn, "SHOW columns FROM `t5_null_vindex` in `ks`", expected80, expected57)
	vitesst.AssertMatchesAny(t, conn, "SHOW columns FROM `t5_null_vindex` where Field = 'id'",
		`[[VARCHAR("id") BLOB("bigint") VARCHAR("NO") BINARY("PRI") NULL VARCHAR("")]]`,
		`[[VARCHAR("id") TEXT("bigint(20)") VARCHAR("NO") VARCHAR("PRI") NULL VARCHAR("")]]`)
}

func TestShowTables(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	qr := vitesst.Exec(t, conn, "show tables")
	assert.Equal(t, "Tables_in_ks", qr.Fields[0].Name)

	// no error on executing `show tables` on system schema
	vitesst.Exec(t, conn, `use mysql`)
	vitesst.Exec(t, conn, "show tables")
	vitesst.Exec(t, conn, "show tables from information_schema")
}

func TestCastConvert(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.AssertMatches(t, conn, `SELECT CAST("test" AS CHAR(60))`, `[[VARCHAR("test")]]`)
}

func TestCompositeIN(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(1, 2), (4, 5)")

	// Just check for correct results. Plan generation is tested in unit tests.
	vitesst.AssertMatches(t, conn, "select id1 from t1 where (id1, id2) in ((1, 2))", "[[INT64(1)]]")
}

func TestSavepointInTx(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "savepoint a")
	vitesst.Exec(t, conn, "start transaction")
	vitesst.Exec(t, conn, "savepoint b")
	vitesst.Exec(t, conn, "rollback to b")
	vitesst.Exec(t, conn, "release savepoint b")
	vitesst.Exec(t, conn, "savepoint b")
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(1,1)") // -80
	vitesst.Exec(t, conn, "savepoint c")
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(4,4)") // 80-
	vitesst.Exec(t, conn, "savepoint d")
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(2,2)") // -80
	vitesst.Exec(t, conn, "savepoint e")

	// Validate all the data.
	vitesst.Exec(t, conn, "use `ks:-80`")
	vitesst.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)] [INT64(2)]]`)
	vitesst.Exec(t, conn, "use `ks:80-`")
	vitesst.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(4)]]`)
	vitesst.Exec(t, conn, "use ks")
	vitesst.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)] [INT64(2)] [INT64(4)]]`)

	_, err := conn.ExecuteFetch("rollback work to savepoint a", 1000, true)
	require.Error(t, err)

	vitesst.Exec(t, conn, "release savepoint d")

	_, err = conn.ExecuteFetch("rollback to d", 1000, true)
	require.Error(t, err)
	vitesst.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)] [INT64(2)] [INT64(4)]]`)

	vitesst.Exec(t, conn, "rollback to c")
	vitesst.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)]]`)

	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(2,2),(3,3),(4,4)")
	vitesst.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)]]`)

	vitesst.Exec(t, conn, "rollback to b")
	vitesst.AssertMatches(t, conn, "select id1 from t1 order by id1", `[]`)

	vitesst.Exec(t, conn, "commit")
	vitesst.AssertMatches(t, conn, "select id1 from t1 order by id1", `[]`)

	vitesst.Exec(t, conn, "start transaction")

	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(2,2),(3,3),(4,4)")
	vitesst.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(2)] [INT64(3)] [INT64(4)]]`)

	// After previous commit all the savepoints are cleared.
	_, err = conn.ExecuteFetch("rollback to b", 1000, true)
	require.Error(t, err)

	vitesst.Exec(t, conn, "rollback")
	vitesst.AssertMatches(t, conn, "select id1 from t1 order by id1", `[]`)
}

func TestSavepointOutsideTx(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "savepoint a")
	vitesst.Exec(t, conn, "savepoint b")

	_, err := conn.ExecuteFetch("rollback to b", 1, true)
	require.Error(t, err)
	_, err = conn.ExecuteFetch("release savepoint a", 1, true)
	require.Error(t, err)
}

func TestSavepointAdditionalCase(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "start transaction")
	vitesst.Exec(t, conn, "savepoint a")
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(1,1)")             // -80
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(2,2),(3,3),(4,4)") // -80 & 80-
	vitesst.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)]]`)

	vitesst.Exec(t, conn, "rollback to a")
	vitesst.AssertMatches(t, conn, "select id1 from t1 order by id1", `[]`)

	vitesst.Exec(t, conn, "commit")
	vitesst.AssertMatches(t, conn, "select id1 from t1 order by id1", `[]`)

	vitesst.Exec(t, conn, "start transaction")
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(1,1)") // -80
	vitesst.Exec(t, conn, "savepoint a")
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(2,2),(3,3)") // -80
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(4,4)")       // 80-
	vitesst.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)]]`)

	vitesst.Exec(t, conn, "rollback to a")
	vitesst.AssertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)]]`)

	vitesst.Exec(t, conn, "rollback")
	vitesst.AssertMatches(t, conn, "select id1 from t1 order by id1", `[]`)
}

func TestExplainPassthrough(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	result := vitesst.Exec(t, conn, "explain select * from t1")
	got := fmt.Sprintf("%v", result.Rows)
	require.Contains(t, got, "SIMPLE") // there is a lot more coming from mysql,
	// but we are trying to make the test less fragile

	result = vitesst.Exec(t, conn, "explain ks.t1")
	require.EqualValues(t, 2, len(result.Rows))
}

func TestXXHash(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "insert into t7_xxhash(uid, phone, msg) values('u-1', 1, 'message')")
	vitesst.AssertMatches(t, conn, "select uid, phone, msg from t7_xxhash where phone = 1", `[[VARCHAR("u-1") INT64(1) VARCHAR("message")]]`)
	vitesst.AssertMatches(t, conn, "select phone, keyspace_id from t7_xxhash_idx", `[[INT64(1) VARBINARY("\x1cU^f\xbfyE^")]]`)
	vitesst.Exec(t, conn, "update t7_xxhash set phone = 2 where uid = 'u-1'")
	vitesst.AssertMatches(t, conn, "select uid, phone, msg from t7_xxhash where phone = 1", `[]`)
	vitesst.AssertMatches(t, conn, "select uid, phone, msg from t7_xxhash where phone = 2", `[[VARCHAR("u-1") INT64(2) VARCHAR("message")]]`)
	vitesst.AssertMatches(t, conn, "select phone, keyspace_id from t7_xxhash_idx", `[[INT64(2) VARBINARY("\x1cU^f\xbfyE^")]]`)
	vitesst.Exec(t, conn, "delete from t7_xxhash where uid = 'u-1'")
	vitesst.AssertMatches(t, conn, "select uid, phone, msg from t7_xxhash where uid = 'u-1'", `[]`)
	vitesst.AssertMatches(t, conn, "select phone, keyspace_id from t7_xxhash_idx", `[]`)
}

func TestShowTablesWithWhereClause(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.AssertMatchesAny(t, conn, "show tables from ks where Tables_in_ks='t6'", `[[VARBINARY("t6")]]`, `[[VARCHAR("t6")]]`)
	vitesst.Exec(t, conn, "begin")
	vitesst.AssertMatchesAny(t, conn, "show tables from ks where Tables_in_ks='t3'", `[[VARBINARY("t3")]]`, `[[VARCHAR("t3")]]`)
}

func TestOffsetAndLimitWithOLAP(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
	vitesst.AssertMatches(t, conn, "select id1 from t1 order by id1 limit 3 offset 2", "[[INT64(3)] [INT64(4)] [INT64(5)]]")
	vitesst.Exec(t, conn, "set workload='olap'")
	vitesst.AssertMatches(t, conn, "select id1 from t1 order by id1 limit 3 offset 2", "[[INT64(3)] [INT64(4)] [INT64(5)]]")
}

func TestSwitchBetweenOlapAndOltp(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.AssertMatches(t, conn, "select @@workload", `[[VARCHAR("OLTP")]]`)

	vitesst.Exec(t, conn, "set workload='olap'")

	vitesst.AssertMatches(t, conn, "select @@workload", `[[VARCHAR("OLAP")]]`)

	vitesst.Exec(t, conn, "set workload='oltp'")

	vitesst.AssertMatches(t, conn, "select @@workload", `[[VARCHAR("OLTP")]]`)
}

func TestFoundRowsOnDualQueries(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "select 42")
	vitesst.AssertMatches(t, conn, "select found_rows()", "[[INT64(1)]]")
}

func TestUseStmtInOLAP(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	queries := []string{"set workload='olap'", "use `ks:80-`", "use `ks:-80`"}
	for i, q := range queries {
		t.Run(fmt.Sprintf("%d-%s", i, q), func(t *testing.T) {
			vitesst.Exec(t, conn, q)
		})
	}
}

func TestInsertStmtInOLAP(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, `set workload='olap'`)
	vitesst.Exec(t, conn, `insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)`)
	vitesst.AssertMatches(t, conn, `select id1 from t1 order by id1`, `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)] [INT64(5)]]`)
}

// TestShardTargetedDMLInOLAP covers https://github.com/vitessio/vitess/issues/19561.
// When a session targets a specific shard, DML is planned as a Send primitive whose
// streaming path (used in OLAP mode) issues a StreamExecute RPC to the tablet. DML
// must succeed on that path and run in an implicit transaction, just like the
// non-streaming path.
func TestShardTargetedDMLInOLAP(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	t.Cleanup(closer)

	vitesst.Exec(t, conn, `set workload='olap'`)
	vitesst.Exec(t, conn, "use `ks:-80`")
	qr := vitesst.Exec(t, conn, `insert into t1(id1, id2) values (1, 1)`)
	require.EqualValues(t, 1, qr.RowsAffected)
	vitesst.AssertMatches(t, conn, `select id1, id2 from t1`, `[[INT64(1) INT64(1)]]`)
}

// TestShardTargetedDMLInOLAPRecordsTabletStats verifies that DML executed on the
// streaming path (StreamExecute, used for shard-targeted DML in OLAP mode) records
// the same per-table query stats on the tablet as the non-streaming Execute path.
// Before the fix, streamed DML returned the correct result but recorded no
// QueryRowsAffected, leaving these DMLs invisible to per-table tablet metrics.
func TestShardTargetedDMLInOLAPRecordsTabletStats(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	t.Cleanup(closer)

	// The DML below is shard-targeted to -80, so it runs on that shard's primary.
	primary := shardPrimaryTablet(t, "-80")
	const statKey = "t1.Insert"
	rowsAffectedBefore := tabletQueryStat(t, primary, "QueryRowsAffected", statKey)

	vitesst.Exec(t, conn, `set workload='olap'`)
	vitesst.Exec(t, conn, "use `ks:-80`")
	qr := vitesst.Exec(t, conn, `insert into t1(id1, id2) values (1, 1)`)
	require.EqualValues(t, 1, qr.RowsAffected)

	rowsAffectedAfter := tabletQueryStat(t, primary, "QueryRowsAffected", statKey)
	require.Equal(t, rowsAffectedBefore+1, rowsAffectedAfter,
		"streamed DML must record QueryRowsAffected on the tablet like the non-streaming path")
}

// TestShardTargetedFailedDMLInOLAPRecordsErrorStats verifies that a DML that fails
// on the streaming path records error stats on the tablet, just like the
// non-streaming Execute path. Before the fix, a failed streamed DML returned the
// error to the client but incremented no QueryErrorCounts.
func TestShardTargetedFailedDMLInOLAPRecordsErrorStats(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	t.Cleanup(closer)

	// The DML below is shard-targeted to -80, so it runs on that shard's primary.
	primary := shardPrimaryTablet(t, "-80")
	const statKey = "t1.Insert"
	errCountBefore := tabletQueryStat(t, primary, "QueryErrorCounts", statKey)

	vitesst.Exec(t, conn, `set workload='olap'`)
	vitesst.Exec(t, conn, "use `ks:-80`")
	// The first insert succeeds; the duplicate primary key makes the second fail
	// on the tablet, exercising streamDML's error path.
	vitesst.Exec(t, conn, `insert into t1(id1, id2) values (1, 1)`)
	_, err := vitesst.ExecAllowError(t, conn, `insert into t1(id1, id2) values (1, 1)`)
	require.ErrorContains(t, err, "errno 1062")

	errCountAfter := tabletQueryStat(t, primary, "QueryErrorCounts", statKey)
	require.Equal(t, errCountBefore+1, errCountAfter,
		"failed streamed DML must record QueryErrorCounts on the tablet like the non-streaming path")
}

func shardPrimaryTablet(t *testing.T, shardName string) *vitesst.Tablet {
	t.Helper()
	shard := clusterInstance.Keyspace(KeyspaceName).Shard(shardName)
	require.NotNilf(t, shard, "no shard %q in keyspace %s", shardName, KeyspaceName)
	primary := shard.Primary()
	require.NotNilf(t, primary, "no primary tablet for shard %q", shardName)
	return primary
}

func tabletQueryStat(t *testing.T, tablet *vitesst.Tablet, varName, key string) float64 {
	t.Helper()
	vars, err := tablet.GetVars(t.Context())
	require.NoError(t, err)
	counter, ok := vars[varName].(map[string]any)
	if !ok {
		return 0
	}
	val, _ := counter[key].(float64)
	return val
}

func TestCreateIndex(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()
	// Test that create index with the correct table name works
	vitesst.Exec(t, conn, `create index i1 on t1 (id1)`)
	// Test routing rules for create index.
	vitesst.Exec(t, conn, `create index i2 on ks.t1000 (id1)`)
}

func TestVersions(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	qr := vitesst.Exec(t, conn, `select @@version`)
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), "vitess")

	qr = vitesst.Exec(t, conn, `select @@version_comment`)
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), "Git revision")
}

func TestFlush(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()
	vitesst.Exec(t, conn, "flush local tables t1, t2")
}

// TestFlushLock tests that ftwrl and unlock tables should unblock other session connections to execute the query.
func TestFlushLock(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	// replica: fail it
	vitesst.Exec(t, conn, "use @replica")
	_, err := vitesst.ExecAllowError(t, conn, "flush tables ks.t1, ks.t2 with read lock")
	require.ErrorContains(t, err, "VT09012: FLUSH statement with REPLICA tablet not allowed")

	// primary: should work
	vitesst.Exec(t, conn, "use @primary")
	vitesst.Exec(t, conn, "flush tables ks.t1, ks.t2 with read lock")

	var cnt atomic.Int32
	go func() {
		ctx := t.Context()
		conn2, err := mysql.Connect(ctx, &vtParams)
		if !assert.NoError(t, err) {
			return
		}
		defer conn2.Close()

		cnt.Add(1)
		vitesst.Exec(t, conn2, "select * from ks.t1 for update")
		cnt.Add(1)
	}()
	for cnt.Load() == 0 {
	}
	// added sleep to let the query execute inside the go routine, which should be blocked.
	time.Sleep(1 * time.Second)
	require.EqualValues(t, 1, cnt.Load())

	// unlock it
	vitesst.Exec(t, conn, "unlock tables")

	// now wait for go routine to complete.
	timeout := time.After(3 * time.Second)
	for cnt.Load() != 2 {
		select {
		case <-timeout:
			require.Fail(t, "test timeout waiting for select query to complete")
		default:
		}
	}
}

func TestShowVariables(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()
	res := vitesst.Exec(t, conn, "show variables like \"%version%\";")
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
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	query := "show global vgtid_executed from ks"
	qr := vitesst.Exec(t, conn, query)
	require.Equal(t, 1, len(qr.Rows))
	require.Equal(t, 2, len(qr.Rows[0]))

	vitesst.Exec(t, conn, `insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)`)
	qr2 := vitesst.Exec(t, conn, query)
	require.Equal(t, 1, len(qr2.Rows))
	require.Equal(t, 2, len(qr2.Rows[0]))

	require.Equal(t, qr.Rows[0][0], qr2.Rows[0][0], "keyspace should be same")
	require.NotEqual(t, qr.Rows[0][1].ToString(), qr2.Rows[0][1].ToString(), "vgtid should have changed")
}

func TestShowGtid(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	query := "show global gtid_executed from ks"
	qr := vitesst.Exec(t, conn, query)
	require.Equal(t, 2, len(qr.Rows))

	res := make(map[string]string, 2)
	for _, row := range qr.Rows {
		require.Equal(t, KeyspaceName, row[0].ToString())
		res[row[2].ToString()] = row[1].ToString()
	}

	vitesst.Exec(t, conn, `insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)`)
	qr2 := vitesst.Exec(t, conn, query)
	require.Equal(t, 2, len(qr2.Rows))

	for _, row := range qr2.Rows {
		require.Equal(t, KeyspaceName, row[0].ToString())
		gtid, exists := res[row[2].ToString()]
		require.True(t, exists, "gtid not cached for row: %v", row)
		require.NotEqual(t, gtid, row[1].ToString())
	}
}

func TestDeleteAlias(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "delete t1 from t1 where id1 = 1")
	vitesst.Exec(t, conn, "delete t.* from t1 t where t.id1 = 1")
}

func TestFunctionInDefault(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	// set the sql mode ALLOW_INVALID_DATES
	vitesst.Exec(t, conn, `SET sql_mode = 'ALLOW_INVALID_DATES'`)

	// test that default expression works for columns.
	vitesst.Exec(t, conn, `create table function_default (x varchar(25) DEFAULT (TRIM(" check ")))`)
	vitesst.Exec(t, conn, "drop table function_default")

	// verify that current_timestamp and it's aliases work as default values
	vitesst.Exec(t, conn, `create table function_default (
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
	vitesst.Exec(t, conn, "drop table function_default")

	vitesst.Exec(t, conn, `create table function_default (ts TIMESTAMP DEFAULT (UTC_TIMESTAMP))`)
	vitesst.Exec(t, conn, "drop table function_default")

	vitesst.Exec(t, conn, `create table function_default (x varchar(25) DEFAULT "check")`)
	vitesst.Exec(t, conn, "drop table function_default")
}

func TestRenameFieldsOnOLAP(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	_ = vitesst.Exec(t, conn, "set workload = olap")

	qr := vitesst.Exec(t, conn, "show tables")
	require.Equal(t, 1, len(qr.Fields))
	assert.Equal(t, `Tables_in_ks`, qr.Fields[0].Name)
	_ = vitesst.Exec(t, conn, "use mysql")
	qr = vitesst.Exec(t, conn, "select @@workload")
	assert.Equal(t, `[[VARCHAR("OLAP")]]`, fmt.Sprintf("%v", qr.Rows))
}

func TestSelectEqualUniqueOuterJoinRightPredicate(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "insert into t1(id1, id2) values (0,10),(1,9),(2,8),(3,7),(4,6),(5,5)")
	vitesst.Exec(t, conn, "insert into t2(id3, id4) values (0,20),(1,19),(2,18),(3,17),(4,16),(5,15)")
	vitesst.AssertMatches(t, conn, `SELECT id3 FROM t1 LEFT JOIN t2 ON t1.id1 = t2.id3 WHERE t2.id3 = 10`, `[]`)
}

func TestSQLSelectLimit(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "insert into t7_xxhash(uid, msg) values(1, 'a'), (2, 'b'), (3, null), (4, 'a'), (5, 'a'), (6, 'b')")

	for _, workload := range []string{"olap", "oltp"} {
		vitesst.Exec(t, conn, "set workload = "+workload)
		vitesst.Exec(t, conn, "set sql_select_limit = 2")
		vitesst.AssertMatches(t, conn, "select uid, msg from t7_xxhash order by uid", `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("2") VARCHAR("b")]]`)
		vitesst.AssertMatches(t, conn, "(select uid, msg from t7_xxhash order by uid)", `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("2") VARCHAR("b")]]`)
		vitesst.AssertMatches(t, conn, "select uid, msg from t7_xxhash order by uid limit 4", `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("2") VARCHAR("b")] [VARCHAR("3") NULL] [VARCHAR("4") VARCHAR("a")]]`)

		// Don't LIMIT subqueries
		vitesst.AssertMatches(t, conn, "select count(*) from (select uid, msg from t7_xxhash order by uid) as subquery", `[[INT64(6)]]`)
		vitesst.AssertMatches(t, conn, "select count(*) from (select 1 union all select 2 union all select 3) as subquery", `[[INT64(3)]]`)

		vitesst.AssertMatches(t, conn, "select 1 union all select 2 union all select 3", `[[INT64(1)] [INT64(2)]]`)

		/*
			planner does not support query with order by in union query. without order by the results are not deterministic for testing purpose
			vitesst.AssertMatches(t, conn, "select uid, msg from t7_xxhash union all select uid, msg from t7_xxhash order by uid", ``)
			vitesst.AssertMatches(t, conn, "select uid, msg from t7_xxhash union all select uid, msg from t7_xxhash order by uid limit 3", ``)
		*/

		//	without order by the results are not deterministic for testing purpose. Checking row count only.
		qr := vitesst.Exec(t, conn, "select /*vt+ PLANNER=gen4 */ uid, msg from t7_xxhash union all select uid, msg from t7_xxhash")
		assert.Equal(t, 2, len(qr.Rows))

		qr = vitesst.Exec(t, conn, "select /*vt+ PLANNER=gen4 */ uid, msg from t7_xxhash union all select uid, msg from t7_xxhash limit 3")
		assert.Equal(t, 3, len(qr.Rows))
	}
}

func TestSQLSelectLimitWithPlanCache(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "insert into t7_xxhash(uid, msg) values(1, 'a'), (2, 'b'), (3, null)")

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
		vitesst.Exec(t, conn, "set workload = "+workload)
		for _, tcase := range tcases {
			vitesst.Exec(t, conn, fmt.Sprintf("set sql_select_limit = %d", tcase.limit))
			vitesst.AssertMatches(t, conn, "select uid, msg from t7_xxhash order by uid", tcase.out)
		}
	}
}

func TestSavepointInReservedConn(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "set session sql_mode = ''")
	vitesst.Exec(t, conn, "BEGIN")
	vitesst.Exec(t, conn, "SAVEPOINT sp_1")
	vitesst.Exec(t, conn, "insert into t7_xxhash(uid, msg) values(1, 'a')")
	vitesst.Exec(t, conn, "RELEASE SAVEPOINT sp_1")
	vitesst.Exec(t, conn, "ROLLBACK")

	vitesst.Exec(t, conn, "set session sql_mode = ''")
	vitesst.Exec(t, conn, "BEGIN")
	vitesst.Exec(t, conn, "SAVEPOINT sp_1")
	vitesst.Exec(t, conn, "RELEASE SAVEPOINT sp_1")
	vitesst.Exec(t, conn, "SAVEPOINT sp_2")
	vitesst.Exec(t, conn, "insert into t7_xxhash(uid, msg) values(2, 'a')")
	vitesst.Exec(t, conn, "RELEASE SAVEPOINT sp_2")
	vitesst.Exec(t, conn, "COMMIT")
	vitesst.AssertMatches(t, conn, "select uid from t7_xxhash", `[[VARCHAR("2")]]`)
}

func TestUnionWithManyInfSchemaQueries(t *testing.T) {
	setupCluster(t)
	// trying to reproduce the problems in https://github.com/vitessio/vitess/issues/9139
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, `SELECT /*vt+ PLANNER=gen4 */ 
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
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "set workload = olap")
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values (1,2)")
	vitesst.AssertMatches(t, conn, "select id1, id2 from t1", `[[INT64(1) INT64(2)]]`)
	vitesst.Exec(t, conn, "commit")
	vitesst.AssertMatches(t, conn, "select id1, id2 from t1", `[[INT64(1) INT64(2)]]`)

	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values (2,3)")
	vitesst.AssertMatches(t, conn, "select id1, id2 from t1 where id1 = 2", `[[INT64(2) INT64(3)]]`)
	vitesst.Exec(t, conn, "rollback")
	vitesst.AssertMatches(t, conn, "select id1, id2 from t1 where id1 = 2", `[]`)
}

func TestCharsetIntro(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "insert into t4 (id1,id2) values (666, _binary'abc')")
	vitesst.Exec(t, conn, "update t4 set id2 = _latin1'xyz' where id1 = 666")
	vitesst.Exec(t, conn, "delete from t4 where id2 = _utf8'xyz'")
	qr := vitesst.Exec(t, conn, "select id1 from t4 where id2 = _utf8mb4'xyz'")
	require.EqualValues(t, 0, qr.RowsAffected)
}

func TestFilterAfterLeftJoin(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "insert into t1 (id1,id2) values (1, 10)")
	vitesst.Exec(t, conn, "insert into t1 (id1,id2) values (2, 3)")
	vitesst.Exec(t, conn, "insert into t1 (id1,id2) values (3, 2)")

	query := "select /*vt+ PLANNER=gen4 */ A.id1, A.id2 from t1 as A left join t1 as B on A.id1 = B.id2 WHERE B.id1 IS NULL"
	vitesst.AssertMatches(t, conn, query, `[[INT64(1) INT64(10)]]`)
}

func TestFilterWithINAfterLeftJoin(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "insert into t1 (id1,id2) values (1, 10)")
	vitesst.Exec(t, conn, "insert into t1 (id1,id2) values (2, 3)")
	vitesst.Exec(t, conn, "insert into t1 (id1,id2) values (3, 2)")
	vitesst.Exec(t, conn, "insert into t1 (id1,id2) values (4, 5)")

	query := "select a.id1, b.id3 from t1 as a left outer join t2 as b on a.id2 = b.id4 WHERE a.id2 = 10 AND (b.id3 IS NULL OR b.id3 IN (1))"
	vitesst.AssertMatches(t, conn, query, `[[INT64(1) NULL]]`)

	vitesst.Exec(t, conn, "insert into t2 (id3,id4) values (1, 10)")

	query = "select a.id1, b.id3 from t1 as a left outer join t2 as b on a.id2 = b.id4 WHERE a.id2 = 10 AND (b.id3 IS NULL OR b.id3 IN (1))"
	vitesst.AssertMatches(t, conn, query, `[[INT64(1) INT64(1)]]`)

	query = "select a.id1, b.id3 from t1 as a left outer join t2 as b on a.id2 = b.id4 WHERE a.id2 = 10 AND (b.id3 IS NULL OR (b.id3, b.id4) IN ((1, 10)))"
	vitesst.AssertMatches(t, conn, query, `[[INT64(1) INT64(1)]]`)
}

func TestDescribeVindex(t *testing.T) {
	setupCluster(t)
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
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.AssertContainsError(t, conn, "", "Query was empty")
	vitesst.AssertContainsError(t, conn, ";", "Query was empty")
	vitesst.AssertIsEmpty(t, conn, "-- this is a comment")
}

// TestJoinWithMergedRouteWithPredicate checks the issue found in https://github.com/vitessio/vitess/issues/10713
func TestJoinWithMergedRouteWithPredicate(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	vitesst.Exec(t, conn, "insert into t1 (id1,id2) values (1, 13)")
	vitesst.Exec(t, conn, "insert into t2 (id3,id4) values (5, 10), (15, 20)")
	vitesst.Exec(t, conn, "insert into t3 (id5,id6,id7) values (13, 5, 8)")

	vitesst.AssertMatches(t, conn, "select t3.id7, t2.id3, t3.id6 from t1 join t3 on t1.id2 = t3.id5 join t2 on t3.id6 = t2.id3 where t1.id2 = 13", `[[INT64(8) INT64(5) INT64(5)]]`)
}

func TestRowCountExceed(t *testing.T) {
	setupCluster(t)
	conn, _ := start(t)
	defer func() {
		// needs special delete logic as it exceeds row count.
		for i := 50; i <= 300; i += 50 {
			vitesst.Exec(t, conn, fmt.Sprintf("delete from t1 where id1 < %d", i))
		}
		conn.Close()
	}()

	for i := range 250 {
		vitesst.Exec(t, conn, fmt.Sprintf("insert into t1 (id1, id2) values (%d, %d)", i, i+1))
	}

	vitesst.AssertContainsError(t, conn, "select id1 from t1 where id1 < 1000", `Row count exceeded 100`)
}

func TestDDLTargeted(t *testing.T) {
	setupCluster(t)
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	vitesst.Exec(t, conn, "use `ks/-80`")
	vitesst.Exec(t, conn, `begin`)
	vitesst.Exec(t, conn, `create table ddl_targeted (id bigint primary key)`)
	// implicit commit on ddl would have closed the open transaction
	// so this execution should happen as autocommit.
	vitesst.Exec(t, conn, `insert into ddl_targeted (id) values (1)`)
	// this will have not impact and the row would have inserted.
	vitesst.Exec(t, conn, `rollback`)
	// validating the row
	vitesst.AssertMatches(t, conn, `select id from ddl_targeted`, `[[INT64(1)]]`)
}

// TestTabletTargeting tests tablet-specific routing with USE keyspace:shard@tablet-alias syntax.
// When shard is specified, tablet-specific routing bypasses vindex-based shard resolution.
func TestTabletTargeting(t *testing.T) {
	setupCluster(t)
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	instances := make(map[string]map[string][]string)

	for _, shard := range clusterInstance.Keyspace("ks").Shards() {
		instances[shard.Name] = make(map[string][]string)
		for _, tablet := range shard.Tablets() {
			instances[shard.Name][tablet.Type()] = append(instances[shard.Name][tablet.Type()], tablet.Alias())
		}
	}

	require.NotEmpty(t, instances["-80"]["primary"][0], "no PRIMARY tablet found for -80 shard")
	require.NotEmpty(t, instances["80-"]["primary"][0], "no PRIMARY tablet found for 80- shard")
	require.NotEmpty(t, instances["-80"]["replica"], "no REPLICA tablets found for -80 shard")
	require.NotEmpty(t, instances["80-"]["replica"], "no REPLICA tablets found for 80- shard")

	// Insert data that would normally hash to 80- shard, but goes to -80 because of shard targeting
	useStmt := fmt.Sprintf("USE `ks:-80@primary|%s`", instances["-80"]["primary"][0])
	vitesst.Exec(t, conn, useStmt)
	vitesst.Exec(t, conn, "INSERT into t1(id1, id2) values(1, 100), (2, 200)")
	// id1=4 hashes to 80-, but we're targeting -80 shard explicitly
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(4, 400)")

	// Verify the data went to -80 shard (not where vindex would have put it)
	vitesst.Exec(t, conn, "USE `ks:-80`")
	vitesst.AssertMatches(t, conn, "select id1 from t1 where id1 in (1, 2, 4) order by id1", "[[INT64(1)] [INT64(2)] [INT64(4)]]")

	// Verify the data did NOT go to 80- shard (where vindex says id1=4 should be)
	vitesst.Exec(t, conn, "USE `ks:80-`")
	vitesst.AssertIsEmpty(t, conn, "select id1 from t1 where id1=4")

	// Transaction with tablet-specific routing maintains sticky connection
	useStmt = fmt.Sprintf("USE `ks:-80@primary|%s`", instances["-80"]["primary"][0])
	vitesst.Exec(t, conn, useStmt)
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(10, 300)")
	// Subsequent queries in transaction should go to same tablet
	vitesst.AssertMatches(t, conn, "select id1 from t1 where id1=10", "[[INT64(10)]]")
	vitesst.Exec(t, conn, "commit")
	vitesst.AssertMatches(t, conn, "select id1 from t1 where id1=10", "[[INT64(10)]]")

	// Rollback with tablet-specific routing
	useStmt = fmt.Sprintf("USE `ks:-80@primary|%s`", instances["-80"]["primary"][0])
	vitesst.Exec(t, conn, useStmt)
	vitesst.Exec(t, conn, "begin")
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(20, 500)")
	vitesst.Exec(t, conn, "rollback")
	vitesst.AssertIsEmpty(t, conn, "select id1 from t1 where id1=20")

	// Invalid tablet alias should fail
	useStmt = "USE `ks:-80@primary|nonexistent-tablet`"
	_, err = conn.ExecuteFetch(useStmt, 1, false)
	require.Error(t, err, "query should fail on invalid tablet")
	require.Contains(t, err.Error(), "invalid tablet alias in target")

	// Tablet alias without shard should fail
	useStmt = fmt.Sprintf("USE `ks@primary|%s`", instances["-80"]["primary"][0])
	_, err = conn.ExecuteFetch(useStmt, 1, false)
	require.Error(t, err, "tablet alias must be used with a shard")

	// Clear tablet targeting returns to normal routing
	// With normal routing, the query for id1=4 will be sent to the wrong shard (80-), so it won't be found.
	// This is expected and demonstrates that vindex routing is back in effect.
	vitesst.Exec(t, conn, "USE ks")
	vitesst.AssertMatches(t, conn, "select id1 from t1 where id1 in (1, 2, 4, 10) order by id1", "[[INT64(1)] [INT64(2)] [INT64(10)]]")

	// Targeting a specific REPLICA tablet allows reads but not writes
	replicaAlias := instances["-80"]["replica"][0]
	useStmt = fmt.Sprintf("USE `ks:-80@replica|%s`", replicaAlias)
	vitesst.Exec(t, conn, useStmt)

	// Reads should work on replica (wait for replication)
	require.Eventually(t, func() bool {
		result, err := conn.ExecuteFetch("select id1 from t1 where id1 in (1, 2, 4, 10) order by id1", 10, false)
		return err == nil && len(result.Rows) == 4
	}, 15*time.Second, 100*time.Millisecond, "replication did not catch up for first replica read")

	// Writes should fail on replica (replicas are read-only)
	_, err = conn.ExecuteFetch("insert into t1(id1, id2) values(99, 999)", 1, false)
	require.Error(t, err, "write should fail on replica tablet")
	require.Contains(t, err.Error(), "1290")

	// Targeting different REPLICA tablets in the same shard
	secondReplicaAlias := instances["-80"]["replica"][1]
	useStmt = fmt.Sprintf("USE `ks:-80@replica|%s`", secondReplicaAlias)
	vitesst.Exec(t, conn, useStmt)

	// Should still be able to read from this different replica (wait for replication)
	require.Eventually(t, func() bool {
		result, err := conn.ExecuteFetch("select id1 from t1 where id1 in (1, 2, 4, 10) order by id1", 10, false)
		return err == nil && len(result.Rows) == 4
	}, 15*time.Second, 100*time.Millisecond, "replication did not catch up for second replica read")

	// Writes should still fail
	_, err = conn.ExecuteFetch("insert into t1(id1, id2) values(98, 998)", 1, false)
	require.Error(t, err, "write should fail on replica tablet")
	require.Contains(t, err.Error(), "1290")

	// Write to primary, verify it replicates to replica
	// This tests that tablet-specific routing doesn't break replication
	useStmt = fmt.Sprintf("USE `ks:-80@primary|%s`", instances["-80"]["primary"][0])
	vitesst.Exec(t, conn, useStmt)
	vitesst.Exec(t, conn, "insert into t1(id1, id2) values(50, 5000)")
	vitesst.AssertMatches(t, conn, "select id1 from t1 where id1=50", "[[INT64(50)]]")

	// Switch to replica and verify the data replicated
	replicaAlias = instances["-80"]["replica"][0]
	useStmt = fmt.Sprintf("USE `ks:-80@replica|%s`", replicaAlias)
	vitesst.Exec(t, conn, useStmt)
	// Wait for replication to catch up
	require.Eventually(t, func() bool {
		result, err := conn.ExecuteFetch("select id1 from t1 where id1=50", 1, false)
		return err == nil && len(result.Rows) == 1
	}, 15*time.Second, 100*time.Millisecond, "replication did not catch up")

	// Query different replicas and verify different server UUIDs
	// This proves we're actually hitting different physical tablets

	// Get server UUID from first replica
	useStmt = fmt.Sprintf("USE `ks:-80@replica|%s`", instances["-80"]["replica"][0])
	vitesst.Exec(t, conn, useStmt)
	var uuid1 string
	for i := range 5 {
		result1 := vitesst.Exec(t, conn, "SELECT @@server_uuid")
		require.NotNil(t, result1)
		require.Greater(t, len(result1.Rows), 0)
		if i > 0 {
			// UUID should be the same across multiple queries to same tablet
			require.Equal(t, uuid1, result1.Rows[0][0].ToString())
		}
		uuid1 = result1.Rows[0][0].ToString()
	}

	// Get server UUID from second replica
	useStmt = fmt.Sprintf("USE `ks:-80@replica|%s`", instances["-80"]["replica"][1])
	vitesst.Exec(t, conn, useStmt)
	var uuid2 string
	for i := range 5 {
		result2 := vitesst.Exec(t, conn, "SELECT @@server_uuid")
		require.NotNil(t, result2)
		require.Greater(t, len(result2.Rows), 0)
		if i > 0 {
			// UUID should be the same across multiple queries to same tablet
			require.Equal(t, uuid2, result2.Rows[0][0].ToString())
		}
		uuid2 = result2.Rows[0][0].ToString()
	}

	// Server UUIDs should be different, proving we're targeting different tablets
	require.NotEqual(t, uuid1, uuid2, "different replicas should have different server UUIDs")
}

// vtgateConfiguration mirrors the keys the vtgate watched config file carries.
// discovery_legacy_replication_lag_algorithm has no omitempty, so it is
// always present, just as the running vtgate reports it.
type vtgateConfiguration struct {
	DiscoveryLowReplicationLag        string `json:"discovery_low_replication_lag,omitempty"`
	DiscoveryHighReplicationLag       string `json:"discovery_high_replication_lag,omitempty"`
	DiscoveryMinServingVttablets      string `json:"discovery_min_number_serving_vttablets,omitempty"`
	DiscoveryLegacyReplicationLagAlgo string `json:"discovery_legacy_replication_lag_algorithm"`
}

// TestDynamicConfig tests the dynamic configurations.
func TestDynamicConfig(t *testing.T) {
	setupCluster(t)
	var config vtgateConfiguration
	rewrite := func(t *testing.T) {
		body, err := json.Marshal(&config)
		require.NoError(t, err)
		require.NoError(t, clusterInstance.VTGate().WriteConfig(t.Context(), string(body)))
	}

	t.Run("DiscoveryLowReplicationLag", func(t *testing.T) {
		// Test initial config value
		waitForVtgateConfig(t, `"discovery_low_replication_lag":30000000000`)
		defer func() {
			// Restore default back.
			config.DiscoveryLowReplicationLag = "30s"
			rewrite(t)
		}()
		config.DiscoveryLowReplicationLag = "15s"
		rewrite(t)
		// Test final config value.
		waitForVtgateConfig(t, `"discovery_low_replication_lag":"15s"`)
	})

	t.Run("DiscoveryHighReplicationLag", func(t *testing.T) {
		// Test initial config value
		waitForVtgateConfig(t, `"discovery_high_replication_lag":7200000000000`)
		defer func() {
			// Restore default back.
			config.DiscoveryHighReplicationLag = "2h"
			rewrite(t)
		}()
		config.DiscoveryHighReplicationLag = "1h"
		rewrite(t)
		// Test final config value.
		waitForVtgateConfig(t, `"discovery_high_replication_lag":"1h"`)
	})

	t.Run("DiscoveryMinServingVttablets", func(t *testing.T) {
		// Test initial config value
		waitForVtgateConfig(t, `"discovery_min_number_serving_vttablets":2`)
		defer func() {
			// Restore default back.
			config.DiscoveryMinServingVttablets = "2"
			rewrite(t)
		}()
		config.DiscoveryMinServingVttablets = "1"
		rewrite(t)
		// Test final config value.
		waitForVtgateConfig(t, `"discovery_min_number_serving_vttablets":"1"`)
	})

	t.Run("DiscoveryLegacyReplicationLagAlgo", func(t *testing.T) {
		// Test initial config value
		waitForVtgateConfig(t, `"discovery_legacy_replication_lag_algorithm":""`)
		defer func() {
			// Restore default back.
			config.DiscoveryLegacyReplicationLagAlgo = "true"
			rewrite(t)
		}()
		config.DiscoveryLegacyReplicationLagAlgo = "false"
		rewrite(t)
		// Test final config value.
		waitForVtgateConfig(t, `"discovery_legacy_replication_lag_algorithm":"false"`)
	})
}

// waitForVtgateConfig polls the vtgate's /debug/config until the expected
// substring appears in the served configuration.
func waitForVtgateConfig(t *testing.T, expected string) {
	t.Helper()
	_, _, err := clusterInstance.VTGate().MakeAPICallRetry(t.Context(), "/debug/config", 30*time.Second, func(status int, body string) bool {
		return status == 200 && strings.Contains(body, expected)
	})
	require.NoError(t, err)
}

func TestLookupErrorMetric(t *testing.T) {
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	oldErrCount := getVtgateApiErrorCounts(t)

	vitesst.Exec(t, conn, `insert into t1 values (1,1)`)
	_, err := vitesst.ExecAllowError(t, conn, `insert into t1 values (2,1)`)
	require.ErrorContains(t, err, `(errno 1062) (sqlstate 23000)`)

	newErrCount := getVtgateApiErrorCounts(t)
	require.EqualValues(t, oldErrCount+1, newErrCount)
}

func getVtgateApiErrorCounts(t *testing.T) float64 {
	apiErr := getVar(t, "VtgateApiErrorCounts")
	if apiErr == nil {
		return 0
	}
	mapErrors := apiErr.(map[string]any)
	val, exists := mapErrors["Execute.ks.primary.ALREADY_EXISTS"]
	if exists {
		return val.(float64)
	}
	return 0
}

func getVar(t *testing.T, key string) any {
	vars, err := clusterInstance.VTGate().GetVars(t.Context())
	require.NoError(t, err)
	require.NotNil(t, vars)

	val, exists := vars[key]
	if !exists {
		return nil
	}
	return val
}

// TestQueryProcessedMetric verifies that query metrics are correctly published.
func TestQueryProcessedMetric(t *testing.T) {
	setupCluster(t)
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
			vitesst.Exec(t, conn, tc.sql)
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
	setupCluster(t)
	conn, closer := start(t)
	defer closer()

	initialQP := getQPMetric(t, "QueryExecutions")
	initialQT := getQPMetric(t, "QueryExecutionsByTable")
	t.Run("explain t1", func(t *testing.T) {
		vitesst.Exec(t, conn, "explain t1")
		updatedQP := getQPMetric(t, "QueryExecutions")
		updatedQT := getQPMetric(t, "QueryExecutionsByTable")
		assert.EqualValuesf(t, 1, getValue(updatedQP, "EXPLAIN.Passthrough.PRIMARY")-getValue(initialQP, "EXPLAIN.Passthrough.PRIMARY"), "queryExecutions metric: %s", "explain")
		assert.EqualValuesf(t, 1, getValue(updatedQT, "EXPLAIN.ks_t1")-getValue(initialQT, "EXPLAIN.ks_t1"), "queryExecutionsByTable metric: %s", "asdasd")
	})

	t.Run("explain `select id1, id2 from t1`", func(t *testing.T) {
		vitesst.ExecAllowError(t, conn, "explain `select id1, id2 from t1`")
		updatedQP := getQPMetric(t, "QueryExecutions")
		updatedQT := getQPMetric(t, "QueryExecutionsByTable")
		assert.EqualValuesf(t, 1, getValue(updatedQP, "EXPLAIN.Passthrough.PRIMARY")-getValue(initialQP, "EXPLAIN.Passthrough.PRIMARY"), "queryExecutions metric: %s", "explain")
		assert.EqualValuesf(t, 1, getValue(updatedQT, "EXPLAIN.ks_t1")-getValue(initialQT, "EXPLAIN.ks_t1"), "queryExecutionsByTable metric: %s", "asdasd")
	})

	t.Run("explain select id1, id2 from t1", func(t *testing.T) {
		vitesst.Exec(t, conn, "explain select id1, id2 from t1")
		updatedQP := getQPMetric(t, "QueryExecutions")
		updatedQT := getQPMetric(t, "QueryExecutionsByTable")
		assert.EqualValuesf(t, 2, getValue(updatedQP, "EXPLAIN.Passthrough.PRIMARY")-getValue(initialQP, "EXPLAIN.Passthrough.PRIMARY"), "queryExecutions metric: %s", "explain")
		assert.EqualValuesf(t, 2, getValue(updatedQT, "EXPLAIN.ks_t1")-getValue(initialQT, "EXPLAIN.ks_t1"), "queryExecutionsByTable metric: %s", "asdasd")
	})
}

func getQPMetric(t *testing.T, metric string) map[string]any {
	t.Helper()

	vars, err := clusterInstance.VTGate().GetVars(t.Context())
	require.NoError(t, err)
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
