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

	"vitess.io/vitess/go/test/utils"

	"github.com/google/go-cmp/cmp"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestSelectNull(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	exec(t, conn, "begin")
	exec(t, conn, "insert into t5_null_vindex(id, idx) values(1, 'a'), (2, 'b'), (3, null)")
	exec(t, conn, "commit")

	assertMatches(t, conn, "select id, idx from t5_null_vindex order by id", "[[INT64(1) VARCHAR(\"a\")] [INT64(2) VARCHAR(\"b\")] [INT64(3) NULL]]")
	assertIsEmpty(t, conn, "select id, idx from t5_null_vindex where idx = null")
	assertMatches(t, conn, "select id, idx from t5_null_vindex where idx is null", "[[INT64(3) NULL]]")
	assertMatches(t, conn, "select id, idx from t5_null_vindex where idx is not null order by id", "[[INT64(1) VARCHAR(\"a\")] [INT64(2) VARCHAR(\"b\")]]")
	assertIsEmpty(t, conn, "select id, idx from t5_null_vindex where id IN (null)")
	assertMatches(t, conn, "select id, idx from t5_null_vindex where id IN (1,2,null) order by id", "[[INT64(1) VARCHAR(\"a\")] [INT64(2) VARCHAR(\"b\")]]")
	assertIsEmpty(t, conn, "select id, idx from t5_null_vindex where id NOT IN (1,null) order by id")
	assertMatches(t, conn, "select id, idx from t5_null_vindex where id NOT IN (1,3)", "[[INT64(2) VARCHAR(\"b\")]]")

	exec(t, conn, "delete from t5_null_vindex")
}

func TestDoStatement(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	exec(t, conn, "do 1")
	exec(t, conn, "do 'a', 1+2,database()")
}

func TestShowColumns(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	expected := `[[VARCHAR("id") TEXT("bigint(20)") VARCHAR("NO") VARCHAR("PRI") NULL VARCHAR("")] [VARCHAR("idx") TEXT("varchar(50)") VARCHAR("YES") VARCHAR("") NULL VARCHAR("")]]`
	assertMatches(t, conn, "show columns from `t5_null_vindex` in `ks`", expected)
	assertMatches(t, conn, "SHOW COLUMNS from `t5_null_vindex` in `ks`", expected)
	assertMatches(t, conn, "SHOW columns FROM `t5_null_vindex` in `ks`", expected)
}

func TestShowTables(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	query := "show tables;"
	qr := exec(t, conn, query)

	assert.Equal(t, "information_schema", qr.Fields[0].Database)
	assert.Equal(t, "Tables_in_ks", qr.Fields[0].Name)
}

func TestCastConvert(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	assertMatches(t, conn, `SELECT CAST("test" AS CHAR(60))`, `[[VARCHAR("test")]]`)
}

func TestCompositeIN(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// clean up before & after
	exec(t, conn, "delete from t1")
	defer exec(t, conn, "delete from t1")

	exec(t, conn, "insert into t1(id1, id2) values(1, 2), (4, 5)")

	// Just check for correct results. Plan generation is tested in unit tests.
	assertMatches(t, conn, "select id1 from t1 where (id1, id2) in ((1, 2))", "[[INT64(1)]]")
}

func TestUnionAll(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// clean up before & after
	exec(t, conn, "delete from t1")
	exec(t, conn, "delete from t2")
	defer exec(t, conn, "delete from t1")
	defer exec(t, conn, "delete from t2")

	exec(t, conn, "insert into t1(id1, id2) values(1, 1), (2, 2)")
	exec(t, conn, "insert into t2(id3, id4) values(3, 3), (4, 4)")

	// union all between two selectuniqueequal
	assertMatches(t, conn, "select id1 from t1 where id1 = 1 union all select id1 from t1 where id1 = 4", "[[INT64(1)]]")

	// union all between two different tables
	assertMatches(t, conn, "(select id1,id2 from t1 order by id1) union all (select id3,id4 from t2 order by id3)",
		"[[INT64(1) INT64(1)] [INT64(2) INT64(2)] [INT64(3) INT64(3)] [INT64(4) INT64(4)]]")

	// union all between two different tables
	result := exec(t, conn, "(select id1,id2 from t1) union all (select id3,id4 from t2)")
	assert.Equal(t, 4, len(result.Rows))

	// union all between two different tables
	assertMatches(t, conn, "select tbl2.id1 FROM  ((select id1 from t1 order by id1 limit 5) union all (select id1 from t1 order by id1 desc limit 5)) as tbl1 INNER JOIN t1 as tbl2  ON tbl1.id1 = tbl2.id1",
		"[[INT64(1)] [INT64(2)] [INT64(2)] [INT64(1)]]")

	exec(t, conn, "insert into t1(id1, id2) values(3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8)")

	// union all between two selectuniquein tables
	assertMatchesNoOrder(t, conn, "select id1 from t1 where id1 in (1, 2, 3, 4, 5, 6, 7, 8) union all select id1 from t1 where id1 in (1, 2, 3, 4, 5, 6, 7, 8)",
		"[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(5)] [INT64(4)] [INT64(6)] [INT64(7)] [INT64(8)] [INT64(1)] [INT64(2)] [INT64(3)] [INT64(5)] [INT64(4)] [INT64(6)] [INT64(7)] [INT64(8)]]")
}

func TestUnionDistinct(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// clean up before & after
	exec(t, conn, "delete from t1")
	exec(t, conn, "delete from t2")
	defer func() {
		exec(t, conn, "set workload = oltp")
		exec(t, conn, "delete from t1")
		exec(t, conn, "delete from t2")
	}()

	exec(t, conn, "insert into t1(id1, id2) values (1, 1), (2, 2), (3,3), (4,4)")
	exec(t, conn, "insert into t2(id3, id4) values (2, 3), (3, 4), (4,4), (5,5)")

	for _, workload := range []string{"oltp", "olap"} {
		t.Run(workload, func(t *testing.T) {
			exec(t, conn, "set workload = "+workload)
			assertMatches(t, conn, "select 1 union select null", "[[INT64(1)] [NULL]]")
			assertMatches(t, conn, "select null union select null", "[[NULL]]")
			assertMatches(t, conn, "select * from (select 1 as col union select 2) as t", "[[INT64(1)] [INT64(2)]]")
			assertMatches(t, conn, "select 1 from dual where 1 IN (select 1 as col union select 2)", "[[INT64(1)]]")

			// test with real data coming from mysql
			assertMatches(t, conn, "select id1 from t1 where id1 = 1 union select id1 from t1 where id1 = 5", "[[INT64(1)]]")
			assertMatchesNoOrder(t, conn, "select id1 from t1 where id1 = 1 union select id1 from t1 where id1 = 4", "[[INT64(1)] [INT64(4)]]")
			assertMatchesNoOrder(t, conn, "select id1 from t1 where id1 = 1 union select 452 union select id1 from t1 where id1 = 4", "[[INT64(1)] [INT64(452)] [INT64(4)]]")
			assertMatchesNoOrder(t, conn, "select id1, id2 from t1 union select 827, 452 union select id3,id4 from t2",
				"[[INT64(4) INT64(4)] [INT64(1) INT64(1)] [INT64(2) INT64(2)] [INT64(3) INT64(3)] [INT64(827) INT64(452)] [INT64(2) INT64(3)] [INT64(3) INT64(4)] [INT64(5) INT64(5)]]")
		})
	}
}

func TestUnionAllOlap(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// clean up before & after
	exec(t, conn, "delete from t1")
	exec(t, conn, "delete from t2")
	defer func() {
		exec(t, conn, "set workload = oltp")
		exec(t, conn, "delete from t1")
		exec(t, conn, "delete from t2")
	}()

	exec(t, conn, "insert into t1(id1, id2) values(1, 1), (2, 2)")
	exec(t, conn, "insert into t2(id3, id4) values(3, 3), (4, 4)")

	exec(t, conn, "set workload = olap")

	// union all between two selectuniqueequal
	assertMatches(t, conn, "select id1 from t1 where id1 = 1 union all select id1 from t1 where id1 = 4", "[[INT64(1)]]")

	// union all between two different tables
	// union all between two different tables
	result := exec(t, conn, "(select id1,id2 from t1 order by id1) union all (select id3,id4 from t2 order by id3)")
	assert.Equal(t, 4, len(result.Rows))

	// union all between two different tables
	result = exec(t, conn, "(select id1,id2 from t1) union all (select id3,id4 from t2)")
	assert.Equal(t, 4, len(result.Rows))

	// union all between two different tables
	result = exec(t, conn, "select tbl2.id1 FROM ((select id1 from t1 order by id1 limit 5) union all (select id1 from t1 order by id1 desc limit 5)) as tbl1 INNER JOIN t1 as tbl2  ON tbl1.id1 = tbl2.id1")
	assert.Equal(t, 4, len(result.Rows))

	exec(t, conn, "set workload = oltp")
	exec(t, conn, "insert into t1(id1, id2) values(3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8)")
	exec(t, conn, "set workload = olap")

	// union all between two selectuniquein tables
	assertMatchesNoOrder(t, conn, "select id1 from t1 where id1 in (1, 2, 3, 4, 5, 6, 7, 8) union all select id1 from t1 where id1 in (1, 2, 3, 4, 5, 6, 7, 8)",
		"[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(5)] [INT64(4)] [INT64(6)] [INT64(7)] [INT64(8)] [INT64(1)] [INT64(2)] [INT64(3)] [INT64(5)] [INT64(4)] [INT64(6)] [INT64(7)] [INT64(8)]]")

}

func TestUnion(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	assertMatches(t, conn, `SELECT 1 UNION SELECT 1 UNION SELECT 1`, `[[INT64(1)]]`)
	assertMatches(t, conn, `SELECT 1,'a' UNION SELECT 1,'a' UNION SELECT 1,'a' ORDER BY 1`, `[[INT64(1) VARCHAR("a")]]`)
	assertMatches(t, conn, `SELECT 1,'z' UNION SELECT 2,'q' UNION SELECT 3,'b' ORDER BY 2`, `[[INT64(3) VARCHAR("b")] [INT64(2) VARCHAR("q")] [INT64(1) VARCHAR("z")]]`)
	assertMatches(t, conn, `SELECT 1,'a' UNION ALL SELECT 1,'a' UNION ALL SELECT 1,'a' ORDER BY 1`, `[[INT64(1) VARCHAR("a")] [INT64(1) VARCHAR("a")] [INT64(1) VARCHAR("a")]]`)
	assertMatches(t, conn, `(SELECT 1,'a') UNION ALL (SELECT 1,'a') UNION ALL (SELECT 1,'a') ORDER BY 1`, `[[INT64(1) VARCHAR("a")] [INT64(1) VARCHAR("a")] [INT64(1) VARCHAR("a")]]`)
	assertMatches(t, conn, `(SELECT 1,'a') ORDER BY 1`, `[[INT64(1) VARCHAR("a")]]`)
	assertMatches(t, conn, `(SELECT 1,'a' order by 1) union (SELECT 1,'a' ORDER BY 1)`, `[[INT64(1) VARCHAR("a")]]`)
}

func TestSavepointInTx(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	exec(t, conn, "savepoint a")
	exec(t, conn, "start transaction")
	exec(t, conn, "savepoint b")
	exec(t, conn, "rollback to b")
	exec(t, conn, "release savepoint b")
	exec(t, conn, "savepoint b")
	exec(t, conn, "insert into t1(id1, id2) values(1,1)") // -80
	exec(t, conn, "savepoint c")
	exec(t, conn, "insert into t1(id1, id2) values(4,4)") // 80-
	exec(t, conn, "savepoint d")
	exec(t, conn, "insert into t1(id1, id2) values(2,2)") // -80
	exec(t, conn, "savepoint e")

	// Validate all the data.
	exec(t, conn, "use `ks:-80`")
	assertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)] [INT64(2)]]`)
	exec(t, conn, "use `ks:80-`")
	assertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(4)]]`)
	exec(t, conn, "use")
	assertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)] [INT64(2)] [INT64(4)]]`)

	_, err = conn.ExecuteFetch("rollback work to savepoint a", 1000, true)
	require.Error(t, err)

	exec(t, conn, "release savepoint d")

	_, err = conn.ExecuteFetch("rollback to d", 1000, true)
	require.Error(t, err)
	assertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)] [INT64(2)] [INT64(4)]]`)

	exec(t, conn, "rollback to c")
	assertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)]]`)

	exec(t, conn, "insert into t1(id1, id2) values(2,2),(3,3),(4,4)")
	assertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)]]`)

	exec(t, conn, "rollback to b")
	assertMatches(t, conn, "select id1 from t1 order by id1", `[]`)

	exec(t, conn, "commit")
	assertMatches(t, conn, "select id1 from t1 order by id1", `[]`)

	exec(t, conn, "start transaction")

	exec(t, conn, "insert into t1(id1, id2) values(2,2),(3,3),(4,4)")
	assertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(2)] [INT64(3)] [INT64(4)]]`)

	// After previous commit all the savepoints are cleared.
	_, err = conn.ExecuteFetch("rollback to b", 1000, true)
	require.Error(t, err)

	exec(t, conn, "rollback")
	assertMatches(t, conn, "select id1 from t1 order by id1", `[]`)
}

func TestSavepointOutsideTx(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	exec(t, conn, "savepoint a")
	exec(t, conn, "savepoint b")

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

	exec(t, conn, "start transaction")
	exec(t, conn, "savepoint a")
	exec(t, conn, "insert into t1(id1, id2) values(1,1)")             // -80
	exec(t, conn, "insert into t1(id1, id2) values(2,2),(3,3),(4,4)") // -80 & 80-
	assertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)]]`)

	exec(t, conn, "rollback to a")
	assertMatches(t, conn, "select id1 from t1 order by id1", `[]`)

	exec(t, conn, "commit")
	assertMatches(t, conn, "select id1 from t1 order by id1", `[]`)

	exec(t, conn, "start transaction")
	exec(t, conn, "insert into t1(id1, id2) values(1,1)") // -80
	exec(t, conn, "savepoint a")
	exec(t, conn, "insert into t1(id1, id2) values(2,2),(3,3)") // -80
	exec(t, conn, "insert into t1(id1, id2) values(4,4)")       // 80-
	assertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)]]`)

	exec(t, conn, "rollback to a")
	assertMatches(t, conn, "select id1 from t1 order by id1", `[[INT64(1)]]`)

	exec(t, conn, "rollback")
	assertMatches(t, conn, "select id1 from t1 order by id1", `[]`)
}

func TestExplainPassthrough(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	result := exec(t, conn, "explain select * from t1")
	got := fmt.Sprintf("%v", result.Rows)
	require.Contains(t, got, "SIMPLE") // there is a lot more coming from mysql,
	// but we are trying to make the test less fragile

	result = exec(t, conn, "explain ks.t1")
	require.EqualValues(t, 2, len(result.Rows))
}

func TestXXHash(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	exec(t, conn, "insert into t7_xxhash(uid, phone, msg) values('u-1', 1, 'message')")
	assertMatches(t, conn, "select uid, phone, msg from t7_xxhash where phone = 1", `[[VARCHAR("u-1") INT64(1) VARCHAR("message")]]`)
	assertMatches(t, conn, "select phone, keyspace_id from t7_xxhash_idx", `[[INT64(1) VARBINARY("\x1cU^f\xbfyE^")]]`)
	exec(t, conn, "update t7_xxhash set phone = 2 where uid = 'u-1'")
	assertMatches(t, conn, "select uid, phone, msg from t7_xxhash where phone = 1", `[]`)
	assertMatches(t, conn, "select uid, phone, msg from t7_xxhash where phone = 2", `[[VARCHAR("u-1") INT64(2) VARCHAR("message")]]`)
	assertMatches(t, conn, "select phone, keyspace_id from t7_xxhash_idx", `[[INT64(2) VARBINARY("\x1cU^f\xbfyE^")]]`)
	exec(t, conn, "delete from t7_xxhash where uid = 'u-1'")
	assertMatches(t, conn, "select uid, phone, msg from t7_xxhash where uid = 'u-1'", `[]`)
	assertMatches(t, conn, "select phone, keyspace_id from t7_xxhash_idx", `[]`)
}

func TestShowTablesWithWhereClause(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	assertMatches(t, conn, "show tables from ks where Tables_in_ks='t6'", `[[VARCHAR("t6")]]`)
	exec(t, conn, "begin")
	assertMatches(t, conn, "show tables from ks where Tables_in_ks='t3'", `[[VARCHAR("t3")]]`)
}

func TestOffsetAndLimitWithOLAP(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	defer exec(t, conn, "set workload=oltp;delete from t1")

	exec(t, conn, "insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
	assertMatches(t, conn, "select id1 from t1 order by id1 limit 3 offset 2", "[[INT64(3)] [INT64(4)] [INT64(5)]]")
	exec(t, conn, "set workload='olap'")
	assertMatches(t, conn, "select id1 from t1 order by id1 limit 3 offset 2", "[[INT64(3)] [INT64(4)] [INT64(5)]]")
}

func TestSwitchBetweenOlapAndOltp(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	assertMatches(t, conn, "select @@workload", `[[VARBINARY("OLTP")]]`)

	exec(t, conn, "set workload='olap'")

	assertMatches(t, conn, "select @@workload", `[[VARBINARY("OLAP")]]`)

	exec(t, conn, "set workload='oltp'")

	assertMatches(t, conn, "select @@workload", `[[VARBINARY("OLTP")]]`)
}

func TestFoundRowsOnDualQueries(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	exec(t, conn, "select 42")
	assertMatches(t, conn, "select found_rows()", "[[UINT64(1)]]")
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
			exec(t, conn, q)
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

	exec(t, conn, `set workload='olap'`)
	_, err = conn.ExecuteFetch(`insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)`, 1000, true)
	require.Error(t, err)
	assertMatches(t, conn, `select id1 from t1 order by id1`, `[]`)
}

func TestDistinct(t *testing.T) {
	defer cluster.PanicHandler(t)

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()
	exec(t, conn, "insert into t3(id5,id6,id7) values(1,3,3), (2,3,4), (3,3,6), (4,5,7), (5,5,6)")
	exec(t, conn, "insert into t7_xxhash(uid,phone) values('1',4), ('2',4), ('3',3), ('4',1), ('5',1)")
	exec(t, conn, "insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'A',1), (3,'b',1), (4,'c',3), (5,'c',4)")
	exec(t, conn, "insert into aggr_test(id, val1, val2) values(6,'d',null), (7,'e',null), (8,'E',1)")
	assertMatches(t, conn, "select distinct val2, count(*) from aggr_test group by val2", `[[NULL INT64(2)] [INT64(1) INT64(4)] [INT64(3) INT64(1)] [INT64(4) INT64(1)]]`)
	assertMatches(t, conn, "select distinct id6 from t3 join t7_xxhash on t3.id5 = t7_xxhash.phone", `[[INT64(3)] [INT64(5)]]`)
	exec(t, conn, "delete from t3")
	exec(t, conn, "delete from t7_xxhash")
	exec(t, conn, "delete from aggr_test")
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
	defer exec(t, conn, `delete from t1`)
	// Test that create view works and the output is as expected
	exec(t, conn, `create view v1 as select * from t1`)
	exec(t, conn, `insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)`)
	// This wont work, since ALTER VSCHEMA ADD TABLE is only supported for unsharded keyspaces
	exec(t, conn, "alter vschema add table v1")
	assertMatches(t, conn, "select * from v1", `[[INT64(1) INT64(1)] [INT64(2) INT64(2)] [INT64(3) INT64(3)] [INT64(4) INT64(4)] [INT64(5) INT64(5)]]`)
}

func TestVersions(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	qr := exec(t, conn, `select @@version`)
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), "vitess")

	qr = exec(t, conn, `select @@version_comment`)
	assert.Contains(t, fmt.Sprintf("%v", qr.Rows), "Git revision")
}

func TestFlush(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	exec(t, conn, "flush local tables t1, t2")
}

func TestShowVariables(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	res := exec(t, conn, "show variables like \"%version%\";")
	found := false
	for _, row := range res.Rows {
		if row[0].ToString() == "version" {
			assert.Contains(t, row[1].ToString(), "vitess")
			found = true
		}
	}
	require.True(t, found, "Expected a row for version in show query")
}

func TestOrderBy(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()
	exec(t, conn, "insert into t4(id1, id2) values(1,'a'), (2,'Abc'), (3,'b'), (4,'c'), (5,'test')")
	exec(t, conn, "insert into t4(id1, id2) values(6,'d'), (7,'e'), (8,'F')")
	// test ordering of varchar column
	assertMatches(t, conn, "select id1, id2 from t4 order by id2 desc", `[[INT64(5) VARCHAR("test")] [INT64(8) VARCHAR("F")] [INT64(7) VARCHAR("e")] [INT64(6) VARCHAR("d")] [INT64(4) VARCHAR("c")] [INT64(3) VARCHAR("b")] [INT64(2) VARCHAR("Abc")] [INT64(1) VARCHAR("a")]]`)
	// test ordering of int column
	assertMatches(t, conn, "select id1, id2 from t4 order by id1 desc", `[[INT64(8) VARCHAR("F")] [INT64(7) VARCHAR("e")] [INT64(6) VARCHAR("d")] [INT64(5) VARCHAR("test")] [INT64(4) VARCHAR("c")] [INT64(3) VARCHAR("b")] [INT64(2) VARCHAR("Abc")] [INT64(1) VARCHAR("a")]]`)

	defer func() {
		exec(t, conn, "set workload = oltp")
		exec(t, conn, "delete from t4")
	}()
	// Test the same queries in streaming mode
	exec(t, conn, "set workload = olap")
	assertMatches(t, conn, "select id1, id2 from t4 order by id2 desc", `[[INT64(5) VARCHAR("test")] [INT64(8) VARCHAR("F")] [INT64(7) VARCHAR("e")] [INT64(6) VARCHAR("d")] [INT64(4) VARCHAR("c")] [INT64(3) VARCHAR("b")] [INT64(2) VARCHAR("Abc")] [INT64(1) VARCHAR("a")]]`)
	assertMatches(t, conn, "select id1, id2 from t4 order by id1 desc", `[[INT64(8) VARCHAR("F")] [INT64(7) VARCHAR("e")] [INT64(6) VARCHAR("d")] [INT64(5) VARCHAR("test")] [INT64(4) VARCHAR("c")] [INT64(3) VARCHAR("b")] [INT64(2) VARCHAR("Abc")] [INT64(1) VARCHAR("a")]]`)
}

func TestSubQueryOnTopOfSubQuery(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	defer exec(t, conn, `delete from t1`)

	exec(t, conn, `insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)`)
	exec(t, conn, `insert into t2(id3, id4) values (1, 3), (2, 4)`)

	assertMatches(t, conn, "select id1 from t1 where id1 not in (select id3 from t2) and id2 in (select id4 from t2) order by id1", `[[INT64(3)] [INT64(4)]]`)
}

func assertMatches(t *testing.T, conn *mysql.Conn, query, expected string) {
	t.Helper()
	qr := exec(t, conn, query)
	got := fmt.Sprintf("%v", qr.Rows)
	diff := cmp.Diff(expected, got)
	if diff != "" {
		t.Errorf("Query: %s (-want +got):\n%s", query, diff)
	}
}
func assertMatchesNoOrder(t *testing.T, conn *mysql.Conn, query, expected string) {
	t.Helper()
	qr := exec(t, conn, query)
	actual := fmt.Sprintf("%v", qr.Rows)
	assert.Equal(t, utils.SortString(expected), utils.SortString(actual), "for query: [%s] expected \n%s \nbut actual \n%s", query, expected, actual)
}

func assertIsEmpty(t *testing.T, conn *mysql.Conn, query string) {
	t.Helper()
	qr := exec(t, conn, query)
	assert.Empty(t, qr.Rows, "for query: "+query)
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err, "for query: "+query)
	return qr
}
