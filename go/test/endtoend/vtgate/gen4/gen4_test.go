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

package vtgate

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

func TestOrderBy(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// insert some data.
	utils.Exec(t, mcmp.VtConn, `insert into t1(id, col) values (100, 123),(10, 12),(1, 13),(1000, 1234)`)

	// Gen4 only supported query.
	utils.AssertMatches(t, mcmp.VtConn, `select col from t1 order by id`, `[[INT64(13)] [INT64(12)] [INT64(123)] [INT64(1234)]]`)

	// Gen4 unsupported query. v3 supported.
	utils.AssertMatches(t, mcmp.VtConn, `select col from t1 order by 1`, `[[INT64(12)] [INT64(13)] [INT64(123)] [INT64(1234)]]`)
}

func TestCorrelatedExistsSubquery(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// insert some data.
	utils.Exec(t, mcmp.VtConn, `insert into t1(id, col) values (100, 123), (10, 12), (1, 13), (4, 13), (1000, 1234)`)
	utils.Exec(t, mcmp.VtConn, `insert into t2(id, tcol1, tcol2) values (100, 13, 1),(9, 7, 15),(1, 123, 123),(1004, 134, 123)`)

	utils.AssertMatches(t, mcmp.VtConn, `select id from t1 where exists(select 1 from t2 where t1.col = t2.tcol2)`,
		`[[INT64(100)]]`)
	utils.AssertMatches(t, mcmp.VtConn, `select id from t1 where exists(select 1 from t2 where t1.col = t2.tcol1) order by id`,
		`[[INT64(1)] [INT64(4)] [INT64(100)]]`)
	utils.AssertMatches(t, mcmp.VtConn, `select id from t1 where id in (select id from t2) order by id`,
		`[[INT64(1)] [INT64(100)]]`)

	utils.AssertMatches(t, mcmp.VtConn, `
select id 
from t1 
where exists(
	select t2.id, count(*) 
	from t2 
	where t1.col = t2.tcol2
    having count(*) > 0
)`,
		`[[INT64(100)]]`)
	utils.AssertMatches(t, mcmp.VtConn, `
select id 
from t1 
where exists(
	select t2.id, count(*) 
	from t2 
	where t1.col = t2.tcol1
) order by id`,
		`[[INT64(1)] [INT64(4)] [INT64(100)]]`)
	utils.AssertMatchesNoOrder(t, mcmp.VtConn, `
select id 
from t1 
where exists(
	select count(*) 
	from t2 
	where t1.col = t2.tcol1
) order by id`,
		`[[INT64(1)] [INT64(4)] [INT64(100)] [INT64(1000)] [INT64(10)]]`)
}

func TestGroupBy(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// insert some data.
	utils.Exec(t, mcmp.VtConn, `insert into t1(id, col) values (1, 123),(2, 12),(3, 13),(4, 1234)`)
	utils.Exec(t, mcmp.VtConn, `insert into t2(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'B')`)

	// Gen4 only supported query.
	utils.AssertMatches(t, mcmp.VtConn, `select tcol2, tcol1, count(id) from t2 group by tcol2, tcol1`,
		`[[VARCHAR("A") VARCHAR("A") INT64(2)] [VARCHAR("A") VARCHAR("B") INT64(1)] [VARCHAR("A") VARCHAR("C") INT64(1)] [VARCHAR("B") VARCHAR("C") INT64(1)] [VARCHAR("C") VARCHAR("A") INT64(1)] [VARCHAR("C") VARCHAR("B") INT64(2)]]`)

	utils.AssertMatches(t, mcmp.VtConn, `select tcol1, tcol1 from t2 order by tcol1`,
		`[[VARCHAR("A") VARCHAR("A")] [VARCHAR("A") VARCHAR("A")] [VARCHAR("A") VARCHAR("A")] [VARCHAR("B") VARCHAR("B")] [VARCHAR("B") VARCHAR("B")] [VARCHAR("B") VARCHAR("B")] [VARCHAR("C") VARCHAR("C")] [VARCHAR("C") VARCHAR("C")]]`)

	utils.AssertMatches(t, mcmp.VtConn, `select tcol1, tcol1 from t1 join t2 on t1.id = t2.id order by tcol1`,
		`[[VARCHAR("A") VARCHAR("A")] [VARCHAR("A") VARCHAR("A")] [VARCHAR("B") VARCHAR("B")] [VARCHAR("C") VARCHAR("C")]]`)

	utils.AssertMatches(t, mcmp.VtConn, `select count(*) k, tcol1, tcol2, "abc" b from t2 group by tcol1, tcol2, b order by k, tcol2, tcol1`,
		`[[INT64(1) VARCHAR("B") VARCHAR("A") VARCHAR("abc")] `+
			`[INT64(1) VARCHAR("C") VARCHAR("A") VARCHAR("abc")] `+
			`[INT64(1) VARCHAR("C") VARCHAR("B") VARCHAR("abc")] `+
			`[INT64(1) VARCHAR("A") VARCHAR("C") VARCHAR("abc")] `+
			`[INT64(2) VARCHAR("A") VARCHAR("A") VARCHAR("abc")] `+
			`[INT64(2) VARCHAR("B") VARCHAR("C") VARCHAR("abc")]]`)
}

func TestJoinBindVars(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	utils.Exec(t, mcmp.VtConn, `insert into t2(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'B')`)
	utils.Exec(t, mcmp.VtConn, `insert into t3(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'B')`)

	utils.AssertMatches(t, mcmp.VtConn, `select t2.tcol1 from t2 join t3 on t2.tcol2 = t3.tcol2 where t2.tcol1 = 'A'`, `[[VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")]]`)
}

func TestDistinctAggregationFunc(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// insert some data.
	mcmp.Exec(`insert into t2(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'A')`)

	// count on primary vindex
	mcmp.Exec(`select tcol1, count(distinct id) from t2 group by tcol1`)

	// count on any column
	mcmp.Exec(`select tcol1, count(distinct tcol2) from t2 group by tcol1`)

	// sum of columns
	mcmp.Exec(`select sum(id), sum(tcol1) from t2`)

	// sum on primary vindex
	mcmp.Exec(`select tcol1, sum(distinct id) from t2 group by tcol1`)

	// sum on any column
	mcmp.Exec(`select tcol1, sum(distinct tcol2) from t2 group by tcol1`)

	// insert more data to get values on sum
	mcmp.Exec(`insert into t2(id, tcol1, tcol2) values (9, 'AA', null),(10, 'AA', '4'),(11, 'AA', '4'),(12, null, '5'),(13, null, '6'),(14, 'BB', '10'),(15, 'BB', '20'),(16, 'BB', 'X')`)

	// multi distinct
	mcmp.Exec(`select tcol1, count(distinct tcol2), sum(distinct tcol2) from t2 group by tcol1`)
}

func TestDistinct(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// insert some data.
	utils.Exec(t, mcmp.VtConn, `insert into t2(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'A')`)

	// multi distinct
	utils.AssertMatches(t, mcmp.VtConn, `select distinct tcol1, tcol2 from t2`,
		`[[VARCHAR("A") VARCHAR("A")] [VARCHAR("A") VARCHAR("C")] [VARCHAR("B") VARCHAR("A")] [VARCHAR("B") VARCHAR("C")] [VARCHAR("C") VARCHAR("A")]]`)
}

func TestSubQueries(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	utils.Exec(t, mcmp.VtConn, `insert into t2(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'B')`)
	utils.Exec(t, mcmp.VtConn, `insert into t3(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'B')`)

	utils.AssertMatches(t, mcmp.VtConn, `select t2.tcol1, t2.tcol2 from t2 where t2.id IN (select id from t3) order by t2.id`, `[[VARCHAR("A") VARCHAR("A")] [VARCHAR("B") VARCHAR("C")] [VARCHAR("A") VARCHAR("C")] [VARCHAR("C") VARCHAR("A")] [VARCHAR("A") VARCHAR("A")] [VARCHAR("B") VARCHAR("C")] [VARCHAR("B") VARCHAR("A")] [VARCHAR("C") VARCHAR("B")]]`)
	utils.AssertMatches(t, mcmp.VtConn, `select t2.tcol1, t2.tcol2 from t2 where t2.id IN (select t3.id from t3 join t2 on t2.id = t3.id) order by t2.id`, `[[VARCHAR("A") VARCHAR("A")] [VARCHAR("B") VARCHAR("C")] [VARCHAR("A") VARCHAR("C")] [VARCHAR("C") VARCHAR("A")] [VARCHAR("A") VARCHAR("A")] [VARCHAR("B") VARCHAR("C")] [VARCHAR("B") VARCHAR("A")] [VARCHAR("C") VARCHAR("B")]]`)

	utils.AssertMatches(t, mcmp.VtConn, `select u_a.a from u_a left join t2 on t2.id IN (select id from t2)`, `[]`)
	// inserting some data in u_a
	utils.Exec(t, mcmp.VtConn, `insert into u_a(id, a) values (1, 1)`)

	// execute same query again.
	qr := utils.Exec(t, mcmp.VtConn, `select u_a.a from u_a left join t2 on t2.id IN (select id from t2)`)
	assert.EqualValues(t, 8, len(qr.Rows))
	for index, row := range qr.Rows {
		assert.EqualValues(t, `[INT64(1)]`, fmt.Sprintf("%v", row), "does not match for row: %d", index+1)
	}

	// fail as projection subquery is not scalar
	_, err := utils.ExecAllowError(t, mcmp.VtConn, `select (select id from t2) from t2 order by id`)
	assert.EqualError(t, err, "subquery returned more than one row (errno 1105) (sqlstate HY000) during query: select (select id from t2) from t2 order by id")

	utils.AssertMatches(t, mcmp.VtConn, `select (select id from t2 order by id limit 1) from t2 order by id limit 2`, `[[INT64(1)] [INT64(1)]]`)
}

func TestPlannerWarning(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// straight_join query
	_ = utils.Exec(t, mcmp.VtConn, `select 1 from t1 straight_join t2 on t1.id = t2.id`)
	utils.AssertMatches(t, mcmp.VtConn, `show warnings`, `[[VARCHAR("Warning") UINT16(1235) VARCHAR("straight join is converted to normal join")]]`)

	// execute same query again.
	_ = utils.Exec(t, mcmp.VtConn, `select 1 from t1 straight_join t2 on t1.id = t2.id`)
	utils.AssertMatches(t, mcmp.VtConn, `show warnings`, `[[VARCHAR("Warning") UINT16(1235) VARCHAR("straight join is converted to normal join")]]`)

	// random query to reset the warning.
	_ = utils.Exec(t, mcmp.VtConn, `select 1 from t1`)

	// execute same query again.
	_ = utils.Exec(t, mcmp.VtConn, `select 1 from t1 straight_join t2 on t1.id = t2.id`)
	utils.AssertMatches(t, mcmp.VtConn, `show warnings`, `[[VARCHAR("Warning") UINT16(1235) VARCHAR("straight join is converted to normal join")]]`)
}

func TestHashJoin(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	utils.Exec(t, mcmp.VtConn, `insert into t1(id, col) values (1, 1),(2, 3),(3, 4),(4, 7)`)

	utils.AssertMatches(t, mcmp.VtConn, `select /*vt+ ALLOW_HASH_JOIN */ t1.id from t1 x join t1 where x.col = t1.col and x.id <= 3 and t1.id >= 3`, `[[INT64(3)]]`)

	utils.Exec(t, mcmp.VtConn, `set workload = olap`)
	defer utils.Exec(t, mcmp.VtConn, `set workload = oltp`)
	utils.AssertMatches(t, mcmp.VtConn, `select /*vt+ ALLOW_HASH_JOIN */ t1.id from t1 x join t1 where x.col = t1.col and x.id <= 3 and t1.id >= 3`, `[[INT64(3)]]`)
}

func TestMultiColumnVindex(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec(`insert into user_region(id, cola, colb) values (1, 1, 2),(2, 30, 40),(3, 500, 600),(4, 30, 40),(5, 10000, 30000),(6, 422333, 40),(7, 30, 60)`)

	for _, workload := range []string{"olap", "oltp"} {
		t.Run(workload, func(t *testing.T) {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf(`set workload = %s`, workload))
			utils.AssertMatches(t, mcmp.VtConn, `select id from user_region where cola = 1 and colb = 2`, `[[INT64(1)]]`)
			utils.AssertMatches(t, mcmp.VtConn, `select id from user_region where cola in (30,422333) and colb = 40 order by id`, `[[INT64(2)] [INT64(4)] [INT64(6)]]`)
			utils.AssertMatches(t, mcmp.VtConn, `select id from user_region where cola in (30,422333) and colb in (40,60) order by id`, `[[INT64(2)] [INT64(4)] [INT64(6)] [INT64(7)]]`)
			utils.AssertMatches(t, mcmp.VtConn, `select id from user_region where cola in (30,422333) and colb in (40,60) and cola = 422333`, `[[INT64(6)]]`)
			utils.AssertMatches(t, mcmp.VtConn, `select id from user_region where cola in (30,422333) and colb in (40,60) and cola = 30 and colb = 60`, `[[INT64(7)]]`)
		})
	}
}

func TestFanoutVindex(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	tcases := []struct {
		regionID int
		exp      string
	}{{
		regionID: 24,
		exp:      `[[INT64(24) INT64(1) VARCHAR("shard--19a0")]]`,
	}, {
		regionID: 25,
		exp:      `[[INT64(25) INT64(2) VARCHAR("shard--19a0")] [INT64(25) INT64(7) VARCHAR("shard-19a0-20")]]`,
	}, {
		regionID: 31,
		exp:      `[[INT64(31) INT64(8) VARCHAR("shard-19a0-20")]]`,
	}, {
		regionID: 32,
		exp:      `[[INT64(32) INT64(14) VARCHAR("shard-20-20c0")] [INT64(32) INT64(19) VARCHAR("shard-20c0-")]]`,
	}, {
		regionID: 33,
		exp:      `[[INT64(33) INT64(20) VARCHAR("shard-20c0-")]]`,
	}}

	defer utils.ExecAllowError(t, mcmp.VtConn, `delete from region_tbl`)
	uid := 1
	// insert data in all shards to know where the query fan-out
	for _, s := range shardedKsShards {
		utils.Exec(t, mcmp.VtConn, fmt.Sprintf("use `%s:%s`", shardedKs, s))
		for _, tcase := range tcases {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("insert into region_tbl(rg,uid,msg) values(%d,%d,'shard-%s')", tcase.regionID, uid, s))
			uid++
		}
	}

	newConn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer newConn.Close()

	for _, workload := range []string{"olap", "oltp"} {
		utils.Exec(t, newConn, fmt.Sprintf(`set workload = %s`, workload))
		for _, tcase := range tcases {
			t.Run(workload+strconv.Itoa(tcase.regionID), func(t *testing.T) {
				sql := fmt.Sprintf("select rg, uid, msg from region_tbl where rg = %d order by uid", tcase.regionID)
				assert.Equal(t, tcase.exp, fmt.Sprintf("%v", utils.Exec(t, newConn, sql).Rows))
			})
		}
	}
}

func TestSubShardVindex(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	tcases := []struct {
		regionID int
		exp      string
	}{{
		regionID: 140,
		exp:      `[[INT64(140) VARBINARY("1") VARCHAR("1") VARCHAR("shard--19a0")]]`,
	}, {
		regionID: 412,
		exp:      `[[INT64(412) VARBINARY("2") VARCHAR("2") VARCHAR("shard--19a0")] [INT64(412) VARBINARY("9") VARCHAR("9") VARCHAR("shard-19a0-20")]]`,
	}, {
		regionID: 24,
		exp:      `[[INT64(24) VARBINARY("10") VARCHAR("10") VARCHAR("shard-19a0-20")]]`,
	}, {
		regionID: 116,
		exp:      `[[INT64(116) VARBINARY("11") VARCHAR("11") VARCHAR("shard-19a0-20")]]`,
	}, {
		regionID: 239,
		exp:      `[[INT64(239) VARBINARY("12") VARCHAR("12") VARCHAR("shard-19a0-20")]]`,
	}, {
		regionID: 89,
		exp:      `[[INT64(89) VARBINARY("20") VARCHAR("20") VARCHAR("shard-20-20c0")] [INT64(89) VARBINARY("27") VARCHAR("27") VARCHAR("shard-20c0-")]]`,
	}, {
		regionID: 109,
		exp:      `[[INT64(109) VARBINARY("28") VARCHAR("28") VARCHAR("shard-20c0-")]]`,
	}}

	uid := 1
	// insert data in all shards to know where the query fan-out
	for _, s := range shardedKsShards {
		utils.Exec(t, mcmp.VtConn, fmt.Sprintf("use `%s:%s`", shardedKs, s))
		for _, tcase := range tcases {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("insert into multicol_tbl(cola,colb,colc,msg) values(%d,_binary '%d','%d','shard-%s')", tcase.regionID, uid, uid, s))
			uid++
		}
	}

	newConn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer newConn.Close()

	defer utils.ExecAllowError(t, newConn, `delete from multicol_tbl`)
	for _, workload := range []string{"olap", "oltp"} {
		utils.Exec(t, newConn, fmt.Sprintf(`set workload = %s`, workload))
		for _, tcase := range tcases {
			t.Run(workload+strconv.Itoa(tcase.regionID), func(t *testing.T) {
				sql := fmt.Sprintf("select cola, colb, colc, msg from multicol_tbl where cola = %d order by cola,msg", tcase.regionID)
				assert.Equal(t, tcase.exp, fmt.Sprintf("%v", utils.Exec(t, newConn, sql).Rows))
			})
		}
	}
}

func TestSubShardVindexDML(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	tcases := []struct {
		regionID       int
		shardsAffected int
	}{{
		regionID:       140, // shard--19a0
		shardsAffected: 1,
	}, {
		regionID:       412, // shard--19a0 and shard-19a0-20
		shardsAffected: 2,
	}, {
		regionID:       24, // shard-19a0-20
		shardsAffected: 1,
	}, {
		regionID:       89, // shard-20-20c0 and shard-20c0-
		shardsAffected: 2,
	}, {
		regionID:       109, // shard-20c0-
		shardsAffected: 1,
	}}

	uid := 1
	// insert data in all shards to know where the query fan-out
	for _, s := range shardedKsShards {
		utils.Exec(t, mcmp.VtConn, fmt.Sprintf("use `%s:%s`", shardedKs, s))
		for _, tcase := range tcases {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("insert into multicol_tbl(cola,colb,colc,msg) values(%d,_binary '%d','%d','shard-%s')", tcase.regionID, uid, uid, s))
			uid++
		}
	}

	newConn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer newConn.Close()

	defer utils.ExecAllowError(t, newConn, `delete from multicol_tbl`)
	for _, tcase := range tcases {
		t.Run(strconv.Itoa(tcase.regionID), func(t *testing.T) {
			qr := utils.Exec(t, newConn, fmt.Sprintf("update multicol_tbl set msg = 'bar' where cola = %d", tcase.regionID))
			assert.EqualValues(t, tcase.shardsAffected, qr.RowsAffected)
		})
	}

	for _, tcase := range tcases {
		t.Run(strconv.Itoa(tcase.regionID), func(t *testing.T) {
			qr := utils.Exec(t, newConn, fmt.Sprintf("delete from multicol_tbl where cola = %d", tcase.regionID))
			assert.EqualValues(t, tcase.shardsAffected, qr.RowsAffected)
		})
	}
}

func TestOuterJoin(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// insert some data.
	utils.Exec(t, mcmp.VtConn, `insert into t1(id, col) values (100, 123), (10, 123), (1, 13), (1000, 1234)`)
	utils.Exec(t, mcmp.VtConn, `insert into t2(id, tcol1, tcol2) values (12, 13, 1),(123, 7, 15),(1, 123, 123),(1004, 134, 123)`)

	// Gen4 only supported query.
	utils.AssertMatchesNoOrder(t, mcmp.VtConn, `select t1.id, t2.tcol1+t2.tcol2 from t1 left join t2 on t1.col = t2.id`, `[[INT64(10) FLOAT64(22)] [INT64(1) NULL] [INT64(100) FLOAT64(22)] [INT64(1000) NULL]]`)
	utils.AssertMatchesNoOrder(t, mcmp.VtConn, `select t1.id, t2.id, t2.tcol1+t1.col+t2.tcol2 from t1 left join t2 on t1.col = t2.id`,
		`[[INT64(10) INT64(123) FLOAT64(145)]`+
			` [INT64(1) NULL NULL]`+
			` [INT64(100) INT64(123) FLOAT64(145)]`+
			` [INT64(1000) NULL NULL]]`)
}

func TestUsingJoin(t *testing.T) {
	require.NoError(t, utils.WaitForAuthoritative(t, shardedKs, "t1", clusterInstance.VtgateProcess.ReadVSchema))
	require.NoError(t, utils.WaitForAuthoritative(t, shardedKs, "t2", clusterInstance.VtgateProcess.ReadVSchema))
	require.NoError(t, utils.WaitForAuthoritative(t, shardedKs, "t3", clusterInstance.VtgateProcess.ReadVSchema))

	mcmp, closer := start(t)
	defer closer()

	// insert some data.
	mcmp.Exec(`insert into t1(id, col) values          (1, 1), (2, 2), (3, 3), (5, 5)`)
	mcmp.Exec(`insert into t2(id, tcol1, tcol2) values (1, 12, 12),(3, 3, 13),(4, 123, 123),(5, 134, 123)`)
	mcmp.Exec(`insert into t3(id, tcol1, tcol2) values (1, 12, 12),(4, 123, 123),(5, 134, 123)`)

	// Gen4 only supported query.
	mcmp.AssertMatchesNoOrderInclColumnNames(`select t1.id from t1 join t2 using(id)`,
		`[[INT64(1)] [INT64(3)] [INT64(5)]]`)
	mcmp.AssertMatchesNoOrderInclColumnNames(`select t2.id from t2 join t3 using (id, tcol1, tcol2)`,
		`[[INT64(1)] [INT64(4)] [INT64(5)]]`)
	mcmp.AssertMatchesNoOrderInclColumnNames(`select * from t2 join t3 using (tcol1)`,
		`[[VARCHAR("12") INT64(1) VARCHAR("12") INT64(1) VARCHAR("12")] `+
			`[VARCHAR("123") INT64(4) VARCHAR("123") INT64(4) VARCHAR("123")] `+
			`[VARCHAR("134") INT64(5) VARCHAR("123") INT64(5) VARCHAR("123")]]`)
	mcmp.AssertMatchesNoOrderInclColumnNames(`select * from t2 join t3 using (tcol1) having tcol1 = 12`,
		`[[VARCHAR("12") INT64(1) VARCHAR("12") INT64(1) VARCHAR("12")]]`)
}

// TestInsertFunction tests the INSERT function
func TestInsertFunction(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec(`insert into t2(id, tcol1, tcol2) values (1, "Test", "This"),(2, "Testing", "Is"),(3, "TEST", "A")`)
	mcmp.AssertMatches(`SELECT INSERT('Quadratic', 3, 4, 'What')`, `[[VARCHAR("QuWhattic")]]`)
	mcmp.AssertMatches(`SELECT INSERT(tcol1, id, 3, tcol2) from t2`, `[[VARCHAR("Thist")] [VARCHAR("TIsing")] [VARCHAR("TEA")]]`)
}

// TestGTIDFunctions tests the gtid functions
func TestGTIDFunctions(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.AssertMatches(`select gtid_subset('3E11FA47-71CA-11E1-9E33-C80AA9429562:23','3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57')`, `[[INT64(1)]]`)
	mcmp.AssertMatches(`select gtid_subtract('3E11FA47-71CA-11E1-9E33-C80AA9429562:23-78','3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57')`, `[[VARCHAR("3e11fa47-71ca-11e1-9e33-c80aa9429562:58-78")]]`)
}

func TestFilterOnLeftOuterJoin(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// insert some data.
	mcmp.Exec(`insert into team (id, name) values (11, 'Acme'), (22, 'B'), (33, 'C')`)
	mcmp.Exec(`insert into team_fact (id, team, fact) values (1, 11, 'A'), (2, 22, 'A'), (3, 33, 'A')`)

	// Gen4 only supported query.
	query := `select team.id
				from team_fact
				  join team on team.id = team_fact.team
				  left outer join team_member on team_member.team = team.id
				where (
				  team_fact.fact = 'A'
				  and team_member.user is null
				  and team_fact.team >= 22
				)`

	mcmp.AssertMatches(query, "[[INT32(22)] [INT32(33)]]")
}

func TestPercentageAndUnderscore(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// insert some data.
	mcmp.Exec(`insert into t2(id, tcol1, tcol2) values (1, 'A%B', 'A%B'),(2, 'C_D', 'E'),(3, 'AB', 'C1D'),(4, 'E', 'A%B'),(5, 'A%B', 'AB'),(6, 'C1D', 'E'),(7, 'C_D', 'A%B'),(8, 'E', 'C_D')`)

	// Verify that %, _ and their escaped counter-parts work in Vitess in the like clause as well as equality clause
	mcmp.Exec(`select * from t2 where tcol1 like "A%B"`)
	mcmp.Exec(`select * from t2 where tcol1 like "A\%B"`)
	mcmp.Exec(`select * from t2 where tcol1 like "C_D"`)
	mcmp.Exec(`select * from t2 where tcol1 like "C\_D"`)

	mcmp.Exec(`select * from t2 where tcol1 = "A%B"`)
	mcmp.Exec(`select * from t2 where tcol1 = "A\%B"`)
	mcmp.Exec(`select * from t2 where tcol1 = "C_D"`)
	mcmp.Exec(`select * from t2 where tcol1 = "C\_D"`)

	// Verify that %, _ and their escaped counter-parts work with filtering on VTGate level
	mcmp.Exec(`select a.tcol1 from t2 a join t2 b where a.tcol1 = b.tcol2 group by a.tcol1 having repeat(a.tcol1,min(a.id)) like "A\%B" order by a.tcol1`)
	mcmp.Exec(`select a.tcol1 from t2 a join t2 b where a.tcol1 = b.tcol2 group by a.tcol1 having repeat(a.tcol1,min(a.id)) like "A%B" order by a.tcol1`)
	mcmp.Exec(`select a.tcol1 from t2 a join t2 b where a.tcol1 = b.tcol2 group by a.tcol1 having repeat(a.tcol1,min(a.id)) = "A\%B" order by a.tcol1`)
	mcmp.Exec(`select a.tcol1 from t2 a join t2 b where a.tcol1 = b.tcol2 group by a.tcol1 having repeat(a.tcol1,min(a.id)) = "A%B" order by a.tcol1`)
	mcmp.Exec(`select a.tcol1 from t2 a join t2 b where a.tcol1 = b.tcol2 group by a.tcol1 having repeat(a.tcol1,min(a.id)) like "C_D%" order by a.tcol1`)
	mcmp.Exec(`select a.tcol1 from t2 a join t2 b where a.tcol1 = b.tcol2 group by a.tcol1 having repeat(a.tcol1,min(a.id)) like "C\_D%" order by a.tcol1`)
	mcmp.Exec(`select a.tcol1 from t2 a join t2 b where a.tcol1 = b.tcol2 group by a.tcol1 having repeat(a.tcol1,min(a.id)) = "C_DC_D" order by a.tcol1`)
	mcmp.Exec(`select a.tcol1 from t2 a join t2 b where a.tcol1 = b.tcol2 group by a.tcol1 having repeat(a.tcol1,min(a.id)) = "C\_DC\_D" order by a.tcol1`)
}
