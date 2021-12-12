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
	"testing"

	"vitess.io/vitess/go/test/endtoend/vtgate/utils"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

func TestOrderBy(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer func() {
		_, _ = utils.ExecAllowError(t, conn, `delete from t1`)
	}()

	// insert some data.
	utils.Exec(t, conn, `insert into t1(id, col) values (100, 123),(10, 12),(1, 13),(1000, 1234)`)

	// Gen4 only supported query.
	utils.AssertMatches(t, conn, `select col from t1 order by id`, `[[INT64(13)] [INT64(12)] [INT64(123)] [INT64(1234)]]`)

	// Gen4 unsupported query. v3 supported.
	utils.AssertMatches(t, conn, `select col from t1 order by 1`, `[[INT64(12)] [INT64(13)] [INT64(123)] [INT64(1234)]]`)

	// unsupported in v3 and Gen4.
	_, err = utils.ExecAllowError(t, conn, `select t1.* from t1 order by id`)
	require.Error(t, err)
}

func TestCorrelatedExistsSubquery(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer func() {
		_, _ = utils.ExecAllowError(t, conn, `delete from t1`)
		_, _ = utils.ExecAllowError(t, conn, `delete from t2`)
	}()
	// insert some data.
	utils.Exec(t, conn, `insert into t1(id, col) values (100, 123),(10, 12), (1, 13), (4, 13),(1000, 1234)`)
	utils.Exec(t, conn, `insert into t2(id, tcol1, tcol2) values (100, 13, 1),(9, 7, 15),(1, 123, 123),(1004, 134, 123)`)

	utils.AssertMatches(t, conn, `select id from t1 where exists(select 1 from t2 where t1.col = t2.tcol2)`, `[[INT64(100)]]`)
	utils.AssertMatches(t, conn, `select id from t1 where exists(select 1 from t2 where t1.col = t2.tcol1) order by id`, `[[INT64(1)] [INT64(4)] [INT64(100)]]`)
	utils.AssertMatches(t, conn, `select id from t1 where id in (select id from t2) order by id`, `[[INT64(1)] [INT64(100)]]`)
}

func TestGroupBy(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer func() {
		_, _ = utils.ExecAllowError(t, conn, `delete from t1`)
		_, _ = utils.ExecAllowError(t, conn, `delete from t2`)
	}()

	// insert some data.
	utils.Exec(t, conn, `insert into t1(id, col) values (1, 123),(2, 12),(3, 13),(4, 1234)`)
	utils.Exec(t, conn, `insert into t2(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'B')`)

	// Gen4 only supported query.
	utils.AssertMatches(t, conn, `select tcol2, tcol1, count(id) from t2 group by tcol2, tcol1`,
		`[[VARCHAR("A") VARCHAR("A") INT64(2)] [VARCHAR("A") VARCHAR("B") INT64(1)] [VARCHAR("A") VARCHAR("C") INT64(1)] [VARCHAR("B") VARCHAR("C") INT64(1)] [VARCHAR("C") VARCHAR("A") INT64(1)] [VARCHAR("C") VARCHAR("B") INT64(2)]]`)

	utils.AssertMatches(t, conn, `select tcol1, tcol1 from t2 order by tcol1`,
		`[[VARCHAR("A") VARCHAR("A")] [VARCHAR("A") VARCHAR("A")] [VARCHAR("A") VARCHAR("A")] [VARCHAR("B") VARCHAR("B")] [VARCHAR("B") VARCHAR("B")] [VARCHAR("B") VARCHAR("B")] [VARCHAR("C") VARCHAR("C")] [VARCHAR("C") VARCHAR("C")]]`)

	utils.AssertMatches(t, conn, `select tcol1, tcol1 from t1 join t2 on t1.id = t2.id order by tcol1`,
		`[[VARCHAR("A") VARCHAR("A")] [VARCHAR("A") VARCHAR("A")] [VARCHAR("B") VARCHAR("B")] [VARCHAR("C") VARCHAR("C")]]`)

	utils.AssertMatches(t, conn, `select count(*) k, tcol1, tcol2, "abc" b from t2 group by tcol1, tcol2, b order by k, tcol2, tcol1`,
		`[[INT64(1) VARCHAR("B") VARCHAR("A") VARCHAR("abc")] `+
			`[INT64(1) VARCHAR("C") VARCHAR("A") VARCHAR("abc")] `+
			`[INT64(1) VARCHAR("C") VARCHAR("B") VARCHAR("abc")] `+
			`[INT64(1) VARCHAR("A") VARCHAR("C") VARCHAR("abc")] `+
			`[INT64(2) VARCHAR("A") VARCHAR("A") VARCHAR("abc")] `+
			`[INT64(2) VARCHAR("B") VARCHAR("C") VARCHAR("abc")]]`)
}

func TestJoinBindVars(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer func() {
		_, _ = utils.ExecAllowError(t, conn, `delete from t2`)
		_, _ = utils.ExecAllowError(t, conn, `delete from t3`)
	}()

	utils.Exec(t, conn, `insert into t2(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'B')`)
	utils.Exec(t, conn, `insert into t3(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'B')`)

	utils.AssertMatches(t, conn, `select t2.tcol1 from t2 join t3 on t2.tcol2 = t3.tcol2 where t2.tcol1 = 'A'`, `[[VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")]]`)
}

func TestDistinctAggregationFunc(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer utils.ExecAllowError(t, conn, `delete from t2`)

	// insert some data.
	utils.Exec(t, conn, `insert into t2(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'A')`)

	// count on primary vindex
	utils.AssertMatches(t, conn, `select tcol1, count(distinct id) from t2 group by tcol1`,
		`[[VARCHAR("A") INT64(3)] [VARCHAR("B") INT64(3)] [VARCHAR("C") INT64(2)]]`)

	// count on any column
	utils.AssertMatches(t, conn, `select tcol1, count(distinct tcol2) from t2 group by tcol1`,
		`[[VARCHAR("A") INT64(2)] [VARCHAR("B") INT64(2)] [VARCHAR("C") INT64(1)]]`)

	// sum of columns
	utils.AssertMatches(t, conn, `select sum(id), sum(tcol1) from t2`,
		`[[DECIMAL(36) FLOAT64(0)]]`)

	// sum on primary vindex
	utils.AssertMatches(t, conn, `select tcol1, sum(distinct id) from t2 group by tcol1`,
		`[[VARCHAR("A") DECIMAL(9)] [VARCHAR("B") DECIMAL(15)] [VARCHAR("C") DECIMAL(12)]]`)

	// sum on any column
	utils.AssertMatches(t, conn, `select tcol1, sum(distinct tcol2) from t2 group by tcol1`,
		`[[VARCHAR("A") DECIMAL(0)] [VARCHAR("B") DECIMAL(0)] [VARCHAR("C") DECIMAL(0)]]`)

	// insert more data to get values on sum
	utils.Exec(t, conn, `insert into t2(id, tcol1, tcol2) values (9, 'AA', null),(10, 'AA', '4'),(11, 'AA', '4'),(12, null, '5'),(13, null, '6'),(14, 'BB', '10'),(15, 'BB', '20'),(16, 'BB', 'X')`)

	// multi distinct
	utils.AssertMatches(t, conn, `select tcol1, count(distinct tcol2), sum(distinct tcol2) from t2 group by tcol1`,
		`[[NULL INT64(2) DECIMAL(11)] [VARCHAR("A") INT64(2) DECIMAL(0)] [VARCHAR("AA") INT64(1) DECIMAL(4)] [VARCHAR("B") INT64(2) DECIMAL(0)] [VARCHAR("BB") INT64(3) DECIMAL(30)] [VARCHAR("C") INT64(1) DECIMAL(0)]]`)
}

func TestDistinct(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer utils.ExecAllowError(t, conn, `delete from t2`)

	// insert some data.
	utils.Exec(t, conn, `insert into t2(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'A')`)

	// multi distinct
	utils.AssertMatches(t, conn, `select distinct tcol1, tcol2 from t2`,
		`[[VARCHAR("A") VARCHAR("A")] [VARCHAR("A") VARCHAR("C")] [VARCHAR("B") VARCHAR("A")] [VARCHAR("B") VARCHAR("C")] [VARCHAR("C") VARCHAR("A")]]`)
}

func TestSubQueries(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer func() {
		_, _ = utils.ExecAllowError(t, conn, `delete from t2`)
		_, _ = utils.ExecAllowError(t, conn, `delete from t3`)
		_, _ = utils.ExecAllowError(t, conn, `delete from u_a`)
	}()

	utils.Exec(t, conn, `insert into t2(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'B')`)
	utils.Exec(t, conn, `insert into t3(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'B')`)

	utils.AssertMatches(t, conn, `select t2.tcol1, t2.tcol2 from t2 where t2.id IN (select id from t3) order by t2.id`, `[[VARCHAR("A") VARCHAR("A")] [VARCHAR("B") VARCHAR("C")] [VARCHAR("A") VARCHAR("C")] [VARCHAR("C") VARCHAR("A")] [VARCHAR("A") VARCHAR("A")] [VARCHAR("B") VARCHAR("C")] [VARCHAR("B") VARCHAR("A")] [VARCHAR("C") VARCHAR("B")]]`)
	utils.AssertMatches(t, conn, `select t2.tcol1, t2.tcol2 from t2 where t2.id IN (select t3.id from t3 join t2 on t2.id = t3.id) order by t2.id`, `[[VARCHAR("A") VARCHAR("A")] [VARCHAR("B") VARCHAR("C")] [VARCHAR("A") VARCHAR("C")] [VARCHAR("C") VARCHAR("A")] [VARCHAR("A") VARCHAR("A")] [VARCHAR("B") VARCHAR("C")] [VARCHAR("B") VARCHAR("A")] [VARCHAR("C") VARCHAR("B")]]`)

	utils.AssertMatches(t, conn, `select u_a.a from u_a left join t2 on t2.id IN (select id from t2)`, `[]`)
	//inserting some data in u_a
	utils.Exec(t, conn, `insert into u_a(id, a) values (1, 1)`)

	// execute same query again.
	qr := utils.Exec(t, conn, `select u_a.a from u_a left join t2 on t2.id IN (select id from t2)`)
	assert.EqualValues(t, 8, len(qr.Rows))
	for index, row := range qr.Rows {
		assert.EqualValues(t, `[INT64(1)]`, fmt.Sprintf("%v", row), "does not match for row: %d", index+1)
	}

	// fail as projection subquery is not scalar
	_, err = utils.ExecAllowError(t, conn, `select (select id from t2) from t2 order by id`)
	assert.EqualError(t, err, "subquery returned more than one row (errno 1105) (sqlstate HY000) during query: select (select id from t2) from t2 order by id")

	utils.AssertMatches(t, conn, `select (select id from t2 order by id limit 1) from t2 order by id limit 2`, `[[INT64(1)] [INT64(1)]]`)
}

func TestPlannerWarning(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// straight_join query
	_ = utils.Exec(t, conn, `select 1 from t1 straight_join t2 on t1.id = t2.id`)
	utils.AssertMatches(t, conn, `show warnings`, `[[VARCHAR("Warning") UINT16(1235) VARCHAR("straight join is converted to normal join")]]`)

	// execute same query again.
	_ = utils.Exec(t, conn, `select 1 from t1 straight_join t2 on t1.id = t2.id`)
	utils.AssertMatches(t, conn, `show warnings`, `[[VARCHAR("Warning") UINT16(1235) VARCHAR("straight join is converted to normal join")]]`)

	// random query to reset the warning.
	_ = utils.Exec(t, conn, `select 1 from t1`)

	// execute same query again.
	_ = utils.Exec(t, conn, `select 1 from t1 straight_join t2 on t1.id = t2.id`)
	utils.AssertMatches(t, conn, `show warnings`, `[[VARCHAR("Warning") UINT16(1235) VARCHAR("straight join is converted to normal join")]]`)
}

func TestHashJoin(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer func() {
		_, _ = utils.ExecAllowError(t, conn, `delete from t1`)
	}()

	utils.Exec(t, conn, `insert into t1(id, col) values (1, 1),(2, 3),(3, 4),(4, 7)`)

	utils.AssertMatches(t, conn, `select /*vt+ ALLOW_HASH_JOIN */ t1.id from t1 x join t1 where x.col = t1.col and x.id <= 3 and t1.id >= 3`, `[[INT64(3)]]`)

	utils.Exec(t, conn, `set workload = olap`)
	defer utils.Exec(t, conn, `set workload = oltp`)
	utils.AssertMatches(t, conn, `select /*vt+ ALLOW_HASH_JOIN */ t1.id from t1 x join t1 where x.col = t1.col and x.id <= 3 and t1.id >= 3`, `[[INT64(3)]]`)
}

func TestMultiColumnVindex(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer utils.ExecAllowError(t, conn, `delete from user_region`)
	utils.Exec(t, conn, `insert into user_region(id, cola, colb) values (1, 1, 2),(2, 30, 40),(3, 500, 600),(4, 30, 40),(5, 10000, 30000),(6, 422333, 40),(7, 30, 60)`)

	for _, workload := range []string{"olap", "oltp"} {
		t.Run(workload, func(t *testing.T) {
			utils.Exec(t, conn, fmt.Sprintf(`set workload = %s`, workload))
			utils.AssertMatches(t, conn, `select id from user_region where cola = 1 and colb = 2`, `[[INT64(1)]]`)
			utils.AssertMatches(t, conn, `select id from user_region where cola in (30,422333) and colb = 40 order by id`, `[[INT64(2)] [INT64(4)] [INT64(6)]]`)
			utils.AssertMatches(t, conn, `select id from user_region where cola in (30,422333) and colb in (40,60) order by id`, `[[INT64(2)] [INT64(4)] [INT64(6)] [INT64(7)]]`)
			utils.AssertMatches(t, conn, `select id from user_region where cola in (30,422333) and colb in (40,60) and cola = 422333`, `[[INT64(6)]]`)
			utils.AssertMatches(t, conn, `select id from user_region where cola in (30,422333) and colb in (40,60) and cola = 30 and colb = 60`, `[[INT64(7)]]`)
		})
	}
}
