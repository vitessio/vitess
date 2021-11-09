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

	"github.com/stretchr/testify/assert"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
)

func TestOrderBy(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer func() {
		_, _ = exec(t, conn, `delete from t1`)
	}()

	// insert some data.
	checkedExec(t, conn, `insert into t1(id, col) values (100, 123),(10, 12),(1, 13),(1000, 1234)`)

	// Gen4 only supported query.
	assertMatches(t, conn, `select col from t1 order by id`, `[[INT64(13)] [INT64(12)] [INT64(123)] [INT64(1234)]]`)

	// Gen4 unsupported query. v3 supported.
	assertMatches(t, conn, `select col from t1 order by 1`, `[[INT64(12)] [INT64(13)] [INT64(123)] [INT64(1234)]]`)

	// unsupported in v3 and Gen4.
	_, err = exec(t, conn, `select t1.* from t1 order by id`)
	require.Error(t, err)
}

func TestCorrelatedExistsSubquery(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer func() {
		_, _ = exec(t, conn, `delete from t1`)
		_, _ = exec(t, conn, `delete from t2`)
	}()
	// insert some data.
	checkedExec(t, conn, `insert into t1(id, col) values (100, 123),(10, 12), (1, 13), (4, 13),(1000, 1234)`)
	checkedExec(t, conn, `insert into t2(id, tcol1, tcol2) values (100, 13, 1),(9, 7, 15),(1, 123, 123),(1004, 134, 123)`)

	assertMatches(t, conn, `select id from t1 where exists(select 1 from t2 where t1.col = t2.tcol2)`, `[[INT64(100)]]`)
	assertMatches(t, conn, `select id from t1 where exists(select 1 from t2 where t1.col = t2.tcol1) order by id`, `[[INT64(1)] [INT64(4)] [INT64(100)]]`)
}

func TestGroupBy(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer func() {
		_, _ = exec(t, conn, `delete from t1`)
		_, _ = exec(t, conn, `delete from t2`)
	}()

	// insert some data.
	checkedExec(t, conn, `insert into t1(id, col) values (1, 123),(2, 12),(3, 13),(4, 1234)`)
	checkedExec(t, conn, `insert into t2(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'B')`)

	// Gen4 only supported query.
	assertMatches(t, conn, `select tcol2, tcol1, count(id) from t2 group by tcol2, tcol1`,
		`[[VARCHAR("A") VARCHAR("A") INT64(2)] [VARCHAR("A") VARCHAR("B") INT64(1)] [VARCHAR("A") VARCHAR("C") INT64(1)] [VARCHAR("B") VARCHAR("C") INT64(1)] [VARCHAR("C") VARCHAR("A") INT64(1)] [VARCHAR("C") VARCHAR("B") INT64(2)]]`)

	assertMatches(t, conn, `select tcol1, tcol1 from t2 order by tcol1`,
		`[[VARCHAR("A") VARCHAR("A")] [VARCHAR("A") VARCHAR("A")] [VARCHAR("A") VARCHAR("A")] [VARCHAR("B") VARCHAR("B")] [VARCHAR("B") VARCHAR("B")] [VARCHAR("B") VARCHAR("B")] [VARCHAR("C") VARCHAR("C")] [VARCHAR("C") VARCHAR("C")]]`)

	assertMatches(t, conn, `select tcol1, tcol1 from t1 join t2 on t1.id = t2.id order by tcol1`,
		`[[VARCHAR("A") VARCHAR("A")] [VARCHAR("A") VARCHAR("A")] [VARCHAR("B") VARCHAR("B")] [VARCHAR("C") VARCHAR("C")]]`)

	assertMatches(t, conn, `select count(*) k, tcol1, tcol2, "abc" b from t2 group by tcol1, tcol2, b order by k, tcol2, tcol1`,
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
		_, _ = exec(t, conn, `delete from t2`)
		_, _ = exec(t, conn, `delete from t3`)
	}()

	checkedExec(t, conn, `insert into t2(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'B')`)
	checkedExec(t, conn, `insert into t3(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'B')`)

	assertMatches(t, conn, `select t2.tcol1 from t2 join t3 on t2.tcol2 = t3.tcol2 where t2.tcol1 = 'A'`, `[[VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")] [VARCHAR("A")]]`)
}

func TestDistinctAggregationFunc(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer exec(t, conn, `delete from t2`)

	// insert some data.
	checkedExec(t, conn, `insert into t2(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'A')`)

	// count on primary vindex
	assertMatches(t, conn, `select tcol1, count(distinct id) from t2 group by tcol1`,
		`[[VARCHAR("A") INT64(3)] [VARCHAR("B") INT64(3)] [VARCHAR("C") INT64(2)]]`)

	// count on any column
	assertMatches(t, conn, `select tcol1, count(distinct tcol2) from t2 group by tcol1`,
		`[[VARCHAR("A") INT64(2)] [VARCHAR("B") INT64(2)] [VARCHAR("C") INT64(1)]]`)

	// sum of columns
	assertMatches(t, conn, `select sum(id), sum(tcol1) from t2`,
		`[[DECIMAL(36) FLOAT64(0)]]`)

	// sum on primary vindex
	assertMatches(t, conn, `select tcol1, sum(distinct id) from t2 group by tcol1`,
		`[[VARCHAR("A") DECIMAL(9)] [VARCHAR("B") DECIMAL(15)] [VARCHAR("C") DECIMAL(12)]]`)

	// sum on any column
	assertMatches(t, conn, `select tcol1, sum(distinct tcol2) from t2 group by tcol1`,
		`[[VARCHAR("A") DECIMAL(0)] [VARCHAR("B") DECIMAL(0)] [VARCHAR("C") DECIMAL(0)]]`)

	// insert more data to get values on sum
	checkedExec(t, conn, `insert into t2(id, tcol1, tcol2) values (9, 'AA', null),(10, 'AA', '4'),(11, 'AA', '4'),(12, null, '5'),(13, null, '6'),(14, 'BB', '10'),(15, 'BB', '20'),(16, 'BB', 'X')`)

	// multi distinct
	assertMatches(t, conn, `select tcol1, count(distinct tcol2), sum(distinct tcol2) from t2 group by tcol1`,
		`[[NULL INT64(2) DECIMAL(11)] [VARCHAR("A") INT64(2) DECIMAL(0)] [VARCHAR("AA") INT64(1) DECIMAL(4)] [VARCHAR("B") INT64(2) DECIMAL(0)] [VARCHAR("BB") INT64(3) DECIMAL(30)] [VARCHAR("C") INT64(1) DECIMAL(0)]]`)
}

func TestDistinct(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer exec(t, conn, `delete from t2`)

	// insert some data.
	checkedExec(t, conn, `insert into t2(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'A')`)

	// multi distinct
	assertMatches(t, conn, `select distinct tcol1, tcol2 from t2`,
		`[[VARCHAR("A") VARCHAR("A")] [VARCHAR("A") VARCHAR("C")] [VARCHAR("B") VARCHAR("A")] [VARCHAR("B") VARCHAR("C")] [VARCHAR("C") VARCHAR("A")]]`)
}

func TestSubQueries(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer func() {
		_, _ = exec(t, conn, `delete from t2`)
		_, _ = exec(t, conn, `delete from t3`)
		_, _ = exec(t, conn, `delete from u_a`)
	}()

	checkedExec(t, conn, `insert into t2(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'B')`)
	checkedExec(t, conn, `insert into t3(id, tcol1, tcol2) values (1, 'A', 'A'),(2, 'B', 'C'),(3, 'A', 'C'),(4, 'C', 'A'),(5, 'A', 'A'),(6, 'B', 'C'),(7, 'B', 'A'),(8, 'C', 'B')`)

	assertMatches(t, conn, `select t2.tcol1, t2.tcol2 from t2 where t2.id IN (select id from t3) order by t2.id`, `[[VARCHAR("A") VARCHAR("A")] [VARCHAR("B") VARCHAR("C")] [VARCHAR("A") VARCHAR("C")] [VARCHAR("C") VARCHAR("A")] [VARCHAR("A") VARCHAR("A")] [VARCHAR("B") VARCHAR("C")] [VARCHAR("B") VARCHAR("A")] [VARCHAR("C") VARCHAR("B")]]`)
	assertMatches(t, conn, `select t2.tcol1, t2.tcol2 from t2 where t2.id IN (select t3.id from t3 join t2 on t2.id = t3.id) order by t2.id`, `[[VARCHAR("A") VARCHAR("A")] [VARCHAR("B") VARCHAR("C")] [VARCHAR("A") VARCHAR("C")] [VARCHAR("C") VARCHAR("A")] [VARCHAR("A") VARCHAR("A")] [VARCHAR("B") VARCHAR("C")] [VARCHAR("B") VARCHAR("A")] [VARCHAR("C") VARCHAR("B")]]`)

	assertMatches(t, conn, `select u_a.a from u_a left join t2 on t2.id IN (select id from t2)`, `[]`)
	//inserting some data in u_a
	checkedExec(t, conn, `insert into u_a(id, a) values (1, 1)`)

	// execute same query again.
	qr := checkedExec(t, conn, `select u_a.a from u_a left join t2 on t2.id IN (select id from t2)`)
	assert.EqualValues(t, 8, len(qr.Rows))
	for index, row := range qr.Rows {
		assert.EqualValues(t, `[INT64(1)]`, fmt.Sprintf("%v", row), "does not match for row: %d", index+1)
	}

	// fail as projection subquery is not scalar
	_, err = exec(t, conn, `select (select id from t2) from t2 order by id`)
	assert.EqualError(t, err, "subquery returned more than one row (errno 1105) (sqlstate HY000) during query: select (select id from t2) from t2 order by id")

	assertMatches(t, conn, `select (select id from t2 order by id limit 1) from t2 order by id limit 2`, `[[INT64(1)] [INT64(1)]]`)
}

func TestPlannerWarning(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// straight_join query
	_ = checkedExec(t, conn, `select 1 from t1 straight_join t2 on t1.id = t2.id`)
	assertMatches(t, conn, `show warnings`, `[[VARCHAR("Warning") UINT16(1235) VARCHAR("straight join is converted to normal join")]]`)

	// execute same query again.
	_ = checkedExec(t, conn, `select 1 from t1 straight_join t2 on t1.id = t2.id`)
	assertMatches(t, conn, `show warnings`, `[[VARCHAR("Warning") UINT16(1235) VARCHAR("straight join is converted to normal join")]]`)

	// random query to reset the warning.
	_ = checkedExec(t, conn, `select 1 from t1`)

	// execute same query again.
	_ = checkedExec(t, conn, `select 1 from t1 straight_join t2 on t1.id = t2.id`)
	assertMatches(t, conn, `show warnings`, `[[VARCHAR("Warning") UINT16(1235) VARCHAR("straight join is converted to normal join")]]`)
}

func assertMatches(t *testing.T, conn *mysql.Conn, query, expected string) {
	t.Helper()
	qr := checkedExec(t, conn, query)
	got := fmt.Sprintf("%v", qr.Rows)
	diff := cmp.Diff(expected, got)
	if diff != "" {
		t.Errorf("Query: %s (-want +got):\n%s", query, diff)
	}
}

func checkedExec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := exec(t, conn, query)
	require.NoError(t, err, "for query: "+query)
	return qr
}

func exec(t *testing.T, conn *mysql.Conn, query string) (*sqltypes.Result, error) {
	t.Helper()
	return conn.ExecuteFetch(query, 1000, true)
}
