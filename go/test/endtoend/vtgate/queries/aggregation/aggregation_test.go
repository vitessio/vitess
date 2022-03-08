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

package aggregation

import (
	"context"
	"fmt"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func start(t *testing.T) (*mysql.Conn, func()) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)

	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, conn, "set workload = oltp")
		utils.Exec(t, conn, "delete from aggr_test")
		utils.Exec(t, conn, "delete from t3")
		utils.Exec(t, conn, "delete from t7_xxhash")
		utils.Exec(t, conn, "delete from aggr_test_dates")
		utils.Exec(t, conn, "delete from t7_xxhash_idx")
	}

	deleteAll()

	return conn, func() {
		deleteAll()
		conn.Close()
		cluster.PanicHandler(t)
	}
}

func TestAggregateTypes(t *testing.T) {
	conn, closer := start(t)
	defer closer()
	utils.Exec(t, conn, "insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'A',1), (3,'b',1), (4,'c',3), (5,'c',4)")
	utils.Exec(t, conn, "insert into aggr_test(id, val1, val2) values(6,'d',null), (7,'e',null), (8,'E',1)")
	utils.AssertMatches(t, conn, "select val1, count(distinct val2), count(*) from aggr_test group by val1", `[[VARCHAR("a") INT64(1) INT64(2)] [VARCHAR("b") INT64(1) INT64(1)] [VARCHAR("c") INT64(2) INT64(2)] [VARCHAR("d") INT64(0) INT64(1)] [VARCHAR("e") INT64(1) INT64(2)]]`)
	utils.AssertMatches(t, conn, "select val1, sum(distinct val2), sum(val2) from aggr_test group by val1", `[[VARCHAR("a") DECIMAL(1) DECIMAL(2)] [VARCHAR("b") DECIMAL(1) DECIMAL(1)] [VARCHAR("c") DECIMAL(7) DECIMAL(7)] [VARCHAR("d") NULL NULL] [VARCHAR("e") DECIMAL(1) DECIMAL(1)]]`)
	utils.AssertMatches(t, conn, "select val1, count(distinct val2) k, count(*) from aggr_test group by val1 order by k desc, val1", `[[VARCHAR("c") INT64(2) INT64(2)] [VARCHAR("a") INT64(1) INT64(2)] [VARCHAR("b") INT64(1) INT64(1)] [VARCHAR("e") INT64(1) INT64(2)] [VARCHAR("d") INT64(0) INT64(1)]]`)
	utils.AssertMatches(t, conn, "select val1, count(distinct val2) k, count(*) from aggr_test group by val1 order by k desc, val1 limit 4", `[[VARCHAR("c") INT64(2) INT64(2)] [VARCHAR("a") INT64(1) INT64(2)] [VARCHAR("b") INT64(1) INT64(1)] [VARCHAR("e") INT64(1) INT64(2)]]`)
	utils.AssertMatches(t, conn, "select ascii(val1) as a, count(*) from aggr_test group by a", `[[INT64(65) INT64(1)] [INT64(69) INT64(1)] [INT64(97) INT64(1)] [INT64(98) INT64(1)] [INT64(99) INT64(2)] [INT64(100) INT64(1)] [INT64(101) INT64(1)]]`)
	utils.AssertMatches(t, conn, "select ascii(val1) as a, count(*) from aggr_test group by a order by a", `[[INT64(65) INT64(1)] [INT64(69) INT64(1)] [INT64(97) INT64(1)] [INT64(98) INT64(1)] [INT64(99) INT64(2)] [INT64(100) INT64(1)] [INT64(101) INT64(1)]]`)
	utils.AssertMatches(t, conn, "select ascii(val1) as a, count(*) from aggr_test group by a order by 2, a", `[[INT64(65) INT64(1)] [INT64(69) INT64(1)] [INT64(97) INT64(1)] [INT64(98) INT64(1)] [INT64(100) INT64(1)] [INT64(101) INT64(1)] [INT64(99) INT64(2)]]`)
	utils.AssertMatches(t, conn, "select val1 as a, count(*) from aggr_test group by a", `[[VARCHAR("a") INT64(2)] [VARCHAR("b") INT64(1)] [VARCHAR("c") INT64(2)] [VARCHAR("d") INT64(1)] [VARCHAR("e") INT64(2)]]`)
	utils.AssertMatches(t, conn, "select val1 as a, count(*) from aggr_test group by a order by a", `[[VARCHAR("a") INT64(2)] [VARCHAR("b") INT64(1)] [VARCHAR("c") INT64(2)] [VARCHAR("d") INT64(1)] [VARCHAR("e") INT64(2)]]`)
	utils.AssertMatches(t, conn, "select val1 as a, count(*) from aggr_test group by a order by 2, a", `[[VARCHAR("b") INT64(1)] [VARCHAR("d") INT64(1)] [VARCHAR("a") INT64(2)] [VARCHAR("c") INT64(2)] [VARCHAR("e") INT64(2)]]`)
}

func TestGroupBy(t *testing.T) {
	conn, closer := start(t)
	defer closer()
	utils.Exec(t, conn, "insert into t3(id5, id6, id7) values(1,1,2), (2,2,4), (3,2,4), (4,1,2), (5,1,2), (6,3,6)")
	// test ordering and group by int column
	utils.AssertMatches(t, conn, "select id6, id7, count(*) k from t3 group by id6, id7 order by k", `[[INT64(3) INT64(6) INT64(1)] [INT64(2) INT64(4) INT64(2)] [INT64(1) INT64(2) INT64(3)]]`)

	// Test the same queries in streaming mode
	utils.Exec(t, conn, "set workload = olap")
	utils.AssertMatches(t, conn, "select id6, id7, count(*) k from t3 group by id6, id7 order by k", `[[INT64(3) INT64(6) INT64(1)] [INT64(2) INT64(4) INT64(2)] [INT64(1) INT64(2) INT64(3)]]`)
}

func TestDistinct(t *testing.T) {
	conn, closer := start(t)
	defer closer()
	utils.Exec(t, conn, "insert into t3(id5,id6,id7) values(1,3,3), (2,3,4), (3,3,6), (4,5,7), (5,5,6)")
	utils.Exec(t, conn, "insert into t7_xxhash(uid,phone) values('1',4), ('2',4), ('3',3), ('4',1), ('5',1)")
	utils.Exec(t, conn, "insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'A',1), (3,'b',1), (4,'c',3), (5,'c',4)")
	utils.Exec(t, conn, "insert into aggr_test(id, val1, val2) values(6,'d',null), (7,'e',null), (8,'E',1)")
	utils.AssertMatches(t, conn, "select distinct val2, count(*) from aggr_test group by val2", `[[NULL INT64(2)] [INT64(1) INT64(4)] [INT64(3) INT64(1)] [INT64(4) INT64(1)]]`)
	utils.AssertMatches(t, conn, "select distinct id6 from t3 join t7_xxhash on t3.id5 = t7_xxhash.phone", `[[INT64(3)] [INT64(5)]]`)
	utils.Exec(t, conn, "delete from t3")
	utils.Exec(t, conn, "delete from t7_xxhash")
	utils.Exec(t, conn, "delete from aggr_test")
}

func TestEqualFilterOnScatter(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'b',2), (3,'c',3), (4,'d',4), (5,'e',5)")

	workloads := []string{"oltp", "olap"}
	for _, workload := range workloads {
		t.Run(workload, func(t *testing.T) {
			utils.Exec(t, conn, fmt.Sprintf("set workload = '%s'", workload))

			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having 1 = 1", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a = 5", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having 5 = a", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a = a", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a = 3+2", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having 1+4 = 3+2", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a = 1", `[]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a = \"1\"", `[]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a = \"5\"", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a = 5.00", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a, val1 from aggr_test group by val1 having a = 1.00", `[[INT64(1) VARCHAR("a")] [INT64(1) VARCHAR("b")] [INT64(1) VARCHAR("c")] [INT64(1) VARCHAR("d")] [INT64(1) VARCHAR("e")]]`)

			utils.AssertContainsError(t, conn, "select /*vt+ PLANNER=gen4 */ 1 from aggr_test having count(*) = 5", `expr cannot be translated, not supported`) // will fail since `count(*)` is a FuncExpr
		})
	}
}

func TestAggrOnJoin(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "insert into t3(id5, id6, id7) values(1,1,1), (2,2,4), (3,2,4), (4,1,2), (5,1,1), (6,3,6)")
	utils.Exec(t, conn, "insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'a',1), (3,'b',1), (4,'c',3), (5,'c',4)")

	utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) from aggr_test a join t3 t on a.val2 = t.id7",
		"[[INT64(8)]]")
	/*
		mysql> select count(*) from aggr_test a join t3 t on a.val2 = t.id7;
		+----------+
		| count(*) |
		+----------+
		|        8 |
		+----------+
		1 row in set (0.00 sec)
	*/
	utils.AssertMatches(
		t,
		conn,
		"select /*vt+ PLANNER=gen4 */ a.val1, count(*) from aggr_test a join t3 t on a.val2 = t.id7 group by a.val1",
		`[[VARCHAR("a") INT64(4)] [VARCHAR("b") INT64(2)] [VARCHAR("c") INT64(2)]]`,
	)
	/*
		mysql> select a.val1, count(*) from aggr_test a join t3 t on a.val2 = t.id7 group by a.val1;
		+------+----------+
		| val1 | count(*) |
		+------+----------+
		| a    |        4 |
		| b    |        2 |
		| c    |        2 |
		+------+----------+
		3 rows in set (0.00 sec)
	*/

	utils.AssertMatches(t, conn, `select /*vt+ PLANNER=gen4 */ max(a1.val2), max(a2.val2), count(*) from aggr_test a1 join aggr_test a2 on a1.val2 = a2.id join t3 t on a2.val2 = t.id7`,
		"[[INT64(3) INT64(1) INT64(8)]]")
	/*
		mysql> select max(a1.val2), max(a2.val2), count(*) from aggr_test a1 join aggr_test a2 on a1.val2 = a2.id join t3 t on a2.val2 = t.id7;
		+--------------+--------------+----------+
		| max(a1.val2) | max(a2.val2) | count(*) |
		+--------------+--------------+----------+
		|            3 |            1 |        8 |
		+--------------+--------------+----------+
		1 row in set (0.00 sec)
	*/

	utils.AssertMatches(t, conn, `select /*vt+ PLANNER=gen4 */ a1.val1, count(distinct a1.val2) from aggr_test a1 join aggr_test a2 on a1.val2 = a2.id join t3 t on a2.val2 = t.id7 group by a1.val1`,
		`[[VARCHAR("a") INT64(1)] [VARCHAR("b") INT64(1)] [VARCHAR("c") INT64(1)]]`)
}

func TestNotEqualFilterOnScatter(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'b',2), (3,'c',3), (4,'d',4), (5,'e',5)")

	workloads := []string{"oltp", "olap"}
	for _, workload := range workloads {
		t.Run(workload, func(t *testing.T) {
			utils.Exec(t, conn, fmt.Sprintf("set workload = '%s'", workload))

			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a != 5", `[]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having 5 != a", `[]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a != a", `[]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a != 3+2", `[]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a != 1", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a != \"1\"", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a != \"5\"", `[]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a != 5.00", `[]`)

			utils.AssertContainsError(t, conn, "select /*vt+ PLANNER=gen4 */ 1 from aggr_test having count(*) != 5", `expr cannot be translated, not supported`) // will fail since `count(*)` is a FuncExpr
		})
	}
}

func TestLessFilterOnScatter(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'b',2), (3,'c',3), (4,'d',4), (5,'e',5)")

	workloads := []string{"oltp", "olap"}
	for _, workload := range workloads {
		t.Run(workload, func(t *testing.T) {
			utils.Exec(t, conn, fmt.Sprintf("set workload = '%s'", workload))
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a < 10", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having 1 < a", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a < a", `[]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a < 3+2", `[]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a < 1", `[]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a < \"10\"", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a < \"5\"", `[]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a < 6.00", `[[INT64(5)]]`)

			utils.AssertContainsError(t, conn, "select /*vt+ PLANNER=gen4 */ 1 from aggr_test having count(*) < 5", `expr cannot be translated, not supported`) // will fail since `count(*)` is a FuncExpr
		})
	}
}

func TestLessEqualFilterOnScatter(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'b',2), (3,'c',3), (4,'d',4), (5,'e',5)")

	workloads := []string{"oltp", "olap"}
	for _, workload := range workloads {
		t.Run(workload, func(t *testing.T) {
			utils.Exec(t, conn, fmt.Sprintf("set workload = '%s'", workload))

			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a <= 10", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having 1 <= a", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a <= a", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a <= 3+2", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a <= 1", `[]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a <= \"10\"", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a <= \"5\"", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a <= 5.00", `[[INT64(5)]]`)

			utils.AssertContainsError(t, conn, "select /*vt+ PLANNER=gen4 */ 1 from aggr_test having count(*) <= 5", `expr cannot be translated, not supported`) // will fail since `count(*)` is a FuncExpr
		})
	}
}

func TestGreaterFilterOnScatter(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'b',2), (3,'c',3), (4,'d',4), (5,'e',5)")

	workloads := []string{"oltp", "olap"}
	for _, workload := range workloads {
		t.Run(workload, func(t *testing.T) {
			utils.Exec(t, conn, fmt.Sprintf("set workload = '%s'", workload))

			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a > 1", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having 1 > a", `[]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a > a", `[]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a > 3+1", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a > 10", `[]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a > \"1\"", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a > \"5\"", `[]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a > 4.00", `[[INT64(5)]]`)

			utils.AssertContainsError(t, conn, "select /*vt+ PLANNER=gen4 */ 1 from aggr_test having count(*) > 5", `expr cannot be translated, not supported`) // will fail since `count(*)` is a FuncExpr
		})
	}
}

func TestGreaterEqualFilterOnScatter(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.Exec(t, conn, "insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'b',2), (3,'c',3), (4,'d',4), (5,'e',5)")

	workloads := []string{"oltp", "olap"}
	for _, workload := range workloads {
		t.Run(workload, func(t *testing.T) {
			utils.Exec(t, conn, fmt.Sprintf("set workload = '%s'", workload))

			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a >= 1", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having 1 >= a", `[]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a >= a", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a >= 3+2", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a >= 10", `[]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a >= \"1\"", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a >= \"5\"", `[[INT64(5)]]`)
			utils.AssertMatches(t, conn, "select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a >= 5.00", `[[INT64(5)]]`)

			utils.AssertContainsError(t, conn, "select /*vt+ PLANNER=gen4 */ 1 from aggr_test having count(*) >= 5", `expr cannot be translated, not supported`) // will fail since `count(*)` is a FuncExpr
		})
	}
}
