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
	"fmt"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, mcmp.VtConn, "set workload = oltp")

		tables := []string{"aggr_test", "t3", "t7_xxhash", "aggr_test_dates", "t7_xxhash_idx"}
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

func TestAggregateTypes(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'A',1), (3,'b',1), (4,'c',3), (5,'c',4)")
	mcmp.Exec("insert into aggr_test(id, val1, val2) values(6,'d',null), (7,'e',null), (8,'E',1)")
	mcmp.AssertMatches("select val1, count(distinct val2), count(*) from aggr_test group by val1", `[[VARCHAR("a") INT64(1) INT64(2)] [VARCHAR("b") INT64(1) INT64(1)] [VARCHAR("c") INT64(2) INT64(2)] [VARCHAR("d") INT64(0) INT64(1)] [VARCHAR("e") INT64(1) INT64(2)]]`)
	mcmp.AssertMatches("select val1, sum(distinct val2), sum(val2) from aggr_test group by val1", `[[VARCHAR("a") DECIMAL(1) DECIMAL(2)] [VARCHAR("b") DECIMAL(1) DECIMAL(1)] [VARCHAR("c") DECIMAL(7) DECIMAL(7)] [VARCHAR("d") NULL NULL] [VARCHAR("e") DECIMAL(1) DECIMAL(1)]]`)
	mcmp.AssertMatches("select val1, count(distinct val2) k, count(*) from aggr_test group by val1 order by k desc, val1", `[[VARCHAR("c") INT64(2) INT64(2)] [VARCHAR("a") INT64(1) INT64(2)] [VARCHAR("b") INT64(1) INT64(1)] [VARCHAR("e") INT64(1) INT64(2)] [VARCHAR("d") INT64(0) INT64(1)]]`)
	mcmp.AssertMatches("select val1, count(distinct val2) k, count(*) from aggr_test group by val1 order by k desc, val1 limit 4", `[[VARCHAR("c") INT64(2) INT64(2)] [VARCHAR("a") INT64(1) INT64(2)] [VARCHAR("b") INT64(1) INT64(1)] [VARCHAR("e") INT64(1) INT64(2)]]`)

	// TODO (@frouioui): following assertions produce different results between MySQL and Vitess
	//  their differences are ignored for now. Fix it.
	// `ascii(val1)` returns an `INT64` on Vitess, and `INT32` on MySQL
	utils.AssertMatches(t, mcmp.VtConn, "select ascii(val1) as a, count(*) from aggr_test group by a", `[[INT64(65) INT64(1)] [INT64(69) INT64(1)] [INT64(97) INT64(1)] [INT64(98) INT64(1)] [INT64(99) INT64(2)] [INT64(100) INT64(1)] [INT64(101) INT64(1)]]`)
	utils.AssertMatches(t, mcmp.VtConn, "select ascii(val1) as a, count(*) from aggr_test group by a order by a", `[[INT64(65) INT64(1)] [INT64(69) INT64(1)] [INT64(97) INT64(1)] [INT64(98) INT64(1)] [INT64(99) INT64(2)] [INT64(100) INT64(1)] [INT64(101) INT64(1)]]`)
	utils.AssertMatches(t, mcmp.VtConn, "select ascii(val1) as a, count(*) from aggr_test group by a order by 2, a", `[[INT64(65) INT64(1)] [INT64(69) INT64(1)] [INT64(97) INT64(1)] [INT64(98) INT64(1)] [INT64(100) INT64(1)] [INT64(101) INT64(1)] [INT64(99) INT64(2)]]`)

	mcmp.AssertMatches("select val1 as a, count(*) from aggr_test group by a", `[[VARCHAR("a") INT64(2)] [VARCHAR("b") INT64(1)] [VARCHAR("c") INT64(2)] [VARCHAR("d") INT64(1)] [VARCHAR("e") INT64(2)]]`)
	mcmp.AssertMatches("select val1 as a, count(*) from aggr_test group by a order by a", `[[VARCHAR("a") INT64(2)] [VARCHAR("b") INT64(1)] [VARCHAR("c") INT64(2)] [VARCHAR("d") INT64(1)] [VARCHAR("e") INT64(2)]]`)
	mcmp.AssertMatches("select val1 as a, count(*) from aggr_test group by a order by 2, a", `[[VARCHAR("b") INT64(1)] [VARCHAR("d") INT64(1)] [VARCHAR("a") INT64(2)] [VARCHAR("c") INT64(2)] [VARCHAR("e") INT64(2)]]`)
}

func TestGroupBy(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("insert into t3(id5, id6, id7) values(1,1,2), (2,2,4), (3,2,4), (4,1,2), (5,1,2), (6,3,6)")
	// test ordering and group by int column
	mcmp.AssertMatches("select id6, id7, count(*) k from t3 group by id6, id7 order by k", `[[INT64(3) INT64(6) INT64(1)] [INT64(2) INT64(4) INT64(2)] [INT64(1) INT64(2) INT64(3)]]`)

	// Test the same queries in streaming mode
	utils.Exec(t, mcmp.VtConn, "set workload = olap")
	mcmp.AssertMatches("select id6, id7, count(*) k from t3 group by id6, id7 order by k", `[[INT64(3) INT64(6) INT64(1)] [INT64(2) INT64(4) INT64(2)] [INT64(1) INT64(2) INT64(3)]]`)
}

func TestDistinct(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("insert into t3(id5,id6,id7) values(1,3,3), (2,3,4), (3,3,6), (4,5,7), (5,5,6)")
	mcmp.Exec("insert into t7_xxhash(uid,phone) values('1',4), ('2',4), ('3',3), ('4',1), ('5',1)")
	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'A',1), (3,'b',1), (4,'c',3), (5,'c',4)")
	mcmp.Exec("insert into aggr_test(id, val1, val2) values(6,'d',null), (7,'e',null), (8,'E',1)")
	mcmp.AssertMatches("select distinct val2, count(*) from aggr_test group by val2", `[[NULL INT64(2)] [INT64(1) INT64(4)] [INT64(3) INT64(1)] [INT64(4) INT64(1)]]`)
	mcmp.AssertMatches("select distinct id6 from t3 join t7_xxhash on t3.id5 = t7_xxhash.phone", `[[INT64(3)] [INT64(5)]]`)
	mcmp.Exec("delete from t3")
	mcmp.Exec("delete from t7_xxhash")
	mcmp.Exec("delete from aggr_test")
}

func TestEqualFilterOnScatter(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'b',2), (3,'c',3), (4,'d',4), (5,'e',5)")

	workloads := []string{"oltp", "olap"}
	for _, workload := range workloads {
		t.Run(workload, func(t *testing.T) {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("set workload = '%s'", workload))

			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having 1 = 1", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a = 5", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having 5 = a", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a = a", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a = 3+2", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having 1+4 = 3+2", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a = 1", `[]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a = \"1\"", `[]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a = \"5\"", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a = 5.00", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a, val1 from aggr_test group by val1 having a = 1.00", `[[INT64(1) VARCHAR("a")] [INT64(1) VARCHAR("b")] [INT64(1) VARCHAR("c")] [INT64(1) VARCHAR("d")] [INT64(1) VARCHAR("e")]]`)

			utils.AssertContainsError(t, mcmp.VtConn, "select /*vt+ PLANNER=gen4 */ 1 from aggr_test having count(*) = 5", `expr cannot be translated, not supported`) // will fail since `count(*)` is a FuncExpr
		})
	}
}

func TestAggrOnJoin(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t3(id5, id6, id7) values(1,1,1), (2,2,4), (3,2,4), (4,1,2), (5,1,1), (6,3,6)")
	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'a',1), (3,'b',1), (4,'c',3), (5,'c',4)")

	mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) from aggr_test a join t3 t on a.val2 = t.id7",
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
	mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ a.val1, count(*) from aggr_test a join t3 t on a.val2 = t.id7 group by a.val1",
		`[[VARCHAR("a") INT64(4)] [VARCHAR("b") INT64(2)] [VARCHAR("c") INT64(2)]]`)
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

	mcmp.AssertMatches(`select /*vt+ PLANNER=gen4 */ max(a1.val2), max(a2.val2), count(*) from aggr_test a1 join aggr_test a2 on a1.val2 = a2.id join t3 t on a2.val2 = t.id7`,
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

	mcmp.AssertMatches(`select /*vt+ PLANNER=gen4 */ a1.val1, count(distinct a1.val2) from aggr_test a1 join aggr_test a2 on a1.val2 = a2.id join t3 t on a2.val2 = t.id7 group by a1.val1`,
		`[[VARCHAR("a") INT64(1)] [VARCHAR("b") INT64(1)] [VARCHAR("c") INT64(1)]]`)
}

func TestNotEqualFilterOnScatter(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'b',2), (3,'c',3), (4,'d',4), (5,'e',5)")

	workloads := []string{"oltp", "olap"}
	for _, workload := range workloads {
		t.Run(workload, func(t *testing.T) {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("set workload = '%s'", workload))

			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a != 5", `[]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having 5 != a", `[]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a != a", `[]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a != 3+2", `[]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a != 1", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a != \"1\"", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a != \"5\"", `[]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a != 5.00", `[]`)

			utils.AssertContainsError(t, mcmp.VtConn, "select /*vt+ PLANNER=gen4 */ 1 from aggr_test having count(*) != 5", `expr cannot be translated, not supported`) // will fail since `count(*)` is a FuncExpr
		})
	}
}

func TestLessFilterOnScatter(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'b',2), (3,'c',3), (4,'d',4), (5,'e',5)")

	workloads := []string{"oltp", "olap"}
	for _, workload := range workloads {
		t.Run(workload, func(t *testing.T) {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("set workload = '%s'", workload))
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a < 10", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having 1 < a", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a < a", `[]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a < 3+2", `[]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a < 1", `[]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a < \"10\"", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a < \"5\"", `[]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a < 6.00", `[[INT64(5)]]`)

			utils.AssertContainsError(t, mcmp.VtConn, "select /*vt+ PLANNER=gen4 */ 1 from aggr_test having count(*) < 5", `expr cannot be translated, not supported`) // will fail since `count(*)` is a FuncExpr
		})
	}
}

func TestLessEqualFilterOnScatter(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'b',2), (3,'c',3), (4,'d',4), (5,'e',5)")

	workloads := []string{"oltp", "olap"}
	for _, workload := range workloads {
		t.Run(workload, func(t *testing.T) {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("set workload = '%s'", workload))

			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a <= 10", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having 1 <= a", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a <= a", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a <= 3+2", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a <= 1", `[]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a <= \"10\"", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a <= \"5\"", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a <= 5.00", `[[INT64(5)]]`)

			utils.AssertContainsError(t, mcmp.VtConn, "select /*vt+ PLANNER=gen4 */ 1 from aggr_test having count(*) <= 5", `expr cannot be translated, not supported`) // will fail since `count(*)` is a FuncExpr
		})
	}
}

func TestGreaterFilterOnScatter(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'b',2), (3,'c',3), (4,'d',4), (5,'e',5)")

	workloads := []string{"oltp", "olap"}
	for _, workload := range workloads {
		t.Run(workload, func(t *testing.T) {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("set workload = '%s'", workload))

			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a > 1", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having 1 > a", `[]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a > a", `[]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a > 3+1", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a > 10", `[]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a > \"1\"", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a > \"5\"", `[]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a > 4.00", `[[INT64(5)]]`)

			utils.AssertContainsError(t, mcmp.VtConn, "select /*vt+ PLANNER=gen4 */ 1 from aggr_test having count(*) > 5", `expr cannot be translated, not supported`) // will fail since `count(*)` is a FuncExpr
		})
	}
}

func TestGreaterEqualFilterOnScatter(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'b',2), (3,'c',3), (4,'d',4), (5,'e',5)")

	workloads := []string{"oltp", "olap"}
	for _, workload := range workloads {
		t.Run(workload, func(t *testing.T) {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("set workload = '%s'", workload))

			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a >= 1", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having 1 >= a", `[]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a >= a", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a >= 3+2", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a >= 10", `[]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a >= \"1\"", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a >= \"5\"", `[[INT64(5)]]`)
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(*) as a from aggr_test having a >= 5.00", `[[INT64(5)]]`)

			utils.AssertContainsError(t, mcmp.VtConn, "select /*vt+ PLANNER=gen4 */ 1 from aggr_test having count(*) >= 5", `expr cannot be translated, not supported`) // will fail since `count(*)` is a FuncExpr
		})
	}
}
