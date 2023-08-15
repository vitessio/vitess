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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, mcmp.VtConn, "set workload = oltp")

		tables := []string{"t9", "aggr_test", "t3", "t7_xxhash", "aggr_test_dates", "t7_xxhash_idx", "t1", "t2", "t10"}
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

	mcmp.AssertMatches("select ascii(val1) as a, count(*) from aggr_test group by a", `[[INT32(65) INT64(1)] [INT32(69) INT64(1)] [INT32(97) INT64(1)] [INT32(98) INT64(1)] [INT32(99) INT64(2)] [INT32(100) INT64(1)] [INT32(101) INT64(1)]]`)
	mcmp.AssertMatches("select ascii(val1) as a, count(*) from aggr_test group by a order by a", `[[INT32(65) INT64(1)] [INT32(69) INT64(1)] [INT32(97) INT64(1)] [INT32(98) INT64(1)] [INT32(99) INT64(2)] [INT32(100) INT64(1)] [INT32(101) INT64(1)]]`)
	mcmp.AssertMatches("select ascii(val1) as a, count(*) from aggr_test group by a order by 2, a", `[[INT32(65) INT64(1)] [INT32(69) INT64(1)] [INT32(97) INT64(1)] [INT32(98) INT64(1)] [INT32(100) INT64(1)] [INT32(101) INT64(1)] [INT32(99) INT64(2)]]`)

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
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ id6+id7, count(*) k from t3 group by id6+id7 order by k", `[[INT64(9) INT64(1)] [INT64(6) INT64(2)] [INT64(3) INT64(3)]]`)

	// Test the same queries in streaming mode
	utils.Exec(t, mcmp.VtConn, "set workload = olap")
	mcmp.AssertMatches("select id6, id7, count(*) k from t3 group by id6, id7 order by k", `[[INT64(3) INT64(6) INT64(1)] [INT64(2) INT64(4) INT64(2)] [INT64(1) INT64(2) INT64(3)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ id6+id7, count(*) k from t3 group by id6+id7 order by k", `[[INT64(9) INT64(1)] [INT64(6) INT64(2)] [INT64(3) INT64(3)]]`)
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
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ 1 from aggr_test having count(*) = 5", `[[INT64(1)]]`)
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

	// having on aggregation on top of join
	mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ a.val1, count(*) from aggr_test a join t3 t on a.val2 = t.id7 group by a.val1 having count(*) = 4",
		`[[VARCHAR("a") INT64(4)]]`)

	mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ a.val1, count(*) as leCount from aggr_test a join t3 t on a.val2 = t.id7 group by a.val1 having leCount = 4",
		`[[VARCHAR("a") INT64(4)]]`)

	mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ a.val1 from aggr_test a join t3 t on a.val2 = t.id7 group by a.val1 having count(*) = 4",
		`[[VARCHAR("a")]]`)
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
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ 1 from aggr_test having count(*) != 5", `[]`)
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
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ 1 from aggr_test having count(*) < 5", `[]`)
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
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ 1 from aggr_test having count(*) <= 5", `[[INT64(1)]]`)
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
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ 1 from aggr_test having count(*) > 5", `[]`)
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
			mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ 1 from aggr_test having count(*) >= 5", `[[INT64(1)]]`)
		})
	}
}

func TestGroupByOnlyFullGroupByOff(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t9(id1, id2, id3) values(1,'a', '1'), (2,'Abc','2'), (3,'b', '3'), (4,'c', '4'), (5,'test', '5')")
	mcmp.Exec("insert into t9(id1, id2, id3) values(6,'a', '11'), (7,'Abc','22'), (8,'b', '33'), (9,'c', '44'), (10,'test', '55')")
	mcmp.Exec("set @@sql_mode = ' '")

	// We do not use AssertMatches here because the results for the second column are random
	_, err := mcmp.ExecAndIgnore("select /*vt+ PLANNER=gen4 */ id2, id3 from t9 group by id2")
	require.NoError(t, err)
}

func TestAggOnTopOfLimit(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',6), (2,'a',1), (3,'b',1), (4,'c',3), (5,'c',4), (6,'b',null), (7,null,2), (8,null,null)")

	for _, workload := range []string{"oltp", "olap"} {
		t.Run(workload, func(t *testing.T) {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("set workload = '%s'", workload))
			mcmp.AssertMatches(" select /*vt+ PLANNER=gen4 */ count(*) from (select id, val1 from aggr_test where val2 < 4 limit 2) as x", "[[INT64(2)]]")
			mcmp.AssertMatches(" select /*vt+ PLANNER=gen4 */ count(val1) from (select id, val1 from aggr_test where val2 < 4 order by val1 desc limit 2) as x", "[[INT64(2)]]")
			mcmp.AssertMatches(" select /*vt+ PLANNER=gen4 */ count(*) from (select id, val1 from aggr_test where val2 is null limit 2) as x", "[[INT64(2)]]")
			mcmp.AssertMatches(" select /*vt+ PLANNER=gen4 */ count(val1) from (select id, val1 from aggr_test where val2 is null limit 2) as x", "[[INT64(1)]]")
			mcmp.AssertMatches(" select /*vt+ PLANNER=gen4 */ count(val2) from (select id, val2 from aggr_test where val2 is null limit 2) as x", "[[INT64(0)]]")
			mcmp.AssertMatches(" select /*vt+ PLANNER=gen4 */ val1, count(*) from (select id, val1 from aggr_test where val2 < 4 order by val1 limit 2) as x group by val1", `[[NULL INT64(1)] [VARCHAR("a") INT64(1)]]`)
			mcmp.AssertMatchesNoOrder(" select /*vt+ PLANNER=gen4 */ val1, count(val2) from (select val1, val2 from aggr_test limit 8) as x group by val1", `[[NULL INT64(1)] [VARCHAR("a") INT64(2)] [VARCHAR("b") INT64(1)] [VARCHAR("c") INT64(2)]]`)

			// mysql returns FLOAT64(0), vitess returns DECIMAL(0)
			mcmp.AssertMatchesNoCompare(" select /*vt+ PLANNER=gen4 */ count(*), sum(val1) from (select id, val1 from aggr_test where val2 < 4 order by val1 desc limit 2) as x", "[[INT64(2) FLOAT64(0)]]", "[[INT64(2) DECIMAL(0)]]")
			mcmp.AssertMatches(" select /*vt+ PLANNER=gen4 */ count(val1), sum(id) from (select id, val1 from aggr_test where val2 < 4 order by val1 desc limit 2) as x", "[[INT64(2) DECIMAL(7)]]")
			mcmp.AssertMatches(" select /*vt+ PLANNER=gen4 */ count(*), sum(id) from (select id, val1 from aggr_test where val2 is null limit 2) as x", "[[INT64(2) DECIMAL(14)]]")
			mcmp.AssertMatches(" select /*vt+ PLANNER=gen4 */ count(val1), sum(id) from (select id, val1 from aggr_test where val2 is null limit 2) as x", "[[INT64(1) DECIMAL(14)]]")
			mcmp.AssertMatches(" select /*vt+ PLANNER=gen4 */ count(val2), sum(val2) from (select id, val2 from aggr_test where val2 is null limit 2) as x", "[[INT64(0) NULL]]")
			mcmp.AssertMatches(" select /*vt+ PLANNER=gen4 */ val1, count(*), sum(id) from (select id, val1 from aggr_test where val2 < 4 order by val1 limit 2) as x group by val1", `[[NULL INT64(1) DECIMAL(7)] [VARCHAR("a") INT64(1) DECIMAL(2)]]`)
			mcmp.AssertMatchesNoOrder(" select /*vt+ PLANNER=gen4 */ val1, count(val2), sum(val2) from (select val1, val2 from aggr_test limit 8) as x group by val1", `[[NULL INT64(1) DECIMAL(2)] [VARCHAR("a") INT64(2) DECIMAL(7)] [VARCHAR("b") INT64(1) DECIMAL(1)] [VARCHAR("c") INT64(2) DECIMAL(7)]]`)
		})
	}
}

func TestEmptyTableAggr(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	for _, workload := range []string{"oltp", "olap"} {
		t.Run(workload, func(t *testing.T) {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("set workload = %s", workload))
			mcmp.AssertMatches(" select /*vt+ PLANNER=gen4 */ count(*) from t1 inner join t2 on (t1.t1_id = t2.id) where t1.value = 'foo'", "[[INT64(0)]]")
			mcmp.AssertMatches(" select /*vt+ PLANNER=gen4 */ count(*) from t2 inner join t1 on (t1.t1_id = t2.id) where t1.value = 'foo'", "[[INT64(0)]]")
			mcmp.AssertMatches(" select /*vt+ PLANNER=gen4 */ t1.`name`, count(*) from t2 inner join t1 on (t1.t1_id = t2.id) where t1.value = 'foo' group by t1.`name`", "[]")
			mcmp.AssertMatches(" select /*vt+ PLANNER=gen4 */ t1.`name`, count(*) from t1 inner join t2 on (t1.t1_id = t2.id) where t1.value = 'foo' group by t1.`name`", "[]")
		})
	}

	mcmp.Exec("insert into t1(t1_id, `name`, `value`, shardkey) values(1,'a1','foo',100), (2,'b1','foo',200), (3,'c1','foo',300), (4,'a1','foo',100), (5,'b1','bar',200)")

	for _, workload := range []string{"oltp", "olap"} {
		t.Run(workload, func(t *testing.T) {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("set workload = %s", workload))
			mcmp.AssertMatches(" select /*vt+ PLANNER=gen4 */ count(*) from t1 inner join t2 on (t1.t1_id = t2.id) where t1.value = 'foo'", "[[INT64(0)]]")
			mcmp.AssertMatches(" select /*vt+ PLANNER=gen4 */ count(*) from t2 inner join t1 on (t1.t1_id = t2.id) where t1.value = 'foo'", "[[INT64(0)]]")
			mcmp.AssertMatches(" select /*vt+ PLANNER=gen4 */ t1.`name`, count(*) from t1 inner join t2 on (t1.t1_id = t2.id) where t1.value = 'foo' group by t1.`name`", "[]")
			mcmp.AssertMatches(" select /*vt+ PLANNER=gen4 */ t1.`name`, count(*) from t2 inner join t1 on (t1.t1_id = t2.id) where t1.value = 'foo' group by t1.`name`", "[]")
		})
	}

}

func TestOrderByCount(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t9(id1, id2, id3) values(1, '1', '1'), (2, '2', '2'), (3, '2', '2'), (4, '3', '3'), (5, '3', '3'), (6, '3', '3')")

	mcmp.AssertMatches("SELECT /*vt+ PLANNER=gen4 */ t9.id2 FROM t9 GROUP BY t9.id2 ORDER BY COUNT(t9.id2) DESC", `[[VARCHAR("3")] [VARCHAR("2")] [VARCHAR("1")]]`)
}

func TestAggregateRandom(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(t1_id, name, value, shardKey) values (1, 'name 1', 'value 1', 1), (2, 'name 2', 'value 2', 2)")
	mcmp.Exec("insert into t2(id, shardKey) values (1, 10), (2, 20)")

	mcmp.AssertMatches("SELECT /*vt+ PLANNER=gen4 */ t1.shardKey, t1.name, count(t2.id) FROM t1 JOIN t2 ON t1.value != t2.shardKey GROUP BY t1.t1_id", `[[INT64(1) VARCHAR("name 1") INT64(2)] [INT64(2) VARCHAR("name 2") INT64(2)]]`)

	mcmp.Exec("set sql_mode=''")
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ tbl0.comm, count(*) from emp as tbl0, emp as tbl1 where tbl0.empno = tbl1.deptno", `[[NULL INT64(0)]]`)
}

// TestAggregateLeftJoin tests that aggregates work with left joins and does not ignore the count when column value does not match the right side table.
func TestAggregateLeftJoin(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(t1_id, name, value, shardKey) values (11, 'r', 'r', 1), (3, 'r', 'r', 0)")
	mcmp.Exec("insert into t2(id, shardKey) values (11, 1)")

	mcmp.AssertMatchesNoOrder("SELECT t1.shardkey FROM t1 LEFT JOIN t2 ON t1.t1_id = t2.id", `[[INT64(1)] [INT64(0)]]`)
	mcmp.AssertMatches("SELECT count(t1.shardkey) FROM t1 LEFT JOIN t2 ON t1.t1_id = t2.id", `[[INT64(2)]]`)
	mcmp.AssertMatches("SELECT count(t2.shardkey) FROM t1 LEFT JOIN t2 ON t1.t1_id = t2.id", `[[INT64(1)]]`)
	mcmp.AssertMatches("SELECT count(*) FROM t1 LEFT JOIN t2 ON t1.t1_id = t2.id", `[[INT64(2)]]`)
	mcmp.AssertMatches("SELECT sum(t1.shardkey) FROM t1 LEFT JOIN t2 ON t1.t1_id = t2.id", `[[DECIMAL(1)]]`)
	mcmp.AssertMatches("SELECT sum(t2.shardkey) FROM t1 LEFT JOIN t2 ON t1.t1_id = t2.id", `[[DECIMAL(1)]]`)
	mcmp.AssertMatches("SELECT count(*) FROM t2 LEFT JOIN t1 ON t1.t1_id = t2.id WHERE IFNULL(t1.name, 'NOTSET') = 'r'", `[[INT64(1)]]`)
}

// TestScalarAggregate tests validates that only count is returned and no additional field is returned.gst
func TestScalarAggregate(t *testing.T) {
	// disable schema tracking to have weight_string column added to query send down to mysql.
	clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, "--schema_change_signal=false")
	require.NoError(t,
		clusterInstance.RestartVtgate())

	// update vtgate params
	vtParams = clusterInstance.GetVTParams(keyspaceName)

	defer func() {
		// roll it back
		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, "--schema_change_signal")
		require.NoError(t,
			clusterInstance.RestartVtgate())
		//  update vtgate params
		vtParams = clusterInstance.GetVTParams(keyspaceName)

	}()

	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'A',1), (3,'b',1), (4,'c',3), (5,'c',4)")
	mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */ count(distinct val1) from aggr_test", `[[INT64(3)]]`)
}

func TestAggregationRandomOnAnAggregatedValue(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t10(k, a, b) values (0, 100, 10), (10, 200, 20);")

	mcmp.AssertMatchesNoOrder("select /*vt+ PLANNER=gen4 */ A.a, A.b, (A.a / A.b) as d from (select sum(a) as a, sum(b) as b from t10 where a = 100) A;",
		`[[DECIMAL(100) DECIMAL(10) DECIMAL(10.0000)]]`)
}

func TestBuggyQueries(t *testing.T) {
	// These queries have been found to be producing the wrong results by the query fuzzer
	// Adding them as end2end tests to make sure we never get them wrong again
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t10(k, a, b) values (0, 100, 10), (10, 200, 20), (20, null, null)")

	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ sum(t1.a) from t10 as t1, t10 as t2",
		`[[DECIMAL(900)]]`)

	mcmp.AssertMatches("select /*vt+ PLANNER=gen4 */t1.a, sum(t1.a), count(*), t1.a, sum(t1.a), count(*) from t10 as t1, t10 as t2 group by t1.a",
		"[[NULL NULL INT64(3) NULL NULL INT64(3)] "+
			"[INT32(100) DECIMAL(300) INT64(3) INT32(100) DECIMAL(300) INT64(3)] "+
			"[INT32(200) DECIMAL(600) INT64(3) INT32(200) DECIMAL(600) INT64(3)]]")

	mcmp.Exec("select /*vt+ PLANNER=gen4 */sum(tbl1.a), min(tbl0.b) from t10 as tbl0, t10 as tbl1 left join t10 as tbl2 on tbl1.a = tbl2.a and tbl1.b = tbl2.k")
	mcmp.Exec("select /*vt+ PLANNER=gen4 */count(*) from t10 left join t10 as t11 on t10.a = t11.b where t11.a")
}

func TestMinMaxAcrossJoins(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("insert into t1(t1_id, name, value, shardKey) values (1, 'name 1', 'value 1', 1), (2, 'name 2', 'value 2', 2)")
	mcmp.Exec("insert into t2(id, shardKey) values (1, 10), (2, 20)")

	mcmp.AssertMatchesNoOrder(
		`SELECT /*vt+ PLANNER=gen4 */ t1.name, max(t1.shardKey), t2.shardKey, min(t2.id) FROM t1 JOIN t2 ON t1.t1_id != t2.shardKey GROUP BY t1.name, t2.shardKey`,
		`[[VARCHAR("name 2") INT64(2) INT64(10) INT64(1)] [VARCHAR("name 1") INT64(1) INT64(10) INT64(1)] [VARCHAR("name 2") INT64(2) INT64(20) INT64(2)] [VARCHAR("name 1") INT64(1) INT64(20) INT64(2)]]`)
}
