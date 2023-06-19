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
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/sqltypes"
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

	// from random/random_test.go
	mcmp.Exec("INSERT INTO emp(empno, ename, job, mgr, hiredate, sal, comm, deptno) VALUES (7369,'SMITH','CLERK',7902,'1980-12-17',800,NULL,20), (7499,'ALLEN','SALESMAN',7698,'1981-02-20',1600,300,30), (7521,'WARD','SALESMAN',7698,'1981-02-22',1250,500,30), (7566,'JONES','MANAGER',7839,'1981-04-02',2975,NULL,20), (7654,'MARTIN','SALESMAN',7698,'1981-09-28',1250,1400,30), (7698,'BLAKE','MANAGER',7839,'1981-05-01',2850,NULL,30), (7782,'CLARK','MANAGER',7839,'1981-06-09',2450,NULL,10), (7788,'SCOTT','ANALYST',7566,'1982-12-09',3000,NULL,20), (7839,'KING','PRESIDENT',NULL,'1981-11-17',5000,NULL,10), (7844,'TURNER','SALESMAN',7698,'1981-09-08',1500,0,30), (7876,'ADAMS','CLERK',7788,'1983-01-12',1100,NULL,20), (7900,'JAMES','CLERK',7698,'1981-12-03',950,NULL,30), (7902,'FORD','ANALYST',7566,'1981-12-03',3000,NULL,20), (7934,'MILLER','CLERK',7782,'1982-01-23',1300,NULL,10)")
	mcmp.Exec("INSERT INTO dept(deptno, dname, loc) VALUES ('10','ACCOUNTING','NEW YORK'), ('20','RESEARCH','DALLAS'), ('30','SALES','CHICAGO'), ('40','OPERATIONS','BOSTON')")

	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ count(*), count(*), count(*) from dept as tbl0, emp as tbl1 where tbl0.deptno = tbl1.deptno group by tbl1.empno order by tbl1.empno",
		`[[INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)] [INT64(1) INT64(1) INT64(1)]]`)
	//mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ count(tbl0.deptno) from dept as tbl0, emp as tbl1 group by tbl1.job order by tbl1.job limit 3",
	//	`[[INT64(8)] [INT64(16)] [INT64(12)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ count(*), count(*) from emp as tbl0 group by tbl0.empno order by tbl0.empno",
		`[[INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)] [INT64(1) INT64(1)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ distinct count(*), tbl0.loc from dept as tbl0 group by tbl0.loc",
		`[[INT64(1) VARCHAR("BOSTON")] [INT64(1) VARCHAR("CHICAGO")] [INT64(1) VARCHAR("DALLAS")] [INT64(1) VARCHAR("NEW YORK")]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ distinct count(*) from dept as tbl0 group by tbl0.loc",
		`[[INT64(1)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ sum(tbl1.comm) from emp as tbl0, emp as tbl1",
		`[[DECIMAL(30800)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ tbl1.mgr, tbl1.mgr, count(*) from emp as tbl1 group by tbl1.mgr",
		`[[NULL NULL INT64(1)] [INT64(7566) INT64(7566) INT64(2)] [INT64(7698) INT64(7698) INT64(5)] [INT64(7782) INT64(7782) INT64(1)] [INT64(7788) INT64(7788) INT64(1)] [INT64(7839) INT64(7839) INT64(3)] [INT64(7902) INT64(7902) INT64(1)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ tbl1.mgr, tbl1.mgr, count(*) from emp as tbl0, emp as tbl1 group by tbl1.mgr",
		`[[NULL NULL INT64(14)] [INT64(7566) INT64(7566) INT64(28)] [INT64(7698) INT64(7698) INT64(70)] [INT64(7782) INT64(7782) INT64(14)] [INT64(7788) INT64(7788) INT64(14)] [INT64(7839) INT64(7839) INT64(42)] [INT64(7902) INT64(7902) INT64(14)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ count(*), count(*), count(tbl0.comm) from emp as tbl0, emp as tbl1 join dept as tbl2",
		`[[INT64(784) INT64(784) INT64(224)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ count(*), count(*) from (select count(*) from dept as tbl0 group by tbl0.deptno) as tbl0, dept as tbl1",
		`[[INT64(16) INT64(16)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ count(*) from (select count(*) from dept as tbl0 group by tbl0.deptno) as tbl0",
		`[[INT64(4)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ min(tbl0.loc) from dept as tbl0",
		`[[VARCHAR("BOSTON")]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ tbl1.empno, max(tbl1.job) from dept as tbl0, emp as tbl1 group by tbl1.empno",
		`[[INT64(7369) VARCHAR("CLERK")] [INT64(7499) VARCHAR("SALESMAN")] [INT64(7521) VARCHAR("SALESMAN")] [INT64(7566) VARCHAR("MANAGER")] [INT64(7654) VARCHAR("SALESMAN")] [INT64(7698) VARCHAR("MANAGER")] [INT64(7782) VARCHAR("MANAGER")] [INT64(7788) VARCHAR("ANALYST")] [INT64(7839) VARCHAR("PRESIDENT")] [INT64(7844) VARCHAR("SALESMAN")] [INT64(7876) VARCHAR("CLERK")] [INT64(7900) VARCHAR("CLERK")] [INT64(7902) VARCHAR("ANALYST")] [INT64(7934) VARCHAR("CLERK")]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ tbl1.ename, max(tbl0.comm) from emp as tbl0, emp as tbl1 group by tbl1.ename",
		`[[VARCHAR("ADAMS") INT64(1400)] [VARCHAR("ALLEN") INT64(1400)] [VARCHAR("BLAKE") INT64(1400)] [VARCHAR("CLARK") INT64(1400)] [VARCHAR("FORD") INT64(1400)] [VARCHAR("JAMES") INT64(1400)] [VARCHAR("JONES") INT64(1400)] [VARCHAR("KING") INT64(1400)] [VARCHAR("MARTIN") INT64(1400)] [VARCHAR("MILLER") INT64(1400)] [VARCHAR("SCOTT") INT64(1400)] [VARCHAR("SMITH") INT64(1400)] [VARCHAR("TURNER") INT64(1400)] [VARCHAR("WARD") INT64(1400)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ tbl0.dname, tbl0.dname, min(tbl0.deptno) from dept as tbl0, dept as tbl1 group by tbl0.dname, tbl0.dname",
		`[[VARCHAR("ACCOUNTING") VARCHAR("ACCOUNTING") INT64(10)] [VARCHAR("OPERATIONS") VARCHAR("OPERATIONS") INT64(40)] [VARCHAR("RESEARCH") VARCHAR("RESEARCH") INT64(20)] [VARCHAR("SALES") VARCHAR("SALES") INT64(30)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ tbl0.dname, min(tbl1.deptno) from dept as tbl0, dept as tbl1 group by tbl0.dname, tbl1.dname",
		`[[VARCHAR("ACCOUNTING") INT64(10)] [VARCHAR("ACCOUNTING") INT64(40)] [VARCHAR("ACCOUNTING") INT64(20)] [VARCHAR("ACCOUNTING") INT64(30)] [VARCHAR("OPERATIONS") INT64(10)] [VARCHAR("OPERATIONS") INT64(40)] [VARCHAR("OPERATIONS") INT64(20)] [VARCHAR("OPERATIONS") INT64(30)] [VARCHAR("RESEARCH") INT64(10)] [VARCHAR("RESEARCH") INT64(40)] [VARCHAR("RESEARCH") INT64(20)] [VARCHAR("RESEARCH") INT64(30)] [VARCHAR("SALES") INT64(10)] [VARCHAR("SALES") INT64(40)] [VARCHAR("SALES") INT64(20)] [VARCHAR("SALES") INT64(30)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ max(tbl0.hiredate) from emp as tbl0",
		`[[DATE("1983-01-12")]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ min(tbl0.deptno) as caggr0, count(*) as caggr1 from dept as tbl0 left join dept as tbl1 on tbl1.loc = tbl1.dname",
		`[[INT64(10) INT64(4)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ count(tbl1.loc) as caggr0 from dept as tbl1 left join dept as tbl2 on tbl1.loc = tbl2.loc where (tbl2.deptno)",
		`[[INT64(4)]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ sum(tbl1.ename), min(tbl0.empno) from emp as tbl0, emp as tbl1 left join dept as tbl2 on tbl1.job = tbl2.loc and tbl1.comm = tbl2.deptno where ('trout') and tbl0.deptno = tbl1.comm",
		`[[NULL NULL]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ distinct max(tbl0.deptno), count(tbl0.job) from emp as tbl0, dept as tbl1 left join dept as tbl2 on tbl1.dname = tbl2.loc and tbl1.dname = tbl2.loc where (tbl2.loc) and tbl0.deptno = tbl1.deptno",
		`[[NULL INT64(0)]]`)

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

func TestComplexAggregation(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("insert into t1(t1_id, `name`, `value`, shardkey) values(1,'a1','foo',100), (2,'b1','foo',200), (3,'c1','foo',300), (4,'a1','foo',100), (5,'d1','toto',200), (6,'c1','tata',893), (7,'a1','titi',2380), (8,'b1','tete',12833), (9,'e1','yoyo',783493)")

	mcmp.Exec("set @@sql_mode = ' '")
	mcmp.Exec(`SELECT /*vt+ PLANNER=gen4 */ 1+COUNT(t1_id) FROM t1`)
	mcmp.Exec(`SELECT /*vt+ PLANNER=gen4 */ COUNT(t1_id)+1 FROM t1`)
	mcmp.Exec(`SELECT /*vt+ PLANNER=gen4 */ COUNT(t1_id)+MAX(shardkey) FROM t1`)
	mcmp.Exec(`SELECT /*vt+ PLANNER=gen4 */ shardkey, MIN(t1_id)+MAX(t1_id) FROM t1 GROUP BY shardkey`)
	mcmp.Exec(`SELECT /*vt+ PLANNER=gen4 */ shardkey + MIN(t1_id)+MAX(t1_id) FROM t1 GROUP BY shardkey`)
	mcmp.Exec(`SELECT /*vt+ PLANNER=gen4 */ name+COUNT(t1_id)+1 FROM t1 GROUP BY name`)
	mcmp.Exec(`SELECT /*vt+ PLANNER=gen4 */ COUNT(*)+shardkey+MIN(t1_id)+1+MAX(t1_id)*SUM(t1_id)+1+name FROM t1 GROUP BY shardkey, name`)
}

// TestGroupConcatAggregation tests the group_concat function with vitess doing the aggregation.
func TestGroupConcatAggregation(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("insert into t1(t1_id, `name`, `value`, shardkey) values(1,'a1',null,100), (2,'b1','foo',20), (3,'c1','foo',10), (4,'a1','foo',100), (5,'d1','toto',200), (6,'c1',null,893), (10,'a1','titi',2380), (20,'b1','tete',12833), (9,'e1','yoyo',783493)")
	mcmp.Exec("insert into t2(id, shardKey) values (1, 10), (2, 20)")

	mQr, vtQr := mcmp.ExecNoCompare(`SELECT /*vt+ PLANNER=gen4 */ group_concat(name) FROM t1`)
	compareRow(t, mQr, vtQr, nil, []int{0})
	mQr, vtQr = mcmp.ExecNoCompare(`SELECT /*vt+ PLANNER=gen4 */ group_concat(value) FROM t1 join t2 on t1.shardKey = t2.shardKey `)
	compareRow(t, mQr, vtQr, nil, []int{0})
	mQr, vtQr = mcmp.ExecNoCompare(`SELECT /*vt+ PLANNER=gen4 */ group_concat(value) FROM t1 join t2 on t1.t1_id = t2.shardKey `)
	compareRow(t, mQr, vtQr, nil, []int{0})
	mQr, vtQr = mcmp.ExecNoCompare(`SELECT /*vt+ PLANNER=gen4 */ group_concat(value) FROM t1 join t2 on t1.shardKey = t2.id `)
	compareRow(t, mQr, vtQr, nil, []int{0})
	mQr, vtQr = mcmp.ExecNoCompare(`SELECT /*vt+ PLANNER=gen4 */ group_concat(value), t1.name FROM t1, t2 group by t1.name`)
	compareRow(t, mQr, vtQr, []int{1}, []int{0})
}

func compareRow(t *testing.T, mRes *sqltypes.Result, vtRes *sqltypes.Result, grpCols []int, fCols []int) {
	require.Equal(t, len(mRes.Rows), len(vtRes.Rows), "mysql and vitess result count does not match")
	for _, row := range vtRes.Rows {
		var grpKey string
		for _, col := range grpCols {
			grpKey += row[col].String()
		}
		var foundKey bool
		for _, mRow := range mRes.Rows {
			var mKey string
			for _, col := range grpCols {
				mKey += mRow[col].String()
			}
			if grpKey != mKey {
				continue
			}
			foundKey = true
			for _, col := range fCols {
				vtFValSplit := strings.Split(row[col].ToString(), ",")
				sort.Strings(vtFValSplit)
				mFValSplit := strings.Split(mRow[col].ToString(), ",")
				sort.Strings(mFValSplit)
				require.True(t, slices.Equal(vtFValSplit, mFValSplit), "mysql and vitess result are not same: vitess:%v, mysql:%v", vtRes.Rows, mRes.Rows)
			}
		}
		require.True(t, foundKey, "mysql and vitess result does not same row: vitess:%v, mysql:%v", vtRes.Rows, mRes.Rows)
	}
}
