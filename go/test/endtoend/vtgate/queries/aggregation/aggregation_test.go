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
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func start(t *testing.T) (utils.MySQLCompare, func()) {
	// ensure that the vschema and the tables have been created before running any tests
	_ = utils.WaitForAuthoritative(t, keyspaceName, "t1", clusterInstance.VtgateProcess.ReadVSchema)

	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, mcmp.VtConn, "set workload = oltp")

		tables := []string{
			"t3",
			"t3_id7_idx",
			"t9",
			"aggr_test",
			"aggr_test_dates",
			"t7_xxhash",
			"t7_xxhash_idx",
			"t1",
			"t2",
			"t10",
			"emp",
			"dept",
			"bet_logs",
		}
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
	mcmp.AssertMatches("select sum(val1) from aggr_test", `[[FLOAT64(0)]]`)
	mcmp.Run("Average for sharded keyspaces", func(mcmp *utils.MySQLCompare) {
		mcmp.SkipIfBinaryIsBelowVersion(19, "vtgate")
		mcmp.AssertMatches("select avg(val1) from aggr_test", `[[FLOAT64(0)]]`)
	})
}

func TestGroupBy(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("insert into t3(id5, id6, id7) values(1,1,2), (2,2,4), (3,2,4), (4,1,2), (5,1,2), (6,3,6)")
	// test ordering and group by int column
	mcmp.AssertMatches("select id6, id7, count(*) k from t3 group by id6, id7 order by k", `[[INT64(3) INT64(6) INT64(1)] [INT64(2) INT64(4) INT64(2)] [INT64(1) INT64(2) INT64(3)]]`)
	mcmp.AssertMatches("select id6+id7, count(*) k from t3 group by id6+id7 order by k", `[[INT64(9) INT64(1)] [INT64(6) INT64(2)] [INT64(3) INT64(3)]]`)

	// Test the same queries in streaming mode
	utils.Exec(t, mcmp.VtConn, "set workload = olap")
	mcmp.AssertMatches("select id6, id7, count(*) k from t3 group by id6, id7 order by k", `[[INT64(3) INT64(6) INT64(1)] [INT64(2) INT64(4) INT64(2)] [INT64(1) INT64(2) INT64(3)]]`)
	mcmp.AssertMatches("select id6+id7, count(*) k from t3 group by id6+id7 order by k", `[[INT64(9) INT64(1)] [INT64(6) INT64(2)] [INT64(3) INT64(3)]]`)
}

func TestEqualFilterOnScatter(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'b',2), (3,'c',3), (4,'d',4), (5,'e',5)")

	workloads := []string{"oltp", "olap"}
	for _, workload := range workloads {
		mcmp.Run(workload, func(mcmp *utils.MySQLCompare) {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("set workload = '%s'", workload))

			mcmp.AssertMatches("select count(*) as a from aggr_test having 1 = 1", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a = 5", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having 5 = a", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a = a", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a = 3+2", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having 1+4 = 3+2", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a = 1", `[]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a = \"1\"", `[]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a = \"5\"", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a = 5.00", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a, val1 from aggr_test group by val1 having a = 1.00", `[[INT64(1) VARCHAR("a")] [INT64(1) VARCHAR("b")] [INT64(1) VARCHAR("c")] [INT64(1) VARCHAR("d")] [INT64(1) VARCHAR("e")]]`)
			mcmp.AssertMatches("select 1 from aggr_test having count(*) = 5", `[[INT64(1)]]`)
		})
	}
}

func TestAggrOnJoin(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t3(id5, id6, id7) values(1,1,1), (2,2,4), (3,2,4), (4,1,2), (5,1,1), (6,3,6)")
	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'a',1), (3,'b',1), (4,'c',3), (5,'c',4)")

	mcmp.AssertMatches("select count(*) from aggr_test a join t3 t on a.val2 = t.id7",
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
	mcmp.AssertMatches("select a.val1, count(*) from aggr_test a join t3 t on a.val2 = t.id7 group by a.val1",
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

	mcmp.AssertMatches(`select max(a1.val2), max(a2.val2), count(*) from aggr_test a1 join aggr_test a2 on a1.val2 = a2.id join t3 t on a2.val2 = t.id7`,
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

	mcmp.AssertMatches(`select a1.val1, count(distinct a1.val2) from aggr_test a1 join aggr_test a2 on a1.val2 = a2.id join t3 t on a2.val2 = t.id7 group by a1.val1`,
		`[[VARCHAR("a") INT64(1)] [VARCHAR("b") INT64(1)] [VARCHAR("c") INT64(1)]]`)

	// having on aggregation on top of join
	mcmp.AssertMatches("select a.val1, count(*) from aggr_test a join t3 t on a.val2 = t.id7 group by a.val1 having count(*) = 4",
		`[[VARCHAR("a") INT64(4)]]`)

	mcmp.AssertMatches("select a.val1, count(*) as leCount from aggr_test a join t3 t on a.val2 = t.id7 group by a.val1 having leCount = 4",
		`[[VARCHAR("a") INT64(4)]]`)

	mcmp.AssertMatches("select a.val1 from aggr_test a join t3 t on a.val2 = t.id7 group by a.val1 having count(*) = 4",
		`[[VARCHAR("a")]]`)

	mcmp.Run("Average in join for sharded", func(mcmp *utils.MySQLCompare) {
		mcmp.SkipIfBinaryIsBelowVersion(19, "vtgate")
		mcmp.AssertMatches(`select avg(a1.val2), avg(a2.val2) from aggr_test a1 join aggr_test a2 on a1.val2 = a2.id join t3 t on a2.val2 = t.id7`,
			"[[DECIMAL(1.5000) DECIMAL(1.0000)]]")

		mcmp.AssertMatches(`select a1.val1, avg(a1.val2) from aggr_test a1 join aggr_test a2 on a1.val2 = a2.id join t3 t on a2.val2 = t.id7 group by a1.val1`,
			`[[VARCHAR("a") DECIMAL(1.0000)] [VARCHAR("b") DECIMAL(1.0000)] [VARCHAR("c") DECIMAL(3.0000)]]`)
	})

}

func TestNotEqualFilterOnScatter(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'b',2), (3,'c',3), (4,'d',4), (5,'e',5)")

	workloads := []string{"oltp", "olap"}
	for _, workload := range workloads {
		mcmp.Run(workload, func(mcmp *utils.MySQLCompare) {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("set workload = '%s'", workload))

			mcmp.AssertMatches("select count(*) as a from aggr_test having a != 5", `[]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having 5 != a", `[]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a != a", `[]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a != 3+2", `[]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a != 1", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a != \"1\"", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a != \"5\"", `[]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a != 5.00", `[]`)
			mcmp.AssertMatches("select 1 from aggr_test having count(*) != 5", `[]`)
		})
	}
}

func TestLessFilterOnScatter(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'b',2), (3,'c',3), (4,'d',4), (5,'e',5)")

	workloads := []string{"oltp", "olap"}
	for _, workload := range workloads {
		mcmp.Run(workload, func(mcmp *utils.MySQLCompare) {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("set workload = '%s'", workload))
			mcmp.AssertMatches("select count(*) as a from aggr_test having a < 10", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having 1 < a", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a < a", `[]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a < 3+2", `[]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a < 1", `[]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a < \"10\"", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a < \"5\"", `[]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a < 6.00", `[[INT64(5)]]`)
			mcmp.AssertMatches("select 1 from aggr_test having count(*) < 5", `[]`)
		})
	}
}

func TestLessEqualFilterOnScatter(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'b',2), (3,'c',3), (4,'d',4), (5,'e',5)")

	workloads := []string{"oltp", "olap"}
	for _, workload := range workloads {
		mcmp.Run(workload, func(mcmp *utils.MySQLCompare) {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("set workload = '%s'", workload))

			mcmp.AssertMatches("select count(*) as a from aggr_test having a <= 10", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having 1 <= a", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a <= a", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a <= 3+2", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a <= 1", `[]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a <= \"10\"", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a <= \"5\"", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a <= 5.00", `[[INT64(5)]]`)
			mcmp.AssertMatches("select 1 from aggr_test having count(*) <= 5", `[[INT64(1)]]`)
		})
	}
}

func TestGreaterFilterOnScatter(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'b',2), (3,'c',3), (4,'d',4), (5,'e',5)")

	workloads := []string{"oltp", "olap"}
	for _, workload := range workloads {
		mcmp.Run(workload, func(mcmp *utils.MySQLCompare) {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("set workload = '%s'", workload))

			mcmp.AssertMatches("select count(*) as a from aggr_test having a > 1", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having 1 > a", `[]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a > a", `[]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a > 3+1", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a > 10", `[]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a > \"1\"", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a > \"5\"", `[]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a > 4.00", `[[INT64(5)]]`)
			mcmp.AssertMatches("select 1 from aggr_test having count(*) > 5", `[]`)
		})
	}
}

func TestGreaterEqualFilterOnScatter(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'b',2), (3,'c',3), (4,'d',4), (5,'e',5)")

	workloads := []string{"oltp", "olap"}
	for _, workload := range workloads {
		mcmp.Run(workload, func(mcmp *utils.MySQLCompare) {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("set workload = '%s'", workload))

			mcmp.AssertMatches("select count(*) as a from aggr_test having a >= 1", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having 1 >= a", `[]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a >= a", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a >= 3+2", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a >= 10", `[]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a >= \"1\"", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a >= \"5\"", `[[INT64(5)]]`)
			mcmp.AssertMatches("select count(*) as a from aggr_test having a >= 5.00", `[[INT64(5)]]`)
			mcmp.AssertMatches("select 1 from aggr_test having count(*) >= 5", `[[INT64(1)]]`)
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
	_, err := mcmp.ExecAndIgnore("select id2, id3 from t9 group by id2")
	require.NoError(t, err)
}

func TestAggOnTopOfLimit(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',6), (2,'a',1), (3,'b',1), (4,'c',3), (5,'c',4), (6,'b',null), (7,null,2), (8,null,null)")

	for _, workload := range []string{"oltp", "olap"} {
		mcmp.Run(workload, func(mcmp *utils.MySQLCompare) {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("set workload = '%s'", workload))
			mcmp.AssertMatches("select count(*) from (select id, val1 from aggr_test where val2 < 4 limit 2) as x", "[[INT64(2)]]")
			mcmp.AssertMatches("select count(val1) from (select id, val1 from aggr_test where val2 < 4 order by val1 desc limit 2) as x", "[[INT64(2)]]")
			mcmp.AssertMatches("select count(*) from (select id, val1 from aggr_test where val2 is null limit 2) as x", "[[INT64(2)]]")
			mcmp.AssertMatches("select count(val1) from (select id, val1 from aggr_test where val2 is null limit 2) as x", "[[INT64(1)]]")
			mcmp.AssertMatches("select count(val2) from (select id, val2 from aggr_test where val2 is null limit 2) as x", "[[INT64(0)]]")
			mcmp.AssertMatches("select val1, count(*) from (select id, val1 from aggr_test where val2 < 4 order by val1 limit 2) as x group by val1", `[[NULL INT64(1)] [VARCHAR("a") INT64(1)]]`)
			mcmp.AssertMatchesNoOrder("select val1, count(val2) from (select val1, val2 from aggr_test limit 8) as x group by val1", `[[NULL INT64(1)] [VARCHAR("a") INT64(2)] [VARCHAR("b") INT64(1)] [VARCHAR("c") INT64(2)]]`)
			mcmp.Run("Average in sharded query", func(mcmp *utils.MySQLCompare) {
				mcmp.SkipIfBinaryIsBelowVersion(19, "vtgate")
				mcmp.AssertMatches("select avg(val2) from (select id, val2 from aggr_test where val2 is null limit 2) as x", "[[NULL]]")
				mcmp.AssertMatchesNoOrder("select val1, avg(val2) from (select val1, val2 from aggr_test limit 8) as x group by val1", `[[NULL DECIMAL(2.0000)] [VARCHAR("a") DECIMAL(3.5000)] [VARCHAR("b") DECIMAL(1.0000)] [VARCHAR("c") DECIMAL(3.5000)]]`)
			})

			// mysql returns FLOAT64(0), vitess returns DECIMAL(0)
			mcmp.AssertMatches("select count(val1), sum(id) from (select id, val1 from aggr_test where val2 < 4 order by val1 desc limit 2) as x", "[[INT64(2) DECIMAL(7)]]")
			mcmp.AssertMatches("select count(*), sum(id) from (select id, val1 from aggr_test where val2 is null limit 2) as x", "[[INT64(2) DECIMAL(14)]]")
			mcmp.AssertMatches("select count(val1), sum(id) from (select id, val1 from aggr_test where val2 is null limit 2) as x", "[[INT64(1) DECIMAL(14)]]")
			mcmp.AssertMatches("select count(val2), sum(val2) from (select id, val2 from aggr_test where val2 is null limit 2) as x", "[[INT64(0) NULL]]")
			mcmp.AssertMatches("select val1, count(*), sum(id) from (select id, val1 from aggr_test where val2 < 4 order by val1 limit 2) as x group by val1", `[[NULL INT64(1) DECIMAL(7)] [VARCHAR("a") INT64(1) DECIMAL(2)]]`)
			mcmp.Run("Average in sharded query", func(mcmp *utils.MySQLCompare) {
				mcmp.SkipIfBinaryIsBelowVersion(19, "vtgate")
				mcmp.AssertMatches("select count(*), sum(val1), avg(val1) from (select id, val1 from aggr_test where val2 < 4 order by val1 desc limit 2) as x", "[[INT64(2) FLOAT64(0) FLOAT64(0)]]")
				mcmp.AssertMatches("select count(val1), sum(id), avg(id) from (select id, val1 from aggr_test where val2 < 4 order by val1 desc limit 2) as x", "[[INT64(2) DECIMAL(7) DECIMAL(3.5000)]]")
				mcmp.AssertMatchesNoOrder("select val1, count(val2), sum(val2), avg(val2) from (select val1, val2 from aggr_test limit 8) as x group by val1",
					`[[NULL INT64(1) DECIMAL(2) DECIMAL(2.0000)] [VARCHAR("a") INT64(2) DECIMAL(7) DECIMAL(3.5000)] [VARCHAR("b") INT64(1) DECIMAL(1) DECIMAL(1.0000)] [VARCHAR("c") INT64(2) DECIMAL(7) DECIMAL(3.5000)]]`)
			})
		})
	}
}

func TestEmptyTableAggr(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	for _, workload := range []string{"oltp", "olap"} {
		mcmp.Run(workload, func(mcmp *utils.MySQLCompare) {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("set workload = %s", workload))
			mcmp.AssertMatches(" select count(*) from t1 inner join t2 on (t1.t1_id = t2.id) where t1.value = 'foo'", "[[INT64(0)]]")
			mcmp.AssertMatches(" select count(*) from t2 inner join t1 on (t1.t1_id = t2.id) where t1.value = 'foo'", "[[INT64(0)]]")
			mcmp.AssertMatches(" select t1.`name`, count(*) from t2 inner join t1 on (t1.t1_id = t2.id) where t1.value = 'foo' group by t1.`name`", "[]")
			mcmp.AssertMatches(" select t1.`name`, count(*) from t1 inner join t2 on (t1.t1_id = t2.id) where t1.value = 'foo' group by t1.`name`", "[]")
			mcmp.Run("Average in sharded query", func(mcmp *utils.MySQLCompare) {
				mcmp.SkipIfBinaryIsBelowVersion(19, "vtgate")
				mcmp.AssertMatches(" select count(t1.value) from t2 inner join t1 on (t1.t1_id = t2.id) where t1.value = 'foo'", "[[INT64(0)]]")
				mcmp.AssertMatches(" select avg(t1.value) from t2 inner join t1 on (t1.t1_id = t2.id) where t1.value = 'foo'", "[[NULL]]")
			})
		})
	}

	mcmp.Exec("insert into t1(t1_id, `name`, `value`, shardkey) values(1,'a1','foo',100), (2,'b1','foo',200), (3,'c1','foo',300), (4,'a1','foo',100), (5,'b1','bar',200)")

	for _, workload := range []string{"oltp", "olap"} {
		mcmp.Run(workload, func(mcmp *utils.MySQLCompare) {
			utils.Exec(t, mcmp.VtConn, fmt.Sprintf("set workload = %s", workload))
			mcmp.AssertMatches(" select count(*) from t1 inner join t2 on (t1.t1_id = t2.id) where t1.value = 'foo'", "[[INT64(0)]]")
			mcmp.AssertMatches(" select count(*) from t2 inner join t1 on (t1.t1_id = t2.id) where t1.value = 'foo'", "[[INT64(0)]]")
			mcmp.AssertMatches(" select t1.`name`, count(*) from t2 inner join t1 on (t1.t1_id = t2.id) where t1.value = 'foo' group by t1.`name`", "[]")
			mcmp.Run("Average in sharded query", func(mcmp *utils.MySQLCompare) {
				mcmp.SkipIfBinaryIsBelowVersion(19, "vtgate")
				mcmp.AssertMatches(" select count(t1.value) from t2 inner join t1 on (t1.t1_id = t2.id) where t1.value = 'foo'", "[[INT64(0)]]")
				mcmp.AssertMatches(" select avg(t1.value) from t2 inner join t1 on (t1.t1_id = t2.id) where t1.value = 'foo'", "[[NULL]]")
				mcmp.AssertMatches(" select t1.`name`, count(*) from t1 inner join t2 on (t1.t1_id = t2.id) where t1.value = 'foo' group by t1.`name`", "[]")
			})
		})
	}

}

func TestOrderByCount(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t9(id1, id2, id3) values(1, '1', '1'), (2, '2', '2'), (3, '2', '2'), (4, '3', '3'), (5, '3', '3'), (6, '3', '3')")

	mcmp.AssertMatches("SELECT t9.id2 FROM t9 GROUP BY t9.id2 ORDER BY COUNT(t9.id2) DESC", `[[VARCHAR("3")] [VARCHAR("2")] [VARCHAR("1")]]`)
}

func TestAggregateAnyValue(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(t1_id, name, value, shardKey) values (1, 'name 1', 'value 1', 1), (2, 'name 2', 'value 2', 2)")
	mcmp.Exec("insert into t2(id, shardKey) values (1, 10), (2, 20)")

	mcmp.AssertMatches("SELECT t1.shardKey, t1.name, count(t2.id) FROM t1 JOIN t2 ON t1.value != t2.shardKey GROUP BY t1.t1_id", `[[INT64(1) VARCHAR("name 1") INT64(2)] [INT64(2) VARCHAR("name 2") INT64(2)]]`)

	mcmp.Exec("set sql_mode=''")
	mcmp.AssertMatches("select tbl0.comm, count(*) from emp as tbl0, emp as tbl1 where tbl0.empno = tbl1.deptno", `[[NULL INT64(0)]]`)
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

	mcmp.Run("Average in sharded query", func(mcmp *utils.MySQLCompare) {
		mcmp.SkipIfBinaryIsBelowVersion(19, "vtgate")
		mcmp.AssertMatches("SELECT avg(t1.shardkey) FROM t1 LEFT JOIN t2 ON t1.t1_id = t2.id", `[[DECIMAL(0.5000)]]`)
		mcmp.AssertMatches("SELECT avg(t2.shardkey) FROM t1 LEFT JOIN t2 ON t1.t1_id = t2.id", `[[DECIMAL(1.0000)]]`)
		aggregations := []string{
			"count(t1.shardkey)",
			"count(t2.shardkey)",
			"sum(t1.shardkey)",
			"sum(t2.shardkey)",
			"avg(t1.shardkey)",
			"avg(t2.shardkey)",
			"count(*)",
		}

		grouping := []string{
			"t1.t1_id",
			"t1.shardKey",
			"t1.value",
			"t2.id",
			"t2.shardKey",
		}

		// quickly construct a big number of left join aggregation queries that have to be executed using the hash join
		for _, agg := range aggregations {
			for _, gb := range grouping {
				query := fmt.Sprintf("SELECT %s FROM t1 LEFT JOIN (select id, shardkey from t2 limit 100) as t2 ON t1.t1_id = t2.id group by %s", agg, gb)
				mcmp.Exec(query)
			}
		}
	})
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
	mcmp.AssertMatches("select count(distinct val1) from aggr_test", `[[INT64(3)]]`)
	mcmp.Run("Average in sharded query", func(mcmp *utils.MySQLCompare) {
		mcmp.SkipIfBinaryIsBelowVersion(19, "vtgate")
		mcmp.AssertMatches("select avg(val1) from aggr_test", `[[FLOAT64(0)]]`)
	})
}

func TestAggregationRandomOnAnAggregatedValue(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t10(k, a, b) values (0, 100, 10), (10, 200, 20);")

	mcmp.AssertMatchesNoOrder("select A.a, A.b, (A.a / A.b) as d from (select sum(a) as a, sum(b) as b from t10 where a = 100) A;",
		`[[DECIMAL(100) DECIMAL(10) DECIMAL(10.0000)]]`)
}

func TestBuggyQueries(t *testing.T) {
	// These queries have been found to be producing the wrong results by the query fuzzer
	// Adding them as end2end tests to make sure we never get them wrong again
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t10(k, a, b) values (0, 100, 10), (10, 200, 20), (20, null, null)")

	mcmp.AssertMatches("select sum(t1.a) from t10 as t1, t10 as t2",
		`[[DECIMAL(900)]]`)

	mcmp.AssertMatches("select t1.a, sum(t1.a), count(*), t1.a, sum(t1.a), count(*) from t10 as t1, t10 as t2 group by t1.a",
		"[[NULL NULL INT64(3) NULL NULL INT64(3)] "+
			"[INT32(100) DECIMAL(300) INT64(3) INT32(100) DECIMAL(300) INT64(3)] "+
			"[INT32(200) DECIMAL(600) INT64(3) INT32(200) DECIMAL(600) INT64(3)]]")

	mcmp.Exec("select sum(tbl1.a), min(tbl0.b) from t10 as tbl0, t10 as tbl1 left join t10 as tbl2 on tbl1.a = tbl2.a and tbl1.b = tbl2.k")
	mcmp.Exec("select count(*) from t10 left join t10 as t11 on t10.a = t11.b where t11.a")
}

func TestMinMaxAcrossJoins(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("insert into t1(t1_id, name, value, shardKey) values (1, 'name 1', 'value 1', 1), (2, 'name 2', 'value 2', 2)")
	mcmp.Exec("insert into t2(id, shardKey) values (1, 10), (2, 20)")

	mcmp.AssertMatchesNoOrder(
		`SELECT t1.name, max(t1.shardKey), t2.shardKey, min(t2.id) FROM t1 JOIN t2 ON t1.t1_id != t2.shardKey GROUP BY t1.name, t2.shardKey`,
		`[[VARCHAR("name 2") INT64(2) INT64(10) INT64(1)] [VARCHAR("name 1") INT64(1) INT64(10) INT64(1)] [VARCHAR("name 2") INT64(2) INT64(20) INT64(2)] [VARCHAR("name 1") INT64(1) INT64(20) INT64(2)]]`)
}

func TestComplexAggregation(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("insert into t1(t1_id, `name`, `value`, shardkey) values(1,'a1','foo',100), (2,'b1','foo',200), (3,'c1','foo',300), (4,'a1','foo',100), (5,'d1','toto',200), (6,'c1','tata',893), (7,'a1','titi',2380), (8,'b1','tete',12833), (9,'e1','yoyo',783493)")

	mcmp.Exec("set @@sql_mode = ' '")
	mcmp.Exec(`SELECT 1+COUNT(t1_id) FROM t1`)
	mcmp.Exec(`SELECT COUNT(t1_id)+1 FROM t1`)
	mcmp.Exec(`SELECT COUNT(t1_id)+MAX(shardkey) FROM t1`)
	mcmp.Exec(`SELECT shardkey, MIN(t1_id)+MAX(t1_id) FROM t1 GROUP BY shardkey`)
	mcmp.Exec(`SELECT shardkey + MIN(t1_id)+MAX(t1_id) FROM t1 GROUP BY shardkey`)
	mcmp.Exec(`SELECT name+COUNT(t1_id)+1 FROM t1 GROUP BY name`)
	mcmp.Exec(`SELECT COUNT(*)+shardkey+MIN(t1_id)+1+MAX(t1_id)*SUM(t1_id)+1+name FROM t1 GROUP BY shardkey, name`)
	mcmp.Run("Average in sharded query", func(mcmp *utils.MySQLCompare) {
		mcmp.SkipIfBinaryIsBelowVersion(19, "vtgate")
		mcmp.Exec(`SELECT COUNT(t1_id)+MAX(shardkey)+AVG(t1_id) FROM t1`)
	})
}

func TestJoinAggregation(t *testing.T) {
	// This is new functionality in Vitess 20
	utils.SkipIfBinaryIsBelowVersion(t, 20, "vtgate")

	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(t1_id, `name`, `value`, shardkey) values(1,'a1','foo',100), (2,'b1','foo',200), (3,'c1','foo',300), (4,'a1','foo',100), (5,'d1','toto',200), (6,'c1','tata',893), (7,'a1','titi',2380), (8,'b1','tete',12833), (9,'e1','yoyo',783493)")

	mcmp.Exec(`insert into bet_logs(id, merchant_game_id, bet_amount, game_id) values
		(1, 1, 22.5, 40), (2, 1, 15.3, 40),
		(3, 2, 22.5, 40), (4, 2, 15.3, 40),
		(5, 3, 22.5, 40), (6, 3, 15.3, 40),
		(7, 3, 22.5, 40), (8, 4, 15.3, 40)
`)

	mcmp.Exec("set @@sql_mode = ' '")
	mcmp.Exec(`SELECT t1.name, SUM(b.bet_amount) AS bet_amount FROM bet_logs as b LEFT JOIN t1 ON b.merchant_game_id = t1.t1_id GROUP BY b.merchant_game_id`)
	mcmp.Exec(`SELECT t1.name, CAST(SUM(b.bet_amount) AS DECIMAL(20,6)) AS bet_amount FROM bet_logs as b LEFT JOIN t1 ON b.merchant_game_id = t1.t1_id GROUP BY b.merchant_game_id`)
}

// TestGroupConcatAggregation tests the group_concat function with vitess doing the aggregation.
func TestGroupConcatAggregation(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("insert into t1(t1_id, `name`, `value`, shardkey) values(1,'a1',null,100), (2,'b1','foo',20), (3,'c1','foo',10), (4,'a1','foo',100), (5,'d1','toto',200), (6,'c1',null,893), (10,'a1','titi',2380), (20,'b1','tete',12833), (9,'e1','yoyo',783493)")
	mcmp.Exec("insert into t2(id, shardKey) values (1, 10), (2, 20)")

	mQr, vtQr := mcmp.ExecNoCompare(`SELECT group_concat(name) FROM t1`)
	compareRow(t, mQr, vtQr, nil, []int{0})
	mQr, vtQr = mcmp.ExecNoCompare(`SELECT group_concat(value) FROM t1 join t2 on t1.shardKey = t2.shardKey `)
	compareRow(t, mQr, vtQr, nil, []int{0})
	mQr, vtQr = mcmp.ExecNoCompare(`SELECT group_concat(value) FROM t1 join t2 on t1.t1_id = t2.shardKey `)
	compareRow(t, mQr, vtQr, nil, []int{0})
	mQr, vtQr = mcmp.ExecNoCompare(`SELECT group_concat(value) FROM t1 join t2 on t1.shardKey = t2.id `)
	compareRow(t, mQr, vtQr, nil, []int{0})
	mQr, vtQr = mcmp.ExecNoCompare(`SELECT group_concat(value), t1.name FROM t1, t2 group by t1.name`)
	compareRow(t, mQr, vtQr, []int{1}, []int{0})
	if versionMet := utils.BinaryIsAtLeastAtVersion(19, "vtgate"); !versionMet {
		// skipping
		return
	}
	mQr, vtQr = mcmp.ExecNoCompare(`SELECT group_concat(name, value) FROM t1`)
	compareRow(t, mQr, vtQr, nil, []int{0})
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

func TestDistinctAggregation(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("insert into t1(t1_id, `name`, `value`, shardkey) values(1,'a1','foo',100), (2,'b1','foo',200), (3,'c1','foo',300), (4,'a1','foo',100), (5,'d1','toto',200), (6,'c1','tata',893), (7,'a1','titi',2380), (8,'b1','tete',12833), (9,'e1','yoyo',783493)")

	tcases := []struct {
		query       string
		expectedErr string
		minVersion  int
	}{{
		query:       `SELECT COUNT(DISTINCT value), SUM(DISTINCT shardkey) FROM t1`,
		expectedErr: "VT12001: unsupported: only one DISTINCT aggregation is allowed in a SELECT: sum(distinct shardkey) (errno 1235) (sqlstate 42000)",
	}, {
		query: `SELECT a.t1_id, SUM(DISTINCT b.shardkey) FROM t1 a, t1 b group by a.t1_id`,
	}, {
		query: `SELECT a.value, SUM(DISTINCT b.shardkey) FROM t1 a, t1 b group by a.value`,
	}, {
		query:       `SELECT count(distinct a.value), SUM(DISTINCT b.t1_id) FROM t1 a, t1 b`,
		expectedErr: "VT12001: unsupported: only one DISTINCT aggregation is allowed in a SELECT: sum(distinct b.t1_id) (errno 1235) (sqlstate 42000)",
	}, {
		query: `SELECT a.value, SUM(DISTINCT b.t1_id), min(DISTINCT a.t1_id) FROM t1 a, t1 b group by a.value`,
	}, {
		minVersion: 19,
		query:      `SELECT count(distinct name, shardkey) from t1`,
	}}

	for _, tc := range tcases {
		if versionMet := utils.BinaryIsAtLeastAtVersion(tc.minVersion, "vtgate"); !versionMet {
			// skipping
			continue
		}
		mcmp.Run(tc.query, func(mcmp *utils.MySQLCompare) {
			_, err := mcmp.ExecAllowError(tc.query)
			if tc.expectedErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.expectedErr)
		})
	}
}

func TestHavingQueries(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	inserts := []string{
		`INSERT INTO emp (empno, ename, job, mgr, hiredate, sal, comm, deptno) VALUES
	(1, 'John', 'Manager', NULL, '2022-01-01', 5000, 500, 1),
	(2, 'Doe', 'Analyst', 1, '2023-01-01', 4500, NULL, 1),
	(3, 'Jane', 'Clerk', 1, '2023-02-01', 3000, 200, 2),
	(4, 'Mary', 'Analyst', 2, '2022-03-01', 4700, NULL, 1),
	(5, 'Smith', 'Salesman', 3, '2023-01-15', 3200, 300, 3)`,
		"INSERT INTO dept (deptno, dname, loc) VALUES (1, 'IT', 'New York'), (2, 'HR', 'London'), (3, 'Sales', 'San Francisco')",
		"INSERT INTO t1 (t1_id, name, value, shardKey) VALUES (1, 'Name1', 'Value1', 100), (2, 'Name2', 'Value2', 100), (3, 'Name1', 'Value3', 200)",
		"INSERT INTO aggr_test_dates (id, val1, val2) VALUES (1, '2023-01-01', '2023-01-02'), (2, '2023-02-01', '2023-02-02'), (3, '2023-03-01', '2023-03-02')",
		"INSERT INTO t10 (k, a, b) VALUES (1, 10, 20), (2, 30, 40), (3, 50, 60)",
		"INSERT INTO t3 (id5, id6, id7) VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)",
		"INSERT INTO t9 (id1, id2, id3) VALUES (1, 'A1', 'B1'), (2, 'A2', 'B2'), (3, 'A1', 'B3')",
		"INSERT INTO aggr_test (id, val1, val2) VALUES (1, 'Test1', 100), (2, 'Test2', 200), (3, 'Test1', 300), (4, 'Test3', 400)",
		"INSERT INTO t2 (id, shardKey) VALUES (1, 100), (2, 200), (3, 300)",
		`INSERT INTO bet_logs (id, merchant_game_id, bet_amount, game_id) VALUES
	(1, 1, 100.0, 10),
	(2, 1, 200.0, 11),
	(3, 2, 300.0, 10),
	(4, 3, 400.0, 12)`,
	}

	for _, insert := range inserts {
		mcmp.Exec(insert)
	}

	queries := []string{
		// The following queries are not allowed by MySQL but Vitess allows them
		// SELECT ename FROM emp GROUP BY ename HAVING sal > 5000
		// SELECT val1, COUNT(val2) FROM aggr_test_dates GROUP BY val1 HAVING val2 > 5
		// SELECT k, a FROM t10 GROUP BY k HAVING b > 2
		// SELECT loc FROM dept GROUP BY loc HAVING COUNT(deptno) AND dname = 'Sales'
		// SELECT AVG(val2) AS average_val2 FROM aggr_test HAVING val1 = 'Test'

		// these first queries are all failing in different ways. let's check that Vitess also fails

		"SELECT deptno, AVG(sal) AS average_salary HAVING average_salary > 5000 FROM emp",
		"SELECT job, COUNT(empno) AS num_employees FROM emp HAVING num_employees > 2",
		"SELECT dname, SUM(sal) FROM dept JOIN emp ON dept.deptno = emp.deptno HAVING AVG(sal) > 6000",
		"SELECT COUNT(*) AS count FROM emp WHERE count > 5",
		"SELECT `name`, AVG(`value`) FROM t1 GROUP BY `name` HAVING `name`",
		"SELECT empno, MAX(sal) FROM emp HAVING COUNT(*) > 3",
		"SELECT id, SUM(bet_amount) AS total_bets FROM bet_logs HAVING total_bets > 1000",
		"SELECT merchant_game_id FROM bet_logs GROUP BY merchant_game_id HAVING SUM(bet_amount)",
		"SELECT shardKey, COUNT(id) FROM t2 HAVING shardKey > 100",
		"SELECT deptno FROM emp GROUP BY deptno HAVING MAX(hiredate) > '2020-01-01'",

		// These queries should not fail
		"SELECT deptno, COUNT(*) AS num_employees FROM emp GROUP BY deptno HAVING num_employees > 5",
		"SELECT ename, SUM(sal) FROM emp GROUP BY ename HAVING SUM(sal) > 10000",
		"SELECT dname, AVG(sal) AS average_salary FROM emp JOIN dept ON emp.deptno = dept.deptno GROUP BY dname HAVING average_salary > 5000",
		"SELECT dname, MAX(sal) AS max_salary FROM emp JOIN dept ON emp.deptno = dept.deptno GROUP BY dname HAVING max_salary < 10000",
		"SELECT YEAR(hiredate) AS year, COUNT(*) FROM emp GROUP BY year HAVING COUNT(*) > 2",
		"SELECT mgr, COUNT(empno) AS managed_employees FROM emp WHERE mgr IS NOT NULL GROUP BY mgr HAVING managed_employees >= 3",
		"SELECT deptno, SUM(comm) AS total_comm FROM emp GROUP BY deptno HAVING total_comm > AVG(total_comm)",
		"SELECT id2, COUNT(*) AS count FROM t9 GROUP BY id2 HAVING count > 1",
		"SELECT val1, COUNT(*) FROM aggr_test GROUP BY val1 HAVING COUNT(*) > 1",
		"SELECT DATE(val1) AS date, SUM(val2) FROM aggr_test_dates GROUP BY date HAVING SUM(val2) > 100",
		"SELECT shardKey, AVG(`value`) FROM t1 WHERE `value` IS NOT NULL GROUP BY shardKey HAVING AVG(`value`) > 10",
		"SELECT job, COUNT(*) AS job_count FROM emp GROUP BY job HAVING job_count > 3",
		"SELECT b, AVG(a) AS avg_a FROM t10 GROUP BY b HAVING AVG(a) > 5",
		"SELECT merchant_game_id, SUM(bet_amount) AS total_bets FROM bet_logs GROUP BY merchant_game_id HAVING total_bets > 1000",
		"SELECT loc, COUNT(deptno) AS num_depts FROM dept GROUP BY loc HAVING num_depts > 1",
		"SELECT `name`, COUNT(*) AS name_count FROM t1 GROUP BY `name` HAVING name_count > 2",
		"SELECT COUNT(*) AS num_jobs FROM emp GROUP BY empno HAVING num_jobs > 1",
		"SELECT id, COUNT(*) AS count FROM t2 GROUP BY id HAVING count > 1",
		"SELECT val2, SUM(id) FROM aggr_test GROUP BY val2 HAVING SUM(id) > 10",
		"SELECT game_id, COUNT(*) AS num_logs FROM bet_logs GROUP BY game_id HAVING num_logs > 5",
	}

	for _, query := range queries {
		mcmp.Run(query, func(mcmp *utils.MySQLCompare) {
			mcmp.ExecAllowAndCompareError(query)
		})
	}
}
