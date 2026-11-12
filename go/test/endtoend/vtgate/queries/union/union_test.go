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

package union

import (
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, mcmp.VtConn, "set workload = oltp")

		tables := []string{"t1", "t1_id2_idx", "t2", "t2_id4_idx", "ref"}
		for _, table := range tables {
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
		}
		for _, shard := range clusterInstance.Keyspaces[0].Shards {
			_, err := shard.PrimaryTablet().VttabletProcess.QueryTablet("delete from ref", keyspaceName, true)
			require.NoError(t, err)
		}
	}

	deleteAll()

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
	}
}

func TestUnionDistinct(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(id1, id2) values (1, 1), (2, 2), (3,3), (4,4)")
	mcmp.Exec("insert into t2(id3, id4) values (2, 3), (3, 4), (4,4), (5,5)")

	for _, workload := range []string{"oltp", "olap"} {
		mcmp.Run(workload, func(mcmp *utils.MySQLCompare) {
			utils.Exec(t, mcmp.VtConn, "set workload = "+workload)
			mcmp.AssertMatches("select 1 union select null", "[[INT64(1)] [NULL]]")
			mcmp.AssertMatches("select null union select null", "[[NULL]]")
			mcmp.AssertMatches("select * from (select 1 as col union select 2) as t", "[[INT64(1)] [INT64(2)]]")

			// test with real data coming from mysql
			mcmp.AssertMatches("select id1 from t1 where id1 = 1 union select id1 from t1 where id1 = 5", "[[INT64(1)]]")
			mcmp.AssertMatchesNoOrder("select id1 from t1 where id1 = 1 union select id1 from t1 where id1 = 4", "[[INT64(1)] [INT64(4)]]")
			mcmp.AssertMatchesNoOrder("select id1 from t1 where id1 = 1 union select 452 union select id1 from t1 where id1 = 4", "[[INT64(1)] [INT64(452)] [INT64(4)]]")
			mcmp.AssertMatchesNoOrder("select id1, id2 from t1 union select 827, 452 union select id3,id4 from t2",
				"[[INT64(4) INT64(4)] [INT64(1) INT64(1)] [INT64(2) INT64(2)] [INT64(3) INT64(3)] [INT64(827) INT64(452)] [INT64(2) INT64(3)] [INT64(3) INT64(4)] [INT64(5) INT64(5)]]")
			mcmp.AssertMatches("select 1 from dual where 1 IN (select 1 as col union select 2)", "[[INT64(1)]]")
			mcmp.AssertMatches("select 1 from dual union select id1 from t1 where id1 in (null)", "[[INT64(1)]]")
			mcmp.AssertMatches("select id1 from t1 where id1 in (null) union select 1 from dual", "[[INT64(1)]]")
			mcmp.AssertMatches(`SELECT 1 from t1 UNION SELECT 2 from t1`, `[[INT64(1)] [INT64(2)]]`)
			mcmp.AssertMatches(`SELECT 5 from t1 UNION SELECT 6 from t1`, `[[INT64(5)] [INT64(6)]]`)
			mcmp.AssertMatchesNoOrder(`SELECT id1 from t1 UNION SELECT id2 from t1`, `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)]]`)
			mcmp.AssertMatchesNoOrder(`SELECT 1 from t1 UNION SELECT id2 from t1`, `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)]]`)
			mcmp.AssertMatchesNoOrder(`SELECT 5 from t1 UNION SELECT id2 from t1`, `[[INT64(5)] [INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)]]`)
			mcmp.AssertMatchesNoOrder(`SELECT id1 from t1 UNION SELECT 2 from t1`, `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)]]`)
			mcmp.AssertMatchesNoOrder(`SELECT id1 from t1 UNION SELECT 5 from t1`, `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)] [INT64(5)]]`)
			mcmp.Exec(`select curdate() from t1 union select 3 from t1`)
			mcmp.Exec(`select curdate() from t1 union select id1 from t1`)
		})
	}
}

func TestUnionAll(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(id1, id2) values(1, 1), (2, 2)")
	mcmp.Exec("insert into t2(id3, id4) values(3, 3), (4, 4)")

	for _, workload := range []string{"oltp", "olap"} {
		mcmp.Run(workload, func(mcmp *utils.MySQLCompare) {
			utils.Exec(t, mcmp.VtConn, "set workload = "+workload)
			// union all between two selectuniqueequal
			mcmp.AssertMatches("select id1 from t1 where id1 = 1 union all select id1 from t1 where id1 = 4", "[[INT64(1)]]")

			// union all between two different tables
			mcmp.AssertMatchesNoOrder("(select id1,id2 from t1 order by id1) union all (select id3,id4 from t2 order by id3)",
				"[[INT64(1) INT64(1)] [INT64(2) INT64(2)] [INT64(3) INT64(3)] [INT64(4) INT64(4)]]")

			// union all between two different tables
			result := mcmp.Exec("(select id1,id2 from t1) union all (select id3,id4 from t2)")
			assert.Len(t, result.Rows, 4)

			// union all between two different tables
			mcmp.AssertMatchesNoOrder("select tbl2.id1 FROM  ((select id1 from t1 order by id1 limit 5) union all (select id1 from t1 order by id1 desc limit 5)) as tbl1 INNER JOIN t1 as tbl2  ON tbl1.id1 = tbl2.id1",
				"[[INT64(1)] [INT64(2)] [INT64(2)] [INT64(1)]]")

			// this test is quite good at uncovering races in the Concatenate engine primitive. make it run many times
			// see: https://github.com/vitessio/vitess/issues/15434
			for range 100 {
				// union all between two select unique in tables
				mcmp.AssertMatchesNoOrder("select id1 from t1 where id1 in (1, 2, 3, 4, 5, 6, 7, 8) union all select id1 from t1 where id1 in (1, 2, 3, 4, 5, 6, 7, 8)",
					"[[INT64(1)] [INT64(2)] [INT64(1)] [INT64(2)]]")
			}

			// 4 tables union all
			mcmp.AssertMatchesNoOrder("select id1, id2 from t1 where id1 = 1 union all select id3,id4 from t2 where id3 = 3 union all select id1, id2 from t1 where id1 = 2 union all select id3,id4 from t2 where id3 = 4",
				"[[INT64(1) INT64(1)] [INT64(2) INT64(2)] [INT64(3) INT64(3)] [INT64(4) INT64(4)]]")
		})
	}
	mcmp.Run("union push down", func(mcmp *utils.MySQLCompare) {
		mcmp.Exec(`
select sum(case when col = 1 then 1 else 0 end) 
from (
	select id1 as col from t1 
	union all 
	select id3 as col from t2
) as t`)
		mcmp.Exec(`
select sum(id1+id2+foo) 
from (
	select id1, id2, id1+id2 as foo from t1 
	union all 
	select id3, id4, id4*id4 from t2
	union all 
	select id3, id4, id1+id4 from t1 join t2 on id3 + 1 = id1
) as t`)
	})
}

func TestUnionAllOrderedLimitPushdown(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 25, "vtgate")
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(id1, id2) values (1, 1), (2, 2), (20, 20), (21, 21), (22, 22), (23, 23), (24, 24), (25, 25)")
	mcmp.Exec("insert into t2(id3, id4) values (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10)")

	mcmp.AssertMatches(
		"select id1 as id from t1 union all select id3 from t2 order by id limit 3",
		"[[INT64(1)] [INT64(2)] [INT64(3)]]",
	)
	mcmp.AssertMatches(
		"select id1 as id from t1 union all select id3 from t2 order by id desc limit 3",
		"[[INT64(25)] [INT64(24)] [INT64(23)]]",
	)
	mcmp.AssertMatches(
		"select 'All' as name from dual union all select cast(id1 as char) as name from t1 order by name",
		`[[VARCHAR("1")] [VARCHAR("2")] [VARCHAR("20")] [VARCHAR("21")] [VARCHAR("22")] [VARCHAR("23")] [VARCHAR("24")] [VARCHAR("25")] [VARCHAR("All")]]`,
	)
	mcmp.AssertMatches(
		"select cast(id1 as signed) as id from t1 union all select cast(id3 as signed) from t2 union select cast(id1 as signed) from t1 order by id limit 5",
		"[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)] [INT64(5)]]",
	)
	mcmp.AssertMatches(
		"select cast(id1 as signed) as id from t1 union all select cast(0 as signed) from dual order by id limit 5",
		"[[INT64(0)] [INT64(1)] [INT64(2)] [INT64(20)] [INT64(21)]]",
	)
	mcmp.AssertMatches(
		"select id1 as id from t1 union all select id3 from t2 union select id1 from t1 order by id limit 5",
		"[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)] [INT64(5)]]",
	)

	// Mixed UNION ALL and UNION must remain executable with or without ORDER BY.
	for _, query := range []string{
		"select id1 as id from t1 union all select id3 from t2 union select id1 from t1 limit 5",
		"select id1 as id from t1 union all select id3 from t2 union select id1 from t1 order by id limit 5",
	} {
		result := utils.Exec(t, mcmp.VtConn, query)
		assert.Len(t, result.Rows, 5)
	}

	// UNION's result collation is resolved from all arms before the final sort.
	mcmp.Exec("select convert(_utf8mb4'z' using utf8mb4) union all select convert(_latin1'a' using latin1) order by 1 limit 1")
}

func TestReferenceUnionAllRowsReturnedOnce(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 25, "vtgate")
	mcmp, closer := start(t)
	defer closer()

	// Seed the same reference row on both shards, as expected for a reference table.
	utils.Exec(t, mcmp.MySQLConn, "insert into ref(id) values (100)")
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		_, err := shard.PrimaryTablet().VttabletProcess.QueryTablet("insert into ref(id) values (100)", keyspaceName, true)
		require.NoError(t, err)
	}
	mcmp.Exec("insert into t1(id1, id2, name) values (1, 1, 'a'), (2, 2, 'A')")

	mcmp.AssertMatchesNoOrder(
		"select id from ref union all select id1 from t1",
		"[[INT64(100)] [INT64(1)] [INT64(2)]]",
	)
	mcmp.AssertMatches(
		"select cast(id as signed) as id from ref union all select cast(id1 as signed) from t1 union select cast(id1 as signed) from t1 order by id limit 5",
		"[[INT64(1)] [INT64(2)] [INT64(100)]]",
	)
	mcmp.AssertMatches(
		"select cast(id as signed) as id from ref union all select cast(id1 as signed) from t1 order by id limit 5",
		"[[INT64(1)] [INT64(2)] [INT64(100)]]",
	)
	mcmp.AssertMatchesNoOrder(
		"select id from ref union all select id1 from t1 where id1 = 1 union all select id1 from t1",
		"[[INT64(100)] [INT64(1)] [INT64(1)] [INT64(2)]]",
	)
	mcmp.AssertMatchesNoOrder(
		"select id1 from t1 where id1 = 1 union all select id from ref union all select id1 from t1",
		"[[INT64(1)] [INT64(100)] [INT64(1)] [INT64(2)]]",
	)
	mcmp.AssertMatchesNoOrder(
		"select 200 from dual union all select id1 from t1 where id1 = 1 union all select id1 from t1",
		"[[INT64(200)] [INT64(1)] [INT64(1)] [INT64(2)]]",
	)
	mcmp.AssertMatchesNoOrder(
		"select 1 from dual where exists (select 1 from t1 where id1 = 1) union all select id1 from t1",
		"[[INT64(1)] [INT64(1)] [INT64(2)]]",
	)
	mcmp.AssertMatchesNoOrder(
		"select r.id from ref r left join t1 u on u.id1 = r.id",
		"[[INT64(100)]]",
	)
	mcmp.AssertMatchesNoOrder(
		"select r.id from ref r left join t1 u on u.id1 = r.id union all select id1 from t1",
		"[[INT64(100)] [INT64(1)] [INT64(2)]]",
	)

	// UNION ALL keeps the reference and sharded inputs on separate routes, so
	// VTGate sorts their rows. Keep real table inputs so MySQL's own sort does
	// not hide an incorrect field-collation fallback in VTGate.
	utils.Exec(t, mcmp.MySQLConn, "update ref set name = 'B' where id = 100")
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		_, err := shard.PrimaryTablet().VttabletProcess.QueryTablet("update ref set name = 'B' where id = 100", keyspaceName, true)
		require.NoError(t, err)
	}
	mcmp.AssertMatches(
		"select name from ref union all select name from t1 order by name",
		"[[VARBINARY(\"A\")] [VARBINARY(\"B\")] [VARBINARY(\"a\")]]",
	)
	mcmp.AssertMatches(
		"select name from ref union select name from t1 order by name",
		"[[VARBINARY(\"A\")] [VARBINARY(\"B\")] [VARBINARY(\"a\")]]",
	)

	// The reference arm has more rows than the LIMIT, and its local first rows
	// are not the first rows of the complete UNION result.
	utils.Exec(t, mcmp.MySQLConn, "insert into ref(id) values (101), (102), (103), (104), (105), (106)")
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		_, err := shard.PrimaryTablet().VttabletProcess.QueryTablet("insert into ref(id) values (101), (102), (103), (104), (105), (106)", keyspaceName, true)
		require.NoError(t, err)
	}
	mcmp.Exec("insert into t1(id1, id2) values (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10)")
	mcmp.AssertMatches(
		"select cast(id as signed) as id from ref union all select cast(id1 as signed) from t1 order by id desc limit 5",
		"[[INT64(106)] [INT64(105)] [INT64(104)] [INT64(103)] [INT64(102)]]",
	)
	// More than five distinct values expose an unordered limit below the sort.
	mcmp.AssertMatches(
		"select cast(id as signed) as id from ref union all select cast(id1 as signed) from t1 union select cast(id1 as signed) from t1 order by id limit 5",
		"[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)] [INT64(5)]]",
	)
	// Each shard has enough copies of 1 to exhaust a pushed LIMIT before
	// VTGate can deduplicate. Later rows must remain available to the sort.
	mcmp.Exec("insert into t2(id3, id4) values (1,1),(2,1),(3,1),(4,1),(5,1),(6,1),(7,1),(8,1),(9,1),(10,1),(11,1),(12,1),(13,1),(14,1),(15,1),(16,1),(17,1),(18,1),(19,1),(20,1),(21,2),(22,3),(23,4),(24,5),(25,6)")
	for _, workload := range []string{"oltp", "olap"} {
		utils.Exec(t, mcmp.VtConn, "set workload = "+workload)
		mcmp.AssertMatches(
			"select distinct id from (select id4 as id from t2 union all select id from ref) t order by id limit 5",
			"[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)] [INT64(5)]]",
		)
	}
	result := utils.Exec(t, mcmp.VtConn, "select id1 from t1 union all select id3 from t2 union select id1 from t1 limit 5")
	assert.Len(t, result.Rows, 5)
}

func TestUnion(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("insert into t1(id1, id2) values(1, 1), (2, 2)")

	mcmp.AssertMatches(`SELECT 1 UNION SELECT 1 UNION SELECT 1`, `[[INT64(1)]]`)
	mcmp.AssertMatches(`SELECT 1,'a' UNION SELECT 1,'a' UNION SELECT 1,'a' ORDER BY 1`, `[[INT64(1) VARCHAR("a")]]`)
	mcmp.AssertMatches(`SELECT 1,'z' UNION SELECT 2,'q' UNION SELECT 3,'b' ORDER BY 2`, `[[INT64(3) VARCHAR("b")] [INT64(2) VARCHAR("q")] [INT64(1) VARCHAR("z")]]`)
	mcmp.AssertMatches(`SELECT 1,'a' UNION ALL SELECT 1,'a' UNION ALL SELECT 1,'a' ORDER BY 1`, `[[INT64(1) VARCHAR("a")] [INT64(1) VARCHAR("a")] [INT64(1) VARCHAR("a")]]`)
	mcmp.AssertMatches(`(SELECT 1,'a') UNION ALL (SELECT 1,'a') UNION ALL (SELECT 1,'a') ORDER BY 1`, `[[INT64(1) VARCHAR("a")] [INT64(1) VARCHAR("a")] [INT64(1) VARCHAR("a")]]`)
	mcmp.AssertMatches(`(SELECT 1,'a') ORDER BY 1`, `[[INT64(1) VARCHAR("a")]]`)
	mcmp.AssertMatches(`(SELECT 1,'a' order by 1) union (SELECT 1,'a' ORDER BY 1)`, `[[INT64(1) VARCHAR("a")]]`)
	mcmp.AssertMatches(`(SELECT id2,'a' from t1 where id1 = 1) union (SELECT 'a',id2 from t1 where id1 = 2)`, `[[VARCHAR("1") VARCHAR("a")] [VARCHAR("a") VARCHAR("2")]]`)
}

// Reference and dual seeds must be returned once when a correlated recursive
// term is empty, even though resetting that term's routing produces Scatter.
func TestRecursiveUnionAllSeedReturnedOnceWithEmptyTerm(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 25, "vtgate")
	mcmp, closer := start(t)
	t.Cleanup(closer)
	utils.Exec(t, mcmp.MySQLConn, "insert into ref(id) values (100)")
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		_, err := shard.PrimaryTablet().VttabletProcess.QueryTablet("insert into ref(id) values (100)", keyspaceName, true)
		require.NoError(t, err)
	}
	for _, workload := range []string{"oltp", "olap"} {
		mcmp.Run(workload, func(mcmp *utils.MySQLCompare) {
			utils.Exec(t, mcmp.VtConn, "set workload = "+workload)
			for _, seed := range []string{"select id from ref", "select 100 as id from dual"} {
				query := "with recursive c as (" + seed + " union all select u.id1 from t1 u join c on u.id1 = c.id) select id from c"
				mcmp.AssertMatches(query, "[[INT64(100)]]")
				mcmp.AssertMatches(query+" union all select id1 from t1", "[[INT64(100)]]")
			}
		})
	}
}
