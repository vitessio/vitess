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
	"fmt"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Backend result fields report character_set_results, which can differ from
// the collation used by an arm's weight_string. Compare the complete SQL with
// MySQL so a unit fixture with idealized field metadata cannot mask that case.
func TestUnionOrderByMixedCollations(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 25, "vtgate")
	mcmp, closer := start(t)
	t.Cleanup(closer)
	mcmp.Exec("set names utf8mb4 collate utf8mb4_0900_ai_ci")
	mcmp.Exec("insert into union_collations(id, name_general, name_bin, name_latin1) values (1, 'unused', 'unused', 'unused'), (4, 'Mango', 'Mango', 'Ångström'), (5, 'Zed', 'Zed', 'Anders'), (6, 'a', 'apple', 'Örebro')")

	for _, workload := range []string{"oltp", "olap"} {
		mcmp.Run(workload, func(mcmp *utils.MySQLCompare) {
			utils.Exec(t, mcmp.VtConn, "set workload = "+workload)
			for _, column := range []string{"name_general", "name_bin", "name_latin1"} {
				constant := "All"
				if column == "name_latin1" {
					constant = "Zed"
				}
				left := fmt.Sprintf("select '%s' as name from union_collations where id = 1", constant)
				right := "select " + column + " as name from union_collations where id in (4, 5, 6)"
				for _, arms := range []string{left + " union all " + right, right + " union all " + left} {
					plan := utils.Exec(t, mcmp.VtConn, "vexplain plan "+arms+" order by name")
					require.Contains(mcmp.AsT(), fmt.Sprint(plan.Rows), "Concatenate", "exercise separate UNION routes")
					for _, suffix := range []string{"asc", "desc", "asc limit 1", "desc limit 1", "asc limit 1, 1"} {
						mcmp.Exec(arms + " order by name " + suffix)
					}
				}
			}
			for _, second := range []string{"name_general", "name_bin", "name_latin1"} {
				mcmp.Exec("select name_general as name from union_collations where id = 1 union all select " + second + " from union_collations where id in (4, 5, 6) order by name")
			}
			for _, coercibility := range []string{
				"name_general collate utf8mb4_general_ci as name from union_collations where id = 1 union all select name_bin collate utf8mb4_bin from union_collations where id = 4",
				"name_general as name from union_collations where id = 1 union all select convert(name_general using utf8mb4) collate utf8mb4_unicode_ci from union_collations where id = 4",
				"name_general as name from union_collations where id = 1 union all select cast(name_general as char character set utf8mb4) from union_collations where id = 4",
			} {
				_, _ = mcmp.ExecAllowAndCompareError("select "+coercibility+" order by name", utils.CompareOptions{})
			}
			// An empty column arm still determines the UNION's collation. Its
			// binary collation orders B before a despite the connection collation.
			result := mcmp.Exec("select 'a' as name from union_collations where id = 1 union all select 'B' from union_collations where id = 4 union all select name_bin from union_collations where id = 2 order by name")
			require.Len(mcmp.AsT(), result.Rows, 2)
			assert.Equal(mcmp.AsT(), "B", result.Rows[0][0].ToString())
			assert.Equal(mcmp.AsT(), "a", result.Rows[1][0].ToString())
		})
	}
}

func TestUnionOrderByLookupCollations(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 25, "vtgate")
	mcmp, closer := start(t)
	t.Cleanup(closer)
	mcmp.Exec("set names utf8mb4 collate utf8mb4_0900_ai_ci")
	mcmp.Exec("insert into t1(id1, id2) values (4, 4), (5, 5), (6, 6)")
	mcmp.Exec("insert into union_collations(id, name_general) values (1, 'unused'), (4, 'Mango'), (5, 'Zed'), (6, 'a')")
	for _, workload := range []string{"oltp", "olap"} {
		mcmp.Run(workload, func(mcmp *utils.MySQLCompare) {
			utils.Exec(t, mcmp.VtConn, "set workload = "+workload)
			for _, predicate := range []string{"t.id2 = 4", "t.id2 in (4, 5, 6)", "t.id2 in (999)"} {
				left := "select 'All' as name from union_collations where id = 1"
				right := "select c.name_general as name from union_collations c join t1 t on t.id1 = c.id where " + predicate
				for _, arms := range []string{left + " union all " + right, right + " union all " + left} {
					plan := utils.Exec(t, mcmp.VtConn, "vexplain plan "+arms+" order by name")
					require.Contains(mcmp.AsT(), fmt.Sprint(plan.Rows), "VindexLookup", "exercise a planable lookup arm")
					mcmp.Exec(arms + " order by name")
					mcmp.Exec(arms + " order by name desc limit 2")
				}
			}
		})
	}
	utils.Exec(t, mcmp.VtConn, "set workload = oltp")
	utils.Exec(t, mcmp.VtConn, "set transaction_mode = 'SINGLE'")
	t.Cleanup(func() {
		_, _ = mcmp.ExecAndIgnore("rollback")
		_, _ = utils.ExecAllowError(t, mcmp.VtConn, "set transaction_mode = 'MULTI'")
	})
	mcmp.Exec("begin")
	// A lookup miss must resolve metadata on the transaction's existing
	// shard. Opening an arbitrary different shard would fail SINGLE mode.
	mcmp.Exec("select 'All' as name from union_collations where id = 4 union all select c.name_general from union_collations c join t1 t on t.id1 = c.id where t.id2 in (999) order by name")
	mcmp.Exec("rollback")
}

func TestUnionOrderByCollationReviewCases(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 25, "vtgate")
	mcmp, closer := start(t)
	t.Cleanup(closer)
	mcmp.Exec("set names utf8mb4 collate utf8mb4_0900_ai_ci")
	mcmp.Exec("insert into union_collations(id, name_general, name_latin1, name_ascii) values (1, 'All', 'Ångström', 'All'), (4, 'Mango', 'Anders', 'Mango')")
	for _, workload := range []string{"oltp", "olap"} {
		mcmp.Run(workload, func(mcmp *utils.MySQLCompare) {
			utils.Exec(t, mcmp.VtConn, "set workload = "+workload)
			for _, arms := range [][2]string{
				{"select convert('All' using big5) as name from union_collations where id = 1", "select name_general collate utf8mb4_0900_ai_ci as name from union_collations where id = 4"},
				{"select convert(0xa4a4 using big5) as name from union_collations where id = 1", "select name_general collate utf8mb4_0900_ai_ci as name from union_collations where id = 4"},
				{"select name_ascii as name from union_collations where id = 1", "select name_latin1 as name from union_collations where id = 4"},
			} {
				for _, reverse := range []bool{false, true} {
					left, right := arms[0], arms[1]
					if reverse {
						left, right = right, left
					}
					for _, order := range []string{"asc", "desc limit 1"} {
						mcmp.Exec(left + " union all " + right + " order by name " + order)
					}
				}
			}
			// Grouped arms keep their existing execution path and hints.
			mcmp.Exec("select /*+ SET_VAR(sql_mode='NO_ENGINE_SUBSTITUTION') */ 'All' collate utf8mb4_general_ci as name from union_collations where id = 1 union all select /*+ SET_VAR(sql_mode='NO_ENGINE_SUBSTITUTION') */ name_general as name from union_collations where id = 4 group by name_bin order by name")
		})
	}
}

func TestUnionOrderByCollationInputs(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 25, "vtgate")
	for _, workload := range []string{"oltp", "olap"} {
		for _, name := range []string{"text", "blob", "grouped", "binary_precedence", "null"} {
			t.Run(workload+"/"+name, func(t *testing.T) {
				mcmp, closer := start(t)
				t.Cleanup(closer)
				mcmp.Exec("set names utf8mb4 collate utf8mb4_0900_ai_ci")
				utils.Exec(t, mcmp.VtConn, "set workload = "+workload)
				mcmp.Exec("insert into union_collations(id,name_general,name_bin,name_text,name_blob) values (1,'a','é','a','a'),(4,'B','z','B','B')")
				column := "name_text"
				switch name {
				case "blob":
					column = "name_blob"
				case "grouped", "null":
					column = "name_general"
				case "binary_precedence":
					column = "name_blob"
				}
				left := "select " + column + " as name from union_collations where id = 1"
				right := "select " + column + " as name from union_collations where id = 4"
				if name == "grouped" {
					right += " group by name_bin"
				}
				if name == "binary_precedence" {
					left = "select convert('All' using big5) as name from union_collations where id = 1"
				}
				if name == "null" {
					left = "select null as name from union_collations where id = 1"
				}
				for _, arms := range []string{left + " union all " + right, right + " union all " + left} {
					for _, order := range []string{"asc", "desc"} {
						query := arms + " order by name " + order
						if name != "text" && name != "null" {
							mcmp.Exec(query)
							continue
						}
						// Check ordering and NULLs independently of UNION field
						// type aggregation (which exposes TEXT as Blob).
						vt, my := utils.Exec(t, mcmp.VtConn, query), utils.Exec(t, mcmp.MySQLConn, query)
						require.Len(t, vt.Fields, 1)
						require.Len(t, vt.Rows, len(my.Rows))
						for i := range my.Rows {
							assert.Equal(t, my.Rows[i][0].IsNull(), vt.Rows[i][0].IsNull())
							assert.Equal(t, my.Rows[i][0].ToString(), vt.Rows[i][0].ToString())
						}
					}
				}
			})
		}
	}
}

func TestUnionOrderByLossyBackendResults(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 25, "vtgate")
	mcmp, closer := start(t)
	t.Cleanup(closer)
	mcmp.Exec("set names utf8mb4 collate utf8mb4_0900_ai_ci")
	mcmp.Exec("insert into union_collations(id,name_bin) values (1,convert(0xc3a9 using utf8mb4)),(4,'z')")
	// VTGate ignores SET character_set_results. Change the isolated tablets'
	// backend connection charset instead, and restore it before other tests.
	t.Cleanup(func() { assert.NoError(t, clusterInstance.WaitForTabletsToHealthyInVtgate()) })
	for _, shard := range clusterInstance.Keyspaces[0].Shards {
		for _, tablet := range shard.Vttablets {
			process := tablet.VttabletProcess
			previous := process.Charset
			require.NoError(t, process.TearDown())
			process.Charset = "ascii"
			t.Cleanup(func() {
				assert.NoError(t, process.TearDown())
				process.Charset = previous
				assert.NoError(t, process.Setup())
				assert.NoError(t, process.WaitForTabletStatus("SERVING"))
			})
			require.NoError(t, process.Setup())
			require.NoError(t, process.WaitForTabletStatus("SERVING"))
		}
	}
	require.NoError(t, clusterInstance.WaitForTabletsToHealthyInVtgate())
	utils.Exec(t, mcmp.MySQLConn, "set character_set_results=ascii")
	probe := utils.Exec(t, mcmp.VtConn, "select name_bin from union_collations where id=1")
	require.Equal(t, "?", probe.Rows[0][0].ToString(), "the regression must use genuinely lossy backend bytes")
	for _, workload := range []string{"oltp", "olap"} {
		utils.Exec(t, mcmp.VtConn, "set workload="+workload)
		left := "select name_bin as name from union_collations where id=1"
		right := "select name_bin as name from union_collations where id=4"
		for _, arms := range []string{left + " union all " + right, right + " union all " + left} {
			result := mcmp.Exec(arms + " order by name asc limit 1")
			require.Equal(t, "z", result.Rows[0][0].ToString())
			result = mcmp.Exec(arms + " order by name desc limit 1")
			require.Equal(t, "?", result.Rows[0][0].ToString(), "keep the result encoding separate from the key")
		}
	}
}

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, mcmp.VtConn, "set workload = oltp")

		tables := []string{"t1", "t1_id2_idx", "t2", "t2_id4_idx", "union_collations"}
		for _, table := range tables {
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
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
