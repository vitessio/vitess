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

package subquery

import (
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, mcmp.VtConn, "set workload = oltp")

		tables := []string{"t1", "t1_id2_idx", "t2", "t2_id4_idx"}
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

func TestSubqueriesHasValues(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(id1, id2) values (0,1),(1,2),(2,3),(3,4),(4,5),(5,6)")
	mcmp.AssertMatches(`SELECT id2 FROM t1 WHERE id1 IN (SELECT id1 FROM t1 WHERE id1 > 10)`, `[]`)
	mcmp.AssertMatches(`SELECT id2 FROM t1 WHERE id1 NOT IN (SELECT id1 FROM t1 WHERE id1 > 10) ORDER BY id2`, `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)] [INT64(5)] [INT64(6)]]`)
}

// Test only supported in >= v14.0.0
func TestSubqueriesExists(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 14, "vtgate")
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(id1, id2) values (0,1),(1,2),(2,3),(3,4),(4,5),(5,6)")
	mcmp.AssertMatches(`SELECT id2 FROM t1 WHERE EXISTS (SELECT id1 FROM t1 WHERE id1 > 0) ORDER BY id2`, `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)] [INT64(5)] [INT64(6)]]`)
}

func TestQueryAndSubQWithLimit(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(id1, id2) values(0,0),(1,1),(2,2),(3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8),(9,9)")
	result := mcmp.Exec(`select id1, id2 from t1 where id1 >= ( select id1 from t1 order by id1 asc limit 1) limit 100`)
	assert.Equal(t, 10, len(result.Rows))
}

func TestSubQueryOnTopOfSubQuery(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec(`insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)`)
	mcmp.Exec(`insert into t2(id3, id4) values (1, 3), (2, 4)`)

	mcmp.AssertMatches("select id1 from t1 where id1 not in (select id3 from t2) and id2 in (select id4 from t2) order by id1", `[[INT64(3)] [INT64(4)]]`)
}

func TestSubqueryInINClause(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(id1, id2) values(0,0),(1,1)")
	mcmp.AssertMatches(`SELECT id2 FROM t1 WHERE id1 IN (SELECT 1 FROM dual)`, `[[INT64(1)]]`)
}

func TestSubqueryInUpdate(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 14, "vtgate")
	mcmp, closer := start(t)
	defer closer()

	conn := mcmp.VtConn

	utils.Exec(t, conn, `insert into t1(id1, id2) values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)`)
	utils.Exec(t, conn, `insert into t2(id3, id4) values (1, 3), (2, 4)`)
	utils.AssertMatches(t, conn, `SELECT id2, keyspace_id FROM t1_id2_idx WHERE id2 IN (2,10)`, `[[INT64(10) VARBINARY("\x16k@\xb4J\xbaK\xd6")]]`)
	utils.Exec(t, conn, `update /*vt+ PLANNER=gen4 */ t1 set id2 = (select count(*) from t2) where id1 = 1`)
	utils.AssertMatches(t, conn, `SELECT id2 FROM t1 WHERE id1 = 1`, `[[INT64(2)]]`)
	utils.AssertMatches(t, conn, `SELECT id2, keyspace_id FROM t1_id2_idx WHERE id2 IN (2,10)`, `[[INT64(2) VARBINARY("\x16k@\xb4J\xbaK\xd6")]]`)
}

func TestSubqueryInReference(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 14, "vtgate")
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec(`insert into t1(id1, id2) values (1,10), (2, 20), (3, 30), (4, 40), (5, 50)`)
	mcmp.AssertMatches(`select exists(select * from t1 where id1 = 3)`, `[[INT64(1)]]`)
	mcmp.AssertMatches(`select exists(select * from t1 where id1 = 9)`, `[[INT64(0)]]`)
	mcmp.AssertMatches(`select exists(select * from t1)`, `[[INT64(1)]]`)
	mcmp.AssertMatches(`select exists(select * from t1 where id2 = 30)`, `[[INT64(1)]]`)
	mcmp.AssertMatches(`select exists(select * from t1 where id2 = 9)`, `[[INT64(0)]]`)
	mcmp.AssertMatches(`select count(*) from t1 where id2 = 9`, `[[INT64(0)]]`)

	mcmp.AssertMatches(`select 1 in (select 1 from t1 where id1 = 3)`, `[[INT64(1)]]`)
	mcmp.AssertMatches(`select 1 in (select 1 from t1 where id1 = 9)`, `[[INT64(0)]]`)
	mcmp.AssertMatches(`select 1 in (select id1 from t1)`, `[[INT64(1)]]`)
	mcmp.AssertMatches(`select 1 in (select 1 from t1 where id2 = 30)`, `[[INT64(1)]]`)
	mcmp.AssertMatches(`select 1 in (select 1 from t1 where id2 = 9)`, `[[INT64(0)]]`)

	mcmp.AssertMatches(`select 1 not in (select 1 from t1 where id1 = 3)`, `[[INT64(0)]]`)
	mcmp.AssertMatches(`select 1 not in (select 1 from t1 where id1 = 9)`, `[[INT64(1)]]`)
	mcmp.AssertMatches(`select 1 not in (select id1 from t1)`, `[[INT64(0)]]`)
	mcmp.AssertMatches(`select 1 not in (select 1 from t1 where id2 = 30)`, `[[INT64(0)]]`)
	mcmp.AssertMatches(`select 1 not in (select 1 from t1 where id2 = 9)`, `[[INT64(1)]]`)

	mcmp.AssertMatches(`select (select id2 from t1 where id1 = 3)`, `[[INT64(30)]]`)
	mcmp.AssertMatches(`select (select id2 from t1 where id1 = 9)`, `[[NULL]]`)
	mcmp.AssertMatches(`select (select id1 from t1 where id2 = 30)`, `[[INT64(3)]]`)
	mcmp.AssertMatches(`select (select id1 from t1 where id2 = 9)`, `[[NULL]]`)
}
