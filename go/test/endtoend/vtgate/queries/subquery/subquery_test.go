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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
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

func TestNotINQueries(t *testing.T) {
	// Tests NOT IN where the RHS contains all rows, some rows and no rows
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(id1, id2) values (0,1),(1,2),(2,3),(3,4),(4,5),(5,6)")
	// no matching rows
	mcmp.AssertMatches(`SELECT id2 FROM t1 WHERE id1 NOT IN (SELECT id1 FROM t1 WHERE id1 > 10) ORDER BY id2`, `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)] [INT64(5)] [INT64(6)]]`)
	mcmp.AssertMatches(`SELECT id2 FROM t1 WHERE id1 NOT IN (SELECT id2 FROM t1 WHERE id2 > 10) ORDER BY id2`, `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)] [INT64(5)] [INT64(6)]]`)

	// some matching rows
	mcmp.AssertMatches(`SELECT id2 FROM t1 WHERE id1 NOT IN (SELECT id1 FROM t1 WHERE id1 > 3) ORDER BY id2`, `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)]]`)
	mcmp.AssertMatches(`SELECT id2 FROM t1 WHERE id1 NOT IN (SELECT id2 FROM t1 WHERE id2 > 3) ORDER BY id2`, `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)]]`)

	// all rows matching
	mcmp.AssertMatches(`SELECT id2 FROM t1 WHERE id1 NOT IN (SELECT id1 FROM t1) ORDER BY id2`, `[]`)
	mcmp.AssertMatches(`SELECT id2 FROM t1 WHERE id1 NOT IN (SELECT id2 FROM t1) ORDER BY id2`, `[[INT64(1)]]`)

}

// Test only supported in >= v16.0.0
func TestSubqueriesExists(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(id1, id2) values (0,1),(1,2),(2,3),(3,4),(4,5),(5,6)")
	mcmp.AssertMatches(`SELECT id2 FROM t1 WHERE EXISTS (SELECT id1 FROM t1 WHERE id1 > 0) ORDER BY id2`, `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)] [INT64(5)] [INT64(6)]]`)
	mcmp.AssertMatches(`select * from (select 1) as tmp where exists(select 1 from t1 where id1 = 1)`, `[[INT32(1)]]`)
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
	mcmp, closer := start(t)
	defer closer()

	conn := mcmp.VtConn

	utils.Exec(t, conn, `insert into t1(id1, id2) values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)`)
	utils.Exec(t, conn, `insert into t2(id3, id4) values (1, 3), (2, 4)`)
	utils.AssertMatches(t, conn, `SELECT id2, keyspace_id FROM t1_id2_idx WHERE id2 IN (2,10)`, `[[INT64(10) VARBINARY("\x16k@\xb4J\xbaK\xd6")]]`)
	utils.Exec(t, conn, `update t1 set id2 = (select count(*) from t2) where id1 = 1`)
	utils.AssertMatches(t, conn, `SELECT id2 FROM t1 WHERE id1 = 1`, `[[INT64(2)]]`)
	utils.AssertMatches(t, conn, `SELECT id2, keyspace_id FROM t1_id2_idx WHERE id2 IN (2,10)`, `[[INT64(2) VARBINARY("\x16k@\xb4J\xbaK\xd6")]]`)
}

func TestSubqueryInReference(t *testing.T) {
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

// TestSubqueryInAggregation validates that subquery work inside aggregation functions.
func TestSubqueryInAggregation(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(id1, id2) values(0,0),(1,1)")
	mcmp.Exec("insert into t2(id3, id4) values(1,2),(5,7)")
	mcmp.Exec(`SELECT max((select min(id2) from t1)) FROM t2`)
	mcmp.Exec(`SELECT max((select group_concat(id1, id2) from t1 where id1 = 1)) FROM t1 where id1 = 1`)
	mcmp.Exec(`SELECT max((select min(id2) from t1 where id2 = 1)) FROM dual`)
	mcmp.Exec(`SELECT max((select min(id2) from t1)) FROM t2 where id4 = 7`)

	// This fails as the planner adds `weight_string` method which make the query fail on MySQL.
	// mcmp.Exec(`SELECT max((select min(id2) from t1 where t1.id1 = t.id1)) FROM t1 t`)
}

// TestSubqueryInDerivedTable tests that subqueries and derived tables
// are handled correctly when there are joins inside the derived table
func TestSubqueryInDerivedTable(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("INSERT INTO t1 (id1, id2) VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500);")
	mcmp.Exec("INSERT INTO t2 (id3, id4) VALUES (10, 1), (20, 2), (30, 3), (40, 4), (50, 99)")
	mcmp.Exec(`select t.a from (select t1.id2, t2.id3, (select id2 from t1 order by id2 limit 1) as a from t1 join t2 on t1.id1 = t2.id4) t`)
	mcmp.Exec(`SELECT COUNT(*) FROM (SELECT DISTINCT t1.id1 FROM t1 JOIN t2 ON t1.id1 = t2.id4) dt`)
}

func TestSubqueries(t *testing.T) {
	// This method tests many types of subqueries. The queries should move to a vitess-tester test file once we have a way to run them.
	// The commented out queries are failing because of wrong types being returned.
	// The tests are commented out until the issue is fixed.
	mcmp, closer := start(t)
	defer closer()
	queries := []string{
		`INSERT INTO user (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'David'), (5, 'Eve'), (6, 'Frank'), (7, 'Grace'), (8, 'Hannah'), (9, 'Ivy'), (10, 'Jack')`,
		`INSERT INTO user_extra (user_id, extra_info) VALUES (1, 'info1'), (1, 'info2'), (2, 'info1'), (3, 'info1'), (3, 'info2'), (4, 'info1'), (5, 'info1'), (6, 'info1'), (7, 'info1'), (8, 'info1')`,
		`SELECT (SELECT COUNT(*) FROM user_extra) AS order_count, id FROM user WHERE id = (SELECT COUNT(*) FROM user_extra)`,
		`SELECT id, (SELECT COUNT(*) FROM user_extra) AS order_count FROM user ORDER BY (SELECT COUNT(*) FROM user_extra)`,
		`SELECT id FROM user WHERE id = (SELECT COUNT(*) FROM user_extra) ORDER BY (SELECT COUNT(*) FROM user_extra)`,
		`SELECT (SELECT COUNT(*) FROM user_extra WHERE user.id = user_extra.user_id) AS extra_count, id, name FROM user WHERE (SELECT COUNT(*) FROM user_extra WHERE user.id = user_extra.user_id) > 0`,
		`SELECT id, name, (SELECT COUNT(*) FROM user_extra WHERE user.id = user_extra.user_id) AS extra_count FROM user ORDER BY (SELECT COUNT(*) FROM user_extra WHERE user.id = user_extra.user_id)`,
		`SELECT id, name FROM user WHERE (SELECT COUNT(*) FROM user_extra WHERE user.id = user_extra.user_id) > 0 ORDER BY (SELECT COUNT(*) FROM user_extra WHERE user.id = user_extra.user_id)`,
		`SELECT id, name, (SELECT COUNT(*) FROM user_extra WHERE user.id = user_extra.user_id) AS extra_count FROM user GROUP BY id, name HAVING COUNT(*) > (SELECT COUNT(*) FROM user_extra WHERE user.id = user_extra.user_id)`,
		`SELECT id, name, COUNT(*) FROM user WHERE (SELECT COUNT(*) FROM user_extra WHERE user.id = user_extra.user_id) > 0 GROUP BY id, name HAVING COUNT(*) > (SELECT COUNT(*) FROM user_extra WHERE user.id = user_extra.user_id)`,
		`SELECT id, round(MAX(id + (SELECT COUNT(*) FROM user_extra where user_id = 42))) as r FROM user WHERE id = 42 GROUP BY id ORDER BY r`,
		`SELECT id, name, (SELECT COUNT(*) FROM user_extra WHERE user.id = user_extra.user_id) * 2 AS double_extra_count FROM user`,
		`SELECT id, name FROM user WHERE id IN (SELECT user_id FROM user_extra WHERE LENGTH(extra_info) > 4)`,
		`SELECT id, COUNT(*) FROM user GROUP BY id HAVING COUNT(*) > (SELECT COUNT(*) FROM user_extra WHERE user_extra.user_id = user.id) + 1`,
		`SELECT id, name FROM user ORDER BY (SELECT COUNT(*) FROM user_extra WHERE user.id = user_extra.user_id) * id`,
		`SELECT id, name, (SELECT COUNT(*) FROM user_extra WHERE user.id = user_extra.user_id) + id AS extra_count_plus_id FROM user`,
		`SELECT id, name FROM user WHERE id IN (SELECT user_id FROM user_extra WHERE extra_info = 'info1') OR id IN (SELECT user_id FROM user_extra WHERE extra_info = 'info2')`,
		`SELECT id, name, (SELECT COUNT(*) FROM user_extra) AS total_extra_count, SUM(id) AS sum_ids FROM user GROUP BY id, name ORDER BY (SELECT COUNT(*) FROM user_extra)`,
		// `SELECT id, name, (SELECT SUM(LENGTH(extra_info)) FROM user_extra) AS total_length_extra_info, AVG(id) AS avg_ids FROM user GROUP BY id, name HAVING (SELECT SUM(LENGTH(extra_info)) FROM user_extra) > 10`,
		`SELECT id, name, (SELECT AVG(LENGTH(extra_info)) FROM user_extra) AS avg_length_extra_info, MAX(id) AS max_id FROM user WHERE id IN (SELECT user_id FROM user_extra) GROUP BY id, name`,
		`SELECT id, name, (SELECT MAX(LENGTH(extra_info)) FROM user_extra) AS max_length_extra_info, MIN(id) AS min_id FROM user GROUP BY id, name ORDER BY (SELECT MAX(LENGTH(extra_info)) FROM user_extra)`,
		`SELECT id, name, (SELECT MIN(LENGTH(extra_info)) FROM user_extra) AS min_length_extra_info, SUM(id) AS sum_ids FROM user GROUP BY id, name HAVING (SELECT MIN(LENGTH(extra_info)) FROM user_extra) < 5`,
		`SELECT id, name, (SELECT COUNT(*) FROM user_extra) AS total_extra_count, AVG(id) AS avg_ids FROM user WHERE id > (SELECT COUNT(*) FROM user_extra) GROUP BY id, name`,
		// `SELECT id, name, (SELECT SUM(LENGTH(extra_info)) FROM user_extra) AS total_length_extra_info, COUNT(id) AS count_ids FROM user GROUP BY id, name ORDER BY (SELECT SUM(LENGTH(extra_info)) FROM user_extra)`,
		// `SELECT id, name, (SELECT COUNT(*) FROM user_extra) AS total_extra_count, (SELECT SUM(LENGTH(extra_info)) FROM user_extra) AS total_length_extra_info, (SELECT AVG(LENGTH(extra_info)) FROM user_extra) AS avg_length_extra_info, (SELECT MAX(LENGTH(extra_info)) FROM user_extra) AS max_length_extra_info, (SELECT MIN(LENGTH(extra_info)) FROM user_extra) AS min_length_extra_info, SUM(id) AS sum_ids FROM user GROUP BY id, name HAVING (SELECT AVG(LENGTH(extra_info)) FROM user_extra) > 2`,
		`SELECT id, name, (SELECT COUNT(*) FROM user_extra) + id AS total_extra_count_plus_id, AVG(id) AS avg_ids FROM user WHERE id < (SELECT MAX(user_id) FROM user_extra) GROUP BY id, name`,
	}

	for idx, query := range queries {
		mcmp.Run(fmt.Sprintf("%d %s", idx, query), func(mcmp *utils.MySQLCompare) {
			mcmp.Exec(query)
		})
	}
}
