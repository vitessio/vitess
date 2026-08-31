/*
Copyright 2022 The Vitess Authors.

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

package misc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"
)

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		tables := []string{"music", "user"}
		for _, table := range tables {
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
		}
	}

	deleteAll()

	mcmp.Exec("insert into music(id, user_id) values(1,1), (2,5), (3,1), (4,2), (5,3), (6,4), (7,5)")
	mcmp.Exec("insert into user(id, name) values(1,'toto'), (2,'tata'), (3,'titi'), (4,'tete'), (5,'foo')")

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
	}
}

func TestDerivedTableWithOrderByLimit(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("select music.id from music join (select id,name from user order by id limit 2) as d on music.user_id = d.id")
}

func TestDerivedAggregationOnRHS(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("set sql_mode = ''")
	mcmp.Exec("select d.a from music join (select id, count(*) as a from user) as d on music.user_id = d.id group by 1")
}

func TestDerivedRemoveInnerOrderBy(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("select count(*) from (select user.id as oui, music.id as non from user join music on user.id = music.user_id order by user.name) as toto")
}

func TestDerivedTableWithHaving(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("set sql_mode = ''")
	// For the given query, we can get any id back, because we aren't grouping by it.
	mcmp.AssertMatchesAnyNoCompare("select * from (select id from user having count(*) >= 1) s",
		"[[INT64(1)]]", "[[INT64(2)]]", "[[INT64(3)]]", "[[INT64(4)]]", "[[INT64(5)]]")
}

func TestDerivedTableColumns(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.AssertMatches(`SELECT t.id FROM (SELECT id FROM user) AS t(id) ORDER BY t.id DESC`,
		`[[INT64(5)] [INT64(4)] [INT64(3)] [INT64(2)] [INT64(1)]]`)
}

// TestDerivedTablesWithLimit tests queries where we have to limit the right hand side of the join.
// We do this by not using the apply join we usually use, and instead use the hash join engine primitive
// These tests exercise these situations
func TestDerivedTablesWithLimit(t *testing.T) {
	// We need full type info before planning this, so we wait for the schema tracker
	require.NoError(t,
		utils.WaitForAuthoritative(t, keyspaceName, "user", clusterInstance.VtgateProcess.ReadVSchema))

	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into user(id, name) values(6,'pikachu')")

	mcmp.AssertMatchesNoOrder(
		`SELECT u.id, m.id FROM
	            (SELECT id, name FROM user LIMIT 10) AS u JOIN
	            (SELECT id, user_id FROM music LIMIT 10) as m on u.id = m.user_id`,
		`[[INT64(1) INT64(1)] [INT64(5) INT64(2)] [INT64(1) INT64(3)] [INT64(2) INT64(4)] [INT64(3) INT64(5)] [INT64(5) INT64(7)] [INT64(4) INT64(6)]]`)

	mcmp.AssertMatchesNoOrder(
		`SELECT u.id, m.id FROM user AS u LEFT JOIN 
                (SELECT id, user_id FROM music LIMIT 10) as m on u.id = m.user_id`,
		`[[INT64(1) INT64(1)] [INT64(5) INT64(2)] [INT64(1) INT64(3)] [INT64(2) INT64(4)] [INT64(3) INT64(5)] [INT64(5) INT64(7)] [INT64(4) INT64(6)] [INT64(6) NULL]]`)
}

// TestDerivedTableColumnAliasWithJoin tests the derived table having alias column and using it in the join condition
func TestDerivedTableColumnAliasWithJoin(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec(`SELECT user.id FROM user join (SELECT id as uid FROM user) t on t.uid = user.id`)
	mcmp.Exec(`SELECT user.id FROM user left join (SELECT id as uid FROM user) t on t.uid = user.id`)
	mcmp.Exec(`SELECT user.id FROM user join (SELECT id FROM user) t(uid) on t.uid = user.id`)
	mcmp.Exec(`SELECT user.id FROM user left join (SELECT id FROM user) t(uid) on t.uid = user.id`)
}

// TestValuesTableConstructor checks a VALUES table constructor against MySQL. VTGate plans these by
// rewriting them into an equivalent SELECT ... UNION ALL ..., so MySQL is the oracle for the
// generated column names, the column types unified across rows, and duplicate rows surviving.
//
// ORDER BY is absent because VTGate rejects it; the unit tests cover that.
func TestValuesTableConstructor(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// The VALUES table constructor was added in MySQL 8.0.19.
	mcmp.SkipIfBinaryIsBelowVersion(8, "mysqld")

	// The generated column names are what the rewrite depends on, so compare them explicitly.
	mcmp.ExecWithColumnCompare("select * from (values row(1, 1)) as sub")
	mcmp.ExecWithColumnCompare("select * from (values row(1, 1), row(2, 2)) as sub")
	mcmp.ExecWithColumnCompare("select sub.column_1, sub.column_0 from (values row(1, 2)) as sub")
	mcmp.ExecWithColumnCompare("select * from (values row(1, 1), row(2, 2)) as sub(a, b)")

	// Heterogeneous rows, where MySQL unifies the column type across the rows.
	mcmp.ExecWithColumnCompare("select * from (values row(1), row('a')) as sub")
	mcmp.ExecWithColumnCompare("select * from (values row(null), row(1)) as sub")
	mcmp.ExecWithColumnCompare("select * from (values row(1, 'a'), row(2, 'bb')) as sub")

	// VALUES keeps duplicate rows, so the rewrite has to use UNION ALL rather than UNION.
	mcmp.Exec("select * from (values row(1), row(1), row(1)) as sub")
	mcmp.Exec("select * from (values row(1), row(2), row(3) limit 2) as sub")

	// The other positions the rewrite covers.
	mcmp.Exec("select user.id from user join (values row(1), row(2)) as sub on user.id = sub.column_0 order by user.id")
	mcmp.Exec("select column_0 from (values row(1), row(2), row(3)) as sub where column_0 > 1 order by column_0")
	mcmp.Exec("select id from user where id in (values row(1), row(2)) order by id")
	mcmp.Exec("select * from ((values row(1)) union all (values row(2))) as sub")
	mcmp.Exec("with x as (values row(1, 2)) select * from x")

	// An aggregate belonging to a nested subquery, which MySQL allows.
	mcmp.Exec("select * from (values row((select count(*) from user))) as sub")
}

// TestValuesTableConstructorErrors checks that the VALUES shapes MySQL rejects are rejected by
// VTGate too, rather than being desugared into a SELECT that MySQL would accept.
func TestValuesTableConstructorErrors(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.SkipIfBinaryIsBelowVersion(8, "mysqld")

	_, err := mcmp.ExecAllowAndCompareError("select * from (values row(1, 1), row(2)) as sub", utils.CompareOptions{})
	require.Error(t, err)

	// The rewritten `select count(*) from dual` would otherwise happily return a row.
	_, err = mcmp.ExecAllowAndCompareError("select * from (values row(count(*))) as sub", utils.CompareOptions{})
	require.Error(t, err)
}
