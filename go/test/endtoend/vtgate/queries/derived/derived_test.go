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

	"vitess.io/vitess/go/test/endtoend/cluster"
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
		cluster.PanicHandler(t)
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
	utils.SkipIfBinaryIsBelowVersion(t, 19, "vtgate")
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
