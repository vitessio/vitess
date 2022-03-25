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
	"context"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func start(t *testing.T) (*mysql.Conn, *mysql.Conn, func()) {
	ctx := context.Background()
	vtConn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)

	mysqlConn, err := mysql.Connect(ctx, &mysqlParams)
	require.Nil(t, err)

	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, vtConn, "set workload = oltp")
		_, _ = utils.ExecAllowErrorCompareMySQLWithOptions(t, vtConn, mysqlConn, "delete from t1", utils.DontCompareResultsOnError)
		_, _ = utils.ExecAllowError(t, vtConn, "delete from t1_id2_idx")
		_, _ = utils.ExecAllowErrorCompareMySQLWithOptions(t, vtConn, mysqlConn, "delete from t2", utils.DontCompareResultsOnError)
		_, _ = utils.ExecAllowError(t, vtConn, "delete from t2_id4_idx")
	}

	deleteAll()

	return vtConn, mysqlConn, func() {
		deleteAll()
		vtConn.Close()
		mysqlConn.Close()
		cluster.PanicHandler(t)
	}
}

func TestSubqueriesHasValues(t *testing.T) {
	vtConn, mysqlConn, closer := start(t)
	defer closer()

	utils.ExecCompareMySQL(t, vtConn, mysqlConn, "insert into t1(id1, id2) values (0,1),(1,2),(2,3),(3,4),(4,5),(5,6)")
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, `SELECT id2 FROM t1 WHERE id1 IN (SELECT id1 FROM t1 WHERE id1 > 10)`, `[]`)
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, `SELECT id2 FROM t1 WHERE id1 NOT IN (SELECT id1 FROM t1 WHERE id1 > 10) ORDER BY id2`, `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)] [INT64(5)] [INT64(6)]]`)
}

// Test only supported in >= v14.0.0
func TestSubqueriesExists(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 14, "vtgate")
	vtConn, mysqlConn, closer := start(t)
	defer closer()

	utils.ExecCompareMySQL(t, vtConn, mysqlConn, "insert into t1(id1, id2) values (0,1),(1,2),(2,3),(3,4),(4,5),(5,6)")
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, `SELECT id2 FROM t1 WHERE EXISTS (SELECT id1 FROM t1 WHERE id1 > 0) ORDER BY id2`, `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)] [INT64(5)] [INT64(6)]]`)
}

func TestQueryAndSubQWithLimit(t *testing.T) {
	vtConn, mysqlConn, closer := start(t)
	defer closer()

	utils.ExecCompareMySQL(t, vtConn, mysqlConn, "insert into t1(id1, id2) values(0,0),(1,1),(2,2),(3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8),(9,9)")
	result := utils.ExecCompareMySQL(t, vtConn, mysqlConn, `select id1, id2 from t1 where id1 >= ( select id1 from t1 order by id1 asc limit 1) limit 100`)
	assert.Equal(t, 10, len(result.Rows))
}

func TestSubQueryOnTopOfSubQuery(t *testing.T) {
	vtConn, mysqlConn, closer := start(t)
	defer closer()

	utils.ExecCompareMySQL(t, vtConn, mysqlConn, `insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)`)
	utils.ExecCompareMySQL(t, vtConn, mysqlConn, `insert into t2(id3, id4) values (1, 3), (2, 4)`)

	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, "select id1 from t1 where id1 not in (select id3 from t2) and id2 in (select id4 from t2) order by id1", `[[INT64(3)] [INT64(4)]]`)
}

func TestSubqueryInINClause(t *testing.T) {
	vtConn, mysqlConn, closer := start(t)
	defer closer()

	defer utils.ExecCompareMySQL(t, vtConn, mysqlConn, `delete from t1`)
	utils.ExecCompareMySQL(t, vtConn, mysqlConn, "insert into t1(id1, id2) values(0,0),(1,1)")
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, `SELECT id2 FROM t1 WHERE id1 IN (SELECT 1 FROM dual)`, `[[INT64(1)]]`)
}
