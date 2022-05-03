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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/vtgate/utils"
)

func TestSubqueriesHasValues(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer utils.Exec(t, conn, `delete from t1`)
	utils.Exec(t, conn, "insert into t1(id1, id2) values (0,1),(1,2),(2,3),(3,4),(4,5),(5,6)")
	utils.AssertMatches(t, conn, `SELECT id2 FROM t1 WHERE id1 IN (SELECT id1 FROM t1 WHERE id1 > 10)`, `[]`)
	utils.AssertMatches(t, conn, `SELECT id2 FROM t1 WHERE id1 NOT IN (SELECT id1 FROM t1 WHERE id1 > 10) ORDER BY id2`, `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)] [INT64(5)] [INT64(6)]]`)
}

// Test only supported in >= v13.0.0
func TestSubqueriesExists(t *testing.T) {
	defer cluster.PanicHandler(t)
	utils.SkipIfBinaryIsBelowVersion(t, 13, "vtgate")
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer utils.Exec(t, conn, `delete from t1`)
	utils.Exec(t, conn, "insert into t1(id1, id2) values (0,1),(1,2),(2,3),(3,4),(4,5),(5,6)")
	utils.AssertMatches(t, conn, `SELECT id2 FROM t1 WHERE EXISTS (SELECT id1 FROM t1 WHERE id1 > 0)`, `[[INT64(1)] [INT64(5)] [INT64(2)] [INT64(3)] [INT64(4)] [INT64(6)]]`)
}

func TestQueryAndSubQWithLimit(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer utils.Exec(t, conn, `delete from t1`)
	utils.Exec(t, conn, "insert into t1(id1, id2) values(0,0),(1,1),(2,2),(3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8),(9,9)")
	result := utils.Exec(t, conn, `select id1, id2 from t1 where id1 >= ( select id1 from t1 order by id1 asc limit 1) limit 100`)
	assert.Equal(t, 10, len(result.Rows))
}

func TestSubQueryOnTopOfSubQuery(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	defer utils.Exec(t, conn, `delete from t1`)

	utils.Exec(t, conn, `insert into t1(id1, id2) values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)`)
	utils.Exec(t, conn, `insert into t2(id3, id4) values (1, 3), (2, 4)`)

	utils.AssertMatches(t, conn, "select id1 from t1 where id1 not in (select id3 from t2) and id2 in (select id4 from t2) order by id1", `[[INT64(3)] [INT64(4)]]`)
}

func TestSubqueryInINClause(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	defer utils.Exec(t, conn, `delete from t1`)
	utils.Exec(t, conn, "insert into t1(id1, id2) values(0,0),(1,1)")
	utils.AssertMatches(t, conn, `SELECT id2 FROM t1 WHERE id1 IN (SELECT 1 FROM dual)`, `[[INT64(1)]]`)
}

func TestSubqueryInReference(t *testing.T) {
	defer cluster.PanicHandler(t)
	utils.SkipIfBinaryIsBelowVersion(t, 13, "vtgate")
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	defer utils.Exec(t, conn, `delete from t1`)

	utils.Exec(t, conn, `insert into t1(id1, id2) values (1,10), (2, 20), (3, 30), (4, 40), (5, 50)`)
	utils.AssertMatches(t, conn, `select exists(select * from t1 where id1 = 3)`, `[[INT64(1)]]`)
	utils.AssertMatches(t, conn, `select exists(select * from t1 where id1 = 9)`, `[[INT64(0)]]`)
	utils.AssertMatches(t, conn, `select exists(select * from t1)`, `[[INT64(1)]]`)
	utils.AssertMatches(t, conn, `select exists(select * from t1 where id2 = 30)`, `[[INT64(1)]]`)
	utils.AssertMatches(t, conn, `select exists(select * from t1 where id2 = 9)`, `[[INT64(0)]]`)
	utils.AssertMatches(t, conn, `select count(*) from t1 where id2 = 9`, `[[INT64(0)]]`)

	utils.AssertMatches(t, conn, `select 1 in (select 1 from t1 where id1 = 3)`, `[[INT64(1)]]`)
	utils.AssertMatches(t, conn, `select 1 in (select 1 from t1 where id1 = 9)`, `[[INT64(0)]]`)
	utils.AssertMatches(t, conn, `select 1 in (select id1 from t1)`, `[[INT64(1)]]`)
	utils.AssertMatches(t, conn, `select 1 in (select 1 from t1 where id2 = 30)`, `[[INT64(1)]]`)
	utils.AssertMatches(t, conn, `select 1 in (select 1 from t1 where id2 = 9)`, `[[INT64(0)]]`)

	utils.AssertMatches(t, conn, `select 1 not in (select 1 from t1 where id1 = 3)`, `[[INT64(0)]]`)
	utils.AssertMatches(t, conn, `select 1 not in (select 1 from t1 where id1 = 9)`, `[[INT64(1)]]`)
	utils.AssertMatches(t, conn, `select 1 not in (select id1 from t1)`, `[[INT64(0)]]`)
	utils.AssertMatches(t, conn, `select 1 not in (select 1 from t1 where id2 = 30)`, `[[INT64(0)]]`)
	utils.AssertMatches(t, conn, `select 1 not in (select 1 from t1 where id2 = 9)`, `[[INT64(1)]]`)

	utils.AssertMatches(t, conn, `select (select id2 from t1 where id1 = 3)`, `[[INT64(30)]]`)
	utils.AssertMatches(t, conn, `select (select id2 from t1 where id1 = 9)`, `[[NULL]]`)
	utils.AssertMatches(t, conn, `select (select id1 from t1 where id2 = 30)`, `[[INT64(3)]]`)
	utils.AssertMatches(t, conn, `select (select id1 from t1 where id2 = 9)`, `[[NULL]]`)
}
