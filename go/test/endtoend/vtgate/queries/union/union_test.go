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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/vtgate/utils"
)

func TestUnionAll(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// clean up before & after
	utils.Exec(t, conn, "delete from t1")
	utils.Exec(t, conn, "delete from t2")
	defer utils.Exec(t, conn, "delete from t1")
	defer utils.Exec(t, conn, "delete from t2")

	utils.Exec(t, conn, "insert into t1(id1, id2) values(1, 1), (2, 2)")
	utils.Exec(t, conn, "insert into t2(id3, id4) values(3, 3), (4, 4)")

	// union all between two selectuniqueequal
	utils.AssertMatches(t, conn, "select id1 from t1 where id1 = 1 union all select id1 from t1 where id1 = 4", "[[INT64(1)]]")

	// union all between two different tables
	utils.AssertMatches(t, conn, "(select id1,id2 from t1 order by id1) union all (select id3,id4 from t2 order by id3)",
		"[[INT64(1) INT64(1)] [INT64(2) INT64(2)] [INT64(3) INT64(3)] [INT64(4) INT64(4)]]")

	// union all between two different tables
	result := utils.Exec(t, conn, "(select id1,id2 from t1) union all (select id3,id4 from t2)")
	assert.Equal(t, 4, len(result.Rows))

	// union all between two different tables
	utils.AssertMatches(t, conn, "select tbl2.id1 FROM  ((select id1 from t1 order by id1 limit 5) union all (select id1 from t1 order by id1 desc limit 5)) as tbl1 INNER JOIN t1 as tbl2  ON tbl1.id1 = tbl2.id1",
		"[[INT64(1)] [INT64(2)] [INT64(2)] [INT64(1)]]")

	utils.Exec(t, conn, "insert into t1(id1, id2) values(3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8)")

	// union all between two select unique in tables
	utils.AssertMatchesNoOrder(t, conn, "select id1 from t1 where id1 in (1, 2, 3, 4, 5, 6, 7, 8) union all select id1 from t1 where id1 in (1, 2, 3, 4, 5, 6, 7, 8)",
		"[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(5)] [INT64(4)] [INT64(6)] [INT64(7)] [INT64(8)] [INT64(1)] [INT64(2)] [INT64(3)] [INT64(5)] [INT64(4)] [INT64(6)] [INT64(7)] [INT64(8)]]")
}

func TestUnionDistinct(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// clean up before & after
	utils.Exec(t, conn, "delete from t1")
	utils.Exec(t, conn, "delete from t2")
	defer func() {
		utils.Exec(t, conn, "set workload = oltp")
		utils.Exec(t, conn, "delete from t1")
		utils.Exec(t, conn, "delete from t2")
	}()

	utils.Exec(t, conn, "insert into t1(id1, id2) values (1, 1), (2, 2), (3,3), (4,4)")
	utils.Exec(t, conn, "insert into t2(id3, id4) values (2, 3), (3, 4), (4,4), (5,5)")

	for _, workload := range []string{"oltp", "olap"} {
		t.Run(workload, func(t *testing.T) {
			utils.Exec(t, conn, "set workload = "+workload)
			utils.AssertMatches(t, conn, "select 1 union select null", "[[INT64(1)] [NULL]]")
			utils.AssertMatches(t, conn, "select null union select null", "[[NULL]]")
			utils.AssertMatches(t, conn, "select * from (select 1 as col union select 2) as t", "[[INT64(1)] [INT64(2)]]")

			// test with real data coming from mysql
			utils.AssertMatches(t, conn, "select id1 from t1 where id1 = 1 union select id1 from t1 where id1 = 5", "[[INT64(1)]]")
			utils.AssertMatchesNoOrder(t, conn, "select id1 from t1 where id1 = 1 union select id1 from t1 where id1 = 4", "[[INT64(1)] [INT64(4)]]")
			utils.AssertMatchesNoOrder(t, conn, "select id1 from t1 where id1 = 1 union select 452 union select id1 from t1 where id1 = 4", "[[INT64(1)] [INT64(452)] [INT64(4)]]")
			utils.AssertMatchesNoOrder(t, conn, "select id1, id2 from t1 union select 827, 452 union select id3,id4 from t2",
				"[[INT64(4) INT64(4)] [INT64(1) INT64(1)] [INT64(2) INT64(2)] [INT64(3) INT64(3)] [INT64(827) INT64(452)] [INT64(2) INT64(3)] [INT64(3) INT64(4)] [INT64(5) INT64(5)]]")
			t.Run("skipped for now", func(t *testing.T) {
				t.Skip()
				utils.AssertMatches(t, conn, "select 1 from dual where 1 IN (select 1 as col union select 2)", "[[INT64(1)]]")
			})
		})

	}
}

func TestUnionAllOlap(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// clean up before & after
	utils.Exec(t, conn, "delete from t1")
	utils.Exec(t, conn, "delete from t2")
	defer func() {
		utils.Exec(t, conn, "set workload = oltp")
		utils.Exec(t, conn, "delete from t1")
		utils.Exec(t, conn, "delete from t2")
	}()

	utils.Exec(t, conn, "insert into t1(id1, id2) values(1, 1), (2, 2)")
	utils.Exec(t, conn, "insert into t2(id3, id4) values(3, 3), (4, 4)")

	utils.Exec(t, conn, "set workload = olap")

	// union all between two selectuniqueequal
	utils.AssertMatches(t, conn, "select id1 from t1 where id1 = 1 union all select id1 from t1 where id1 = 4", "[[INT64(1)]]")

	// union all between two different tables
	// union all between two different tables
	result := utils.Exec(t, conn, "(select id1,id2 from t1 order by id1) union all (select id3,id4 from t2 order by id3)")
	assert.Equal(t, 4, len(result.Rows))

	// union all between two different tables
	result = utils.Exec(t, conn, "(select id1,id2 from t1) union all (select id3,id4 from t2)")
	assert.Equal(t, 4, len(result.Rows))

	// union all between two different tables
	result = utils.Exec(t, conn, "select tbl2.id1 FROM ((select id1 from t1 order by id1 limit 5) union all (select id1 from t1 order by id1 desc limit 5)) as tbl1 INNER JOIN t1 as tbl2  ON tbl1.id1 = tbl2.id1")
	assert.Equal(t, 4, len(result.Rows))

	utils.Exec(t, conn, "set workload = oltp")
	utils.Exec(t, conn, "insert into t1(id1, id2) values(3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8)")
	utils.Exec(t, conn, "set workload = olap")

	// union all between two selectuniquein tables
	utils.AssertMatchesNoOrder(t, conn, "select id1 from t1 where id1 in (1, 2, 3, 4, 5, 6, 7, 8) union all select id1 from t1 where id1 in (1, 2, 3, 4, 5, 6, 7, 8)",
		"[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(5)] [INT64(4)] [INT64(6)] [INT64(7)] [INT64(8)] [INT64(1)] [INT64(2)] [INT64(3)] [INT64(5)] [INT64(4)] [INT64(6)] [INT64(7)] [INT64(8)]]")

}

func TestUnion(t *testing.T) {
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.AssertMatches(t, conn, `SELECT 1 UNION SELECT 1 UNION SELECT 1`, `[[INT64(1)]]`)
	utils.AssertMatches(t, conn, `SELECT 1,'a' UNION SELECT 1,'a' UNION SELECT 1,'a' ORDER BY 1`, `[[INT64(1) VARCHAR("a")]]`)
	utils.AssertMatches(t, conn, `SELECT 1,'z' UNION SELECT 2,'q' UNION SELECT 3,'b' ORDER BY 2`, `[[INT64(3) VARCHAR("b")] [INT64(2) VARCHAR("q")] [INT64(1) VARCHAR("z")]]`)
	utils.AssertMatches(t, conn, `SELECT 1,'a' UNION ALL SELECT 1,'a' UNION ALL SELECT 1,'a' ORDER BY 1`, `[[INT64(1) VARCHAR("a")] [INT64(1) VARCHAR("a")] [INT64(1) VARCHAR("a")]]`)
	utils.AssertMatches(t, conn, `(SELECT 1,'a') UNION ALL (SELECT 1,'a') UNION ALL (SELECT 1,'a') ORDER BY 1`, `[[INT64(1) VARCHAR("a")] [INT64(1) VARCHAR("a")] [INT64(1) VARCHAR("a")]]`)
	utils.AssertMatches(t, conn, `(SELECT 1,'a') ORDER BY 1`, `[[INT64(1) VARCHAR("a")]]`)
	utils.AssertMatches(t, conn, `(SELECT 1,'a' order by 1) union (SELECT 1,'a' ORDER BY 1)`, `[[INT64(1) VARCHAR("a")]]`)
}
