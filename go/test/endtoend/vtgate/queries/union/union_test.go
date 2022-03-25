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

	"vitess.io/vitess/go/test/endtoend/cluster"

	"vitess.io/vitess/go/test/endtoend/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

func start(t *testing.T) (*mysql.Conn, *mysql.Conn, func()) {
	ctx := context.Background()
	vtConn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)

	mysqlConn, err := mysql.Connect(ctx, &mysqlParams)
	require.Nil(t, err)

	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, vtConn, "set workload = oltp")

		tables := []string{"t1", "t1_id2_idx", "t2", "t2_id4_idx"}
		conns := []*mysql.Conn{vtConn, mysqlConn}
		for _, conn := range conns {
			for _, table := range tables {
				_, _ = utils.ExecAllowError(t, conn, "delete from "+table)
			}
		}
	}

	deleteAll()

	return vtConn, mysqlConn, func() {
		deleteAll()
		vtConn.Close()
		mysqlConn.Close()
		cluster.PanicHandler(t)
	}
}

func TestUnionAll(t *testing.T) {
	vtConn, mysqlConn, closer := start(t)
	defer closer()

	utils.ExecCompareMySQL(t, vtConn, mysqlConn, "insert into t1(id1, id2) values(1, 1), (2, 2)")
	utils.ExecCompareMySQL(t, vtConn, mysqlConn, "insert into t2(id3, id4) values(3, 3), (4, 4)")

	// union all between two selectuniqueequal
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, "select id1 from t1 where id1 = 1 union all select id1 from t1 where id1 = 4", "[[INT64(1)]]")

	// union all between two different tables
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, "(select id1,id2 from t1 order by id1) union all (select id3,id4 from t2 order by id3)",
		"[[INT64(1) INT64(1)] [INT64(2) INT64(2)] [INT64(3) INT64(3)] [INT64(4) INT64(4)]]")

	// union all between two different tables
	result := utils.ExecCompareMySQL(t, vtConn, mysqlConn, "(select id1,id2 from t1) union all (select id3,id4 from t2)")
	assert.Equal(t, 4, len(result.Rows))

	// union all between two different tables
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, "select tbl2.id1 FROM  ((select id1 from t1 order by id1 limit 5) union all (select id1 from t1 order by id1 desc limit 5)) as tbl1 INNER JOIN t1 as tbl2  ON tbl1.id1 = tbl2.id1",
		"[[INT64(1)] [INT64(2)] [INT64(2)] [INT64(1)]]")

	utils.ExecCompareMySQL(t, vtConn, mysqlConn, "insert into t1(id1, id2) values(3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8)")

	// union all between two select unique in tables
	utils.AssertMatchesNoOrderCompareMySQL(t, vtConn, mysqlConn, "select id1 from t1 where id1 in (1, 2, 3, 4, 5, 6, 7, 8) union all select id1 from t1 where id1 in (1, 2, 3, 4, 5, 6, 7, 8)",
		"[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(5)] [INT64(4)] [INT64(6)] [INT64(7)] [INT64(8)] [INT64(1)] [INT64(2)] [INT64(3)] [INT64(5)] [INT64(4)] [INT64(6)] [INT64(7)] [INT64(8)]]")
}

func TestUnionDistinct(t *testing.T) {
	vtConn, mysqlConn, closer := start(t)
	defer closer()

	utils.ExecCompareMySQL(t, vtConn, mysqlConn, "insert into t1(id1, id2) values (1, 1), (2, 2), (3,3), (4,4)")
	utils.ExecCompareMySQL(t, vtConn, mysqlConn, "insert into t2(id3, id4) values (2, 3), (3, 4), (4,4), (5,5)")

	for _, workload := range []string{"oltp", "olap"} {
		t.Run(workload, func(t *testing.T) {
			utils.Exec(t, vtConn, "set workload = "+workload)
			utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, "select 1 union select null", "[[INT64(1)] [NULL]]")
			utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, "select null union select null", "[[NULL]]")
			utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, "select * from (select 1 as col union select 2) as t", "[[INT64(1)] [INT64(2)]]")

			// test with real data coming from mysql
			utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, "select id1 from t1 where id1 = 1 union select id1 from t1 where id1 = 5", "[[INT64(1)]]")
			utils.AssertMatchesNoOrderCompareMySQL(t, vtConn, mysqlConn, "select id1 from t1 where id1 = 1 union select id1 from t1 where id1 = 4", "[[INT64(1)] [INT64(4)]]")
			utils.AssertMatchesNoOrderCompareMySQL(t, vtConn, mysqlConn, "select id1 from t1 where id1 = 1 union select 452 union select id1 from t1 where id1 = 4", "[[INT64(1)] [INT64(452)] [INT64(4)]]")
			utils.AssertMatchesNoOrderCompareMySQL(t, vtConn, mysqlConn, "select id1, id2 from t1 union select 827, 452 union select id3,id4 from t2",
				"[[INT64(4) INT64(4)] [INT64(1) INT64(1)] [INT64(2) INT64(2)] [INT64(3) INT64(3)] [INT64(827) INT64(452)] [INT64(2) INT64(3)] [INT64(3) INT64(4)] [INT64(5) INT64(5)]]")
			t.Run("skipped for now", func(t *testing.T) {
				t.Skip()
				utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, "select 1 from dual where 1 IN (select 1 as col union select 2)", "[[INT64(1)]]")
			})
		})

	}
}

func TestUnionAllOlap(t *testing.T) {
	vtConn, mysqlConn, closer := start(t)
	defer closer()

	utils.ExecCompareMySQL(t, vtConn, mysqlConn, "insert into t1(id1, id2) values(1, 1), (2, 2)")
	utils.ExecCompareMySQL(t, vtConn, mysqlConn, "insert into t2(id3, id4) values(3, 3), (4, 4)")

	utils.Exec(t, vtConn, "set workload = olap")

	// union all between two selectuniqueequal
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, "select id1 from t1 where id1 = 1 union all select id1 from t1 where id1 = 4", "[[INT64(1)]]")

	// union all between two different tables
	// union all between two different tables
	result := utils.ExecCompareMySQL(t, vtConn, mysqlConn, "(select id1,id2 from t1 order by id1) union all (select id3,id4 from t2 order by id3)")
	assert.Equal(t, 4, len(result.Rows))

	// union all between two different tables
	result = utils.ExecCompareMySQL(t, vtConn, mysqlConn, "(select id1,id2 from t1) union all (select id3,id4 from t2)")
	assert.Equal(t, 4, len(result.Rows))

	// union all between two different tables
	result = utils.ExecCompareMySQL(t, vtConn, mysqlConn, "select tbl2.id1 FROM ((select id1 from t1 order by id1 limit 5) union all (select id1 from t1 order by id1 desc limit 5)) as tbl1 INNER JOIN t1 as tbl2  ON tbl1.id1 = tbl2.id1")
	assert.Equal(t, 4, len(result.Rows))

	utils.Exec(t, vtConn, "set workload = oltp")
	utils.ExecCompareMySQL(t, vtConn, mysqlConn, "insert into t1(id1, id2) values(3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8)")
	utils.Exec(t, vtConn, "set workload = olap")

	// union all between two selectuniquein tables
	utils.AssertMatchesNoOrderCompareMySQL(t, vtConn, mysqlConn, "select id1 from t1 where id1 in (1, 2, 3, 4, 5, 6, 7, 8) union all select id1 from t1 where id1 in (1, 2, 3, 4, 5, 6, 7, 8)",
		"[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(5)] [INT64(4)] [INT64(6)] [INT64(7)] [INT64(8)] [INT64(1)] [INT64(2)] [INT64(3)] [INT64(5)] [INT64(4)] [INT64(6)] [INT64(7)] [INT64(8)]]")

}

func TestUnion(t *testing.T) {
	vtConn, mysqlConn, closer := start(t)
	defer closer()

	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, `SELECT 1 UNION SELECT 1 UNION SELECT 1`, `[[INT64(1)]]`)
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, `SELECT 1,'a' UNION SELECT 1,'a' UNION SELECT 1,'a' ORDER BY 1`, `[[INT64(1) VARCHAR("a")]]`)
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, `SELECT 1,'z' UNION SELECT 2,'q' UNION SELECT 3,'b' ORDER BY 2`, `[[INT64(3) VARCHAR("b")] [INT64(2) VARCHAR("q")] [INT64(1) VARCHAR("z")]]`)
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, `SELECT 1,'a' UNION ALL SELECT 1,'a' UNION ALL SELECT 1,'a' ORDER BY 1`, `[[INT64(1) VARCHAR("a")] [INT64(1) VARCHAR("a")] [INT64(1) VARCHAR("a")]]`)
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, `(SELECT 1,'a') UNION ALL (SELECT 1,'a') UNION ALL (SELECT 1,'a') ORDER BY 1`, `[[INT64(1) VARCHAR("a")] [INT64(1) VARCHAR("a")] [INT64(1) VARCHAR("a")]]`)
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, `(SELECT 1,'a') ORDER BY 1`, `[[INT64(1) VARCHAR("a")]]`)
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, `(SELECT 1,'a' order by 1) union (SELECT 1,'a' ORDER BY 1)`, `[[INT64(1) VARCHAR("a")]]`)
}
