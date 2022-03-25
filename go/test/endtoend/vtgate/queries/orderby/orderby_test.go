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

package orderby

import (
	"context"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

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

func TestSimpleOrderBy(t *testing.T) {
	vtConn, mysqlConn, closer := start(t)
	defer closer()

	utils.ExecCompareMySQL(t, vtConn, mysqlConn, "insert into t1(id1, id2) values (0,10),(1,9),(2,8),(3,7),(4,6),(5,5)")
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, `SELECT id2 FROM t1 ORDER BY id2 ASC`, `[[INT64(5)] [INT64(6)] [INT64(7)] [INT64(8)] [INT64(9)] [INT64(10)]]`)
}

func TestOrderBy(t *testing.T) {
	vtConn, mysqlConn, closer := start(t)
	defer closer()

	utils.ExecCompareMySQL(t, vtConn, mysqlConn, "insert into t4(id1, id2) values(1,'a'), (2,'Abc'), (3,'b'), (4,'c'), (5,'test')")
	utils.ExecCompareMySQL(t, vtConn, mysqlConn, "insert into t4(id1, id2) values(6,'d'), (7,'e'), (8,'F')")
	// test ordering of varchar column
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, "select id1, id2 from t4 order by id2 desc", `[[INT64(5) VARCHAR("test")] [INT64(8) VARCHAR("F")] [INT64(7) VARCHAR("e")] [INT64(6) VARCHAR("d")] [INT64(4) VARCHAR("c")] [INT64(3) VARCHAR("b")] [INT64(2) VARCHAR("Abc")] [INT64(1) VARCHAR("a")]]`)
	// test ordering of int column
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, "select id1, id2 from t4 order by id1 desc", `[[INT64(8) VARCHAR("F")] [INT64(7) VARCHAR("e")] [INT64(6) VARCHAR("d")] [INT64(5) VARCHAR("test")] [INT64(4) VARCHAR("c")] [INT64(3) VARCHAR("b")] [INT64(2) VARCHAR("Abc")] [INT64(1) VARCHAR("a")]]`)

	defer func() {
		utils.Exec(t, vtConn, "set workload = oltp")
		utils.ExecCompareMySQL(t, vtConn, mysqlConn, "delete from t4")
	}()
	// Test the same queries in streaming mode
	utils.Exec(t, vtConn, "set workload = olap")
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, "select id1, id2 from t4 order by id2 desc", `[[INT64(5) VARCHAR("test")] [INT64(8) VARCHAR("F")] [INT64(7) VARCHAR("e")] [INT64(6) VARCHAR("d")] [INT64(4) VARCHAR("c")] [INT64(3) VARCHAR("b")] [INT64(2) VARCHAR("Abc")] [INT64(1) VARCHAR("a")]]`)
	utils.AssertMatchesCompareMySQL(t, vtConn, mysqlConn, "select id1, id2 from t4 order by id1 desc", `[[INT64(8) VARCHAR("F")] [INT64(7) VARCHAR("e")] [INT64(6) VARCHAR("d")] [INT64(5) VARCHAR("test")] [INT64(4) VARCHAR("c")] [INT64(3) VARCHAR("b")] [INT64(2) VARCHAR("Abc")] [INT64(1) VARCHAR("a")]]`)
}
