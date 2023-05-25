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
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"

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

func TestSimpleOrderBy(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(id1, id2) values (0,10),(1,9),(2,8),(3,7),(4,6),(5,5)")
	mcmp.AssertMatches(`SELECT id2 FROM t1 ORDER BY id2 ASC`, `[[INT64(5)] [INT64(6)] [INT64(7)] [INT64(8)] [INT64(9)] [INT64(10)]]`)
}

func TestOrderBy(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t4(id1, id2) values(1,'a'), (2,'Abc'), (3,'b'), (4,'c'), (5,'test')")
	mcmp.Exec("insert into t4(id1, id2) values(6,'d'), (7,'e'), (8,'F')")
	// test ordering of varchar column
	mcmp.AssertMatches("select id1, id2 from t4 order by id2 desc", `[[INT64(5) VARCHAR("test")] [INT64(8) VARCHAR("F")] [INT64(7) VARCHAR("e")] [INT64(6) VARCHAR("d")] [INT64(4) VARCHAR("c")] [INT64(3) VARCHAR("b")] [INT64(2) VARCHAR("Abc")] [INT64(1) VARCHAR("a")]]`)
	// test ordering of int column
	mcmp.AssertMatches("select id1, id2 from t4 order by id1 desc", `[[INT64(8) VARCHAR("F")] [INT64(7) VARCHAR("e")] [INT64(6) VARCHAR("d")] [INT64(5) VARCHAR("test")] [INT64(4) VARCHAR("c")] [INT64(3) VARCHAR("b")] [INT64(2) VARCHAR("Abc")] [INT64(1) VARCHAR("a")]]`)
	// test ordering of complex column
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ id1, id2 from t4 order by reverse(id2) desc", `[[INT64(5) VARCHAR("test")] [INT64(8) VARCHAR("F")] [INT64(7) VARCHAR("e")] [INT64(6) VARCHAR("d")] [INT64(2) VARCHAR("Abc")] [INT64(4) VARCHAR("c")] [INT64(3) VARCHAR("b")] [INT64(1) VARCHAR("a")]]`)

	defer func() {
		utils.Exec(t, mcmp.VtConn, "set workload = oltp")
		_, _ = mcmp.ExecAndIgnore("delete from t4")
	}()
	// Test the same queries in streaming mode
	utils.Exec(t, mcmp.VtConn, "set workload = olap")
	mcmp.AssertMatches("select id1, id2 from t4 order by id2 desc", `[[INT64(5) VARCHAR("test")] [INT64(8) VARCHAR("F")] [INT64(7) VARCHAR("e")] [INT64(6) VARCHAR("d")] [INT64(4) VARCHAR("c")] [INT64(3) VARCHAR("b")] [INT64(2) VARCHAR("Abc")] [INT64(1) VARCHAR("a")]]`)
	mcmp.AssertMatches("select id1, id2 from t4 order by id1 desc", `[[INT64(8) VARCHAR("F")] [INT64(7) VARCHAR("e")] [INT64(6) VARCHAR("d")] [INT64(5) VARCHAR("test")] [INT64(4) VARCHAR("c")] [INT64(3) VARCHAR("b")] [INT64(2) VARCHAR("Abc")] [INT64(1) VARCHAR("a")]]`)
	mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ id1, id2 from t4 order by reverse(id2) desc", `[[INT64(5) VARCHAR("test")] [INT64(8) VARCHAR("F")] [INT64(7) VARCHAR("e")] [INT64(6) VARCHAR("d")] [INT64(2) VARCHAR("Abc")] [INT64(4) VARCHAR("c")] [INT64(3) VARCHAR("b")] [INT64(1) VARCHAR("a")]]`)
}
