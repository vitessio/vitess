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
	if utils.BinaryIsAtLeastAtVersion(17, "vtgate") {
		mcmp.AssertMatches("select id1, id2 from t4 order by reverse(id2) desc", `[[INT64(5) VARCHAR("test")] [INT64(8) VARCHAR("F")] [INT64(7) VARCHAR("e")] [INT64(6) VARCHAR("d")] [INT64(2) VARCHAR("Abc")] [INT64(4) VARCHAR("c")] [INT64(3) VARCHAR("b")] [INT64(1) VARCHAR("a")]]`)
	}

	defer func() {
		utils.Exec(t, mcmp.VtConn, "set workload = oltp")
		_, _ = mcmp.ExecAndIgnore("delete from t4")
	}()
	// Test the same queries in streaming mode
	utils.Exec(t, mcmp.VtConn, "set workload = olap")
	mcmp.AssertMatches("select id1, id2 from t4 order by id2 desc", `[[INT64(5) VARCHAR("test")] [INT64(8) VARCHAR("F")] [INT64(7) VARCHAR("e")] [INT64(6) VARCHAR("d")] [INT64(4) VARCHAR("c")] [INT64(3) VARCHAR("b")] [INT64(2) VARCHAR("Abc")] [INT64(1) VARCHAR("a")]]`)
	mcmp.AssertMatches("select id1, id2 from t4 order by id1 desc", `[[INT64(8) VARCHAR("F")] [INT64(7) VARCHAR("e")] [INT64(6) VARCHAR("d")] [INT64(5) VARCHAR("test")] [INT64(4) VARCHAR("c")] [INT64(3) VARCHAR("b")] [INT64(2) VARCHAR("Abc")] [INT64(1) VARCHAR("a")]]`)
	if utils.BinaryIsAtLeastAtVersion(17, "vtgate") {
		mcmp.AssertMatches("select id1, id2 from t4 order by reverse(id2) desc", `[[INT64(5) VARCHAR("test")] [INT64(8) VARCHAR("F")] [INT64(7) VARCHAR("e")] [INT64(6) VARCHAR("d")] [INT64(2) VARCHAR("Abc")] [INT64(4) VARCHAR("c")] [INT64(3) VARCHAR("b")] [INT64(1) VARCHAR("a")]]`)
	}
}

func TestOrderByComplex(t *testing.T) {
	// tests written to try to trick the ORDER BY engine and planner
	utils.SkipIfBinaryIsBelowVersion(t, 20, "vtgate")

	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into user(id, col, email) values(1,1,'a'), (2,2,'Abc'), (3,3,'b'), (4,4,'c'), (5,2,'test'), (6,1,'test'), (7,2,'a'), (8,3,'b'), (9,4,'c3'), (10,2,'d')")

	queries := []string{
		"select email, max(col) from user group by email order by col",
		"select email, max(col) from user group by email order by col + 1",
		"select email, max(col) from user group by email order by max(col)",
		"select email, max(col) from user group by email order by max(col) + 1",
		"select email, max(col) from user group by email order by min(col)",
		"select email, max(col) as col from user group by email order by col",
		"select email, max(col) as col from user group by email order by max(col)",
		"select email, max(col) as col from user group by email order by col + 1",
		"select email, max(col) as col from user group by email order by email + col",
		"select email, max(col) as col from user group by email order by email + max(col)",
		"select email, max(col) as col from user group by email order by email, col",
		"select email, max(col) as xyz from user group by email order by email, xyz",
		"select email, max(col) as xyz from user group by email order by email, max(xyz)",
		"select email, max(col) as xyz from user group by email order by email, abs(xyz)",
		"select email, max(col) as xyz from user group by email order by email, max(col)",
		"select email, max(col) as xyz from user group by email order by email, abs(col)",
		"select email, max(col) as xyz from user group by email order by xyz + email",
		"select email, max(col) as xyz from user group by email order by abs(xyz) + email",
		"select email, max(col) as xyz from user group by email order by abs(xyz)",
		"select email, max(col) as xyz from user group by email order by abs(col)",
		"select email, max(col) as max_col from user group by email order by max_col desc, length(email)",
		"select email, max(col) as max_col, min(col) as min_col from user group by email order by max_col - min_col",
		"select email, max(col) as col1, count(*) as col2 from user group by email order by col2 * col1",
		"select email, sum(col) as sum_col from user group by email having sum_col > 10 order by sum_col / count(email)",
		"select email, max(col) as max_col, char_length(email) as len_email from user group by email order by len_email, max_col desc",
		"select email, max(col) as col_alias from user group by email order by case when col_alias > 100 then 0 else 1 end, col_alias",
		"select email, count(*) as cnt, max(col) as max_col from user group by email order by cnt desc, max_col + cnt",
		"select email, max(col) as max_col from user group by email order by if(max_col > 50, max_col, -max_col) desc",
		"select email, max(col) as col, sum(col) as sum_col from user group by email order by col * sum_col desc",
		"select email, max(col) as col, (select min(col) from user as u2 where u2.email = user.email) as min_col from user group by email order by col - min_col",
		"select email, max(col) as max_col, (max(col) % 10) as mod_col from user group by email order by mod_col, max_col",
		"select email, max(col) as 'value', count(email) as 'number' from user group by email order by 'number', 'value'",
		"select email, max(col) as col, concat('email: ', email, ' col: ', max(col)) as complex_alias from user group by email order by complex_alias desc",
		"select email, max(col) as max_col from user group by email union select email, min(col) as min_col from user group by email order by email",
		"select email, max(col) as col from user where col > 50 group by email order by col desc",
		"select email, max(col) as col from user group by email order by length(email), col",
		"select email, max(col) as max_col, substring(email, 1, 3) as sub_email from user group by email order by sub_email, max_col desc",
		"select email, max(col) as max_col from user group by email order by reverse(email), max_col",
		"select email, max(col) as max_col from user group by email having max_col > avg(max_col) order by max_col desc",
		"select email, count(*) as count, max(col) as max_col from user group by email order by count * max_col desc",
		"select email, max(col) as col_alias from user group by email order by col_alias limit 10",
		"select email, max(col) as col from user group by email order by col desc, email",
		"select concat(email, ' ', max(col)) as combined from user group by email order by combined desc",
		"select email, max(col) as max_col from user group by email order by ascii(email), max_col",
		"select email, char_length(email) as email_length, max(col) as max_col from user group by email order by email_length desc, max_col",
		"select email, max(col) as col from user group by email having col between 10 and 100 order by col",
		"select email, max(col) as max_col, min(col) as min_col from user group by email order by max_col + min_col desc",
		"select email, max(col) as 'max', count(*) as 'count' from user group by email order by 'max' desc, 'count'",
		"select email, max(col) as max_col from (select email, col from user where col > 20) as filtered group by email order by max_col",
		"select a.email, a.max_col from (select email, max(col) as max_col from user group by email) as a order by a.max_col desc",
		"select email, max(col) as max_col from user where email like 'a%' group by email order by max_col, email",
		`select email, max(col) as max_col from user group by email union select email, avg(col) as avg_col from user group by email order by email desc`,
	}

	for _, query := range queries {
		mcmp.Run(query, func(mcmp *utils.MySQLCompare) {
			_, _ = mcmp.ExecAllowAndCompareError(query)
		})
	}
}
