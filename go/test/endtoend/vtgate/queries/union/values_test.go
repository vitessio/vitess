/*
Copyright 2026 The Vitess Authors.

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
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestValuesStatement(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(id1, id2) values (1, 1), (2, 2)")

	for _, workload := range []string{"oltp", "olap"} {
		mcmp.Run(workload, func(mcmp *utils.MySQLCompare) {
			utils.Exec(t, mcmp.VtConn, "set workload = "+workload)

			// top-level VALUES statement
			mcmp.AssertMatches("values row(1, 'a'), row(2, 'b')", `[[INT64(1) VARCHAR("a")] [INT64(2) VARCHAR("b")]]`)

			// MySQL silently ignores ORDER BY on a VALUES statement, while vtgate
			// deliberately sorts the rows itself. The results diverge by design, so
			// we assert directly against vtgate instead of comparing with MySQL.
			utils.AssertMatches(t, mcmp.VtConn, "values row(2), row(1) order by column_0", `[[INT64(1)] [INT64(2)]]`)
			utils.AssertMatches(t, mcmp.VtConn, "values row(2), row(1) order by column_0 limit 1", `[[INT64(1)]]`)

			// union between a sharded table and a VALUES statement: the VALUES row
			// must show up exactly once even though the select scatters over shards
			mcmp.AssertMatchesNoOrder("select id1 from t1 union values row(1)", `[[INT64(1)] [INT64(2)]]`)
			mcmp.AssertMatchesNoOrder("select id1 from t1 union values row(42)", `[[INT64(1)] [INT64(2)] [INT64(42)]]`)

			// UNION ALL with an ordered VALUES arm: vtgate sorts the VALUES arm and
			// adds a weight_string helper column for the sort. That helper column
			// must be truncated so both Concatenate arms produce the same number of
			// columns; otherwise the query fails at execution. The row multiset is
			// order-independent here, so this still compares against MySQL.
			mcmp.AssertMatchesNoOrder("(values row(2), row(1) order by column_0) union all select id1 from t1", `[[INT64(1)] [INT64(1)] [INT64(2)] [INT64(2)]]`)

			// MySQL ignores ORDER BY on the VALUES arm while vtgate honors it, so the
			// ordering diverges by design: assert against vtgate directly. Both arms
			// are single-shard reference routes, so the overall order is deterministic.
			utils.AssertMatches(t, mcmp.VtConn, "values row(1) union all (values row(3), row(2) order by column_0)", `[[INT64(1)] [INT64(2)] [INT64(3)]]`)

			// A volatile ordering expression must be evaluated exactly once: the
			// value vtgate sorts by has to be the value it returns. If the sort
			// key were computed from a re-evaluation of rand(), the returned
			// column_0 values would not come back in ascending order. Compare
			// against a sorted copy of the returned values (not MySQL, which
			// ignores ORDER BY on VALUES).
			qr := utils.Exec(t, mcmp.VtConn, "values row(rand()), row(rand()), row(rand()), row(rand()) order by column_0 + 0")
			got := make([]float64, 0, len(qr.Rows))
			for _, row := range qr.Rows {
				f, err := row[0].ToFloat64()
				require.NoError(t, err)
				got = append(got, f)
			}
			require.True(t, sort.Float64sAreSorted(got), "returned column_0 values must be in ascending order, got %v", got)

			// VALUES as a derived table, with generated and explicit column names
			mcmp.AssertMatches("select * from (values row(1, 'a'), row(2, 'b')) as dt", `[[INT64(1) VARCHAR("a")] [INT64(2) VARCHAR("b")]]`)
			mcmp.AssertMatches("select dt.c1 from (values row(1, 'a'), row(2, 'b')) as dt(c1, c2)", `[[INT64(1)] [INT64(2)]]`)
			mcmp.AssertMatches("select * from (values row(1, 'a'), row(2, 'b')) as dt where dt.column_0 = 2", `[[INT64(2) VARCHAR("b")]]`)
		})
	}
}
