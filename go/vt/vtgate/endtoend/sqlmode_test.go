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

package endtoend

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

// Under sql_mode=ANSI, MySQL returns an error for a set function with an outer
// reference that cannot be aggregated in the outer query against which the
// reference resolves (see "Server SQL Modes" in the MySQL reference manual);
// without ANSI the same query runs, with the aggregate treated as if over a
// constant. The session's ANSI must reach MySQL for that enforcement to fire —
// the mode is forwarded rather than emulated — which this exercises through a
// real vtgate and MySQL on the one path where the construct reaches MySQL at
// all: a shard-targeted session, which routes the query wholesale. (On planned
// routes the vtgate rejects correlated aggregate subqueries before any backend
// is consulted, under any sql_mode.)
func TestSQLModeANSIOuterRefAggregate(t *testing.T) {
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	const query = "select id1 from t1 where t1.id1 in (select max(t1.id2) from t2)"

	// on a planned route the vtgate rejects the construct itself, mode or no mode
	exec(t, conn, "use ks")
	_, err = conn.ExecuteFetch(query, 1000, true)
	require.ErrorContains(t, err, "correlated subquery")

	// a shard-targeted session routes the query wholesale to MySQL
	exec(t, conn, "use `ks:-80`")

	// without ANSI, MySQL accepts the query
	exec(t, conn, query)

	exec(t, conn, "set sql_mode = 'ANSI'")
	_, err = conn.ExecuteFetch(query, 1000, true)
	require.ErrorContains(t, err, "Invalid use of group function")

	// and the session keeps working under ANSI after the failed statement
	exec(t, conn, "select id1 from t1 limit 1")
}
