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

package mysqlctl

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/sqlmode"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
)

// TestExecuteSuperQueryListTaintedDiscardsConnection verifies that operator-supplied SQL
// executed through the tainted variant cannot leak session state (e.g. sql_mode) into the
// dba pool: the connection is discarded, and the next pool use dials a fresh connection —
// observable through the neutralization statement every new connection runs.
func TestExecuteSuperQueryListTaintedDiscardsConnection(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	dbc := dbconfigs.NewTestDBConfigs(*db.ConnParams(), *db.ConnParams(), "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("set sql_mode = 'NO_BACKSLASH_ESCAPES'", &sqltypes.Result{})
	db.AddQuery("select 42", &sqltypes.Result{})

	// a normal super query dials one pool connection: one neutralization
	require.NoError(t, testMysqld.ExecuteSuperQueryList(t.Context(), []string{"select 42"}))
	require.Equal(t, 1, db.GetQueryCalledNum(sqlmode.NeutralizeSessionQuery))

	// the tainted variant reuses the pooled connection (no new dial), executes the
	// session-changing SQL, and discards the connection instead of recycling it
	require.NoError(t, testMysqld.ExecuteSuperQueryListTainted(t.Context(), []string{"set sql_mode = 'NO_BACKSLASH_ESCAPES'"}))
	require.Equal(t, 1, db.GetQueryCalledNum("set sql_mode = 'NO_BACKSLASH_ESCAPES'"))

	// the discarded connection is gone: the next pool use dials fresh and re-neutralizes
	require.NoError(t, testMysqld.ExecuteSuperQueryList(t.Context(), []string{"select 42"}))
	require.Equal(t, 2, db.GetQueryCalledNum(sqlmode.NeutralizeSessionQuery))
}
