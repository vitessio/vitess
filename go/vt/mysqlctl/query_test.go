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
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/sqlmode"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
)

// TestExecuteSuperQueryListMulti checks that an entry of the list may hold
// several statements, which is what operator written SQL such as the init SQL of
// a backup does, and that such an entry neither fails nor stops the entry after
// it.
func TestExecuteSuperQueryListMulti(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQueryPattern(".*", &sqltypes.Result{})

	params := *db.ConnParams()
	testMysqld := NewMysqld(dbconfigs.NewTestDBConfigs(params, params, "fakesqldb"))
	defer testMysqld.Close()

	require.NoError(t, testMysqld.ExecuteSuperQueryListMulti(t.Context(), []string{
		"set global read_only=off;set global super_read_only=off",
		"optimize no_write_to_binlog table mysql.gtid_executed",
	}))

	// Every statement has to reach the server on its own. An entry holding
	// several would otherwise arrive as a single query that the server cannot
	// parse, which is also what would keep the entry after it from running.
	for _, statement := range []string{
		"set global read_only=off",
		"set global super_read_only=off",
		"optimize no_write_to_binlog table mysql.gtid_executed",
	} {
		require.Equal(t, 1, db.GetQueryCalledNum(statement), "statement %q did not reach the server on its own, query log: %v", statement, db.QueryLog())
	}

	// The connection is discarded rather than handed back to the dba pool: it can
	// send a batch, which nothing else drawing on the pool expects, and operator
	// written SQL may have changed session state such as sql_mode. The pool opens
	// a replacement for a discarded connection, which is observable through the
	// neutralization statement every new connection runs: one for the connection
	// the list ran on, and one for its replacement.
	require.Equal(t, 2, db.GetQueryCalledNum(sqlmode.NeutralizeSessionQuery),
		"the connection went back to the dba pool instead of being discarded")

	conn, err := getPoolReconnect(t.Context(), testMysqld.dbaPool)
	require.NoError(t, err)
	t.Cleanup(conn.Recycle)

	// An exchange already in flight is interrupted by killing it and, if that
	// does not bring it back, by closing the connection underneath it. Without
	// the second half a server that answers nothing holds the caller for as long
	// as it likes.
	t.Run("a stuck exchange is interrupted", func(t *testing.T) {
		killGraceTimeout = 100 * time.Millisecond
		t.Cleanup(func() { killGraceTimeout = 5 * time.Second })

		conn, err := getPoolReconnect(t.Context(), testMysqld.dbaPool)
		require.NoError(t, err)
		t.Cleanup(conn.Recycle)

		ctx, cancel := context.WithCancel(t.Context())
		running := make(chan struct{})
		go func() {
			<-running
			cancel()
		}()

		err = testMysqld.executeWithContext(ctx, conn, comSetOption, func() error {
			close(running)
			// Stand in for a read the server never answers: it comes back when
			// the connection is closed, and not before.
			for !conn.Conn.IsClosed() {
				time.Sleep(time.Millisecond)
			}
			return errors.New("connection closed underneath the exchange")
		})
		require.ErrorIs(t, err, context.Canceled)
		require.True(t, conn.Conn.IsClosed(), "the connection has to be closed to interrupt the exchange")
	})

	// The capability exchanges are a blocking write and read each, so they run
	// under the caller's context like the queries do.
	t.Run("a done context stops an exchange before it starts", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		ran := false
		err := testMysqld.executeWithContext(ctx, conn, comSetOption, func() error {
			ran = true
			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, ran)
	})
}

// TestExecuteFetchContextTimeoutRedactsQuery verifies that when a query is
// killed because its context expired, the query logged on the kill path has
// its password redacted, like the exec log on the happy path.
func TestExecuteFetchContextTimeoutRedactsQuery(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	// Capture the structured log output.
	var logBuf bytes.Buffer
	oldLogger := log.SwapLogger(slog.New(slog.NewTextHandler(&logBuf, nil)))
	defer log.SwapLogger(oldLogger)

	query := `START xxx USER = 'vt_repl', PASSWORD = 'secret'`
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery(query, &sqltypes.Result{})
	// Block the query until the timeout path kills the connection, so the
	// kill branch is taken deterministically.
	unblock := make(chan struct{})
	db.SetBeforeFunc(query, func() {
		<-unblock
	})
	db.AddQueryPatternWithCallback(`kill \d+`, &sqltypes.Result{}, func(string) {
		close(unblock)
	})

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := testMysqld.ExecuteSuperQueryList(ctx, []string{query})
	// The fake returns the blocked query's result successfully once the kill
	// releases it, so this lands in the "ExecuteFetch() may have succeeded
	// before we tried to kill it" branch. The query can only complete after
	// the kill callback fires, so reaching here proves the kill path ran.
	require.NoError(t, err)

	logs := logBuf.String()
	assert.Contains(t, logs, "killing connID")
	assert.Contains(t, logs, `PASSWORD = '****'`)
	assert.NotContains(t, logs, "secret")
}

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
