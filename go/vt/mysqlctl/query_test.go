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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/sqlmode"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log/logtest"
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
	// setup statement every new connection runs: one for the connection
	// the list ran on, and one for its replacement.
	require.Equal(t, 2, db.GetQueryCalledNum(sqlmode.SessionSetupQuery("utf8mb4", "utf8mb4_0900_ai_ci")),
		"the connection went back to the dba pool instead of being discarded")

	conn, err := getPoolReconnect(t.Context(), testMysqld.dbaPool)
	require.NoError(t, err)
	t.Cleanup(conn.Recycle)

	// An exchange already in flight is interrupted by killing it and, if that
	// does not bring it back, by closing the connection underneath it. Without
	// the second half a server that answers nothing holds the caller for as long
	// as it likes.
	t.Run("a stuck exchange is interrupted", func(t *testing.T) {
		previousGrace := killGraceTimeout
		killGraceTimeout = 100 * time.Millisecond
		t.Cleanup(func() { killGraceTimeout = previousGrace })

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
			// The fake server never sends anything unasked, so this is a read the
			// server never answers: it comes back when the connection is closed,
			// and not before.
			_, err := conn.Conn.ReadPacket()
			return err
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
	t.Cleanup(db.Close)

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	t.Cleanup(testMysqld.Close)

	logs := logtest.Capture(t, slog.LevelInfo)

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
	t.Cleanup(cancel)
	err := testMysqld.ExecuteSuperQueryList(ctx, []string{query})
	// The fake returns the blocked query's result successfully once the kill
	// releases it, so this lands in the "ExecuteFetch() may have succeeded
	// before we tried to kill it" branch. The query can only complete after
	// the kill callback fires, so reaching here proves the kill path ran.
	require.NoError(t, err)

	redacted := `START xxx USER = 'vt_repl', PASSWORD = '****'`
	assert.Equal(t, redacted, logs.Record(t, "exec")["query"])
	killed := logs.Record(t, "Mysqld.executeWithContext(): killing connection due to timeout of query")
	assert.Equal(t, redacted, killed["query"])
	assert.Contains(t, killed, "connection_id")
	assert.NotContains(t, logs.String(), "secret")
}

// TestExecuteFetchDirectContextTimeoutRedactsQuery verifies that the kill
// path of executeFetchDirectContext redacts the password in the logged query.
func TestExecuteFetchDirectContextTimeoutRedactsQuery(t *testing.T) {
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	t.Cleanup(testMysqld.Close)

	query := `START xxx USER = 'vt_repl', PASSWORD = 'secret'`
	db.AddQuery(query, &sqltypes.Result{})
	unblock := make(chan struct{})
	db.SetBeforeFunc(query, func() {
		<-unblock
	})
	db.AddQueryPatternWithCallback(`kill \d+`, &sqltypes.Result{}, func(string) {
		close(unblock)
	})

	conn, err := testMysqld.GetDbaConnection(t.Context())
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	logs := logtest.Capture(t, slog.LevelInfo)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	t.Cleanup(cancel)
	_, _ = testMysqld.executeFetchDirectContext(ctx, conn, query)

	killed := logs.Record(t, "Mysqld.executeFetchDirectContext(): killing connection due to timeout of query")
	assert.Equal(t, `START xxx USER = 'vt_repl', PASSWORD = '****'`, killed["query"])
	assert.Contains(t, killed, "connection_id")
	assert.NotContains(t, logs.String(), "secret")
}

// TestExecuteSuperQueryListTaintedDiscardsConnection verifies that operator-supplied SQL
// executed through the tainted variant cannot leak session state (e.g. sql_mode) into the
// dba pool: the connection is discarded, and the next pool use dials a fresh connection —
// observable through the setup statement every new connection runs.
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
	require.Equal(t, 1, db.GetQueryCalledNum(sqlmode.SessionSetupQuery("utf8mb4", "utf8mb4_0900_ai_ci")))

	// the tainted variant reuses the pooled connection (no new dial), executes the
	// session-changing SQL, and discards the connection instead of recycling it
	require.NoError(t, testMysqld.ExecuteSuperQueryListTainted(t.Context(), []string{"set sql_mode = 'NO_BACKSLASH_ESCAPES'"}))
	require.Equal(t, 1, db.GetQueryCalledNum("set sql_mode = 'NO_BACKSLASH_ESCAPES'"))

	// the discarded connection is gone: the next pool use dials fresh and re-neutralizes
	require.NoError(t, testMysqld.ExecuteSuperQueryList(t.Context(), []string{"select 42"}))
	require.Equal(t, 2, db.GetQueryCalledNum(sqlmode.SessionSetupQuery("utf8mb4", "utf8mb4_0900_ai_ci")))
}

// TestSuperQueriesHonorContext checks that DBA-pool calls return when their
// context ends while mysqld isn't answering their query, even if the handshake
// of the connection that replaces the one closed on cancellation never
// completes. Callers make these calls while holding the tablet's action lock,
// which a hung mysqld must not keep held.
func TestSuperQueriesHonorContext(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		call  func(context.Context, *Mysqld) error
		setup func(*fakesqldb.DB)
	}{{
		name:  "semi-sync enable",
		query: "SET GLOBAL rpl_semi_sync_source_enabled = 1, GLOBAL rpl_semi_sync_replica_enabled = 1",
		call: func(ctx context.Context, mysqld *Mysqld) error {
			mysqld.semiSyncType = mysql.SemiSyncTypeSource
			return mysqld.SetSemiSyncEnabled(ctx, true, true)
		},
	}, {
		name:  "semi-sync plugin check",
		query: "SHOW VARIABLES LIKE 'rpl_semi_sync_%_enabled'",
		call: func(ctx context.Context, mysqld *Mysqld) error {
			_, err := mysqld.SemiSyncExtensionLoaded(ctx)
			return err
		},
	}, {
		name:  "promotion status read",
		query: "SHOW REPLICA STATUS",
		call: func(ctx context.Context, mysqld *Mysqld) error {
			_, err := mysqld.Promote(ctx, nil)
			return err
		},
	}, {
		name:  "replication status",
		query: "SHOW REPLICA STATUS",
		call: func(ctx context.Context, mysqld *Mysqld) error {
			_, err := mysqld.ReplicationStatus(ctx)
			return err
		},
	}, {
		name:  "executed position",
		query: "SELECT @@global.gtid_executed",
		call: func(ctx context.Context, mysqld *Mysqld) error {
			_, err := mysqld.PrimaryPosition(ctx)
			return err
		},
	}, {
		name:  "primary status",
		query: "SHOW BINARY LOG STATUS",
		call: func(ctx context.Context, mysqld *Mysqld) error {
			_, err := mysqld.PrimaryStatus(ctx)
			return err
		},
	}, {
		name:  "primary status UUID",
		query: "SELECT @@global.server_uuid",
		call: func(ctx context.Context, mysqld *Mysqld) error {
			_, err := mysqld.PrimaryStatus(ctx)
			return err
		},
		setup: func(db *fakesqldb.DB) {
			// The first read must succeed so cancellation reaches the UUID read.
			status := sqltypes.MakeTestResult(sqltypes.MakeTestFields("File|Position|Binlog_Do_DB|Binlog_Ignore_DB|Executed_Gtid_Set", "varchar|uint64|varchar|varchar|varchar"), "mysql-bin.000001|123|||")
			db.AddQuery("SHOW BINARY LOG STATUS", status)
		},
	}, {
		name:  "read-only check",
		query: "SHOW VARIABLES LIKE 'read_only'",
		call: func(ctx context.Context, mysqld *Mysqld) error {
			_, err := mysqld.IsReadOnly(ctx)
			return err
		},
	}, {
		name:  "promotion position",
		query: "SELECT @@global.gtid_executed",
		call: func(ctx context.Context, mysqld *Mysqld) error {
			_, err := mysqld.Promote(ctx, nil)
			return err
		},
	}, {
		name:  "receiver restart",
		query: "STOP REPLICA IO_THREAD",
		call: func(ctx context.Context, mysqld *Mysqld) error {
			return mysqld.RestartReplicationReceiver(ctx, nil)
		},
	}, {
		name:  "semi-sync blocked check",
		query: "select variable_value from performance_schema.global_status where regexp_like(variable_name, 'Rpl_semi_sync_(source|master)_wait_sessions')",
		call: func(ctx context.Context, mysqld *Mysqld) error {
			_, err := mysqld.IsSemiSyncBlocked(ctx)
			return err
		},
	}, {
		name:  "super_read_only",
		query: "SET GLOBAL super_read_only = 'ON'",
		call: func(ctx context.Context, mysqld *Mysqld) error {
			_, err := mysqld.SetSuperReadOnly(ctx, true, WithLockWaitTimeout(time.Second))
			return err
		},
	}, {
		name:  "global read lock",
		query: "FLUSH TABLES WITH READ LOCK",
		call: func(ctx context.Context, mysqld *Mysqld) error {
			return mysqld.AcquireGlobalReadLock(ctx)
		},
	}, {
		name:  "connection check",
		query: "SELECT 1",
		call: func(ctx context.Context, mysqld *Mysqld) error {
			conn, err := getPoolReconnect(ctx, mysqld.dbaPool)
			if err == nil {
				conn.Recycle()
			}
			return err
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			previousGrace := killGraceTimeout
			killGraceTimeout = 100 * time.Millisecond
			t.Cleanup(func() { killGraceTimeout = previousGrace })
			db := fakesqldb.New(t)
			t.Cleanup(db.Close)
			db.AddQueryPattern(".*", &sqltypes.Result{})
			if tc.setup != nil {
				tc.setup(db)
			}
			// The fake server does not answer the query until the test ends.
			release := make(chan struct{})
			started := make(chan struct{})
			var startOnce sync.Once
			db.AddQuery(tc.query, &sqltypes.Result{})
			db.SetBeforeFunc(tc.query, func() {
				startOnce.Do(func() { close(started) })
				<-release
			})

			params := *db.ConnParams()
			dbcfgs := dbconfigs.NewTestDBConfigs(params, params, "fakesqldb")
			mysqld := NewMysqld(dbcfgs)
			t.Cleanup(mysqld.Close)
			// Cleanups run in reverse, so the blocked query returns before the
			// pool closes.
			t.Cleanup(func() { close(release) })
			// Only the first pool connection completes its handshake.
			mysqld.dbaPool.Close()
			mysqld.dbaPool = dbconnpool.NewConnectionPool("DbaConnPool", nil, dbaPoolSize, DbaIdleTimeout, 0, 0)
			var connects atomic.Int32
			mysqld.dbaPool.ConnPool.Open(func(ctx context.Context) (*dbconnpool.DBConnection, error) {
				if connects.Add(1) == 1 {
					return dbconnpool.NewDBConnection(ctx, dbcfgs.DbaWithDB())
				}
				select {
				case <-release:
				case <-ctx.Done():
				}
				return nil, errors.New("handshake did not complete")
			}, nil)

			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			returned := make(chan error, 1)
			go func() { returned <- tc.call(ctx, mysqld) }()
			require.Eventually(t, func() bool {
				select {
				case <-started:
					return true
				default:
					return false
				}
			}, 30*time.Second, 10*time.Millisecond, "the query did not reach the server")
			cancel()
			var err error
			require.Eventually(t, func() bool {
				select {
				case err = <-returned:
					return true
				default:
					return false
				}
			}, 30*time.Second, 10*time.Millisecond, "the call did not return after its context ended")
			// SetSemiSyncEnabled formats the error rather than wrapping it.
			require.ErrorContains(t, err, context.Canceled.Error())
			// The canceled call closed its connection, so the pool dials a
			// replacement, whose handshake never completes.
			assert.Eventually(t, func() bool { return connects.Load() > 1 }, 30*time.Second, 10*time.Millisecond)
		})
	}
}

// TestReleaseGlobalReadLockHonorsContext checks that ReleaseGlobalReadLock
// returns when its context ends, even if UNLOCK TABLES succeeds while the
// timed-out query is being killed, which closes the connection, and the
// replacement's handshake never completes.
func TestReleaseGlobalReadLockHonorsContext(t *testing.T) {
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	db.AddQueryPattern(".*", &sqltypes.Result{})
	params := *db.ConnParams()
	dbcfgs := dbconfigs.NewTestDBConfigs(params, params, "fakesqldb")
	mysqld := NewMysqld(dbcfgs)
	t.Cleanup(mysqld.Close)
	// Only the first pool connection completes its handshake.
	mysqld.dbaPool.Close()
	mysqld.dbaPool = dbconnpool.NewConnectionPool("DbaConnPool", nil, dbaPoolSize, DbaIdleTimeout, 0, 0)
	var connects atomic.Int32
	mysqld.dbaPool.ConnPool.Open(func(ctx context.Context) (*dbconnpool.DBConnection, error) {
		if connects.Add(1) == 1 {
			return dbconnpool.NewDBConnection(ctx, dbcfgs.DbaWithDB())
		}
		<-ctx.Done()
		return nil, errors.New("handshake did not complete")
	}, nil)
	require.NoError(t, mysqld.AcquireGlobalReadLock(t.Context()))

	// UNLOCK TABLES succeeds once the kill of the timed-out query starts.
	unlocking := make(chan struct{})
	killed := make(chan struct{})
	db.AddQuery("UNLOCK TABLES", &sqltypes.Result{})
	db.SetBeforeFunc("UNLOCK TABLES", func() {
		close(unlocking)
		<-killed
	})
	kill := fmt.Sprintf("kill %d", mysqld.lockConn.Conn.ID())
	db.AddQuery(kill, &sqltypes.Result{})
	db.SetBeforeFunc(kill, func() { close(killed) })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	returned := make(chan error, 1)
	go func() { returned <- mysqld.ReleaseGlobalReadLock(ctx) }()
	require.Eventually(t, func() bool {
		select {
		case <-unlocking:
			return true
		default:
			return false
		}
	}, 30*time.Second, 10*time.Millisecond, "UNLOCK TABLES did not run")
	cancel()
	var err error
	require.Eventually(t, func() bool {
		select {
		case err = <-returned:
			return true
		default:
			return false
		}
	}, 30*time.Second, 10*time.Millisecond, "ReleaseGlobalReadLock did not return after its context ended")
	require.NoError(t, err)
}
