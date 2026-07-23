/*
Copyright 2019 The Vitess Authors.

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
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
)

func testRedacted(t *testing.T, source, expected string) {
	assert.Equal(t, expected, redactPassword(source))
}

func TestRedactSourcePassword(t *testing.T) {
	// regular test case
	testRedacted(t, `CHANGE REPLICATION SOURCE TO
  SOURCE_PASSWORD = 'AAA',
  SOURCE_CONNECT_RETRY = 1
`,
		`CHANGE REPLICATION SOURCE TO
  SOURCE_PASSWORD = '****',
  SOURCE_CONNECT_RETRY = 1
`)

	// empty password
	testRedacted(t, `CHANGE REPLICATION SOURCE TO
  SOURCE_PASSWORD = '',
  SOURCE_CONNECT_RETRY = 1
`,
		`CHANGE REPLICATION SOURCE TO
  SOURCE_PASSWORD = '****',
  SOURCE_CONNECT_RETRY = 1
`)

	// no beginning match
	testRedacted(t, "aaaaaaaaaaaaaa", "aaaaaaaaaaaaaa")

	// no end match
	testRedacted(t, `CHANGE REPLICATION SOURCE TO
  SOURCE_PASSWORD = 'AAA`, `CHANGE REPLICATION SOURCE TO
  SOURCE_PASSWORD = 'AAA`)
}

func TestRedactMasterPassword(t *testing.T) {
	// regular test case
	testRedacted(t, `CHANGE MASTER TO
  MASTER_PASSWORD = 'AAA',
  MASTER_CONNECT_RETRY = 1
`,
		`CHANGE MASTER TO
  MASTER_PASSWORD = '****',
  MASTER_CONNECT_RETRY = 1
`)

	// empty password
	testRedacted(t, `CHANGE MASTER TO
  MASTER_PASSWORD = '',
  MASTER_CONNECT_RETRY = 1
`,
		`CHANGE MASTER TO
  MASTER_PASSWORD = '****',
  MASTER_CONNECT_RETRY = 1
`)

	// no beginning match
	testRedacted(t, "aaaaaaaaaaaaaa", "aaaaaaaaaaaaaa")

	// no end match
	testRedacted(t, `CHANGE MASTER TO
  MASTER_PASSWORD = 'AAA`, `CHANGE MASTER TO
  MASTER_PASSWORD = 'AAA`)
}

func TestRedactIdentifiedByPassword(t *testing.T) {
	testRedacted(t, "CLONE INSTANCE FROM 'user'@'host':3306 IDENTIFIED BY 'secret' REQUIRE SSL",
		"CLONE INSTANCE FROM 'user'@'host':3306 IDENTIFIED BY '****' REQUIRE SSL")
}

func TestRedactPassword(t *testing.T) {
	// regular case
	testRedacted(t, `START xxx USER = 'vt_repl', PASSWORD = 'AAA'`,
		`START xxx USER = 'vt_repl', PASSWORD = '****'`)

	// empty password
	testRedacted(t, `START xxx USER = 'vt_repl', PASSWORD = ''`,
		`START xxx USER = 'vt_repl', PASSWORD = '****'`)

	// no end match
	testRedacted(t, `START xxx USER = 'vt_repl', PASSWORD = 'AAA`,
		`START xxx USER = 'vt_repl', PASSWORD = 'AAA`)

	// both primary password and password
	testRedacted(t, `START xxx
  SOURCE_PASSWORD = 'AAA',
  PASSWORD = 'BBB'
`,
		`START xxx
  SOURCE_PASSWORD = '****',
  PASSWORD = '****'
`)
}

func TestWaitForReplicationStart(t *testing.T) {
	db := fakesqldb.New(t)
	fakemysqld := NewFakeMysqlDaemon(db)

	defer func() {
		db.Close()
		fakemysqld.Close()
	}()

	err := WaitForReplicationStart(t.Context(), fakemysqld, 2)
	require.NoError(t, err)

	fakemysqld.ReplicationStatusError = errors.New("test error")
	err = WaitForReplicationStart(t.Context(), fakemysqld, 2)
	require.ErrorContains(t, err, "test error")

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(sqltypes.MakeTestFields("Last_SQL_Error|Last_IO_Error", "varchar|varchar"), "test sql error|test io error"))

	err = WaitForReplicationStart(t.Context(), testMysqld, 2)
	assert.ErrorContains(t, err, "Last_SQL_Error: test sql error, Last_IO_Error: test io error")
}

func TestPrepareReplicaForShutdown(t *testing.T) {
	const (
		readDurability  = "SELECT @@global.innodb_flush_log_at_trx_commit, @@global.sync_binlog, @@global.sync_relay_log"
		setFlushLog     = "SET GLOBAL innodb_flush_log_at_trx_commit = 1"
		setSyncBinlog   = "SET GLOBAL sync_binlog = 1"
		setSyncRelayLog = "SET GLOBAL sync_relay_log = 1"
		flushEngineLogs = "FLUSH NO_WRITE_TO_BINLOG ENGINE LOGS"
		flushBinaryLogs = "FLUSH NO_WRITE_TO_BINLOG BINARY LOGS"
		flushRelayLogs  = "FLUSH NO_WRITE_TO_BINLOG RELAY LOGS"
		stopIOThread    = "STOP REPLICA IO_THREAD"
		stopSQLThread   = "STOP REPLICA SQL_THREAD"
	)
	replicaStatus := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
		"source|Yes|Yes",
	)
	relaxedDurability := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"@@global.innodb_flush_log_at_trx_commit|@@global.sync_binlog|@@global.sync_relay_log",
			"int64|int64|int64",
		),
		"2|0|10000",
	)
	replicaState := &replicaShutdownState{
		startReceiver:       true,
		startApplier:        true,
		flushLogAtTrxCommit: "2",
		syncBinlog:          "0",
		syncRelayLog:        "10000",
	}

	testCases := []struct {
		name            string
		status          *sqltypes.Result
		durability      *sqltypes.Result
		rejectedQuery   string
		rejectedError   error
		wantError       string
		wantState       *replicaShutdownState
		wantFlushLog    int
		wantSyncBinlog  int
		wantSet         int
		wantFlushEngine int
		wantFlushBinary int
		wantFlush       int
		wantStop        int
		wantStopSQL     int
	}{
		{
			name:            "replica",
			status:          replicaStatus,
			wantState:       replicaState,
			wantFlushLog:    1,
			wantSyncBinlog:  1,
			wantSet:         1,
			wantFlushEngine: 1,
			wantFlushBinary: 1,
			wantFlush:       1,
			wantStop:        1,
			wantStopSQL:     1,
		},
		{
			name:   "not a replica",
			status: &sqltypes.Result{},
		},
		{
			name:          "cannot read durability settings",
			status:        replicaStatus,
			rejectedQuery: readDurability,
			rejectedError: assert.AnError,
			wantError:     "failed to read the durability settings before shutdown",
		},
		{
			name:   "malformed durability settings result",
			status: replicaStatus,
			durability: sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("@@global.innodb_flush_log_at_trx_commit", "int64"),
				"2",
			),
			wantError: "unexpected result reading the durability settings before shutdown",
		},
		{
			name:          "cannot enable redo log flushing",
			status:        replicaStatus,
			rejectedQuery: setFlushLog,
			rejectedError: assert.AnError,
			wantError:     "failed to establish the crash-safety durability fence before shutdown",
			wantState:     replicaState,
			wantFlushLog:  1,
		},
		{
			name:           "cannot enable binary log syncing",
			status:         replicaStatus,
			rejectedQuery:  setSyncBinlog,
			rejectedError:  assert.AnError,
			wantError:      "failed to establish the crash-safety durability fence before shutdown",
			wantState:      replicaState,
			wantFlushLog:   1,
			wantSyncBinlog: 1,
		},
		{
			name:           "cannot enable relay log syncing",
			status:         replicaStatus,
			rejectedQuery:  setSyncRelayLog,
			rejectedError:  assert.AnError,
			wantError:      "failed to establish the crash-safety durability fence before shutdown",
			wantState:      replicaState,
			wantFlushLog:   1,
			wantSyncBinlog: 1,
			wantSet:        1,
		},
		{
			name:            "cannot flush engine logs",
			status:          replicaStatus,
			rejectedQuery:   flushEngineLogs,
			rejectedError:   assert.AnError,
			wantError:       "failed to establish the crash-safety durability fence before shutdown",
			wantState:       replicaState,
			wantFlushLog:    1,
			wantSyncBinlog:  1,
			wantSet:         1,
			wantFlushEngine: 1,
		},
		{
			name:            "cannot flush binary logs",
			status:          replicaStatus,
			rejectedQuery:   flushBinaryLogs,
			rejectedError:   assert.AnError,
			wantError:       "failed to establish the crash-safety durability fence before shutdown",
			wantState:       replicaState,
			wantFlushLog:    1,
			wantSyncBinlog:  1,
			wantSet:         1,
			wantFlushEngine: 1,
			wantFlushBinary: 1,
		},
		{
			name:            "cannot flush relay logs",
			status:          replicaStatus,
			rejectedQuery:   flushRelayLogs,
			rejectedError:   assert.AnError,
			wantError:       "failed to establish the crash-safety durability fence before shutdown",
			wantState:       replicaState,
			wantFlushLog:    1,
			wantSyncBinlog:  1,
			wantSet:         1,
			wantFlushEngine: 1,
			wantFlushBinary: 1,
			wantFlush:       1,
		},
		{
			name:            "interrupted receiver stop",
			status:          replicaStatus,
			rejectedQuery:   stopIOThread,
			rejectedError:   context.DeadlineExceeded,
			wantState:       replicaState,
			wantFlushLog:    1,
			wantSyncBinlog:  1,
			wantSet:         1,
			wantFlushEngine: 1,
			wantFlushBinary: 1,
			wantFlush:       1,
			wantStop:        1,
			wantStopSQL:     1,
		},
		{
			name:            "interrupted applier stop",
			status:          replicaStatus,
			rejectedQuery:   stopSQLThread,
			rejectedError:   context.DeadlineExceeded,
			wantState:       replicaState,
			wantFlushLog:    1,
			wantSyncBinlog:  1,
			wantSet:         1,
			wantFlushEngine: 1,
			wantFlushBinary: 1,
			wantFlush:       1,
			wantStop:        1,
			wantStopSQL:     1,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			db := fakesqldb.New(t)
			defer db.Close()
			db.AddQuery("SELECT 1", &sqltypes.Result{})
			db.AddQuery("SHOW REPLICA STATUS", testCase.status)
			if testCase.rejectedQuery == readDurability {
				db.AddRejectedQuery(readDurability, testCase.rejectedError)
			} else if testCase.durability != nil {
				db.AddQuery(readDurability, testCase.durability)
			} else {
				db.AddQuery(readDurability, relaxedDurability)
			}
			for _, query := range []string{setFlushLog, setSyncBinlog, setSyncRelayLog, flushEngineLogs, flushBinaryLogs, flushRelayLogs, stopIOThread, stopSQLThread} {
				if query == testCase.rejectedQuery {
					db.AddRejectedQuery(query, testCase.rejectedError)
					continue
				}
				db.AddQuery(query, &sqltypes.Result{})
			}

			params := db.ConnParams()
			cp := *params
			dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
			testMysqld := NewMysqld(dbc)
			defer testMysqld.Close()

			var capturedState *replicaShutdownState
			state, err := testMysqld.prepareReplicaForShutdown(t.Context(), func(state *replicaShutdownState) {
				capturedState = state
			})
			if testCase.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, testCase.wantError)
			}
			assert.Equal(t, testCase.wantState, state)
			assert.Equal(t, testCase.wantState, capturedState, "the captured state must be published before the mutating phase")
			assert.Equal(t, testCase.wantFlushLog, db.GetQueryCalledNum(setFlushLog))
			assert.Equal(t, testCase.wantSyncBinlog, db.GetQueryCalledNum(setSyncBinlog))
			assert.Equal(t, testCase.wantSet, db.GetQueryCalledNum(setSyncRelayLog))
			assert.Equal(t, testCase.wantFlushEngine, db.GetQueryCalledNum(flushEngineLogs))
			assert.Equal(t, testCase.wantFlushBinary, db.GetQueryCalledNum(flushBinaryLogs))
			assert.Equal(t, testCase.wantFlush, db.GetQueryCalledNum(flushRelayLogs))
			assert.Equal(t, testCase.wantStop, db.GetQueryCalledNum(stopIOThread))
			assert.Equal(t, testCase.wantStopSQL, db.GetQueryCalledNum(stopSQLThread))
		})
	}
}

func TestRestoreReplicaAfterFailedShutdown(t *testing.T) {
	const (
		restoreFlushLog     = "SET GLOBAL innodb_flush_log_at_trx_commit = 2"
		restoreSyncBinlog   = "SET GLOBAL sync_binlog = 0"
		restoreSyncRelayLog = "SET GLOBAL sync_relay_log = 10000"
		startReplication    = "START REPLICA"
		startSQLThread      = "START REPLICA SQL_THREAD"
		startIOThread       = "START REPLICA IO_THREAD"
	)
	state := func(startReceiver, startApplier bool) *replicaShutdownState {
		return &replicaShutdownState{
			startReceiver:       startReceiver,
			startApplier:        startApplier,
			flushLogAtTrxCommit: "2",
			syncBinlog:          "0",
			syncRelayLog:        "10000",
		}
	}

	testCases := []struct {
		name          string
		state         *replicaShutdownState
		rejectedQuery string
		wantSet       int
		wantStart     string
	}{
		{
			name:      "both threads were running",
			state:     state(true, true),
			wantSet:   1,
			wantStart: startReplication,
		},
		{
			name:      "only the applier was running",
			state:     state(false, true),
			wantSet:   1,
			wantStart: startSQLThread,
		},
		{
			name:      "only the receiver was running",
			state:     state(true, false),
			wantSet:   1,
			wantStart: startIOThread,
		},
		{
			name:    "no threads were running",
			state:   state(false, false),
			wantSet: 1,
		},
		{
			name:          "settings restore fails but threads are still restarted",
			state:         state(true, true),
			rejectedQuery: restoreFlushLog,
			wantSet:       1,
			wantStart:     startReplication,
		},
		{
			name:          "thread restart failure is tolerated",
			state:         state(true, true),
			rejectedQuery: startReplication,
			wantSet:       1,
			wantStart:     startReplication,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			db := fakesqldb.New(t)
			defer db.Close()
			db.AddQuery("SELECT 1", &sqltypes.Result{})
			for _, query := range []string{restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog, startReplication, startSQLThread, startIOThread} {
				if query == testCase.rejectedQuery {
					db.AddRejectedQuery(query, assert.AnError)
					continue
				}
				db.AddQuery(query, &sqltypes.Result{})
			}

			params := db.ConnParams()
			cp := *params
			dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
			testMysqld := NewMysqld(dbc)
			defer testMysqld.Close()

			testMysqld.restoreReplicaAfterFailedShutdown(t.Context(), testCase.state)
			assert.Equal(t, testCase.wantSet, db.GetQueryCalledNum(restoreFlushLog))
			for _, start := range []string{startReplication, startSQLThread, startIOThread} {
				want := 0
				if start == testCase.wantStart {
					want = 1
				}
				assert.Equal(t, want, db.GetQueryCalledNum(start), "unexpected call count for %q", start)
			}
		})
	}
}

// TestShutdownRestoresReplicaAfterLatePreparation covers the case where a
// crash-safety mutation is still in flight when both bounded waits expire: the
// shutdown must return without blocking on it, and the mutation landing later
// must still trigger the replica state restore in the background.
func TestShutdownRestoresReplicaAfterLatePreparation(t *testing.T) {
	const (
		readDurability      = "SELECT @@global.innodb_flush_log_at_trx_commit, @@global.sync_binlog, @@global.sync_relay_log"
		setFlushLog         = "SET GLOBAL innodb_flush_log_at_trx_commit = 1"
		restoreFlushLog     = "SET GLOBAL innodb_flush_log_at_trx_commit = 2"
		restoreSyncBinlog   = "SET GLOBAL sync_binlog = 0"
		restoreSyncRelayLog = "SET GLOBAL sync_relay_log = 10000"
		startReplication    = "START REPLICA"
	)

	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
		"source|Yes|Yes",
	))
	db.AddQuery(readDurability, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"@@global.innodb_flush_log_at_trx_commit|@@global.sync_binlog|@@global.sync_relay_log",
			"int64|int64|int64",
		),
		"2|0|10000",
	))
	for _, query := range []string{setFlushLog, restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog, startReplication} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	db.AddQueryPattern("kill .*", &sqltypes.Result{})

	// Block the first mutating statement until released, so the preparation is
	// still in flight when both of the shutdown's bounded waits expire.
	release := make(chan struct{})
	db.SetBeforeFunc(setFlushLog, func() { <-release })

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	err := testMysqld.shutdownWithReplicaCrashSafety(t.Context(), 100*time.Millisecond, func() error {
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)
	// The mutation is still blocked, so nothing has been restored yet.
	assert.Zero(t, db.GetQueryCalledNum(restoreFlushLog))
	assert.Zero(t, db.GetQueryCalledNum(startReplication))

	// Let the blocked mutation land after the shutdown already returned: the
	// background restore must now undo it.
	close(release)
	assert.Eventually(t, func() bool {
		return db.GetQueryCalledNum(restoreFlushLog) == 1 &&
			db.GetQueryCalledNum(restoreSyncBinlog) == 1 &&
			db.GetQueryCalledNum(restoreSyncRelayLog) == 1 &&
			db.GetQueryCalledNum(startReplication) == 1
	}, 30*time.Second, 10*time.Millisecond, "the late-landing preparation was not restored in the background")
}

// TestCloseWaitsForPendingReplicaRestore covers the short-lived caller case
// (the mysqlctl CLI defers Close right after Shutdown): Close must wait for a
// background restoration armed by a failed shutdown instead of closing the
// connection pools out from under it.
func TestCloseWaitsForPendingReplicaRestore(t *testing.T) {
	const (
		readDurability      = "SELECT @@global.innodb_flush_log_at_trx_commit, @@global.sync_binlog, @@global.sync_relay_log"
		setFlushLog         = "SET GLOBAL innodb_flush_log_at_trx_commit = 1"
		restoreFlushLog     = "SET GLOBAL innodb_flush_log_at_trx_commit = 2"
		restoreSyncBinlog   = "SET GLOBAL sync_binlog = 0"
		restoreSyncRelayLog = "SET GLOBAL sync_relay_log = 10000"
		startReplication    = "START REPLICA"
	)

	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
		"source|Yes|Yes",
	))
	db.AddQuery(readDurability, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"@@global.innodb_flush_log_at_trx_commit|@@global.sync_binlog|@@global.sync_relay_log",
			"int64|int64|int64",
		),
		"2|0|10000",
	))
	for _, query := range []string{setFlushLog, restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog, startReplication} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	db.AddQueryPattern("kill .*", &sqltypes.Result{})

	release := make(chan struct{})
	db.SetBeforeFunc(setFlushLog, func() { <-release })

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)

	err := testMysqld.shutdownWithReplicaCrashSafety(t.Context(), 100*time.Millisecond, func() error {
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)

	// Close begins while the mutation is still blocked -- exactly what the CLI
	// does via its deferred Close. It must wait for the restoration, so once
	// the mutation lands the restore still finds live connection pools.
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		testMysqld.Close()
	}()
	close(release)
	select {
	case <-closed:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "Close did not return after the pending restoration completed")
	}
	assert.Equal(t, 1, db.GetQueryCalledNum(restoreFlushLog), "the restoration must complete before Close closes the pools")
	assert.Equal(t, 1, db.GetQueryCalledNum(startReplication), "the restoration must complete before Close closes the pools")
}

// TestRestoreSurvivesClosedPools covers a server-side stop that outlives even
// Close's patience: the owner has given up and closed the connection pools
// while the stop is still draining. When the stop finally completes, the
// restoration must still restart replication -- it must not depend on the
// pools the owner already closed.
func TestRestoreSurvivesClosedPools(t *testing.T) {
	const (
		readDurability      = "SELECT @@global.innodb_flush_log_at_trx_commit, @@global.sync_binlog, @@global.sync_relay_log"
		setFlushLog         = "SET GLOBAL innodb_flush_log_at_trx_commit = 1"
		setSyncBinlog       = "SET GLOBAL sync_binlog = 1"
		setSyncRelayLog     = "SET GLOBAL sync_relay_log = 1"
		flushEngineLogs     = "FLUSH NO_WRITE_TO_BINLOG ENGINE LOGS"
		flushBinaryLogs     = "FLUSH NO_WRITE_TO_BINLOG BINARY LOGS"
		flushRelayLogs      = "FLUSH NO_WRITE_TO_BINLOG RELAY LOGS"
		stopIOThread        = "STOP REPLICA IO_THREAD"
		stopSQLThread       = "STOP REPLICA SQL_THREAD"
		restoreFlushLog     = "SET GLOBAL innodb_flush_log_at_trx_commit = 2"
		restoreSyncBinlog   = "SET GLOBAL sync_binlog = 0"
		restoreSyncRelayLog = "SET GLOBAL sync_relay_log = 10000"
		startReplication    = "START REPLICA"
	)

	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
		"source|Yes|Yes",
	))
	db.AddQuery(readDurability, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"@@global.innodb_flush_log_at_trx_commit|@@global.sync_binlog|@@global.sync_relay_log",
			"int64|int64|int64",
		),
		"2|0|10000",
	))
	for _, query := range []string{
		setFlushLog, setSyncBinlog, setSyncRelayLog, flushEngineLogs, flushBinaryLogs, flushRelayLogs,
		stopIOThread, stopSQLThread, restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog, startReplication,
	} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	db.AddQueryPattern("kill .*", &sqltypes.Result{})

	// Block the applier stop until released: the server-side stop is still
	// draining when the shutdown fails and the owner gives up.
	release := make(chan struct{})
	db.SetBeforeFunc(stopSQLThread, func() { <-release })

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)

	err := testMysqld.shutdownWithReplicaCrashSafety(t.Context(), 100*time.Millisecond, func() error {
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)

	// The owner has given up waiting: the pools are closed while the stop is
	// still draining, as after a timed-out Close.
	testMysqld.dbaPool.Close()
	testMysqld.appPool.Close()
	assert.Zero(t, db.GetQueryCalledNum(restoreFlushLog))
	assert.Zero(t, db.GetQueryCalledNum(startReplication))

	// The stop finally completes: the restoration must still restart
	// replication, using its dedicated connection.
	close(release)
	assert.Eventually(t, func() bool {
		return db.GetQueryCalledNum(restoreFlushLog) == 1 &&
			db.GetQueryCalledNum(restoreSyncBinlog) == 1 &&
			db.GetQueryCalledNum(restoreSyncRelayLog) == 1 &&
			db.GetQueryCalledNum(startReplication) == 1
	}, 30*time.Second, 10*time.Millisecond, "the restoration must survive the closed pools and restart replication once the stop drains")
}

// TestShutdownSkipsRestoreForPreMutationHang covers a preparation hung in a
// read-only probe: nothing was changed, so a failed shutdown must not arm a
// background restoration (which would otherwise wait -- and on long-lived
// callers leak -- indefinitely), and no restore statements may ever run.
func TestShutdownSkipsRestoreForPreMutationHang(t *testing.T) {
	const (
		readDurability   = "SELECT @@global.innodb_flush_log_at_trx_commit, @@global.sync_binlog, @@global.sync_relay_log"
		restoreFlushLog  = "SET GLOBAL innodb_flush_log_at_trx_commit = 2"
		startReplication = "START REPLICA"
	)

	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
		"source|Yes|Yes",
	))
	db.AddQuery(readDurability, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields(
			"@@global.innodb_flush_log_at_trx_commit|@@global.sync_binlog|@@global.sync_relay_log",
			"int64|int64|int64",
		),
		"2|0|10000",
	))
	db.AddQuery(restoreFlushLog, &sqltypes.Result{})
	db.AddQuery(startReplication, &sqltypes.Result{})
	db.AddQueryPattern("kill .*", &sqltypes.Result{})

	// Hang the preparation in its read-only state capture, before any
	// mutating statement is issued.
	release := make(chan struct{})
	db.SetBeforeFunc(readDurability, func() { <-release })

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	err := testMysqld.shutdownWithReplicaCrashSafety(t.Context(), 100*time.Millisecond, func() error {
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)

	// Even once the hung probe resolves, nothing was mutated, so no
	// restoration may run.
	close(release)
	assert.Eventually(t, func() bool {
		return db.GetQueryCalledNum(readDurability) == 1
	}, 30*time.Second, 10*time.Millisecond, "the hung probe never resolved")
	assert.Never(t, func() bool {
		return db.GetQueryCalledNum(restoreFlushLog) > 0 || db.GetQueryCalledNum(startReplication) > 0
	}, 2*time.Second, 50*time.Millisecond, "a preparation that never mutated anything must not trigger a restoration")
}

// TestReplicaShutdownPreparationBudget pins the mapping from the caller's
// shutdown timeout to the crash-safety preparation budget: an immediate,
// no-wait shutdown (mysqlctl shutdown --wait-time=0) must grant the
// preparation no budget at all rather than its default one.
func TestReplicaShutdownPreparationBudget(t *testing.T) {
	assert.Zero(t, replicaShutdownPreparationBudget(0), "a no-wait shutdown must skip the preparation")
	assert.Zero(t, replicaShutdownPreparationBudget(-time.Second), "a negative timeout must skip the preparation")
	assert.Equal(t, 5*time.Second, replicaShutdownPreparationBudget(5*time.Second),
		"a short timeout caps the preparation budget")
	assert.Equal(t, replicaShutdownPreparationTimeout, replicaShutdownPreparationBudget(time.Hour),
		"a long timeout leaves the default preparation budget")
}

// TestShutdownSkipsPreparationForZeroTimeout covers an immediate, no-wait
// shutdown (mysqlctl shutdown --wait-time=0): the crash-safety preparation
// must be skipped entirely rather than granted its own budget before the
// shutdown is even issued.
func TestShutdownSkipsPreparationForZeroTimeout(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	shutdownCalled := false
	err := testMysqld.shutdownWithReplicaCrashSafety(t.Context(), 0, func() error {
		shutdownCalled = true
		return nil
	})
	require.NoError(t, err)
	assert.True(t, shutdownCalled, "the shutdown itself must still run")
	assert.Zero(t, db.GetQueryCalledNum("SHOW REPLICA STATUS"), "the preparation must not run with a zero timeout")

	err = testMysqld.shutdownWithReplicaCrashSafety(t.Context(), 0, func() error {
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError, "a failing shutdown's error must pass through unchanged")
	assert.Zero(t, db.GetQueryCalledNum("SHOW REPLICA STATUS"), "the preparation must not run with a zero timeout")
}

func TestGetMysqlPort(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW VARIABLES LIKE 'port'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field|test_field2", "varchar|uint64"), "test_port|12"))
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	res, err := testMysqld.GetMysqlPort(ctx)
	assert.Equal(t, int32(12), res)
	require.NoError(t, err)

	db.AddQuery("SHOW VARIABLES LIKE 'port'", &sqltypes.Result{})
	res, err = testMysqld.GetMysqlPort(ctx)
	require.ErrorContains(t, err, "no port variable in mysql")
	assert.Equal(t, int32(0), res)
}

func TestGetServerID(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("select @@global.server_id", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "uint64"), "12"))
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := t.Context()
	res, err := testMysqld.GetServerID(ctx)
	assert.Equal(t, uint32(12), res)
	require.NoError(t, err)

	db.AddQuery("select @@global.server_id", &sqltypes.Result{})
	res, err = testMysqld.GetServerID(ctx)
	require.ErrorContains(t, err, "no server_id in mysql")
	assert.Equal(t, uint32(0), res)
}

func TestGetServerUUID(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	uuid := "test_uuid"
	db.AddQuery("SELECT @@global.server_uuid", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), uuid))

	ctx := t.Context()
	res, err := testMysqld.GetServerUUID(ctx)
	assert.Equal(t, uuid, res)
	require.NoError(t, err)

	db.AddQuery("SELECT @@global.server_uuid", &sqltypes.Result{})
	res, err = testMysqld.GetServerUUID(ctx)
	require.Error(t, err)
	assert.Empty(t, res)
}

func TestWaitSourcePos(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SELECT @@global.gtid_executed", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:12-17"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := t.Context()
	err := testMysqld.WaitSourcePos(ctx, replication.Position{GTIDSet: replication.Mysql56GTIDSet{}})
	require.NoError(t, err)

	db.AddQuery("SELECT @@global.gtid_executed", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), "invalid_id"))
	err = testMysqld.WaitSourcePos(ctx, replication.Position{GTIDSet: replication.Mysql56GTIDSet{}})
	assert.ErrorContains(t, err, "invalid MySQL 5.6 GTID set")
}

func TestReplicationStatus(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), "test_status"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	res, err := testMysqld.ReplicationStatus(t.Context())
	require.NoError(t, err)
	assert.True(t, res.ReplicationLagUnknown)

	db.AddQuery("SHOW REPLICA STATUS", &sqltypes.Result{})
	res, err = testMysqld.ReplicationStatus(t.Context())
	require.Error(t, err)
	assert.False(t, res.ReplicationLagUnknown)
}

func TestPrimaryStatus(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW MASTER STATUS", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), "test_status"))
	db.AddQuery("SHOW BINARY LOG STATUS", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), "test_status"))
	db.AddQuery("SELECT @@global.server_uuid", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), "test_uuid"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := t.Context()
	res, err := testMysqld.PrimaryStatus(ctx)
	require.NoError(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, "test_uuid", res.ServerUUID)

	db.AddQuery("SHOW MASTER STATUS", &sqltypes.Result{})
	db.AddQuery("SHOW BINARY LOG STATUS", &sqltypes.Result{})
	_, err = testMysqld.PrimaryStatus(ctx)
	assert.ErrorContains(t, err, "no master status")
}

func TestReplicationConfiguration(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SELECT * FROM performance_schema.replication_connection_configuration", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field|HEARTBEAT_INTERVAL|field2", "varchar|float64|varchar"), "test_status|4.5000|test"))
	db.AddQuery("select @@global.replica_net_timeout", sqltypes.MakeTestResult(sqltypes.MakeTestFields("@@global.replica_net_timeout", "int64"), "9"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := t.Context()
	replConfig, err := testMysqld.ReplicationConfiguration(ctx)
	require.NoError(t, err)
	assert.NotNil(t, replConfig)
	require.EqualValues(t, math.Round(replConfig.HeartbeatInterval*2), replConfig.ReplicaNetTimeout)

	db.AddQuery("SELECT * FROM performance_schema.replication_connection_configuration", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field|HEARTBEAT_INTERVAL|field2", "varchar|float64|varchar")))
	replConfig, err = testMysqld.ReplicationConfiguration(ctx)
	require.NoError(t, err)
	assert.Nil(t, replConfig)
}

func TestGetGTIDPurged(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SELECT @@global.gtid_purged", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:12-17"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := t.Context()
	res, err := testMysqld.GetGTIDPurged(ctx)
	require.NoError(t, err)
	assert.Equal(t, "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8:12-17", res.String())
}

func TestPrimaryPosition(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SELECT @@global.gtid_executed", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:12-17"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	res, err := testMysqld.PrimaryPosition(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8:12-17", res.String())
}

func TestSetReplicationPosition(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("RESET MASTER", &sqltypes.Result{})
	db.AddQuery("RESET BINARY LOGS AND GTIDS", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := t.Context()

	pos := replication.Position{GTIDSet: replication.Mysql56GTIDSet{}}
	sid := replication.SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	pos.GTIDSet = pos.GTIDSet.AddGTID(replication.Mysql56GTID{Server: sid, Sequence: 1})

	err := testMysqld.SetReplicationPosition(ctx, pos)
	require.Error(t, err)

	// We expect this query to be executed
	db.AddQuery("SET GLOBAL gtid_purged = '00010203-0405-0607-0809-0a0b0c0d0e0f:1'", &sqltypes.Result{})

	err = testMysqld.SetReplicationPosition(ctx, pos)
	assert.NoError(t, err)
}

func TestSetReplicationSource(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("RESET MASTER", &sqltypes.Result{})
	db.AddQuery("RESET BINARY LOGS AND GTIDS", &sqltypes.Result{})
	db.AddQuery("STOP REPLICA", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := t.Context()

	// We expect query containing passed host and port to be executed
	err := testMysqld.SetReplicationSource(ctx, "test_host", 2, 0, true, true)
	require.ErrorContains(t, err, `SOURCE_HOST = 'test_host'`)
	require.ErrorContains(t, err, `SOURCE_PORT = 2`)
	assert.ErrorContains(t, err, `CHANGE REPLICATION SOURCE TO`)
}

func TestResetReplication(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW GLOBAL VARIABLES LIKE 'rpl_semi_sync%'", &sqltypes.Result{})
	db.AddQuery("STOP REPLICA", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := t.Context()
	err := testMysqld.ResetReplication(ctx)
	require.ErrorContains(t, err, "RESET REPLICA ALL")

	// We expect this query to be executed
	db.AddQuery("RESET REPLICA ALL", &sqltypes.Result{})
	err = testMysqld.ResetReplication(ctx)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "RESET MASTER") || strings.Contains(err.Error(), "RESET BINARY LOGS AND GTIDS"))

	// We expect this query to be executed
	db.AddQuery("RESET MASTER", &sqltypes.Result{})
	db.AddQuery("RESET BINARY LOGS AND GTIDS", &sqltypes.Result{})
	err = testMysqld.ResetReplication(ctx)
	assert.NoError(t, err)
}

func TestResetReplicationParameters(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW GLOBAL VARIABLES LIKE 'rpl_semi_sync%'", &sqltypes.Result{})
	db.AddQuery("STOP REPLICA", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := t.Context()
	err := testMysqld.ResetReplicationParameters(ctx)
	require.ErrorContains(t, err, "RESET REPLICA ALL")

	// We expect this query to be executed
	db.AddQuery("RESET REPLICA ALL", &sqltypes.Result{})
	err = testMysqld.ResetReplicationParameters(ctx)
	assert.NoError(t, err)
}

func TestFindReplicas(t *testing.T) {
	db := fakesqldb.New(t)
	fakemysqld := NewFakeMysqlDaemon(db)

	defer func() {
		db.Close()
		fakemysqld.Close()
	}()

	fakemysqld.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SHOW PROCESSLIST": sqltypes.MakeTestResult(sqltypes.MakeTestFields("Id|User|Host|db|Command|Time|State|Info", "varchar|varchar|varchar|varchar|varchar|varchar|varchar|varchar"), "1|user1|localhost:12|db1|Binlog Dump|54|Has sent all binlog to replica|NULL"),
	}

	res, err := FindReplicas(t.Context(), fakemysqld)
	require.NoError(t, err)

	want, err := net.LookupHost("localhost")
	require.NoError(t, err)

	assert.Equal(t, want, res)
}

func TestGetBinlogInformation(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SELECT @@global.binlog_format, @@global.log_bin, @@global.log_replica_updates, @@global.binlog_row_image", sqltypes.MakeTestResult(sqltypes.MakeTestFields("@@global.binlog_format|@@global.log_bin|@@global.log_replica_updates|@@global.binlog_row_image", "varchar|int64|int64|varchar"), "binlog|1|2|row_image"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := t.Context()
	bin, logBin, replicaUpdate, rowImage, err := testMysqld.GetBinlogInformation(ctx)
	require.NoError(t, err)
	assert.Equal(t, "binlog", bin)
	assert.Equal(t, "row_image", rowImage)
	assert.True(t, logBin)
	assert.False(t, replicaUpdate)
}

func TestGetGTIDMode(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	in := "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8,8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:12-17"
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("select @@global.gtid_mode", sqltypes.MakeTestResult(sqltypes.MakeTestFields("test_field", "varchar"), in))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := t.Context()
	res, err := testMysqld.GetGTIDMode(ctx)
	require.NoError(t, err)
	assert.Equal(t, in, res)
}

func TestFlushBinaryLogs(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	// We expect this query to be executed
	err := testMysqld.FlushBinaryLogs(t.Context())
	assert.ErrorContains(t, err, "FLUSH BINARY LOGS")
}

func TestGetBinaryLogs(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	db.AddQuery("SHOW BINARY LOGS", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field", "varchar"), "binlog1", "binlog2"))

	res, err := testMysqld.GetBinaryLogs(t.Context())
	require.NoError(t, err)
	assert.Len(t, res, 2)
	assert.Contains(t, res, "binlog1")
	assert.Contains(t, res, "binlog2")
}

func TestGetPreviousGTIDs(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW BINLOG EVENTS IN 'binlog' LIMIT 2", sqltypes.MakeTestResult(sqltypes.MakeTestFields("Event_type|Info", "varchar|varchar"), "Previous_gtids|8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	ctx := t.Context()
	res, err := testMysqld.GetPreviousGTIDs(ctx, "binlog")
	require.NoError(t, err)
	assert.Equal(t, "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8", res)
}

func TestSetSemiSyncEnabled(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	// We expect this query to be executed
	err := testMysqld.SetSemiSyncEnabled(t.Context(), true, true)
	require.ErrorIs(t, err, ErrNoSemiSync)

	// We expect this query to be executed
	err = testMysqld.SetSemiSyncEnabled(t.Context(), true, false)
	require.ErrorIs(t, err, ErrNoSemiSync)

	// We expect this query to be executed
	err = testMysqld.SetSemiSyncEnabled(t.Context(), false, true)
	assert.ErrorIs(t, err, ErrNoSemiSync)
}

func TestSemiSyncEnabled(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_%_enabled'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|varchar"), "rpl_semi_sync_source_enabled|OFF", "rpl_semi_sync_replica_enabled|ON"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	p, r := testMysqld.SemiSyncEnabled(t.Context())
	assert.False(t, p)
	assert.True(t, r)
}

func TestSemiSyncStatus(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_%_enabled'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|varchar"), "rpl_semi_sync_source_enabled|ON", "rpl_semi_sync_replica_enabled|ON"))
	db.AddQuery("SHOW STATUS LIKE 'Rpl_semi_sync_%_status'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|varchar"), "Rpl_semi_sync_source_status|ON", "Rpl_semi_sync_replica_status|OFF"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	p, r := testMysqld.SemiSyncStatus(t.Context())
	assert.True(t, p)
	assert.False(t, r)
}

func TestSemiSyncClients(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_%_enabled'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|varchar"), "rpl_semi_sync_source_enabled|ON", "rpl_semi_sync_replica_enabled|ON"))
	db.AddQuery("SHOW STATUS LIKE 'Rpl_semi_sync_source_clients'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|uint64"), "val1|12"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	res := testMysqld.SemiSyncClients(t.Context())
	assert.Equal(t, uint32(12), res)
}

func TestSemiSyncSettings(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_%_enabled'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|varchar"), "rpl_semi_sync_source_enabled|ON", "rpl_semi_sync_replica_enabled|ON"))
	db.AddQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_%'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|uint64"), "rpl_semi_sync_source_timeout|123", "rpl_semi_sync_source_wait_for_replica_count|80"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	timeout, replicas := testMysqld.SemiSyncSettings(t.Context())
	assert.Equal(t, uint64(123), timeout)
	assert.Equal(t, uint32(80), replicas)
}

func TestSemiSyncReplicationStatus(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_%_enabled'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|varchar"), "rpl_semi_sync_source_enabled|ON", "rpl_semi_sync_replica_enabled|ON"))
	db.AddQuery("SHOW STATUS LIKE 'rpl_semi_sync_replica_status'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|uint64"), "rpl_semi_sync_replica_status|ON"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	res, err := testMysqld.SemiSyncReplicationStatus(t.Context())
	require.NoError(t, err)
	assert.True(t, res)

	db.AddQuery("SHOW STATUS LIKE 'rpl_semi_sync_replica_status'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|uint64"), "rpl_semi_sync_replica_status|OFF"))

	res, err = testMysqld.SemiSyncReplicationStatus(t.Context())
	require.NoError(t, err)
	assert.False(t, res)
}

func TestSemiSyncExtensionLoaded(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	ctx := t.Context()

	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_%_enabled'", sqltypes.MakeTestResult(sqltypes.MakeTestFields("field1|field2", "varchar|varchar"), "rpl_semi_sync_source_enabled|ON", "rpl_semi_sync_replica_enabled|ON"))

	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	res, err := testMysqld.SemiSyncExtensionLoaded(ctx)
	require.NoError(t, err)
	assert.Contains(t, []mysql.SemiSyncType{mysql.SemiSyncTypeSource, mysql.SemiSyncTypeMaster}, res)

	db.AddQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_%_enabled'", &sqltypes.Result{})

	res, err = testMysqld.SemiSyncExtensionLoaded(ctx)
	require.NoError(t, err)
	assert.Equal(t, mysql.SemiSyncTypeOff, res)
}

func TestSetSuperReadOnlyLockWaitTimeout(t *testing.T) {
	newTestMysqld := func(t *testing.T) (*fakesqldb.DB, *Mysqld) {
		db := fakesqldb.New(t)
		t.Cleanup(db.Close)

		params := db.ConnParams()
		cp := *params
		dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

		db.AddQuery("SELECT 1", &sqltypes.Result{})
		db.AddQuery("SELECT @@global.super_read_only", sqltypes.MakeTestResult(sqltypes.MakeTestFields("@@global.super_read_only", "int64"), "0"))
		db.AddQuery("SET SESSION lock_wait_timeout = 1", &sqltypes.Result{})
		db.AddQuery("SET SESSION lock_wait_timeout = @@global.lock_wait_timeout", &sqltypes.Result{})
		db.AddQuery("SET GLOBAL super_read_only = 'ON'", &sqltypes.Result{})

		testMysqld := NewMysqld(dbc)
		t.Cleanup(testMysqld.Close)
		return db, testMysqld
	}

	t.Run("applies the session lock_wait_timeout before enabling", func(t *testing.T) {
		db, testMysqld := newTestMysqld(t)

		resetFunc, err := testMysqld.SetSuperReadOnly(t.Context(), true, WithLockWaitTimeout(time.Second))
		require.NoError(t, err)
		assert.NotNil(t, resetFunc)

		queryLog := db.QueryLog()
		setIdx := strings.Index(queryLog, "set session lock_wait_timeout = 1")
		enableIdx := strings.Index(queryLog, "set global super_read_only = 'on'")
		require.NotEqual(t, -1, setIdx, "expected the session lock_wait_timeout to be set, got queries: %s", queryLog)
		require.NotEqual(t, -1, enableIdx, "expected super_read_only to be enabled, got queries: %s", queryLog)
		assert.Less(t, setIdx, enableIdx, "lock_wait_timeout must be set before enabling super_read_only")
		assert.Equal(t, 1, db.GetQueryCalledNum("SET SESSION lock_wait_timeout = @@global.lock_wait_timeout"), "the session lock_wait_timeout must be restored on success")
	})

	t.Run("rounds the timeout up to whole seconds", func(t *testing.T) {
		db, testMysqld := newTestMysqld(t)
		db.AddQuery("SET SESSION lock_wait_timeout = 2", &sqltypes.Result{})

		_, err := testMysqld.SetSuperReadOnly(t.Context(), true, WithLockWaitTimeout(500*time.Millisecond))
		require.NoError(t, err)
		assert.Equal(t, 1, db.GetQueryCalledNum("SET SESSION lock_wait_timeout = 1"))

		_, err = testMysqld.SetSuperReadOnly(t.Context(), true, WithLockWaitTimeout(1500*time.Millisecond))
		require.NoError(t, err)
		assert.Equal(t, 1, db.GetQueryCalledNum("SET SESSION lock_wait_timeout = 2"))
	})

	t.Run("default leaves lock_wait_timeout untouched", func(t *testing.T) {
		db, testMysqld := newTestMysqld(t)

		resetFunc, err := testMysqld.SetSuperReadOnly(t.Context(), true)
		require.NoError(t, err)
		assert.NotNil(t, resetFunc)

		assert.NotContains(t, db.QueryLog(), "lock_wait_timeout")
	})

	t.Run("enabling failure still surfaces the error", func(t *testing.T) {
		db, testMysqld := newTestMysqld(t)
		db.AddRejectedQuery("SET GLOBAL super_read_only = 'ON'", sqlerror.NewSQLError(sqlerror.ERLockWaitTimeout, sqlerror.SSUnknownSQLState, "Lock wait timeout exceeded; try restarting transaction"))

		_, err := testMysqld.SetSuperReadOnly(t.Context(), true, WithLockWaitTimeout(time.Second))
		require.ErrorContains(t, err, "Lock wait timeout exceeded")
		assert.Equal(t, 1, db.GetQueryCalledNum("SET SESSION lock_wait_timeout = @@global.lock_wait_timeout"), "the session must be restored after a clean statement failure")
	})

	t.Run("reset function does not apply the lock_wait_timeout", func(t *testing.T) {
		db, testMysqld := newTestMysqld(t)
		db.AddQuery("SET GLOBAL super_read_only = 'OFF'", &sqltypes.Result{})

		resetFunc, err := testMysqld.SetSuperReadOnly(t.Context(), true, WithLockWaitTimeout(time.Second))
		require.NoError(t, err)
		require.NotNil(t, resetFunc)

		require.NoError(t, resetFunc())

		assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'OFF'"))
		assert.Equal(t, 1, db.GetQueryCalledNum("SET SESSION lock_wait_timeout = 1"), "the reset must not bound its lock wait")
	})

	t.Run("unknown lock_wait_timeout proceeds without a bound", func(t *testing.T) {
		db, testMysqld := newTestMysqld(t)
		db.AddRejectedQuery("SET SESSION lock_wait_timeout = 1", sqlerror.NewSQLError(sqlerror.ERUnknownSystemVariable, sqlerror.SSUnknownSQLState, "Unknown system variable 'lock_wait_timeout'"))

		resetFunc, err := testMysqld.SetSuperReadOnly(t.Context(), true, WithLockWaitTimeout(time.Second))
		require.NoError(t, err)
		assert.NotNil(t, resetFunc)

		assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL super_read_only = 'ON'"))
		assert.Equal(t, 0, db.GetQueryCalledNum("SET SESSION lock_wait_timeout = @@global.lock_wait_timeout"), "must not restore a lock_wait_timeout that was never set")
	})
}
