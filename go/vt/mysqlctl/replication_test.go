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
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
	replicaStateReceiverInterrupted := &replicaShutdownState{
		startReceiver:           true,
		startApplier:            true,
		flushLogAtTrxCommit:     "2",
		syncBinlog:              "0",
		syncRelayLog:            "10000",
		receiverStopInterrupted: true,
	}
	replicaStateApplierInterrupted := &replicaShutdownState{
		startReceiver:          true,
		startApplier:           true,
		flushLogAtTrxCommit:    "2",
		syncBinlog:             "0",
		syncRelayLog:           "10000",
		applierStopInterrupted: true,
	}
	replicaStateCycleReceiver := &replicaShutdownState{
		startReceiver:       true,
		startApplier:        true,
		flushLogAtTrxCommit: "2",
		syncBinlog:          "0",
		syncRelayLog:        "10000",
		cycleReceiver:       true,
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
		// The fence statements are independent: a failure in any one of them
		// must not short-circuit the remaining statements or the thread stops.
		{
			name:            "cannot enable redo log flushing",
			status:          replicaStatus,
			rejectedQuery:   setFlushLog,
			rejectedError:   assert.AnError,
			wantError:       "failed to establish the crash-safety durability fence before shutdown",
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
			name:            "cannot enable binary log syncing",
			status:          replicaStatus,
			rejectedQuery:   setSyncBinlog,
			rejectedError:   assert.AnError,
			wantError:       "failed to establish the crash-safety durability fence before shutdown",
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
			name:            "cannot enable relay log syncing",
			status:          replicaStatus,
			rejectedQuery:   setSyncRelayLog,
			rejectedError:   assert.AnError,
			wantError:       "failed to establish the crash-safety durability fence before shutdown",
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
			wantFlushBinary: 1,
			wantFlush:       1,
			wantStop:        1,
			wantStopSQL:     1,
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
			wantFlush:       1,
			wantStop:        1,
			wantStopSQL:     1,
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
			wantStop:        1,
			wantStopSQL:     1,
		},
		{
			name:            "interrupted receiver stop",
			status:          replicaStatus,
			rejectedQuery:   stopIOThread,
			rejectedError:   sqlerror.NewSQLError(sqlerror.ERStopReplicaIOThreadTimeout, sqlerror.SSUnknownSQLState, "STOP REPLICA IO_THREAD timed out"),
			wantState:       replicaStateReceiverInterrupted,
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
			rejectedError:   sqlerror.NewSQLError(sqlerror.ERStopReplicaSQLThreadTimeout, sqlerror.SSUnknownSQLState, "STOP REPLICA SQL_THREAD timed out"),
			wantState:       replicaStateApplierInterrupted,
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
			name:            "definitive stop failure is not marked interrupted",
			status:          replicaStatus,
			rejectedQuery:   stopSQLThread,
			rejectedError:   assert.AnError,
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
			name:            "monitor stop timeout cycles the receiver",
			status:          replicaStatus,
			rejectedQuery:   stopIOThread,
			rejectedError:   sqlerror.NewSQLError(sqlerror.ERStopReplicaMonitorIOThreadTimeout, sqlerror.SSUnknownSQLState, "monitor stop timed out"),
			wantState:       replicaStateCycleReceiver,
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
			state, err := testMysqld.prepareReplicaForShutdown(t.Context(), nil, func(state *replicaShutdownState) {
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

	showReplicaStatus := "SHOW REPLICA STATUS"
	stoppedStatus := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
		"source|No|No",
	)
	runningStatus := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
		"source|Yes|Yes",
	)

	testCases := []struct {
		name             string
		state            *replicaShutdownState
		rejectedQuery    string
		boundedCtx       bool
		wantSet          int
		wantSetAtLeast   bool
		wantStart        string
		wantStartAtLeast bool
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
			// The persistently failing setting keeps the restoration retrying
			// until its deadline: bound it and expect repeated attempts.
			boundedCtx:     true,
			wantSet:        2,
			wantSetAtLeast: true,
			wantStart:      startReplication,
		},
		{
			name:             "thread restart failure is tolerated",
			state:            state(true, true),
			rejectedQuery:    startReplication,
			boundedCtx:       true,
			wantSet:          1,
			wantStart:        startReplication,
			wantStartAtLeast: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			db := fakesqldb.New(t)
			defer db.Close()
			db.AddQuery("SELECT 1", &sqltypes.Result{})
			// The threads start out stopped; a successful start is observed as
			// the status flipping to running.
			db.AddQuery(showReplicaStatus, stoppedStatus)
			for _, query := range []string{restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog, startReplication, startSQLThread, startIOThread} {
				if query == testCase.rejectedQuery {
					db.AddRejectedQuery(query, assert.AnError)
					continue
				}
				db.AddQuery(query, &sqltypes.Result{})
			}
			if testCase.wantStart != "" && testCase.wantStart != testCase.rejectedQuery {
				db.SetBeforeFunc(testCase.wantStart, func() {
					db.AddQuery(showReplicaStatus, runningStatus)
				})
			}

			params := db.ConnParams()
			cp := *params
			dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
			testMysqld := NewMysqld(dbc)
			defer testMysqld.Close()

			ctx := t.Context()
			if testCase.boundedCtx {
				// The reconcile keeps retrying a failing start until its
				// deadline: bound it so the test can observe it give up.
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
				defer cancel()
			}
			testMysqld.restoreReplicaAfterFailedShutdown(ctx, testCase.state, 10*time.Millisecond, 30*time.Second)
			if testCase.wantSetAtLeast {
				assert.GreaterOrEqual(t, db.GetQueryCalledNum(restoreFlushLog), testCase.wantSet)
			} else {
				assert.Equal(t, testCase.wantSet, db.GetQueryCalledNum(restoreFlushLog))
			}
			for _, start := range []string{startReplication, startSQLThread, startIOThread} {
				got := db.GetQueryCalledNum(start)
				switch {
				case start != testCase.wantStart:
					assert.Zero(t, got, "unexpected call count for %q", start)
				case testCase.wantStartAtLeast:
					assert.GreaterOrEqual(t, got, 1, "unexpected call count for %q", start)
				default:
					assert.Equal(t, 1, got, "unexpected call count for %q", start)
				}
			}
		})
	}
}

// TestRestoreRetriesConnectUntilMysqldReturns covers a replica that is
// briefly unreachable right after a failed shutdown: the restoration must
// keep retrying to connect rather than give up on the first failure, and it
// must converge once mysqld accepts connections again.
func TestRestoreRetriesConnectUntilMysqldReturns(t *testing.T) {
	const (
		showReplicaStatus   = "SHOW REPLICA STATUS"
		restoreFlushLog     = "SET GLOBAL innodb_flush_log_at_trx_commit = 2"
		restoreSyncBinlog   = "SET GLOBAL sync_binlog = 0"
		restoreSyncRelayLog = "SET GLOBAL sync_relay_log = 10000"
		startReplication    = "START REPLICA"
	)

	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery(showReplicaStatus, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
		"source|No|No",
	))
	for _, query := range []string{restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog, startReplication} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	// A successful start is observed as the status flipping to running.
	db.SetBeforeFunc(startReplication, func() {
		db.AddQuery(showReplicaStatus, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
			"source|Yes|Yes",
		))
	})
	// mysqld starts out refusing connections, as it may right after the failed
	// shutdown that armed the restoration.
	db.EnableConnFail()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	state := &replicaShutdownState{
		startReceiver:       true,
		startApplier:        true,
		flushLogAtTrxCommit: "2",
		syncBinlog:          "0",
		syncRelayLog:        "10000",
	}
	restored := make(chan struct{})
	go func() {
		defer close(restored)
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		testMysqld.restoreReplicaAfterFailedShutdown(ctx, state, 10*time.Millisecond, 30*time.Second)
	}()

	// The restoration must survive the connection failures rather than give
	// up.
	select {
	case <-restored:
		require.FailNow(t, "the restoration gave up while mysqld was unreachable")
	case <-time.After(500 * time.Millisecond):
	}
	// mysqld accepts connections again: the restoration must now converge.
	db.DisableConnFail()
	select {
	case <-restored:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the restoration did not complete after mysqld became reachable")
	}
	assert.Equal(t, 1, db.GetQueryCalledNum(restoreFlushLog), "the durability settings must be restored once mysqld is reachable")
	assert.Equal(t, 1, db.GetQueryCalledNum(startReplication), "replication must be restarted once mysqld is reachable")
}

// TestRestoreBoundsConnectRetries covers the flip side: a mysqld that stays
// continuously unreachable is exiting after all, so the restoration must give
// up once its connect budget is exhausted -- well before the overall restore
// deadline -- rather than hold the pending-restore accounting (and Close's
// bounded wait on it) for the full restore budget.
func TestRestoreBoundsConnectRetries(t *testing.T) {
	const restoreFlushLog = "SET GLOBAL innodb_flush_log_at_trx_commit = 2"

	db := fakesqldb.New(t)
	defer db.Close()
	db.EnableConnFail()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	state := &replicaShutdownState{
		startReceiver:       true,
		startApplier:        true,
		flushLogAtTrxCommit: "2",
		syncBinlog:          "0",
		syncRelayLog:        "10000",
	}
	restored := make(chan struct{})
	go func() {
		defer close(restored)
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Minute)
		defer cancel()
		testMysqld.restoreReplicaAfterFailedShutdown(ctx, state, 10*time.Millisecond, time.Second)
	}()

	// It must retry through the connect budget rather than give up on the
	// first failure...
	select {
	case <-restored:
		require.FailNow(t, "the restoration gave up before its connect budget was exhausted")
	case <-time.After(400 * time.Millisecond):
	}
	// ...and once mysqld has stayed unreachable past the budget, it must give
	// up long before the restoration deadline.
	select {
	case <-restored:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the restoration did not give up on a continuously unreachable mysqld")
	}
	assert.Zero(t, db.GetQueryCalledNum(restoreFlushLog))
}

// TestRestoreRetriesStatusRead covers a replication status read failing
// mid-restore (e.g. on a connection broken by the failed shutdown): the
// restoration must reconnect and retry within its budget rather than give up
// and leave replication stopped.
func TestRestoreRetriesStatusRead(t *testing.T) {
	const (
		showReplicaStatus   = "SHOW REPLICA STATUS"
		restoreFlushLog     = "SET GLOBAL innodb_flush_log_at_trx_commit = 2"
		restoreSyncBinlog   = "SET GLOBAL sync_binlog = 0"
		restoreSyncRelayLog = "SET GLOBAL sync_relay_log = 10000"
		startReplication    = "START REPLICA"
	)

	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery(showReplicaStatus, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
		"source|No|No",
	))
	for _, query := range []string{restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog, startReplication} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	// A successful start is observed as the status flipping to running.
	db.SetBeforeFunc(startReplication, func() {
		db.AddQuery(showReplicaStatus, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
			"source|Yes|Yes",
		))
	})
	// The status reads fail to begin with.
	db.AddRejectedQuery(showReplicaStatus, assert.AnError)

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	state := &replicaShutdownState{
		startReceiver:       true,
		startApplier:        true,
		flushLogAtTrxCommit: "2",
		syncBinlog:          "0",
		syncRelayLog:        "10000",
	}
	restored := make(chan struct{})
	go func() {
		defer close(restored)
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		testMysqld.restoreReplicaAfterFailedShutdown(ctx, state, 10*time.Millisecond, 30*time.Second)
	}()

	// The restoration must keep retrying the failing status read.
	assert.Eventually(t, func() bool {
		return db.GetQueryCalledNum(showReplicaStatus) >= 3
	}, 30*time.Second, 10*time.Millisecond, "the restoration did not retry the failing status read")
	// The status reads recover: the restoration must now converge.
	db.DeleteRejectedQuery(showReplicaStatus)
	select {
	case <-restored:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the restoration did not complete after the status reads recovered")
	}
	assert.Equal(t, 1, db.GetQueryCalledNum(restoreFlushLog), "the durability settings must be restored exactly once")
	assert.Equal(t, 1, db.GetQueryCalledNum(startReplication), "replication must be restarted once the status reads recover")
}

// TestPrepareKeepsInheritedStateOnConnectFailure covers a takeover
// preparation that cannot connect: the caller has already cancelled the
// pending restoration it inherited the state from, so the preparation now
// owns that state and must hand it back -- published and returned -- even
// when it fails before reaching mysqld. Dropping it would leave a
// subsequently failed shutdown with nothing to arm a replacement restoration
// from, stranding the replica fenced with replication stopped.
func TestPrepareKeepsInheritedStateOnConnectFailure(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.EnableConnFail()

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	inherited := &replicaShutdownState{
		startReceiver:       true,
		startApplier:        true,
		flushLogAtTrxCommit: "2",
		syncBinlog:          "0",
		syncRelayLog:        "10000",
	}
	var captured *replicaShutdownState
	state, err := testMysqld.prepareReplicaForShutdown(t.Context(), inherited, func(state *replicaShutdownState) {
		captured = state
	})
	require.ErrorContains(t, err, "failed to connect to MySQL before shutdown")
	assert.Same(t, inherited, state, "the inherited state must be returned so the caller can arm a replacement restoration")
	assert.Same(t, inherited, captured, "the inherited state must be published even when the preparation cannot connect")
}

// TestTakeoverConnectFailureKeepsRestoreOwnership covers the same case
// end-to-end: a retrying shutdown takes over a pending restoration, its
// preparation cannot reconnect, and the shutdown fails again. The inherited
// state must not be dropped: a replacement restoration must be armed, and it
// must converge the replica once mysqld is reachable again.
func TestTakeoverConnectFailureKeepsRestoreOwnership(t *testing.T) {
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
		startSQLThread      = "START REPLICA SQL_THREAD"
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
		stopIOThread, restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog, startReplication, startSQLThread,
	} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	// The applier stop hits rpl_stop_replica_timeout: the stop stays pending,
	// so the first restoration resets the durability settings and then waits
	// it out, staying pending for the retry to take over.
	db.AddRejectedQuery(stopSQLThread, sqlerror.NewSQLError(sqlerror.ERStopReplicaSQLThreadTimeout, sqlerror.SSUnknownSQLState, "STOP REPLICA SQL_THREAD timed out"))
	db.AddQueryPattern("kill .*", &sqltypes.Result{})
	db.SetBeforeFunc(startSQLThread, func() {
		db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
			"source|Yes|Yes",
		))
	})

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	err := testMysqld.shutdownWithReplicaCrashSafety(t.Context(), 200*time.Millisecond, func() error {
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)
	assert.Eventually(t, func() bool {
		return db.GetQueryCalledNum(restoreFlushLog) == 1
	}, 30*time.Second, 10*time.Millisecond, "the first restoration did not reset the durability settings")

	// mysqld stops accepting connections: the retry takes over the pending
	// restoration but its preparation cannot connect, and the shutdown fails
	// again.
	db.EnableConnFail()
	err = testMysqld.shutdownWithReplicaCrashSafety(t.Context(), 200*time.Millisecond, func() error {
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)

	// mysqld accepts connections again and the pending stop settles: the
	// replacement restoration must exist and converge the inherited state.
	db.DisableConnFail()
	db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
		"source|Yes|No",
	))
	assert.Eventually(t, func() bool {
		return db.GetQueryCalledNum(startSQLThread) == 1
	}, 30*time.Second, 10*time.Millisecond, "no replacement restoration converged the replica: the takeover dropped the inherited state")
}

// TestRestoreStopsWhenNoLongerReplica covers the server ceasing to be a
// replica mid-restoration (e.g. it was promoted, or its replication
// configuration was reset): there are no threads left to reconcile and the
// durability settings are the new role's to manage, so the restoration must
// stop -- without writing the relaxed settings -- rather than retry for its
// whole budget, holding Close's bounded wait with it.
func TestRestoreStopsWhenNoLongerReplica(t *testing.T) {
	const (
		showReplicaStatus   = "SHOW REPLICA STATUS"
		restoreFlushLog     = "SET GLOBAL innodb_flush_log_at_trx_commit = 2"
		restoreSyncBinlog   = "SET GLOBAL sync_binlog = 0"
		restoreSyncRelayLog = "SET GLOBAL sync_relay_log = 10000"
	)

	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	// An empty result is how a server that is not (any longer) a replica
	// answers.
	db.AddQuery(showReplicaStatus, &sqltypes.Result{})
	for _, query := range []string{restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog} {
		db.AddQuery(query, &sqltypes.Result{})
	}

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	state := &replicaShutdownState{
		startReceiver:       true,
		startApplier:        true,
		flushLogAtTrxCommit: "2",
		syncBinlog:          "0",
		syncRelayLog:        "10000",
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	testMysqld.restoreReplicaAfterFailedShutdown(ctx, state, 10*time.Millisecond, 30*time.Second)
	assert.Equal(t, 1, db.GetQueryCalledNum(showReplicaStatus), "the restoration must stop on the first not-a-replica answer instead of retrying")
	assert.Zero(t, db.GetQueryCalledNum(restoreFlushLog), "a server that is no longer a replica must not receive the relaxed durability settings")
}

// TestRestoreRetriesSettingsRestore covers a transient failure while
// restoring the durability settings: like the thread restarts, the settings
// must be retried within the budget rather than attempted only once.
func TestRestoreRetriesSettingsRestore(t *testing.T) {
	const (
		restoreFlushLog     = "SET GLOBAL innodb_flush_log_at_trx_commit = 2"
		restoreSyncBinlog   = "SET GLOBAL sync_binlog = 0"
		restoreSyncRelayLog = "SET GLOBAL sync_relay_log = 10000"
	)

	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
		"source|No|No",
	))
	for _, query := range []string{restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	// The first setting fails to begin with.
	db.AddRejectedQuery(restoreFlushLog, assert.AnError)

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	// No threads to reconcile: the restoration's only work is the settings.
	state := &replicaShutdownState{
		flushLogAtTrxCommit: "2",
		syncBinlog:          "0",
		syncRelayLog:        "10000",
	}
	restored := make(chan struct{})
	go func() {
		defer close(restored)
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		testMysqld.restoreReplicaAfterFailedShutdown(ctx, state, 10*time.Millisecond, 30*time.Second)
	}()

	// The restoration must keep retrying the failing setting.
	assert.Eventually(t, func() bool {
		return db.GetQueryCalledNum(restoreFlushLog) >= 3
	}, 30*time.Second, 10*time.Millisecond, "the restoration did not retry the failing durability setting")
	// The setting recovers: the restoration must now complete.
	db.DeleteRejectedQuery(restoreFlushLog)
	select {
	case <-restored:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the restoration did not complete after the setting recovered")
	}
}

// TestRestoreRetriesSettingsWhenThreadsAlreadyConverged covers a transient
// durability-setting failure while the replication threads already report the
// desired state: the reconcile convergence must not end the restoration with
// the settings still at the shutdown fence values (a live replica left
// permanently on sync_binlog=1 etc.), but keep retrying them within the
// budget.
func TestRestoreRetriesSettingsWhenThreadsAlreadyConverged(t *testing.T) {
	const (
		showReplicaStatus   = "SHOW REPLICA STATUS"
		restoreFlushLog     = "SET GLOBAL innodb_flush_log_at_trx_commit = 2"
		restoreSyncBinlog   = "SET GLOBAL sync_binlog = 0"
		restoreSyncRelayLog = "SET GLOBAL sync_relay_log = 10000"
		startReplication    = "START REPLICA"
	)

	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	// Both threads already run as desired: the reconcile has nothing to do.
	db.AddQuery(showReplicaStatus, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
		"source|Yes|Yes",
	))
	for _, query := range []string{restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog, startReplication} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	// The first setting fails to begin with.
	db.AddRejectedQuery(restoreFlushLog, assert.AnError)

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	state := &replicaShutdownState{
		startReceiver:       true,
		startApplier:        true,
		flushLogAtTrxCommit: "2",
		syncBinlog:          "0",
		syncRelayLog:        "10000",
	}
	restored := make(chan struct{})
	go func() {
		defer close(restored)
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		testMysqld.restoreReplicaAfterFailedShutdown(ctx, state, 10*time.Millisecond, 30*time.Second)
	}()

	// The restoration must keep retrying the failing setting instead of
	// declaring convergence on the threads alone.
	assert.Eventually(t, func() bool {
		return db.GetQueryCalledNum(restoreFlushLog) >= 3
	}, 30*time.Second, 10*time.Millisecond, "the restoration converged without restoring the durability settings")
	// The setting recovers: the restoration must now complete, and it must
	// never have restarted the already-running threads.
	db.DeleteRejectedQuery(restoreFlushLog)
	select {
	case <-restored:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the restoration did not complete after the setting recovered")
	}
	assert.Zero(t, db.GetQueryCalledNum(startReplication))
}

// TestRestoreRetriesSettingsWhenThreadStartUnavailable covers the same gap on
// flavors without executable replication-thread commands (file position, MySQL
// Group Replication): having nothing to start must only skip the thread
// start, not abandon a durability-settings restore that is still retrying.
func TestRestoreRetriesSettingsWhenThreadStartUnavailable(t *testing.T) {
	const (
		showSlaveStatus     = "SHOW SLAVE STATUS"
		restoreFlushLog     = "SET GLOBAL innodb_flush_log_at_trx_commit = 2"
		restoreSyncBinlog   = "SET GLOBAL sync_binlog = 0"
		restoreSyncRelayLog = "SET GLOBAL sync_relay_log = 10000"
		unsupported         = "unsupported"
	)

	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	// The threads are observed stopped, so the restoration would want to
	// start them -- but the file position flavor has no executable start.
	db.AddQuery(showSlaveStatus, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Master_Host|Slave_IO_Running|Slave_SQL_Running", "varchar|varchar|varchar"),
		"source|No|No",
	))
	for _, query := range []string{restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog, unsupported} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	// The first setting fails to begin with.
	db.AddRejectedQuery(restoreFlushLog, assert.AnError)

	params := db.ConnParams()
	cp := *params
	cp.Flavor = replication.FilePosFlavorID
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	state := &replicaShutdownState{
		startReceiver:       true,
		startApplier:        true,
		flushLogAtTrxCommit: "2",
		syncBinlog:          "0",
		syncRelayLog:        "10000",
	}
	restored := make(chan struct{})
	go func() {
		defer close(restored)
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		testMysqld.restoreReplicaAfterFailedShutdown(ctx, state, 10*time.Millisecond, 30*time.Second)
	}()

	// The restoration must keep retrying the failing setting rather than end
	// with the unavailable thread start.
	assert.Eventually(t, func() bool {
		return db.GetQueryCalledNum(restoreFlushLog) >= 3
	}, 30*time.Second, 10*time.Millisecond, "the restoration gave up the settings when the thread start was unavailable")
	// The setting recovers: the restoration must now complete without ever
	// issuing the "unsupported" start.
	db.DeleteRejectedQuery(restoreFlushLog)
	select {
	case <-restored:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the restoration did not complete after the setting recovered")
	}
	assert.Zero(t, db.GetQueryCalledNum(unsupported))
}

// TestRestoreStopsSettingsWhenPromotedMidRestore covers a role change racing
// the restoration (Codex review): promotion and RESET REPLICA ALL do not
// serialize with the restore, so every pass must verify the server is still a
// replica BEFORE applying the relaxed durability settings, and the
// restoration must end the moment it is not. Otherwise a retrying SET could
// land on -- and clobber the configuration of -- a newly promoted primary.
func TestRestoreStopsSettingsWhenPromotedMidRestore(t *testing.T) {
	const (
		showReplicaStatus   = "SHOW REPLICA STATUS"
		restoreFlushLog     = "SET GLOBAL innodb_flush_log_at_trx_commit = 2"
		restoreSyncBinlog   = "SET GLOBAL sync_binlog = 0"
		restoreSyncRelayLog = "SET GLOBAL sync_relay_log = 10000"
	)

	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery(showReplicaStatus, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
		"source|Yes|Yes",
	))
	for _, query := range []string{restoreSyncBinlog, restoreSyncRelayLog} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	// The first setting keeps failing, so the restoration keeps retrying it.
	db.AddQuery(restoreFlushLog, &sqltypes.Result{})
	db.AddRejectedQuery(restoreFlushLog, assert.AnError)
	// The server is promoted while the restoration is retrying: from the
	// fourth status probe on, it is no longer a replica. (BeforeFunc runs
	// after the current call's result is fetched, so a swap on the third
	// probe takes effect on the fourth.)
	var probes atomic.Int64
	db.SetBeforeFunc(showReplicaStatus, func() {
		if probes.Add(1) == 3 {
			db.AddQuery(showReplicaStatus, &sqltypes.Result{})
		}
	})

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	state := &replicaShutdownState{
		startReceiver:       true,
		startApplier:        true,
		flushLogAtTrxCommit: "2",
		syncBinlog:          "0",
		syncRelayLog:        "10000",
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	testMysqld.restoreReplicaAfterFailedShutdown(ctx, state, 10*time.Millisecond, 30*time.Second)

	// Three passes saw a replica and were allowed to try the settings; the
	// fourth observed the promotion and must not have touched them first.
	assert.Equal(t, 3, db.GetQueryCalledNum(restoreFlushLog),
		"no settings write may happen on a pass that observed the promotion")
}

// TestRestoreReconnectsWhenSettingsRestoreLosesConnection covers a settings
// restore whose connection dies mid-SET when there are no replication threads
// to reconcile: the status probe -- which is what resets a broken connection
// in the reconcile path -- is never reached there, so the settings-only path
// must reconnect itself rather than retry the dead connection until the
// restore deadline (holding Close's bounded wait with it).
func TestRestoreReconnectsWhenSettingsRestoreLosesConnection(t *testing.T) {
	const (
		restoreFlushLog     = "SET GLOBAL innodb_flush_log_at_trx_commit = 2"
		restoreSyncBinlog   = "SET GLOBAL sync_binlog = 0"
		restoreSyncRelayLog = "SET GLOBAL sync_relay_log = 10000"
	)

	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
		"source|No|No",
	))
	for _, query := range []string{restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	// The first SET loses its connection mid-statement.
	var dropped atomic.Bool
	db.SetBeforeFunc(restoreFlushLog, func() {
		if dropped.CompareAndSwap(false, true) {
			db.CloseAllConnections()
		}
	})

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	// No threads to reconcile: the restoration's only work is the settings.
	state := &replicaShutdownState{
		flushLogAtTrxCommit: "2",
		syncBinlog:          "0",
		syncRelayLog:        "10000",
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	testMysqld.restoreReplicaAfterFailedShutdown(ctx, state, 10*time.Millisecond, 30*time.Second)
	assert.GreaterOrEqual(t, db.GetQueryCalledNum(restoreSyncRelayLog), 1,
		"the settings must be restored on a fresh connection after the first one broke")
}

// TestReplicaShutdownSkipsUnavailableThreadCommands covers flavors whose
// replication-thread commands cannot be executed: the file position flavor
// returns "unsupported" for them, so the crash-safety preparation must not
// issue them -- they would just fail and log a spurious warning on every
// shutdown -- and a restoration must not spin retrying an unexecutable start.
func TestReplicaShutdownSkipsUnavailableThreadCommands(t *testing.T) {
	const (
		showSlaveStatus = "SHOW SLAVE STATUS"
		readDurability  = "SELECT @@global.innodb_flush_log_at_trx_commit, @@global.sync_binlog, @@global.sync_relay_log"
		flushRelayLogs  = "FLUSH NO_WRITE_TO_BINLOG RELAY LOGS"
		unsupported     = "unsupported"
	)
	newFilePosMysqld := func(t *testing.T, db *fakesqldb.DB) *Mysqld {
		params := db.ConnParams()
		cp := *params
		cp.Flavor = replication.FilePosFlavorID
		dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
		testMysqld := NewMysqld(dbc)
		t.Cleanup(testMysqld.Close)
		return testMysqld
	}

	t.Run("the preparation skips the thread stops", func(t *testing.T) {
		db := fakesqldb.New(t)
		defer db.Close()
		db.AddQuery("SELECT 1", &sqltypes.Result{})
		// The file position flavor reads the replication status with the old
		// terminology.
		db.AddQuery(showSlaveStatus, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("Master_Host|Slave_IO_Running|Slave_SQL_Running", "varchar|varchar|varchar"),
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
			"SET GLOBAL innodb_flush_log_at_trx_commit = 1",
			"SET GLOBAL sync_binlog = 1",
			"SET GLOBAL sync_relay_log = 1",
			"FLUSH NO_WRITE_TO_BINLOG ENGINE LOGS",
			"FLUSH NO_WRITE_TO_BINLOG BINARY LOGS",
			flushRelayLogs,
			unsupported,
		} {
			db.AddQuery(query, &sqltypes.Result{})
		}
		testMysqld := newFilePosMysqld(t, db)

		state, err := testMysqld.prepareReplicaForShutdown(t.Context(), nil, func(*replicaShutdownState) {})
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, 1, db.GetQueryCalledNum(flushRelayLogs), "the durability fence must still be applied")
		assert.Zero(t, db.GetQueryCalledNum(unsupported), `the "unsupported" thread stops must not be issued`)
	})

	t.Run("the restoration skips the thread starts", func(t *testing.T) {
		db := fakesqldb.New(t)
		defer db.Close()
		db.AddQuery("SELECT 1", &sqltypes.Result{})
		// The threads are observed stopped, so the restoration would want to
		// start them -- but the flavor has no executable start.
		db.AddQuery(showSlaveStatus, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("Master_Host|Slave_IO_Running|Slave_SQL_Running", "varchar|varchar|varchar"),
			"source|No|No",
		))
		for _, query := range []string{
			"SET GLOBAL innodb_flush_log_at_trx_commit = 2",
			"SET GLOBAL sync_binlog = 0",
			"SET GLOBAL sync_relay_log = 10000",
			unsupported,
		} {
			db.AddQuery(query, &sqltypes.Result{})
		}
		testMysqld := newFilePosMysqld(t, db)

		state := &replicaShutdownState{
			startReceiver:       true,
			startApplier:        true,
			flushLogAtTrxCommit: "2",
			syncBinlog:          "0",
			syncRelayLog:        "10000",
		}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		testMysqld.restoreReplicaAfterFailedShutdown(ctx, state, 10*time.Millisecond, 30*time.Second)
		assert.Equal(t, 1, db.GetQueryCalledNum("SET GLOBAL sync_relay_log = 10000"), "the durability settings must still be restored")
		assert.Zero(t, db.GetQueryCalledNum(unsupported), `the "unsupported" thread start must not be issued`)
	})
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
	// background restore must now undo it. The preparation never reached the
	// thread stops, so the reconcile must find the threads already running as
	// desired and not blindly restart them.
	close(release)
	assert.Eventually(t, func() bool {
		return db.GetQueryCalledNum(restoreFlushLog) == 1 &&
			db.GetQueryCalledNum(restoreSyncBinlog) == 1 &&
			db.GetQueryCalledNum(restoreSyncRelayLog) == 1
	}, 30*time.Second, 10*time.Millisecond, "the late-landing preparation was not restored in the background")
	assert.Zero(t, db.GetQueryCalledNum(startReplication), "threads that were never stopped must not be restarted")
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
	// The preparation never reached the thread stops, so the reconcile finds
	// the threads already running as desired.
	assert.Zero(t, db.GetQueryCalledNum(startReplication), "threads that were never stopped must not be restarted")
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
	// draining when the shutdown fails and the owner gives up. When it finally
	// drains, the threads report stopped, and a successful start flips them
	// back to running.
	release := make(chan struct{})
	db.SetBeforeFunc(stopSQLThread, func() {
		<-release
		db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
			"source|No|No",
		))
	})
	db.SetBeforeFunc(startReplication, func() {
		db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
			"source|Yes|Yes",
		))
	})

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

// TestTakeoverTimeoutHandsOffToReplacementRestore covers a takeover whose
// cancelled restoration outlives the bounded wait: the retry proceeds without
// preparation, and if its shutdown also fails, restore ownership must survive
// the failed handoff -- a replacement restoration must wait for the cancelled
// one to fully exit and then converge the replica, which the cancelled one
// (fast-failing on its cancelled context) never will.
func TestTakeoverTimeoutHandsOffToReplacementRestore(t *testing.T) {
	const (
		showReplicaStatus   = "SHOW REPLICA STATUS"
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
		startSQLThread      = "START REPLICA SQL_THREAD"
	)

	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	db.AddQuery(showReplicaStatus, sqltypes.MakeTestResult(
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
		stopIOThread, restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog, startReplication, startSQLThread,
	} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	// The applier stop stays pending, so the first attempt's restoration is in
	// its reconcile when the retry arrives.
	db.AddRejectedQuery(stopSQLThread, sqlerror.NewSQLError(sqlerror.ERStopReplicaSQLThreadTimeout, sqlerror.SSUnknownSQLState, "STOP REPLICA SQL_THREAD timed out"))
	// The KILL of the cancelled restoration's blocked status read is itself
	// blocked, so the cancelled restoration outlives the retry's bounded wait.
	releaseKill := make(chan struct{})
	killReleased := sync.OnceFunc(func() { close(releaseKill) })
	defer killReleased()
	db.AddQueryPatternWithCallback("kill .*", &sqltypes.Result{}, func(string) { <-releaseKill })
	db.SetBeforeFunc(startSQLThread, func() {
		db.AddQuery(showReplicaStatus, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
			"source|Yes|Yes",
		))
	})

	// Installed upfront (fakesqldb's SetBeforeFunc must not race live
	// queries), armed later: once armed, the restoration's next status poll
	// blocks until released.
	var blockStatus atomic.Bool
	statusBlocked := make(chan struct{})
	releaseStatus := make(chan struct{})
	statusReleased := sync.OnceFunc(func() { close(releaseStatus) })
	defer statusReleased()
	signalBlocked := sync.OnceFunc(func() { close(statusBlocked) })
	db.SetBeforeFunc(showReplicaStatus, func() {
		if blockStatus.Load() {
			signalBlocked()
			<-releaseStatus
		}
	})

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)

	err := testMysqld.shutdownWithReplicaCrashSafety(t.Context(), 200*time.Millisecond, func() error {
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)
	assert.Eventually(t, func() bool {
		return db.GetQueryCalledNum(restoreFlushLog) == 1
	}, 30*time.Second, 10*time.Millisecond, "the first restoration did not reset the durability settings")

	// Arm the block, and wait until the restoration is actually blocked there
	// so the retry's cancellation cannot resolve it in time.
	blockStatus.Store(true)
	select {
	case <-statusBlocked:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the first restoration never reached its blocked status poll")
	}

	// The retry cannot take the restoration over in time: it must proceed
	// without preparation but hand ownership to a replacement on failure.
	err = testMysqld.shutdownWithReplicaCrashSafety(t.Context(), 200*time.Millisecond, func() error {
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, 1, db.GetQueryCalledNum(readDurability), "the retry must not prepare during a failed takeover")

	// Unblock everything: the cancelled restoration exits without restoring,
	// and the replacement must then converge the replica.
	statusReleased()
	killReleased()
	db.AddQuery(showReplicaStatus, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
		"source|Yes|No",
	))
	assert.Eventually(t, func() bool {
		return db.GetQueryCalledNum(startSQLThread) == 1 &&
			db.GetQueryCalledNum(restoreFlushLog) == 2
	}, 30*time.Second, 10*time.Millisecond, "the replacement restoration must converge the replica after the cancelled one exits")
	testMysqld.Close()
}

// TestShutdownCancelledDuringPreparationSkipsShutdown covers a caller whose
// context is cancelled while the crash-safety preparation is in flight: the
// shutdown itself must not run afterwards (the hookless mysqladmin path is not
// context-aware, so it would stop mysqld after the client already received the
// error), and whatever the preparation changed must still be restored.
func TestShutdownCancelledDuringPreparationSkipsShutdown(t *testing.T) {
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
	for _, query := range []string{setFlushLog, setSyncBinlog, setSyncRelayLog, flushEngineLogs, flushBinaryLogs, flushRelayLogs, stopIOThread, stopSQLThread, restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog, startReplication} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	db.AddQueryPattern("kill .*", &sqltypes.Result{})

	// The preparation blocks in its first mutating statement; the caller
	// cancels while it is there.
	fenceEntered := make(chan struct{})
	signalEntered := sync.OnceFunc(func() { close(fenceEntered) })
	release := make(chan struct{})
	defer close(release)
	db.SetBeforeFunc(setFlushLog, func() {
		signalEntered()
		<-release
	})

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	var shutdownCalled atomic.Bool
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- testMysqld.shutdownWithReplicaCrashSafety(ctx, 30*time.Second, func() error {
			shutdownCalled.Store(true)
			return assert.AnError
		})
	}()
	select {
	case <-fenceEntered:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the preparation never reached its first mutating statement")
	}
	cancel()

	select {
	case err := <-done:
		require.ErrorContains(t, err, "shutdown cancelled during the crash-safety preparation")
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the cancelled shutdown did not return")
	}
	assert.False(t, shutdownCalled.Load(), "the shutdown must not run after the caller was cancelled")

	// Whatever the preparation changed must still be restored in the
	// background; the threads were never stopped, so no restart is needed.
	assert.Eventually(t, func() bool {
		return db.GetQueryCalledNum(restoreFlushLog) == 1 &&
			db.GetQueryCalledNum(restoreSyncBinlog) == 1 &&
			db.GetQueryCalledNum(restoreSyncRelayLog) == 1
	}, 30*time.Second, 10*time.Millisecond, "the cancelled preparation's changes were not restored")
	assert.Zero(t, db.GetQueryCalledNum(startReplication))
}

// TestShutdownIdempotentWhenStoppedWhileQueued covers a mysqld that another
// attempt fully stopped after this attempt's already-stopped check but before
// its shutdown ran: the recheck under the shutdown gate must report the same
// idempotent success instead of a spurious hook/mysqladmin failure.
func TestShutdownIdempotentWhenStoppedWhileQueued(t *testing.T) {
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
	for _, query := range []string{setFlushLog, setSyncBinlog, setSyncRelayLog, flushEngineLogs, flushBinaryLogs, flushRelayLogs, stopIOThread, stopSQLThread} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	db.AddQueryPattern("kill .*", &sqltypes.Result{})

	// No hooks exist under this VTROOT, so a shutdown that proceeds past the
	// recheck would go down the mysqladmin path and fail.
	t.Setenv("VTROOT", t.TempDir())

	// mysqld appears running at entry; the concurrent attempt "completes"
	// while this attempt is preparing.
	dir := t.TempDir()
	cnf := &Mycnf{
		SocketFile: filepath.Join(dir, "mysql.sock"),
		PidFile:    filepath.Join(dir, "mysql.pid"),
	}
	require.NoError(t, os.WriteFile(cnf.PidFile, []byte("12345\n"), 0o600))
	db.SetBeforeFunc(setFlushLog, func() {
		_ = os.Remove(cnf.PidFile)
	})

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	err := testMysqld.Shutdown(t.Context(), cnf, false, 30*time.Second)
	require.NoError(t, err, "a mysqld stopped by a concurrent attempt must yield the same idempotent success")
}

// TestShutdownWaitHonorsCancellation covers a shutdown attempt whose context
// expires while it is queued behind a slow concurrent attempt: it must return
// the context error without preparing or shutting down -- a caller whose RPC
// already failed with DeadlineExceeded must not shut mysqld down afterwards.
func TestShutdownWaitHonorsCancellation(t *testing.T) {
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
	// Attempt A's failed shutdown arms a restoration; its settings must be
	// servable or Close would wait out the full restore budget.
	for _, query := range []string{
		setFlushLog, setSyncBinlog, setSyncRelayLog, flushEngineLogs, flushBinaryLogs, flushRelayLogs,
		stopIOThread, stopSQLThread, restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog,
	} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	db.AddQueryPattern("kill .*", &sqltypes.Result{})

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)

	// Attempt A prepares, then blocks inside its shutdown.
	aEntered := make(chan struct{})
	aRelease := make(chan struct{})
	releaseA := sync.OnceFunc(func() { close(aRelease) })
	defer releaseA()
	aDone := make(chan error, 1)
	go func() {
		aDone <- testMysqld.shutdownWithReplicaCrashSafety(t.Context(), 200*time.Millisecond, func() error {
			close(aEntered)
			<-aRelease
			return assert.AnError
		})
	}()
	select {
	case <-aEntered:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "attempt A never reached its shutdown")
	}

	// Attempt B's deadline expires while queued behind A: it must give up
	// without shutting down.
	bEntered := make(chan struct{})
	bCtx, bCancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer bCancel()
	bDone := make(chan error, 1)
	go func() {
		bDone <- testMysqld.shutdownWithReplicaCrashSafety(bCtx, 200*time.Millisecond, func() error {
			close(bEntered)
			return assert.AnError
		})
	}()
	select {
	case err := <-bDone:
		require.ErrorContains(t, err, "shutdown cancelled while waiting for a concurrent shutdown attempt")
	case <-time.After(30 * time.Second):
		require.FailNow(t, "attempt B did not honor its cancellation while queued")
	}
	select {
	case <-bEntered:
		require.FailNow(t, "attempt B must not shut mysqld down after its deadline expired")
	default:
	}
	assert.Equal(t, 1, db.GetQueryCalledNum(readDurability), "attempt B must not have prepared")

	// A completes normally afterwards.
	releaseA()
	select {
	case err := <-aDone:
		require.ErrorIs(t, err, assert.AnError)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "attempt A did not return")
	}
	testMysqld.Close()
}

// TestShutdownSerializesAcrossMysqldInstances covers shutdown attempts from
// separate processes (Codex review): each mysqlctl CLI invocation builds its
// own Mysqld object, so the in-process gate cannot serialize them, and one
// failed attempt's background restoration could reset the durability fence
// beneath another process's shutdown. The interprocess lock must make a
// second instance wait the first out -- including its pending restoration,
// which the first instance holds the lock across until Close. flock
// contention is per open file description, so a second Mysqld in this
// process contends exactly as a second process would.
func TestShutdownSerializesAcrossMysqldInstances(t *testing.T) {
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
		stopIOThread, stopSQLThread, restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog,
	} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	db.AddQueryPattern("kill .*", &sqltypes.Result{})

	// No hooks, and mysqladmin has no server to talk to: the shutdown itself
	// fails, arming the restoration.
	t.Setenv("VTROOT", t.TempDir())
	dir := t.TempDir()
	cnf := &Mycnf{
		SocketFile: filepath.Join(dir, "mysql.sock"),
		PidFile:    filepath.Join(dir, "mysql.pid"),
	}
	require.NoError(t, os.WriteFile(cnf.PidFile, []byte("12345\n"), 0o600))

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")

	// Instance A's shutdown fails, arming a restoration; A holds the
	// interprocess lock until Close.
	mysqldA := NewMysqld(dbc)
	err := mysqldA.Shutdown(t.Context(), cnf, false, 30*time.Second)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "concurrent mysqld shutdown")

	// Instance B (a separate "process") must wait behind A rather than
	// interleave with A's restoration.
	mysqldB := NewMysqld(dbc)
	defer mysqldB.Close()
	bCtx, bCancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer bCancel()
	err = mysqldB.Shutdown(bCtx, cnf, false, 30*time.Second)
	require.ErrorContains(t, err, "waiting for a concurrent mysqld shutdown in another process")

	// A closes -- after its restoration finished -- releasing the lock: B's
	// retry now proceeds to its own attempt and fails on the shutdown itself,
	// not on the lock.
	mysqldA.Close()
	err = mysqldB.Shutdown(t.Context(), cnf, false, 30*time.Second)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "concurrent mysqld shutdown")
}

// TestConcurrentShutdownAttemptsSerialize covers two overlapping shutdown
// attempts (e.g. concurrent mysqlctld shutdown RPCs, which the gRPC server
// does not serialize): while one attempt is still inside its shutdown, the
// other must not begin its own preparation -- otherwise the first attempt's
// failure restore could reset the durability fence beneath the second. The
// second attempt must instead wait, then take over the first's pending
// restoration and apply a fresh fence before its own shutdown.
func TestConcurrentShutdownAttemptsSerialize(t *testing.T) {
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
		startSQLThread      = "START REPLICA SQL_THREAD"
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
		stopIOThread, restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog, startReplication, startSQLThread,
	} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	// The applier stop stays pending, so the first attempt's restoration
	// remains in flight when the second attempt takes over.
	db.AddRejectedQuery(stopSQLThread, sqlerror.NewSQLError(sqlerror.ERStopReplicaSQLThreadTimeout, sqlerror.SSUnknownSQLState, "STOP REPLICA SQL_THREAD timed out"))
	db.AddQueryPattern("kill .*", &sqltypes.Result{})
	db.SetBeforeFunc(startSQLThread, func() {
		db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
			"source|Yes|Yes",
		))
	})

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)

	// Attempt A prepares, then blocks inside its shutdown.
	aEntered := make(chan struct{})
	aRelease := make(chan struct{})
	aDone := make(chan error, 1)
	go func() {
		aDone <- testMysqld.shutdownWithReplicaCrashSafety(t.Context(), 200*time.Millisecond, func() error {
			close(aEntered)
			<-aRelease
			return assert.AnError
		})
	}()
	select {
	case <-aEntered:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "attempt A never reached its shutdown")
	}

	// Attempt B arrives while A is still shutting down: it must not prepare.
	bEntered := make(chan struct{})
	bFence := -1
	bDone := make(chan error, 1)
	go func() {
		bDone <- testMysqld.shutdownWithReplicaCrashSafety(t.Context(), 200*time.Millisecond, func() error {
			bFence = db.GetQueryCalledNum(setFlushLog)
			close(bEntered)
			return assert.AnError
		})
	}()
	assert.Never(t, func() bool {
		select {
		case <-bEntered:
			return true
		default:
		}
		return db.GetQueryCalledNum(readDurability) > 1 || db.GetQueryCalledNum(setFlushLog) > 1
	}, 2*time.Second, 50*time.Millisecond, "attempt B must not prepare or shut down while attempt A is still shutting down")

	// A fails; B then takes over A's pending restoration, re-fences, and shuts
	// down with the fence in place.
	close(aRelease)
	select {
	case err := <-aDone:
		require.ErrorIs(t, err, assert.AnError)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "attempt A did not return")
	}
	select {
	case err := <-bDone:
		require.ErrorIs(t, err, assert.AnError)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "attempt B did not return")
	}
	assert.Equal(t, 2, bFence, "attempt B must have re-applied the fence before its shutdown")
	assert.Equal(t, 1, db.GetQueryCalledNum(readDurability), "attempt B must inherit the state, not re-capture it")

	// The pending stop settles: B's restoration converges the replica.
	db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
		"source|Yes|No",
	))
	assert.Eventually(t, func() bool {
		return db.GetQueryCalledNum(startSQLThread) == 1
	}, 30*time.Second, 10*time.Millisecond, "the surviving restoration must converge the replica after the stop settles")
	testMysqld.Close()
}

// TestShutdownRetryTakesOverRestoreWithBlockedStop covers a shutdown retry
// while the previous attempt's interrupted stop is still blocked server-side
// and its restoration is waiting it out: the retry must take ownership --
// cancel the pending restoration, inherit its recorded state without
// re-capturing (the live state is half-restored, not the operator's), re-apply
// a fresh fence, and on failure arm its own restoration, which must converge
// the replica exactly once when the stop finally drains.
func TestShutdownRetryTakesOverRestoreWithBlockedStop(t *testing.T) {
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

	// The first attempt's applier stop drains until released; its restoration
	// stays pending across the retry.
	release := make(chan struct{})
	db.SetBeforeFunc(stopSQLThread, func() {
		<-release
		db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
			"source|No|No",
		))
	})
	db.SetBeforeFunc(startReplication, func() {
		db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
			"source|Yes|Yes",
		))
	})

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)

	err := testMysqld.shutdownWithReplicaCrashSafety(t.Context(), 100*time.Millisecond, func() error {
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)

	// The retry takes over the pending restoration: it must not re-capture
	// state (that would read the half-restored state), but it must re-apply a
	// fresh fence.
	err = testMysqld.shutdownWithReplicaCrashSafety(t.Context(), 100*time.Millisecond, func() error {
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, 1, db.GetQueryCalledNum(readDurability), "the retry must not re-capture state while a restoration is pending")
	assert.Equal(t, 2, db.GetQueryCalledNum(setFlushLog), "the retry must re-apply a fresh fence")

	// The drain settles: the retry's restoration converges the replica back to
	// its true prior state, restarting replication exactly once (the taken-over
	// restoration was cancelled before it could). Both owners reset the
	// durability settings -- the first before the takeover, the retry's after
	// -- so those run twice.
	close(release)
	assert.Eventually(t, func() bool {
		return db.GetQueryCalledNum(restoreFlushLog) == 2 &&
			db.GetQueryCalledNum(startReplication) == 1
	}, 30*time.Second, 10*time.Millisecond, "the retry's restoration must converge the replica exactly once")
	testMysqld.Close()
}

// TestShutdownRetryRefencesDuringPendingRestore covers the critical retry
// race: the previous attempt's restoration has already reset the durability
// settings and is waiting out an interrupted stop when the retry arrives. The
// retry must not shut down unfenced -- it must take over the restoration,
// inherit its state, and re-apply a fresh fence before invoking the shutdown.
func TestShutdownRetryRefencesDuringPendingRestore(t *testing.T) {
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
		startSQLThread      = "START REPLICA SQL_THREAD"
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
		stopIOThread, restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog, startReplication, startSQLThread,
	} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	// The applier stop hits rpl_stop_replica_timeout: the stop stays pending,
	// so the restoration resets the durability settings and then waits it out.
	db.AddRejectedQuery(stopSQLThread, sqlerror.NewSQLError(sqlerror.ERStopReplicaSQLThreadTimeout, sqlerror.SSUnknownSQLState, "STOP REPLICA SQL_THREAD timed out"))
	db.AddQueryPattern("kill .*", &sqltypes.Result{})
	db.SetBeforeFunc(startSQLThread, func() {
		db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
			"source|Yes|Yes",
		))
	})

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)

	err := testMysqld.shutdownWithReplicaCrashSafety(t.Context(), 200*time.Millisecond, func() error {
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)

	// The restoration resets the durability settings, then waits out the
	// pending stop.
	assert.Eventually(t, func() bool {
		return db.GetQueryCalledNum(restoreFlushLog) == 1
	}, 30*time.Second, 10*time.Millisecond, "the first restoration did not reset the durability settings")

	// The retry must re-apply a fresh fence before invoking the shutdown: the
	// previous fence is gone.
	fenceAtShutdown := -1
	err = testMysqld.shutdownWithReplicaCrashSafety(t.Context(), 200*time.Millisecond, func() error {
		fenceAtShutdown = db.GetQueryCalledNum(setFlushLog)
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, 2, fenceAtShutdown, "the retry must re-apply the fence before shutting down")
	assert.Equal(t, 1, db.GetQueryCalledNum(readDurability), "the retry must inherit the state, not re-capture it")

	// The pending stop finally settles: the retry's restoration restarts the
	// applier and converges to the true prior state.
	db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
		"source|Yes|No",
	))
	assert.Eventually(t, func() bool {
		return db.GetQueryCalledNum(startSQLThread) == 1 &&
			db.GetQueryCalledNum(restoreFlushLog) == 2
	}, 30*time.Second, 10*time.Millisecond, "the retry's restoration must converge the replica after the stop settles")
	assert.Zero(t, db.GetQueryCalledNum(startReplication))
	testMysqld.Close()
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

// TestPreparationSkipsStopsAfterDeadline covers a preparation whose deadline
// is consumed by the receiver stop: the applier stop must then be skipped
// rather than dispatched, and above all must not be marked interrupted --
// that would make the restoration (and mysqlctl's deferred Close with it)
// wait for an applier stop that was never sent.
func TestPreparationSkipsStopsAfterDeadline(t *testing.T) {
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

	db := fakesqldb.New(t)
	defer db.Close()
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
	for _, query := range []string{setFlushLog, setSyncBinlog, setSyncRelayLog, flushEngineLogs, flushBinaryLogs, flushRelayLogs, stopIOThread, stopSQLThread} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	db.AddQueryPattern("kill .*", &sqltypes.Result{})

	// The receiver stop consumes the entire preparation budget.
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	db.SetBeforeFunc(stopIOThread, func() { <-ctx.Done() })

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	state, err := testMysqld.prepareReplicaForShutdown(ctx, nil, func(*replicaShutdownState) {})
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Zero(t, db.GetQueryCalledNum(stopSQLThread), "the applier stop must not be dispatched after the deadline")
	assert.False(t, state.applierStopInterrupted, "an applier stop that was never dispatched must not be marked interrupted")
	assert.False(t, state.receiverStopInterrupted, "the receiver stop completed, late but without error")
}

// TestStopInterrupted pins the classification of stop failures: only outcomes
// that may leave the server-side stop pending -- a client-side cut
// (context errors) or the server reporting rpl_stop_replica_timeout -- count
// as interrupted; definitive server errors do not.
func TestStopInterrupted(t *testing.T) {
	// Pin the numeric error codes so a wrong constant cannot hide behind
	// self-consistent tests (ER_STOP_REPLICA_SQL_THREAD_TIMEOUT,
	// ER_STOP_REPLICA_IO_THREAD_TIMEOUT, ER_STOP_REPLICA_MONITOR_IO_THREAD_TIMEOUT).
	assert.EqualValues(t, 1875, sqlerror.ERStopReplicaSQLThreadTimeout)
	assert.EqualValues(t, 1876, sqlerror.ERStopReplicaIOThreadTimeout)
	assert.EqualValues(t, 4011, sqlerror.ERStopReplicaMonitorIOThreadTimeout)

	assert.True(t, stopInterrupted(context.DeadlineExceeded))
	assert.True(t, stopInterrupted(fmt.Errorf("wrapped: %w", context.Canceled)))
	assert.True(t, stopInterrupted(sqlerror.NewSQLError(sqlerror.ERStopReplicaIOThreadTimeout, sqlerror.SSUnknownSQLState, "timed out")))
	assert.True(t, stopInterrupted(sqlerror.NewSQLError(sqlerror.ERStopReplicaSQLThreadTimeout, sqlerror.SSUnknownSQLState, "timed out")))
	assert.True(t, stopInterrupted(sqlerror.NewSQLError(sqlerror.CRServerLost, sqlerror.SSUnknownSQLState, "lost connection during query")))
	assert.False(t, stopInterrupted(assert.AnError))
	assert.False(t, stopInterrupted(sqlerror.NewSQLError(sqlerror.CRServerGone, sqlerror.SSUnknownSQLState, "server has gone away")))
	assert.False(t, stopInterrupted(sqlerror.NewSQLError(sqlerror.ERStopReplicaMonitorIOThreadTimeout, sqlerror.SSUnknownSQLState, "monitor stop timed out")))
	assert.False(t, stopInterrupted(sqlerror.NewSQLError(sqlerror.ERUnknownSystemVariable, sqlerror.SSUnknownSQLState, "unknown variable")))
}

// TestPreparationTreatsLostStopResponseAsInterrupted covers losing the
// connection after a stop was accepted: the client returns CRServerLost when
// the COM_QUERY was written but the response read failed, so the server may
// still be completing the stop. The preparation must mark it interrupted so a
// restoration waits for the possibly-landing stop to settle.
func TestPreparationTreatsLostStopResponseAsInterrupted(t *testing.T) {
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

	db := fakesqldb.New(t)
	defer db.Close()
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
	for _, query := range []string{setFlushLog, setSyncBinlog, setSyncRelayLog, flushEngineLogs, flushBinaryLogs, flushRelayLogs, stopIOThread, stopSQLThread} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	db.AddQueryPattern("kill .*", &sqltypes.Result{})

	// The server accepts the applier stop but the connection is lost before
	// the response is read.
	db.SetBeforeFunc(stopSQLThread, func() {
		db.CloseAllConnections()
	})

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	state, err := testMysqld.prepareReplicaForShutdown(t.Context(), nil, func(*replicaShutdownState) {})
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.True(t, state.applierStopInterrupted, "a stop whose response was lost may still land and must be treated as interrupted")
	assert.False(t, state.receiverStopInterrupted, "the receiver stop completed normally")
}

// TestRestoreReconcilesInterruptedStop covers a STOP REPLICA that was
// interrupted (e.g. killed at the preparation deadline, or one that hit
// rpl_stop_replica_timeout) and is still draining when the restoration runs: a
// single START issued while the thread is still draining would be a no-op and
// the stop landing afterwards would leave replication stopped, so the
// restoration must wait for the interrupted stop to settle and only then
// restart the thread.
func TestRestoreReconcilesInterruptedStop(t *testing.T) {
	const (
		showReplicaStatus   = "SHOW REPLICA STATUS"
		restoreFlushLog     = "SET GLOBAL innodb_flush_log_at_trx_commit = 2"
		restoreSyncBinlog   = "SET GLOBAL sync_binlog = 0"
		restoreSyncRelayLog = "SET GLOBAL sync_relay_log = 10000"
		startReplication    = "START REPLICA"
		startSQLThread      = "START REPLICA SQL_THREAD"
	)

	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	// The receiver is already running; the applier still reports running
	// because the interrupted stop is draining.
	db.AddQuery(showReplicaStatus, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
		"source|Yes|Yes",
	))
	for _, query := range []string{restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog, startReplication, startSQLThread} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	// A successful applier start is observed as the status flipping back to
	// fully running.
	db.SetBeforeFunc(startSQLThread, func() {
		db.AddQuery(showReplicaStatus, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
			"source|Yes|Yes",
		))
	})

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	state := &replicaShutdownState{
		startReceiver:          true,
		startApplier:           true,
		flushLogAtTrxCommit:    "2",
		syncBinlog:             "0",
		syncRelayLog:           "10000",
		applierStopInterrupted: true,
	}
	restored := make(chan struct{})
	go func() {
		defer close(restored)
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		testMysqld.restoreReplicaAfterFailedShutdown(ctx, state, 10*time.Millisecond, 30*time.Second)
	}()

	// While the interrupted stop is still draining, the restoration must wait
	// -- it must not fire a START that would be a no-op.
	assert.Eventually(t, func() bool {
		return db.GetQueryCalledNum(showReplicaStatus) >= 3
	}, 30*time.Second, 10*time.Millisecond, "the restoration did not poll the replication status")
	assert.Zero(t, db.GetQueryCalledNum(startReplication), "no start may be issued while the interrupted stop is draining")
	assert.Zero(t, db.GetQueryCalledNum(startSQLThread), "no start may be issued while the interrupted stop is draining")

	// The stop settles: the applier reports stopped, and the restoration must
	// now restart it.
	db.AddQuery(showReplicaStatus, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
		"source|Yes|No",
	))
	select {
	case <-restored:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the restoration did not complete after the interrupted stop settled")
	}
	assert.Equal(t, 1, db.GetQueryCalledNum(startSQLThread), "the applier must be restarted once the interrupted stop has settled")
	assert.Zero(t, db.GetQueryCalledNum(startReplication))
}

// TestRestoreCyclesReceiverAfterMonitorTimeout covers a receiver stop that
// failed because stopping the connection-failover monitor timed out
// (SOURCE_CONNECTION_AUTO_FAILOVER, MySQL error 4011): the receiver keeps
// running but its monitor may be left stopped, and SHOW REPLICA STATUS cannot
// observe the monitor. The restoration must cycle the receiver -- stop it,
// then start it -- so the restart brings the monitor back with it.
func TestRestoreCyclesReceiverAfterMonitorTimeout(t *testing.T) {
	const (
		showReplicaStatus   = "SHOW REPLICA STATUS"
		restoreFlushLog     = "SET GLOBAL innodb_flush_log_at_trx_commit = 2"
		restoreSyncBinlog   = "SET GLOBAL sync_binlog = 0"
		restoreSyncRelayLog = "SET GLOBAL sync_relay_log = 10000"
		stopIOThread        = "STOP REPLICA IO_THREAD"
		startReplication    = "START REPLICA"
		startIOThread       = "START REPLICA IO_THREAD"
	)

	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQuery("SELECT 1", &sqltypes.Result{})
	// Both threads report running; the monitor's state is not observable.
	db.AddQuery(showReplicaStatus, sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
		"source|Yes|Yes",
	))
	for _, query := range []string{restoreFlushLog, restoreSyncBinlog, restoreSyncRelayLog, stopIOThread, startReplication, startIOThread} {
		db.AddQuery(query, &sqltypes.Result{})
	}
	// The cycle's stop takes effect: the receiver reports stopped; its restart
	// then brings it (and its monitor) back.
	db.SetBeforeFunc(stopIOThread, func() {
		db.AddQuery(showReplicaStatus, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
			"source|No|Yes",
		))
	})
	db.SetBeforeFunc(startIOThread, func() {
		db.AddQuery(showReplicaStatus, sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("Source_Host|Replica_IO_Running|Replica_SQL_Running", "varchar|varchar|varchar"),
			"source|Yes|Yes",
		))
	})

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	state := &replicaShutdownState{
		startReceiver:       true,
		startApplier:        true,
		flushLogAtTrxCommit: "2",
		syncBinlog:          "0",
		syncRelayLog:        "10000",
		cycleReceiver:       true,
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	testMysqld.restoreReplicaAfterFailedShutdown(ctx, state, 10*time.Millisecond, 30*time.Second)

	assert.Equal(t, 1, db.GetQueryCalledNum(stopIOThread), "the receiver must be stopped to cycle it")
	assert.Equal(t, 1, db.GetQueryCalledNum(startIOThread), "the receiver must be restarted after the cycle stop takes effect")
	assert.Zero(t, db.GetQueryCalledNum(startReplication))
}

// TestKillConnectionBoundsItsIO covers killing a connection on a wedged
// server: the KILL execution itself must be bounded so it cannot hang its
// caller, and it must not depend on the DBA pool.
func TestKillConnectionBoundsItsIO(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	release := make(chan struct{})
	defer close(release)
	db.AddQueryPatternWithCallback("kill .*", &sqltypes.Result{}, func(string) { <-release })

	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
	testMysqld := NewMysqld(dbc)
	defer testMysqld.Close()

	done := make(chan error, 1)
	go func() {
		done <- testMysqld.killConnectionWithTimeout(42, 200*time.Millisecond)
	}()
	select {
	case err := <-done:
		require.Error(t, err, "a KILL that cannot complete must time out")
	case <-time.After(30 * time.Second):
		require.FailNow(t, "killConnection did not honor its timeout while the KILL was blocked")
	}
}

// TestDirectExecutorsUnblockWhenKillFails covers a server that blocks the
// original query and cannot service the KILL either: the direct context-aware
// executors must close the dedicated connection to unblock the in-flight call
// client-side, so cancellation stays bounded instead of waiting on the
// original query indefinitely.
func TestDirectExecutorsUnblockWhenKillFails(t *testing.T) {
	const blockedQuery = "SELECT SLEEP(1000)"

	newBlockedDB := func(t *testing.T, query string) (*fakesqldb.DB, chan struct{}, *Mysqld) {
		t.Helper()
		db := fakesqldb.New(t)
		t.Cleanup(db.Close)
		release := make(chan struct{})
		t.Cleanup(func() { close(release) })
		db.AddQuery(query, &sqltypes.Result{})
		db.SetBeforeFunc(query, func() { <-release })
		// The KILL is deliberately not registered: it fails immediately, as on
		// a server that cannot service it.

		params := db.ConnParams()
		cp := *params
		dbc := dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb")
		testMysqld := NewMysqld(dbc)
		t.Cleanup(testMysqld.Close)
		return db, release, testMysqld
	}

	t.Run("executeFetchDirectContext", func(t *testing.T) {
		_, _, testMysqld := newBlockedDB(t, blockedQuery)
		conn, err := testMysqld.GetDbaConnection(t.Context())
		require.NoError(t, err)
		defer conn.Close()

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := testMysqld.executeFetchDirectContext(ctx, conn, blockedQuery)
			done <- err
		}()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.DeadlineExceeded)
		case <-time.After(30 * time.Second):
			require.FailNow(t, "executeFetchDirectContext did not unblock after the failed KILL")
		}
	})

	t.Run("showReplicationStatusDirectContext", func(t *testing.T) {
		_, _, testMysqld := newBlockedDB(t, "SHOW REPLICA STATUS")
		conn, err := testMysqld.GetDbaConnection(t.Context())
		require.NoError(t, err)
		defer conn.Close()

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := testMysqld.showReplicationStatusDirectContext(ctx, conn)
			done <- err
		}()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.DeadlineExceeded)
		case <-time.After(30 * time.Second):
			require.FailNow(t, "showReplicationStatusDirectContext did not unblock after the failed KILL")
		}
	})
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
