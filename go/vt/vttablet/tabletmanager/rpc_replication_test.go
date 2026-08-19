/*
Copyright 2023 The Vitess Authors.

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

package tabletmanager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/semisyncmonitor"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func newTestReplicationTM(tablet *topodatapb.Tablet, mysqlDaemon mysqlctl.MysqlDaemon, ts *topo.Server) *TabletManager {
	waitForGrantsComplete := make(chan struct{})
	close(waitForGrantsComplete)

	return &TabletManager{
		actionSema:             semaphore.NewWeighted(1),
		TopoServer:             ts,
		MysqlDaemon:            mysqlDaemon,
		tabletAlias:            tablet.Alias,
		_waitForGrantsComplete: waitForGrantsComplete,
		tmState: &tmState{
			displayState: displayState{
				tablet: tablet,
			},
		},
	}
}

func recoverableReplicationInitError() error {
	return sqlerror.NewSQLError(sqlerror.ERMasterInfo, sqlerror.SSUnknownSQLState, "Could not initialize master info structure; more error messages can be found in the MySQL error log")
}

// TestWaitForGrantsToHaveApplied tests that waitForGrantsToHaveApplied only succeeds after waitForDBAGrants has been called.
func TestWaitForGrantsToHaveApplied(t *testing.T) {
	tm := &TabletManager{
		_waitForGrantsComplete: make(chan struct{}),
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := tm.waitForGrantsToHaveApplied(ctx)
	require.ErrorContains(t, err, "deadline exceeded")

	err = tm.waitForDBAGrants(nil, 0)
	require.NoError(t, err)

	secondContext, secondCancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer secondCancel()
	err = tm.waitForGrantsToHaveApplied(secondContext)
	require.NoError(t, err)
}

type (
	demotePrimaryStallQS struct {
		tabletserver.Controller
		qsWaitChan     chan any
		primaryStalled atomic.Bool
	}

	restartReplicationMysqlDaemon struct {
		*mysqlctl.FakeMysqlDaemon
		cancel                   context.CancelFunc
		postStopTimeoutRemaining chan time.Duration
		stopDelay                time.Duration
	}

	fullStatusMysqlDaemon struct {
		*mysqlctl.FakeMysqlDaemon
		status *replicationdatapb.FullStatus
		err    error
		calls  int
	}
)

func (d *demotePrimaryStallQS) SetDemotePrimaryStalled(val bool) {
	d.primaryStalled.Store(val)
}

func (d *demotePrimaryStallQS) IsServing() bool {
	<-d.qsWaitChan
	return false
}

func TestPrimaryStatusIncludesServerVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)

	err := tm.ChangeType(ctx, topodatapb.TabletType_PRIMARY, false)
	require.NoError(t, err)

	fakeMysqlDaemon := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	fakeMysqlDaemon.Version = "Ver 8.0.35"

	status, err := tm.PrimaryStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, "Ver 8.0.35", status.ServerVersion)
}

func TestReplicationStatusIncludesServerVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)

	fakeMysqlDaemon := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	fakeMysqlDaemon.Version = "Ver 8.0.35"

	status, err := tm.ReplicationStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, "Ver 8.0.35", status.ServerVersion)
}

// TestReadStatusSlowVersionLookup verifies the read RPCs polled fleet-wide by
// vtorc (ReplicationStatus, PrimaryStatus) bound the best-effort version lookup:
// a slow/stalled mysqld version query must not consume the caller's whole RPC
// deadline and fail the poll. The status is returned with an empty ServerVersion
// instead, degrading to position-only ordering.
func TestReadStatusSlowVersionLookup(t *testing.T) {
	tests := []struct {
		name string
		call func(ctx context.Context, tm *TabletManager) (string, error)
	}{
		{
			name: "ReplicationStatus",
			call: func(ctx context.Context, tm *TabletManager) (string, error) {
				status, err := tm.ReplicationStatus(ctx)
				if err != nil {
					return "", err
				}
				return status.ServerVersion, nil
			},
		},
		{
			name: "PrimaryStatus",
			call: func(ctx context.Context, tm *TabletManager) (string, error) {
				status, err := tm.PrimaryStatus(ctx)
				if err != nil {
					return "", err
				}
				return status.ServerVersion, nil
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeMysqlDaemon := newTestMysqlDaemon(t, 1)
			tm := newTestReplicationTM(newTestTablet(t, 100, "ks", "0", nil), fakeMysqlDaemon, nil)
			// A version lookup that never completes on its own, so the bounded helper
			// must cap it and fall back to "".
			tm.MysqlDaemon = &countingVersionDaemon{
				FakeMysqlDaemon: fakeMysqlDaemon,
				version:         "Ver 8.0.35",
				delay:           time.Hour,
			}

			const deadline = 30 * time.Second
			ctx, cancel := context.WithTimeout(t.Context(), deadline)
			defer cancel()

			start := time.Now()
			version, err := tc.call(ctx, tm)
			elapsed := time.Since(start)

			require.NoError(t, err, "a slow version lookup must not fail the status poll")
			require.Empty(t, version, "version degrades to empty when the lookup is bounded out")
			// Without the bound the lookup would run to the full 30s deadline; the
			// bounded helper caps it near maxVersionLookupBudget (2s). A generous 15s
			// upper bound keeps this CI-safe while still proving the bound applies.
			require.Less(t, elapsed, 15*time.Second, "the bounded lookup must not run to the caller's full deadline")
		})
	}
}

func TestDemotePrimaryIncludesServerVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)

	err := tm.ChangeType(ctx, topodatapb.TabletType_PRIMARY, false)
	require.NoError(t, err)

	fakeMysqlDaemon := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	fakeMysqlDaemon.Version = "Ver 8.0.35"
	fakeMysqlDaemon.DB().SetNeverFail(true)

	tm.SemiSyncMonitor.Open()

	status, err := tm.DemotePrimary(ctx, false)
	require.NoError(t, err)
	assert.Equal(t, "Ver 8.0.35", status.ServerVersion)
}

func (rmd *restartReplicationMysqlDaemon) StopReplication(context.Context, map[string]string) error {
	if rmd.cancel != nil {
		rmd.cancel()
	}
	if rmd.stopDelay > 0 {
		time.Sleep(rmd.stopDelay)
	}
	return nil
}

func (rmd *restartReplicationMysqlDaemon) SemiSyncExtensionLoaded(ctx context.Context) (mysql.SemiSyncType, error) {
	if err := ctx.Err(); err != nil {
		return mysql.SemiSyncTypeUnknown, err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return mysql.SemiSyncTypeUnknown, errors.New("post-STOP context must have a deadline")
	}
	if rmd.postStopTimeoutRemaining != nil {
		rmd.postStopTimeoutRemaining <- time.Until(deadline)
	}
	return mysql.SemiSyncTypeOff, nil
}

func (fmd *fullStatusMysqlDaemon) CollectFullStatusData(context.Context) (*replicationdatapb.FullStatus, error) {
	fmd.calls++
	return fmd.status, fmd.err
}

func TestFullStatusUsesCollectedData(t *testing.T) {
	fakeMysqlDaemon := newTestMysqlDaemon(t, 1)
	t.Cleanup(fakeMysqlDaemon.DB().Close)
	mysqlDaemon := &fullStatusMysqlDaemon{
		FakeMysqlDaemon: fakeMysqlDaemon,
		status: &replicationdatapb.FullStatus{
			ServerId:               42,
			ServerUuid:             "test-uuid",
			Version:                "8.0.35",
			VersionComment:         "MySQL Community Server - GPL",
			ReadOnly:               true,
			SuperReadOnly:          true,
			GtidMode:               "ON",
			BinlogFormat:           "ROW",
			BinlogRowImage:         "FULL",
			LogBinEnabled:          true,
			LogReplicaUpdates:      true,
			SemiSyncPrimaryEnabled: true,
			SemiSyncPrimaryStatus:  true,
		},
	}
	tablet := newTestTablet(t, 100, "ks", "0", nil)
	tm := newTestReplicationTM(tablet, mysqlDaemon, nil)
	tm.QueryServiceControl = tabletservermock.NewController()
	tm.SemiSyncMonitor = semisyncmonitor.CreateTestSemiSyncMonitor(fakeMysqlDaemon.DB(), exporter)

	status, err := tm.FullStatus(t.Context())
	require.NoError(t, err)
	require.NotNil(t, status)

	assert.Equal(t, 1, mysqlDaemon.calls)
	assert.Equal(t, uint32(42), status.ServerId)
	assert.Equal(t, "test-uuid", status.ServerUuid)
	assert.Equal(t, "8.0.35", status.Version)
	assert.Equal(t, "MySQL Community Server - GPL", status.VersionComment)
	assert.True(t, status.ReadOnly)
	assert.True(t, status.SuperReadOnly)
	assert.Equal(t, "ON", status.GtidMode)
	assert.Equal(t, "ROW", status.BinlogFormat)
	assert.Equal(t, "FULL", status.BinlogRowImage)
	assert.True(t, status.LogBinEnabled)
	assert.True(t, status.LogReplicaUpdates)
	assert.True(t, status.SemiSyncPrimaryEnabled)
	assert.True(t, status.SemiSyncPrimaryStatus)
	assert.Equal(t, topodatapb.TabletType_REPLICA, status.TabletType)
}

// TestFullStatusRejectsMissingCollectedData covers a collector that reports
// neither data nor an error. There is no second collection path to fall back
// to, so FullStatus must fail rather than report an empty status as the truth.
func TestFullStatusRejectsMissingCollectedData(t *testing.T) {
	fakeMysqlDaemon := newTestMysqlDaemon(t, 1)
	t.Cleanup(fakeMysqlDaemon.DB().Close)
	mysqlDaemon := &fullStatusMysqlDaemon{
		FakeMysqlDaemon: fakeMysqlDaemon,
	}
	tablet := newTestTablet(t, 100, "ks", "0", nil)
	tm := newTestReplicationTM(tablet, mysqlDaemon, nil)
	tm.QueryServiceControl = tabletservermock.NewController()

	status, err := tm.FullStatus(t.Context())

	require.ErrorContains(t, err, "returned no data")
	assert.Nil(t, status)
	assert.Equal(t, 1, mysqlDaemon.calls)
}

func TestFullStatusReturnsCollectorError(t *testing.T) {
	fakeMysqlDaemon := newTestMysqlDaemon(t, 1)
	t.Cleanup(fakeMysqlDaemon.DB().Close)
	mysqlDaemon := &fullStatusMysqlDaemon{
		FakeMysqlDaemon: fakeMysqlDaemon,
		err:             errors.New("collector failed"),
	}
	tablet := newTestTablet(t, 100, "ks", "0", nil)
	tm := newTestReplicationTM(tablet, mysqlDaemon, nil)
	tm.QueryServiceControl = tabletservermock.NewController()

	status, err := tm.FullStatus(t.Context())

	require.ErrorContains(t, err, "collector failed")
	assert.Nil(t, status)
	assert.Equal(t, 1, mysqlDaemon.calls)
}

// TestFullStatusCollectsEveryField pins each FullStatus field to the value its
// query answers with, so that a field the collector maps differently, drops or
// stops populating is caught.
func TestFullStatusCollectsEveryField(t *testing.T) {
	db := fakesqldb.New(t)
	t.Cleanup(db.Close)

	params := db.ConnParams()
	cp := *params
	mysqld := mysqlctl.NewMysqld(dbconfigs.NewTestDBConfigs(cp, cp, "fakesqldb"))
	t.Cleanup(mysqld.Close)

	db.AddQuery("SELECT 1", &sqltypes.Result{})

	// Answers for the collected path.
	db.AddQueryPattern(
		"SELECT @@global.server_id AS server_id,.*",
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("server_id|server_uuid|version|version_comment|read_only|super_read_only|gtid_mode|binlog_format|log_bin|log_replica_updates|binlog_row_image|gtid_purged|replica_net_timeout", "uint64|varchar|varchar|varchar|int64|int64|varchar|varchar|int64|int64|varchar|varchar|int64"),
			"42|test-uuid|8.0.35|MySQL Community Server - GPL|1|1|ON|ROW|1|1|FULL|8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5|9",
		),
	)
	db.AddQueryPattern(
		"SELECT variable_name, variable_value FROM performance_schema.global_variables WHERE variable_name IN .*",
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
			"rpl_semi_sync_source_enabled|ON",
			"rpl_semi_sync_replica_enabled|ON",
			"rpl_semi_sync_source_timeout|10000",
			"rpl_semi_sync_source_wait_for_replica_count|2",
		),
	)
	db.AddQueryPattern(
		"SELECT variable_name, variable_value FROM performance_schema.global_status WHERE variable_name IN .*",
		sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
			"Rpl_semi_sync_source_status|ON",
			"Rpl_semi_sync_replica_status|ON",
			"Rpl_semi_sync_source_clients|3",
		),
	)

	db.AddQuery("SHOW REPLICA STATUS", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Last_SQL_Error|Last_IO_Error", "varchar|varchar"),
		"|",
	))
	db.AddQuery("SHOW BINARY LOG STATUS", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("File|Position|Binlog_Do_DB|Binlog_Ignore_DB|Executed_Gtid_Set", "varchar|int64|varchar|varchar|varchar"),
		"binlog.000001|154|||8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-8",
	))
	db.AddQuery("SELECT @@global.gtid_purged", sqltypes.MakeTestResult(sqltypes.MakeTestFields("gtid_purged", "varchar"), "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5"))
	db.AddQuery("SELECT * FROM performance_schema.replication_connection_configuration", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("HEARTBEAT_INTERVAL", "float64"),
		"4.5",
	))

	tablet := newTestTablet(t, 100, "ks", "0", nil)
	tm := newTestReplicationTM(tablet, mysqld, nil)
	tm.QueryServiceControl = tabletservermock.NewController()
	tm.SemiSyncMonitor = semisyncmonitor.CreateTestSemiSyncMonitor(db, exporter)

	collected, err := tm.FullStatus(t.Context())
	require.NoError(t, err)
	require.NotNil(t, collected)
	// Every field below comes from a batched query, so a reintroduced per-field
	// read would show up here.
	assert.Zero(t, db.GetQueryCalledNum("select @@global.server_id"))

	assert.Equal(t, uint32(42), collected.ServerId)
	assert.Equal(t, "test-uuid", collected.ServerUuid)
	assert.Equal(t, "8.0.35", collected.Version)
	assert.Equal(t, "MySQL Community Server - GPL", collected.VersionComment)
	assert.True(t, collected.ReadOnly)
	assert.True(t, collected.SuperReadOnly)
	assert.Equal(t, "ON", collected.GtidMode)
	assert.Equal(t, "ROW", collected.BinlogFormat)
	assert.Equal(t, "FULL", collected.BinlogRowImage)
	assert.True(t, collected.LogBinEnabled)
	assert.True(t, collected.LogReplicaUpdates)
	assert.True(t, collected.SemiSyncPrimaryEnabled)
	assert.True(t, collected.SemiSyncReplicaEnabled)
	assert.True(t, collected.SemiSyncPrimaryStatus)
	assert.True(t, collected.SemiSyncReplicaStatus)
	assert.Equal(t, uint32(3), collected.SemiSyncPrimaryClients)
	assert.Equal(t, uint64(10000), collected.SemiSyncPrimaryTimeout)
	assert.Equal(t, uint32(2), collected.SemiSyncWaitForReplicaCount)
	assert.Equal(t, "MySQL56/8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1-5", collected.GtidPurged)
	require.NotNil(t, collected.ReplicationStatus)
	require.NotNil(t, collected.PrimaryStatus)
	assert.Equal(t, "test-uuid", collected.PrimaryStatus.ServerUuid)
	require.NotNil(t, collected.ReplicationConfiguration)
	assert.Equal(t, int32(9), collected.ReplicationConfiguration.ReplicaNetTimeout)
	assert.Equal(t, topodatapb.TabletType_REPLICA, collected.TabletType)
}

// TestDemotePrimaryStalled checks that if demote primary takes too long, then we mark it as stalled.
func TestDemotePrimaryStalled(t *testing.T) {
	// Set remote operation timeout to a very low value.
	origVal := topo.RemoteOperationTimeout
	topo.RemoteOperationTimeout = 100 * time.Millisecond
	defer func() {
		topo.RemoteOperationTimeout = origVal
	}()

	// Create a fake query service control to intercept calls from DemotePrimary function.
	qsc := &demotePrimaryStallQS{
		qsWaitChan: make(chan any),
	}
	// Create a tablet manager with a replica type tablet.
	fakeDb := newTestMysqlDaemon(t, 1)
	tm := &TabletManager{
		actionSema:  semaphore.NewWeighted(1),
		MysqlDaemon: fakeDb,
		tmState: &tmState{
			displayState: displayState{
				tablet: newTestTablet(t, 100, "ks", "-", map[string]string{}),
			},
		},
		QueryServiceControl: qsc,
		SemiSyncMonitor:     semisyncmonitor.CreateTestSemiSyncMonitor(fakeDb.DB(), exporter),
	}

	go func() {
		tm.demotePrimary(t.Context(), false /* revertPartialFailure */, false /* force */)
	}()
	// We make IsServing stall by making it wait on a channel.
	// This should cause the demote primary operation to be stalled.
	require.Eventually(t, func() bool {
		return qsc.primaryStalled.Load()
	}, 5*time.Second, 100*time.Millisecond)

	// Unblock the DemotePrimary call by closing the channel.
	close(qsc.qsWaitChan)

	// Eventually demote primary will succeed, and we want the stalled field to be cleared.
	require.Eventually(t, func() bool {
		return !qsc.primaryStalled.Load()
	}, 5*time.Second, 100*time.Millisecond)
}

// TestDemotePrimaryLockWaitTimeout checks that a demotion enables super_read_only
// with a 1 second lock_wait_timeout, so that it fails fast instead of stalling
// behind metadata locks held by in-flight queries.
func TestDemotePrimaryLockWaitTimeout(t *testing.T) {
	old := demotePrimaryLockWaitTimeout
	demotePrimaryLockWaitTimeout = time.Second
	t.Cleanup(func() { demotePrimaryLockWaitTimeout = old })

	fakeDb := newTestMysqlDaemon(t, 1)
	tm := &TabletManager{
		actionSema:  semaphore.NewWeighted(1),
		MysqlDaemon: fakeDb,
		tmState: &tmState{
			displayState: displayState{
				tablet: newTestTablet(t, 100, "ks", "-", map[string]string{}),
			},
		},
		QueryServiceControl: tabletservermock.NewController(),
		SemiSyncMonitor:     semisyncmonitor.CreateTestSemiSyncMonitor(fakeDb.DB(), exporter),
	}

	_, err := tm.demotePrimary(t.Context(), false /* revertPartialFailure */, false /* force */)
	require.NoError(t, err)

	assert.True(t, fakeDb.SuperReadOnly.Load(), "demotePrimary must enable super_read_only")
	assert.Equal(t, time.Second, fakeDb.SetSuperReadOnlyLockWaitTimeout, "demotePrimary must enable super_read_only with a 1s lock_wait_timeout")
}

// TestDemotePrimaryLockWaitTimeoutDisabledByDefault checks that a demotion does not pass a
// lock_wait_timeout bound when demotePrimaryLockWaitTimeout is left at its zero-value default.
func TestDemotePrimaryLockWaitTimeoutDisabledByDefault(t *testing.T) {
	require.Zero(t, demotePrimaryLockWaitTimeout, "test requires the flag to be at its default value")

	fakeDb := newTestMysqlDaemon(t, 1)
	tm := &TabletManager{
		actionSema:  semaphore.NewWeighted(1),
		MysqlDaemon: fakeDb,
		tmState: &tmState{
			displayState: displayState{
				tablet: newTestTablet(t, 100, "ks", "-", map[string]string{}),
			},
		},
		QueryServiceControl: tabletservermock.NewController(),
		SemiSyncMonitor:     semisyncmonitor.CreateTestSemiSyncMonitor(fakeDb.DB(), exporter),
	}

	_, err := tm.demotePrimary(t.Context(), false /* revertPartialFailure */, false /* force */)
	require.NoError(t, err)

	assert.True(t, fakeDb.SuperReadOnly.Load(), "demotePrimary must enable super_read_only")
	assert.Zero(t, fakeDb.SetSuperReadOnlyLockWaitTimeout, "demotePrimary must not bound lock_wait_timeout when the flag is disabled")
}

// TestDemotePrimaryWaitingForSemiSyncUnblock tests that demote primary unblocks if the primary is blocked on semi-sync ACKs
// and doesn't issue the set super read-only query until all writes waiting on semi-sync ACKs have gone through.
func TestDemotePrimaryWaitingForSemiSyncUnblock(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	// Make the tablet a primary.
	err := tm.ChangeType(ctx, topodatapb.TabletType_PRIMARY, false)
	require.NoError(t, err)
	fakeMysqlDaemon := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	fakeDb := fakeMysqlDaemon.DB()
	fakeDb.SetNeverFail(true)

	tm.SemiSyncMonitor.Open()
	// Add a universal insert query pattern that would block until we make it unblock.
	// ExecuteFetchMulti will execute each statement separately, so we need to add SET query.
	fakeDb.AddQueryPattern("SET SESSION lock_wait_timeout=.*", &sqltypes.Result{})
	ch := make(chan int)
	fakeDb.AddQueryPatternWithCallback("^INSERT INTO.*", sqltypes.MakeTestResult(nil), func(s string) {
		<-ch
	})
	// Add a fake query that makes the semi-sync monitor believe that the tablet is blocked on semi-sync ACKs.
	fakeDb.AddQuery("SELECT /*+ MAX_EXECUTION_TIME(500) */ variable_name, variable_value FROM performance_schema.global_status WHERE REGEXP_LIKE(variable_name, 'Rpl_semi_sync_(source|master)_(wait_sessions|yes_tx)')", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
		"Rpl_semi_sync_source_wait_sessions|1",
		"Rpl_semi_sync_source_yes_tx|5",
	))

	// Verify that in the beginning the tablet is serving.
	require.True(t, tm.QueryServiceControl.IsServing())

	// Start the demote primary operation in a go routine.
	var demotePrimaryFinished atomic.Bool
	go func() {
		_, err := tm.demotePrimary(ctx, false /* revertPartialFailure */, false /* force */)
		if !assert.NoError(t, err) {
			return
		}
		demotePrimaryFinished.Store(true)
	}()

	// Wait for the demote primary operation to have changed the serving state.
	// After that point, we can assume that the demote primary gets blocked on writes waiting for semi-sync ACKs.
	require.Eventually(t, func() bool {
		return !tm.QueryServiceControl.IsServing()
	}, 5*time.Second, 100*time.Millisecond)

	// DemotePrimary shouldn't have finished yet.
	require.False(t, demotePrimaryFinished.Load())
	// We shouldn't have seen the super-read only query either.
	require.False(t, fakeMysqlDaemon.SuperReadOnly.Load())

	// Now we unblock the semi-sync monitor.
	fakeDb.AddQuery("SELECT /*+ MAX_EXECUTION_TIME(1000) */ variable_name, variable_value FROM performance_schema.global_status WHERE REGEXP_LIKE(variable_name, 'Rpl_semi_sync_(source|master)_(wait_sessions|yes_tx)')", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
		"Rpl_semi_sync_source_wait_sessions|0",
		"Rpl_semi_sync_source_yes_tx|5",
	))
	close(ch)

	// This should unblock the demote primary operation eventually.
	require.Eventually(t, func() bool {
		return demotePrimaryFinished.Load()
	}, 5*time.Second, 100*time.Millisecond)
	// We should have also seen the super-read only query.
	require.True(t, fakeMysqlDaemon.SuperReadOnly.Load())
}

// TestDemotePrimaryWithSemiSyncProgressDetection tests that demote primary proceeds
// without blocking when transactions are making progress (ackedTrxs increasing between checks).
func TestDemotePrimaryWithSemiSyncProgressDetection(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	// Make the tablet a primary.
	err := tm.ChangeType(ctx, topodatapb.TabletType_PRIMARY, false)
	require.NoError(t, err)
	fakeMysqlDaemon := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	fakeDb := fakeMysqlDaemon.DB()
	fakeDb.SetNeverFail(true)

	tm.SemiSyncMonitor.Open()

	// Set up the query to show waiting sessions, but with progress (ackedTrxs increasing).
	// The monitor makes TWO calls to getSemiSyncStats with a sleep between them.
	// We add the query result multiple times. The fakesqldb will return them in order (FIFO).
	// First few calls: waiting sessions present, ackedTrxs=5.
	for range 3 {
		fakeDb.AddQuery("SELECT /*+ MAX_EXECUTION_TIME(1000) */ variable_name, variable_value FROM performance_schema.global_status WHERE REGEXP_LIKE(variable_name, 'Rpl_semi_sync_(source|master)_(wait_sessions|yes_tx)')", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
			"Rpl_semi_sync_source_wait_sessions|1",
			"Rpl_semi_sync_source_yes_tx|5",
		))
	}
	// Next calls: waiting sessions present, but ackedTrxs=6 (progress!).
	for range 10 {
		fakeDb.AddQuery("SELECT /*+ MAX_EXECUTION_TIME(1000) */ variable_name, variable_value FROM performance_schema.global_status WHERE REGEXP_LIKE(variable_name, 'Rpl_semi_sync_(source|master)_(wait_sessions|yes_tx)')", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
			"Rpl_semi_sync_source_wait_sessions|1",
			"Rpl_semi_sync_source_yes_tx|6",
		))
	}

	// Verify that in the beginning the tablet is serving.
	require.True(t, tm.QueryServiceControl.IsServing())

	// Start the demote primary operation in a go routine.
	var demotePrimaryFinished atomic.Bool
	go func() {
		_, err := tm.demotePrimary(ctx, false /* revertPartialFailure */, false /* force */)
		if !assert.NoError(t, err) {
			return
		}
		demotePrimaryFinished.Store(true)
	}()

	// Wait for the demote primary operation to have changed the serving state.
	require.Eventually(t, func() bool {
		return !tm.QueryServiceControl.IsServing()
	}, 5*time.Second, 100*time.Millisecond)

	// DemotePrimary should finish quickly because progress is being made.
	// It should NOT wait for semi-sync to unblock since ackedTrxs is increasing.
	require.Eventually(t, func() bool {
		return demotePrimaryFinished.Load()
	}, 5*time.Second, 100*time.Millisecond)

	// We should have seen the super-read only query.
	require.True(t, fakeMysqlDaemon.SuperReadOnly.Load())
}

// TestDemotePrimaryWhenSemiSyncBecomesUnblockedBetweenChecks tests that demote primary
// proceeds immediately when waiting sessions drops to 0 between the two checks.
func TestDemotePrimaryWhenSemiSyncBecomesUnblockedBetweenChecks(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	// Make the tablet a primary.
	err := tm.ChangeType(ctx, topodatapb.TabletType_PRIMARY, false)
	require.NoError(t, err)
	fakeMysqlDaemon := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	fakeDb := fakeMysqlDaemon.DB()
	fakeDb.SetNeverFail(true)

	tm.SemiSyncMonitor.Open()

	// Set up the query to show waiting sessions on first call, but 0 on second call.
	// This simulates the semi-sync becoming unblocked between the two checks.
	// The fakesqldb returns results in FIFO order.
	// First call: waiting sessions present.
	fakeDb.AddQuery("SELECT /*+ MAX_EXECUTION_TIME(1000) */ variable_name, variable_value FROM performance_schema.global_status WHERE REGEXP_LIKE(variable_name, 'Rpl_semi_sync_(source|master)_(wait_sessions|yes_tx)')", sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
		"Rpl_semi_sync_source_wait_sessions|2",
		"Rpl_semi_sync_source_yes_tx|5",
	))
	// Second and subsequent calls: no waiting sessions (unblocked!).
	for range 10 {
		fakeDb.AddQuery("SELECT /*+ MAX_EXECUTION_TIME(1000) */ variable_name, variable_value FROM performance_schema.global_status WHERE REGEXP_LIKE(variable_name, 'Rpl_semi_sync_(source|master)_(wait_sessions|yes_tx)')", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
			"Rpl_semi_sync_source_wait_sessions|0",
			"Rpl_semi_sync_source_yes_tx|5",
		))
	}

	// Verify that in the beginning the tablet is serving.
	require.True(t, tm.QueryServiceControl.IsServing())

	// Start the demote primary operation in a go routine.
	var demotePrimaryFinished atomic.Bool
	go func() {
		_, err := tm.demotePrimary(ctx, false /* revertPartialFailure */, false /* force */)
		if !assert.NoError(t, err) {
			return
		}
		demotePrimaryFinished.Store(true)
	}()

	// Wait for the demote primary operation to have changed the serving state.
	require.Eventually(t, func() bool {
		return !tm.QueryServiceControl.IsServing()
	}, 5*time.Second, 100*time.Millisecond)

	// DemotePrimary should finish quickly because semi-sync became unblocked.
	require.Eventually(t, func() bool {
		return demotePrimaryFinished.Load()
	}, 5*time.Second, 100*time.Millisecond)

	// We should have seen the super-read only query.
	require.True(t, fakeMysqlDaemon.SuperReadOnly.Load())
}

// TestUndoDemotePrimaryStateChange tests that UndoDemotePrimary
// if able to change the state of the tablet to Primary if there
// is a mismatch with the tablet record.
func TestUndoDemotePrimaryStateChange(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	ti, err := ts.UpdateTabletFields(ctx, tm.Tablet().Alias, func(tablet *topodatapb.Tablet) error {
		tablet.Type = topodatapb.TabletType_PRIMARY
		tablet.PrimaryTermStartTime = protoutil.TimeToProto(time.Now())
		return nil
	})
	require.NoError(t, err)

	// Check that the tablet is initially a replica.
	require.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
	// Verify that the tablet record says the tablet should be a primary.
	require.Equal(t, topodatapb.TabletType_PRIMARY, ti.Type)

	err = tm.UndoDemotePrimary(ctx, false)
	require.NoError(t, err)
	require.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	require.Equal(t, ti.PrimaryTermStartTime, tm.Tablet().PrimaryTermStartTime)
	require.True(t, tm.QueryServiceControl.IsServing())
	isReadOnly, err := tm.MysqlDaemon.IsReadOnly(ctx)
	require.NoError(t, err)
	require.False(t, isReadOnly)
}

func TestHandleRecoverableReplicationInitializationError(t *testing.T) {
	testCases := []struct {
		name          string
		inputErr      error
		shouldRestart bool
	}{
		{
			name:          "relay log info repository error",
			inputErr:      sqlerror.NewSQLError(sqlerror.ERReplicaRelayLogInfoInitRepository, sqlerror.SSUnknownSQLState, "Replica failed to initialize relay log info structure from the repository"),
			shouldRestart: true,
		},
		{
			name:          "master info error",
			inputErr:      sqlerror.NewSQLError(sqlerror.ERMasterInfo, sqlerror.SSUnknownSQLState, "Could not initialize master info structure; more error messages can be found in the MySQL error log"),
			shouldRestart: true,
		},
		{
			name:          "connection metadata repository error",
			inputErr:      sqlerror.NewSQLError(sqlerror.ERReplicaConnectionMetadataInitRepository, sqlerror.SSUnknownSQLState, "Replica failed to initialize connection metadata structure from the repository"),
			shouldRestart: true,
		},
		{
			name:          "applier metadata message with wrong errno",
			inputErr:      sqlerror.NewSQLError(sqlerror.ERUnknownError, sqlerror.SSUnknownSQLState, "Replica failed to initialize applier metadata structure from the repository"),
			shouldRestart: false,
		},
		{
			name:          "mysqlctl wrapped master info error",
			inputErr:      errors.New("ExecuteFetch(START REPLICA) failed: Could not initialize master info structure; more error messages can be found in the MySQL error log (errno 1201) (sqlstate HY000)"),
			shouldRestart: true,
		},
		{
			name:          "native mysql master info error",
			inputErr:      errors.New("ERROR 1201 (HY000): Could not initialize master info structure; more error messages can be found in the MySQL error log"),
			shouldRestart: true,
		},
		{
			name:          "unrelated error",
			inputErr:      errors.New("unexpected replication failure"),
			shouldRestart: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fakeMysqlDaemon := newTestMysqlDaemon(t, 1)
			if tc.shouldRestart {
				fakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
					"STOP REPLICA",
					"RESET REPLICA",
					"START REPLICA",
				}
			}

			tablet := newTestTablet(t, 100, "ks", "0", nil)
			tm := &TabletManager{
				MysqlDaemon: fakeMysqlDaemon,
				tabletAlias: tablet.Alias,
				tmState: &tmState{
					displayState: displayState{
						tablet: tablet,
					},
				},
			}

			err := tm.handleRecoverableReplicationInitError(t.Context(), tc.inputErr)
			if tc.shouldRestart {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.inputErr)
			}

			require.NoError(t, fakeMysqlDaemon.CheckSuperQueryList())
		})
	}
}

// TestStartReplicationRecoversFromRecoverableReplicationInitError verifies StartReplication self-heals recoverable init failures.
func TestStartReplicationRecoversFromRecoverableReplicationInitError(t *testing.T) {
	fakeMysqlDaemon := newTestMysqlDaemon(t, 1)
	fakeMysqlDaemon.StartReplicationError = recoverableReplicationInitError()
	fakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP REPLICA",
		"RESET REPLICA",
		"START REPLICA",
	}

	tm := newTestReplicationTM(newTestTablet(t, 100, "ks", "0", nil), fakeMysqlDaemon, nil)
	err := tm.StartReplication(t.Context(), false)
	require.NoError(t, err)
	require.NoError(t, fakeMysqlDaemon.CheckSuperQueryList())
}

// TestRestartReplicationCompletesAfterContextCancellation verifies a successful
// stop is followed by a bounded start when the RPC context is canceled.
func TestRestartReplicationCompletesAfterContextCancellation(t *testing.T) {
	fakeMysqlDaemon := newTestMysqlDaemon(t, 1)
	fakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{"START REPLICA"}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	restartMysqlDaemon := &restartReplicationMysqlDaemon{
		FakeMysqlDaemon: fakeMysqlDaemon,
		cancel:          cancel,
	}
	tm := newTestReplicationTM(newTestTablet(t, 100, "ks", "0", nil), fakeMysqlDaemon, nil)
	tm.MysqlDaemon = restartMysqlDaemon

	err := tm.RestartReplication(ctx, false)
	require.NoError(t, err)
	require.NoError(t, fakeMysqlDaemon.CheckSuperQueryList())
	assert.True(t, fakeMysqlDaemon.Replicating, "replication must be started after STOP succeeds")
}

func TestRestartReplicationUsesCallerTimeoutAfterStop(t *testing.T) {
	fakeMysqlDaemon := newTestMysqlDaemon(t, 1)
	fakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{"START REPLICA"}
	ctx, cancel := context.WithTimeout(t.Context(), 2*topo.RemoteOperationTimeout)
	t.Cleanup(cancel)

	postStopTimeoutRemaining := make(chan time.Duration, 1)
	restartMysqlDaemon := &restartReplicationMysqlDaemon{
		FakeMysqlDaemon:          fakeMysqlDaemon,
		cancel:                   cancel,
		postStopTimeoutRemaining: postStopTimeoutRemaining,
	}
	tm := newTestReplicationTM(newTestTablet(t, 100, "ks", "0", nil), fakeMysqlDaemon, nil)
	tm.MysqlDaemon = restartMysqlDaemon

	err := tm.RestartReplication(ctx, false)
	require.NoError(t, err)
	require.NoError(t, fakeMysqlDaemon.CheckSuperQueryList())
	assert.LessOrEqual(t, <-postStopTimeoutRemaining, topo.RemoteOperationTimeout,
		"post-STOP budget must be capped at RemoteOperationTimeout")
}

// TestRestartReplicationDeductsPreStopTimeFromPostStopBudget verifies time spent
// before and during STOP is deducted from the post-STOP timeout budget.
func TestRestartReplicationDeductsPreStopTimeFromPostStopBudget(t *testing.T) {
	oldRemoteOpTimeout := topo.RemoteOperationTimeout
	topo.RemoteOperationTimeout = 30 * time.Second
	t.Cleanup(func() {
		topo.RemoteOperationTimeout = oldRemoteOpTimeout
	})

	fakeMysqlDaemon := newTestMysqlDaemon(t, 1)
	fakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{"START REPLICA"}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)

	postStopTimeoutRemaining := make(chan time.Duration, 1)
	restartMysqlDaemon := &restartReplicationMysqlDaemon{
		FakeMysqlDaemon:          fakeMysqlDaemon,
		postStopTimeoutRemaining: postStopTimeoutRemaining,
		stopDelay:                time.Second,
	}
	tm := newTestReplicationTM(newTestTablet(t, 100, "ks", "0", nil), fakeMysqlDaemon, nil)
	tm.MysqlDaemon = restartMysqlDaemon

	err := tm.RestartReplication(ctx, false)
	require.NoError(t, err)
	require.NoError(t, fakeMysqlDaemon.CheckSuperQueryList())

	remaining := <-postStopTimeoutRemaining
	assert.Greater(t, remaining, 8*time.Second)
	assert.Less(t, remaining, 9500*time.Millisecond, "time spent in STOP must be deducted from the post-STOP budget")
}

// TestRestartReplicationRecoversFromRecoverableReplicationInitializationError verifies RestartReplication self-heals recoverable init failures.
func TestRestartReplicationRecoversFromRecoverableReplicationInitializationError(t *testing.T) {
	fakeMysqlDaemon := newTestMysqlDaemon(t, 1)
	fakeMysqlDaemon.StartReplicationError = recoverableReplicationInitError()
	fakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP REPLICA",
		"STOP REPLICA",
		"RESET REPLICA",
		"START REPLICA",
	}

	tm := newTestReplicationTM(newTestTablet(t, 100, "ks", "0", nil), fakeMysqlDaemon, nil)
	err := tm.RestartReplication(t.Context(), false)
	require.NoError(t, err)
	require.NoError(t, fakeMysqlDaemon.CheckSuperQueryList())
}

// TestFixSemiSyncAndReplicationRecoversFromRecoverableReplicationInitializationError verifies semi-sync restart path self-heals recoverable init failures.
func TestFixSemiSyncAndReplicationRecoversFromRecoverableReplicationInitializationError(t *testing.T) {
	fakeMysqlDaemon := newTestMysqlDaemon(t, 1)
	fakeMysqlDaemon.Replicating = true
	fakeMysqlDaemon.StartReplicationError = recoverableReplicationInitError()
	fakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"STOP REPLICA",
		"STOP REPLICA",
		"RESET REPLICA",
		"START REPLICA",
	}

	tm := newTestReplicationTM(newTestTablet(t, 100, "ks", "0", nil), fakeMysqlDaemon, nil)
	err := tm.fixSemiSyncAndReplication(t.Context(), topodatapb.TabletType_REPLICA, SemiSyncActionUnset)
	require.NoError(t, err)
	require.NoError(t, fakeMysqlDaemon.CheckSuperQueryList())
}

func TestSetReplicationSourceRecovery(t *testing.T) {
	t.Run("InitReplica recovers from start replication error", func(t *testing.T) {
		ctx := t.Context()
		ts := memorytopo.NewServer(ctx, "cell1")

		// Create a shard with a primary that InitReplica will point to.
		_, err := ts.GetOrCreateShard(ctx, "ks", "0")
		require.NoError(t, err)

		parent := &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "cell1",
				Uid:  200,
			},
			Keyspace:      "ks",
			Shard:         "0",
			Type:          topodatapb.TabletType_PRIMARY,
			MysqlHostname: "mysql-primary",
			MysqlPort:     3306,
		}
		require.NoError(t, ts.CreateTablet(ctx, parent))

		fakeMysqlDaemon := newTestMysqlDaemon(t, 1)

		// Let the source change succeed, then fail the explicit START REPLICA so
		// the recovery path is exercised after the source is already configured.
		fakeMysqlDaemon.SetReplicationSourceInputs = []string{"mysql-primary:3306"}
		fakeMysqlDaemon.StartReplicationError = recoverableReplicationInitError()
		fakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
			"FAKE RESET BINARY LOGS AND GTIDS",
			"FAKE SET GLOBAL gtid_purged",
			"FAKE SET SOURCE",
			"STOP REPLICA",
			"RESET REPLICA",
			"START REPLICA",
		}

		tm := newTestReplicationTM(newTestTablet(t, 100, "ks", "0", nil), fakeMysqlDaemon, ts)

		// InitReplica should recover the start failure and still complete.
		err = tm.InitReplica(ctx, parent.Alias, "", 0, false)
		require.NoError(t, err)
		require.Equal(t, "mysql-primary", fakeMysqlDaemon.CurrentSourceHost)
		require.EqualValues(t, 3306, fakeMysqlDaemon.CurrentSourcePort)
		require.NoError(t, fakeMysqlDaemon.CheckSuperQueryList())
	})

	t.Run("SetReplicationSource recovers on source change for running replica", func(t *testing.T) {
		ctx := t.Context()
		ts := memorytopo.NewServer(ctx, "cell1")

		tablet := newTestTablet(t, 100, "ks", "0", nil)
		fakeMysqlDaemon := newTestMysqlDaemon(t, 1)

		// Start from a running replica that still points at the old primary.
		fakeMysqlDaemon.Replicating = true
		fakeMysqlDaemon.CurrentSourceHost = "mysql-old-primary"
		fakeMysqlDaemon.CurrentSourcePort = 3305
		fakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
			"STOP REPLICA",
			"STOP REPLICA",
			"FAKE RESET REPLICA ALL",
			"FAKE SET SOURCE",
			"START REPLICA",
		}

		setSourceCalls := 0

		// Fail the first source-change attempt after the internal STOP REPLICA.
		// The second attempt should succeed after recovery has cleared the broken
		// metadata and reapplied the requested source.
		fakeMysqlDaemon.SetReplicationSourceFunc = func(ctx context.Context, host string, port int32, heartbeatInterval float64, stopReplicationBefore bool, startReplicationAfter bool) error {
			setSourceCalls++

			require.Equal(t, "mysql-new-primary", host)
			require.EqualValues(t, 3306, port)
			require.Zero(t, heartbeatInterval)
			require.False(t, startReplicationAfter)

			if setSourceCalls == 1 {
				require.True(t, stopReplicationBefore)
				require.NoError(t, fakeMysqlDaemon.ExecuteSuperQueryList(ctx, []string{"STOP REPLICA"}))
				return recoverableReplicationInitError()
			}

			if setSourceCalls == 2 {
				require.False(t, stopReplicationBefore)
				require.NoError(t, fakeMysqlDaemon.ExecuteSuperQueryList(ctx, []string{"FAKE SET SOURCE"}))

				fakeMysqlDaemon.CurrentSourceHost = host
				fakeMysqlDaemon.CurrentSourcePort = port

				return nil
			}

			return fmt.Errorf("unexpected SetReplicationSource call %d", setSourceCalls)
		}

		tm := &TabletManager{
			actionSema:             semaphore.NewWeighted(1),
			BatchCtx:               ctx,
			TopoServer:             ts,
			MysqlDaemon:            fakeMysqlDaemon,
			tmc:                    newFakeTMClient(),
			tabletAlias:            tablet.Alias,
			_waitForGrantsComplete: make(chan struct{}),
			tmState: &tmState{
				displayState: displayState{
					tablet: tablet,
				},
			},
		}
		close(tm._waitForGrantsComplete)

		// Register both the replica and the new primary in topo.
		_, err := ts.GetOrCreateShard(ctx, "ks", "0")
		require.NoError(t, err)
		require.NoError(t, ts.CreateTablet(ctx, tablet))

		parent := &topodatapb.Tablet{
			Alias: &topodatapb.TabletAlias{
				Cell: "cell1",
				Uid:  200,
			},
			Keyspace:      "ks",
			Shard:         "0",
			Type:          topodatapb.TabletType_PRIMARY,
			MysqlHostname: "mysql-new-primary",
			MysqlPort:     3306,
		}
		require.NoError(t, ts.CreateTablet(ctx, parent))

		// SetReplicationSource should recover the source-change error, then
		// leave the replica configured for the new primary.
		err = tm.SetReplicationSource(ctx, parent.Alias, 0, "", false, false, 0)
		require.NoError(t, err)

		require.Equal(t, 2, setSourceCalls)
		require.Equal(t, "mysql-new-primary", fakeMysqlDaemon.CurrentSourceHost)
		require.EqualValues(t, 3306, fakeMysqlDaemon.CurrentSourcePort)
		require.NoError(t, fakeMysqlDaemon.CheckSuperQueryList())
	})

	t.Run("non-running replica reapplies source after recoverable source error", func(t *testing.T) {
		fakeMysqlDaemon := newTestMysqlDaemon(t, 1)
		fakeMysqlDaemon.CurrentSourceHost = "mysql-old-primary"
		fakeMysqlDaemon.CurrentSourcePort = 3305
		fakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
			"FAKE RESET REPLICA ALL",
			"FAKE SET SOURCE",
		}

		setSourceCalls := 0

		// When replication was not running, recovery should clear any stale source
		// settings and reapply the requested source without starting replication.
		fakeMysqlDaemon.SetReplicationSourceFunc = func(ctx context.Context, host string, port int32, heartbeatInterval float64, stopReplicationBefore bool, startReplicationAfter bool) error {
			setSourceCalls++

			require.Equal(t, "mysql-new-primary", host)
			require.EqualValues(t, 3306, port)
			require.False(t, stopReplicationBefore)
			require.False(t, startReplicationAfter)

			if setSourceCalls == 1 {
				return recoverableReplicationInitError()
			}

			if setSourceCalls == 2 {
				require.NoError(t, fakeMysqlDaemon.ExecuteSuperQueryList(ctx, []string{"FAKE SET SOURCE"}))

				fakeMysqlDaemon.CurrentSourceHost = host
				fakeMysqlDaemon.CurrentSourcePort = port

				return nil
			}

			return fmt.Errorf("unexpected SetReplicationSource call %d", setSourceCalls)
		}

		tm := newTestReplicationTM(newTestTablet(t, 100, "ks", "0", nil), fakeMysqlDaemon, nil)

		err := tm.setReplicationSourceRecoverable(t.Context(), "mysql-new-primary", 3306, 0, false, false)
		require.NoError(t, err)
		require.Equal(t, 2, setSourceCalls)
		require.Equal(t, "mysql-new-primary", fakeMysqlDaemon.CurrentSourceHost)
		require.EqualValues(t, 3306, fakeMysqlDaemon.CurrentSourcePort)
		require.NoError(t, fakeMysqlDaemon.CheckSuperQueryList())
	})

	t.Run("non-running replica with start requested reapplies source and starts replication", func(t *testing.T) {
		fakeMysqlDaemon := newTestMysqlDaemon(t, 1)
		fakeMysqlDaemon.CurrentSourceHost = "mysql-old-primary"
		fakeMysqlDaemon.CurrentSourcePort = 3305
		fakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
			"FAKE RESET REPLICA ALL",
			"FAKE SET SOURCE",
			"START REPLICA",
		}

		setSourceCalls := 0

		// A source-change failure can happen before the new source is applied.
		// Recovery should clear the old source settings, reapply the requested
		// source, and only then start replication.
		fakeMysqlDaemon.SetReplicationSourceFunc = func(ctx context.Context, host string, port int32, heartbeatInterval float64, stopReplicationBefore bool, startReplicationAfter bool) error {
			setSourceCalls++

			require.Equal(t, "mysql-new-primary", host)
			require.EqualValues(t, 3306, port)
			require.False(t, stopReplicationBefore)
			require.False(t, startReplicationAfter)

			if setSourceCalls == 1 {
				return recoverableReplicationInitError()
			}

			if setSourceCalls == 2 {
				require.NoError(t, fakeMysqlDaemon.ExecuteSuperQueryList(ctx, []string{"FAKE SET SOURCE"}))

				fakeMysqlDaemon.CurrentSourceHost = host
				fakeMysqlDaemon.CurrentSourcePort = port

				return nil
			}

			return fmt.Errorf("unexpected SetReplicationSource call %d", setSourceCalls)
		}

		tm := newTestReplicationTM(newTestTablet(t, 100, "ks", "0", nil), fakeMysqlDaemon, nil)

		err := tm.setReplicationSourceRecoverable(t.Context(), "mysql-new-primary", 3306, 0, false, true)
		require.NoError(t, err)
		require.Equal(t, 2, setSourceCalls)
		require.Equal(t, "mysql-new-primary", fakeMysqlDaemon.CurrentSourceHost)
		require.EqualValues(t, 3306, fakeMysqlDaemon.CurrentSourcePort)
		require.NoError(t, fakeMysqlDaemon.CheckSuperQueryList())
	})
}

func TestStopReplicationAndGetStatus_ServerVersion(t *testing.T) {
	tests := []struct {
		name            string
		mode            replicationdatapb.StopReplicationMode
		replicating     bool
		ioRunning       bool
		expectedQueries []string
		stopIOErr       error
		stopReplErr     error
		afterStatusErr  bool
		expectErr       string
	}{
		{
			name:            "IOTHREADONLY success",
			mode:            replicationdatapb.StopReplicationMode_IOTHREADONLY,
			replicating:     true,
			ioRunning:       true,
			expectedQueries: []string{"STOP REPLICA IO_THREAD"},
		},
		{
			name:        "IOTHREADONLY with IO thread already stopped",
			mode:        replicationdatapb.StopReplicationMode_IOTHREADONLY,
			replicating: false,
			ioRunning:   false,
		},
		{
			name:        "IOTHREADONLY with stopIOThread failure",
			mode:        replicationdatapb.StopReplicationMode_IOTHREADONLY,
			replicating: true,
			ioRunning:   true,
			stopIOErr:   errors.New("injected IO stop error"),
			expectErr:   "stop io thread failed",
		},
		{
			name:            "IOTHREADONLY with after-status failure",
			mode:            replicationdatapb.StopReplicationMode_IOTHREADONLY,
			replicating:     true,
			ioRunning:       true,
			expectedQueries: []string{"STOP REPLICA IO_THREAD"},
			afterStatusErr:  true,
			expectErr:       "acquiring replication status failed",
		},
		{
			name:            "IOANDSQLTHREAD success",
			mode:            replicationdatapb.StopReplicationMode_IOANDSQLTHREAD,
			replicating:     true,
			ioRunning:       true,
			expectedQueries: []string{"STOP REPLICA"},
		},
		{
			name:        "IOANDSQLTHREAD with replication not healthy",
			mode:        replicationdatapb.StopReplicationMode_IOANDSQLTHREAD,
			replicating: false,
			ioRunning:   false,
		},
		{
			name:            "IOANDSQLTHREAD with after-status failure",
			mode:            replicationdatapb.StopReplicationMode_IOANDSQLTHREAD,
			replicating:     true,
			ioRunning:       true,
			expectedQueries: []string{"STOP REPLICA"},
			afterStatusErr:  true,
			expectErr:       "acquiring replication status failed",
		},
		{
			name:        "IOANDSQLTHREAD with stopReplication failure",
			mode:        replicationdatapb.StopReplicationMode_IOANDSQLTHREAD,
			replicating: true,
			ioRunning:   true,
			stopReplErr: errors.New("injected stop error"),
			expectErr:   "stop replication failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeMysqlDaemon := newTestMysqlDaemon(t, 1)
			fakeMysqlDaemon.Replicating = tc.replicating
			fakeMysqlDaemon.IOThreadRunning = tc.ioRunning
			fakeMysqlDaemon.Version = "Ver 8.0.35"

			if tc.expectedQueries != nil {
				fakeMysqlDaemon.ExpectedExecuteSuperQueryList = tc.expectedQueries
			}
			if tc.stopIOErr != nil {
				fakeMysqlDaemon.ExecuteSuperQueryErrorMap = map[string]error{
					"STOP REPLICA IO_THREAD": tc.stopIOErr,
				}
			}
			if tc.stopReplErr != nil {
				fakeMysqlDaemon.StopReplicationError = tc.stopReplErr
			}
			if tc.afterStatusErr {
				// The callback fires during the stop query execution, which happens
				// before the second ReplicationStatus call that fetches the "after" state.
				fakeMysqlDaemon.ExecuteSuperQueryListCallback = func() {
					fakeMysqlDaemon.ReplicationStatusError = errors.New("injected after-status error")
				}
			}

			tm := newTestReplicationTM(newTestTablet(t, 100, "ks", "0", nil), fakeMysqlDaemon, nil)

			resp, err := tm.StopReplicationAndGetStatus(t.Context(), tc.mode)
			if tc.expectErr != "" {
				require.ErrorContains(t, err, tc.expectErr)
			} else {
				require.NoError(t, err)
			}

			require.NotNil(t, resp.Status)

			// ServerVersion is only populated on the success paths. On error returns
			// the RPC layer discards the status (grpctmserver copies it only when
			// err == nil), so the tablet deliberately skips the version fetch there to
			// avoid an unobservable MySQL query under the TabletManager lock.
			if tc.expectErr != "" {
				require.Empty(t, resp.Status.Before.ServerVersion)
				return
			}

			require.Equal(t, "Ver 8.0.35", resp.Status.Before.ServerVersion)
			if resp.Status.After != nil {
				require.Equal(t, "Ver 8.0.35", resp.Status.After.ServerVersion)
			}
		})
	}
}

// TestStopReplicationAndGetStatus_SlowVersionLookup verifies the wiring: when the
// post-mutation version lookup is slow (cold cache), StopReplicationAndGetStatus
// still returns the stopped-replication status (with an empty ServerVersion)
// rather than failing with a deadline error — the mutation already happened, so
// the response must be delivered.
func TestStopReplicationAndGetStatus_SlowVersionLookup(t *testing.T) {
	fakeMysqlDaemon := newTestMysqlDaemon(t, 1)
	fakeMysqlDaemon.Replicating = true
	fakeMysqlDaemon.IOThreadRunning = true
	fakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA IO_THREAD"}

	tm := newTestReplicationTM(newTestTablet(t, 100, "ks", "0", nil), fakeMysqlDaemon, nil)
	// Swap in a version daemon whose lookup never completes on its own, so the
	// bounded post-mutation helper must cap it and fall back to "". The wrapper
	// embeds the same fakeMysqlDaemon and overrides only GetVersionString, so all
	// other calls (ReplicationStatus, the STOP REPLICA IO_THREAD query, etc.) still
	// route to the fake and behave as configured above.
	tm.MysqlDaemon = &countingVersionDaemon{
		FakeMysqlDaemon: fakeMysqlDaemon,
		version:         "Ver 8.0.35",
		delay:           time.Hour,
	}

	const deadline = 30 * time.Second
	ctx, cancel := context.WithTimeout(t.Context(), deadline)
	defer cancel()

	start := time.Now()
	resp, err := tm.StopReplicationAndGetStatus(ctx, replicationdatapb.StopReplicationMode_IOTHREADONLY)
	elapsed := time.Since(start)

	require.NoError(t, err, "a slow version lookup must not fail the RPC after replication was stopped")
	require.NotNil(t, resp.Status)
	require.NotNil(t, resp.Status.After, "the post-stop status must still be returned")
	require.Empty(t, resp.Status.Before.ServerVersion, "version degrades to empty when the lookup is bounded out")
	// The bounded helper caps the lookup near maxVersionLookupBudget (2s), so
	// the RPC must return promptly rather than run to the full deadline. Without the
	// bound it would block for the entire 30s. A generous 15s upper bound keeps this
	// CI-safe while still proving the lookup was bounded, not run to the deadline.
	require.Less(t, elapsed, 15*time.Second, "the bounded lookup must not run to the caller's full deadline")
}

// TestStopReplicationAndGetStatus_SlowVersionLookupNoOp verifies the no-op early
// returns (IO thread already stopped, or replication not running) also bound the
// version lookup. No stop is performed on these paths, but the status was already
// read successfully; a slow cold-cache lookup under the caller's full context must
// not burn the deadline and fail the RPC with DEADLINE_EXCEEDED, which in ERS would
// drop a reachable tablet purely over optional version metadata.
func TestStopReplicationAndGetStatus_SlowVersionLookupNoOp(t *testing.T) {
	tests := []struct {
		name string
		mode replicationdatapb.StopReplicationMode
	}{
		{
			name: "IO thread only, IO already stopped",
			mode: replicationdatapb.StopReplicationMode_IOTHREADONLY,
		},
		{
			name: "full stop, replication not running",
			mode: replicationdatapb.StopReplicationMode_IOANDSQLTHREAD,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeMysqlDaemon := newTestMysqlDaemon(t, 1)
			// Replication is not running, so both StopReplicationMode branches take
			// their no-op early return without issuing any STOP REPLICA query.
			fakeMysqlDaemon.Replicating = false
			fakeMysqlDaemon.IOThreadRunning = false

			tm := newTestReplicationTM(newTestTablet(t, 100, "ks", "0", nil), fakeMysqlDaemon, nil)
			// A version lookup that never completes on its own, so the bounded helper
			// must cap it and fall back to "".
			tm.MysqlDaemon = &countingVersionDaemon{
				FakeMysqlDaemon: fakeMysqlDaemon,
				version:         "Ver 8.0.35",
				delay:           time.Hour,
			}

			const deadline = 30 * time.Second
			ctx, cancel := context.WithTimeout(t.Context(), deadline)
			defer cancel()

			start := time.Now()
			resp, err := tm.StopReplicationAndGetStatus(ctx, tc.mode)
			elapsed := time.Since(start)

			require.NoError(t, err, "a slow version lookup must not fail the no-op RPC")
			require.NotNil(t, resp.Status)
			require.NotNil(t, resp.Status.After, "the no-op path returns before as after")
			require.Empty(t, resp.Status.Before.ServerVersion, "version degrades to empty when the lookup is bounded out")
			// Without the bound the lookup would run to the full 30s deadline; the
			// bounded helper caps it near maxVersionLookupBudget (2s). A generous
			// 15s upper bound keeps this CI-safe while still proving the bound applies.
			require.Less(t, elapsed, 15*time.Second, "the bounded lookup must not run to the caller's full deadline")
		})
	}
}

// countingVersionDaemon wraps a FakeMysqlDaemon to count GetVersionString calls
// and optionally return an error or block, so we can assert the version cache and
// deadline-bounding behavior.
type countingVersionDaemon struct {
	*mysqlctl.FakeMysqlDaemon
	calls   atomic.Int64
	version string
	err     error
	// delay, if set, makes GetVersionString block for up to delay, returning early
	// with ctx.Err() if the context is cancelled first. It simulates a slow
	// cold-cache lookup.
	delay time.Duration
}

func (d *countingVersionDaemon) GetVersionString(ctx context.Context) (string, error) {
	d.calls.Add(1)
	if d.delay > 0 {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(d.delay):
		}
	}
	if d.err != nil {
		return "", d.err
	}
	return d.version, nil
}

func TestGetMySQLVersionStringCache(t *testing.T) {
	t.Run("caches within TTL", func(t *testing.T) {
		daemon := &countingVersionDaemon{
			FakeMysqlDaemon: newTestMysqlDaemon(t, 1),
			version:         "Ver 8.0.35",
		}
		tm := &TabletManager{MysqlDaemon: daemon}

		for range 5 {
			require.Equal(t, "Ver 8.0.35", tm.getMySQLVersionString(t.Context()))
		}
		require.EqualValues(t, 1, daemon.calls.Load(), "should query mysqld only once within the TTL")
	})

	t.Run("refetches after TTL", func(t *testing.T) {
		daemon := &countingVersionDaemon{
			FakeMysqlDaemon: newTestMysqlDaemon(t, 1),
			version:         "Ver 8.0.35",
		}
		tm := &TabletManager{MysqlDaemon: daemon}

		require.Equal(t, "Ver 8.0.35", tm.getMySQLVersionString(t.Context()))
		// Expire the cache by backdating the fetch time beyond the TTL.
		tm.mysqlVersion.mu.Lock()
		tm.mysqlVersion.fetchedAt = time.Now().Add(-2 * mysqlVersionCacheTTL)
		tm.mysqlVersion.mu.Unlock()

		require.Equal(t, "Ver 8.0.35", tm.getMySQLVersionString(t.Context()))
		require.EqualValues(t, 2, daemon.calls.Load(), "should re-query mysqld after the TTL expires")
	})

	t.Run("error is not cached", func(t *testing.T) {
		daemon := &countingVersionDaemon{
			FakeMysqlDaemon: newTestMysqlDaemon(t, 1),
			err:             errors.New("mysqld down"),
		}
		tm := &TabletManager{MysqlDaemon: daemon}

		require.Empty(t, tm.getMySQLVersionString(t.Context()))
		require.Empty(t, tm.getMySQLVersionString(t.Context()))
		require.EqualValues(t, 2, daemon.calls.Load(), "should retry after an error rather than cache the empty result")
	})

	// Exercised under -race to prove the lock-drop-across-fetch design is sound.
	// The lock is intentionally released during the fetch, so a cold-cache burst
	// may fetch more than once; every caller must still observe the same value.
	t.Run("concurrent callers are race-free and consistent", func(t *testing.T) {
		daemon := &countingVersionDaemon{
			FakeMysqlDaemon: newTestMysqlDaemon(t, 1),
			version:         "Ver 8.0.35",
		}
		tm := &TabletManager{MysqlDaemon: daemon}

		const goroutines = 20
		var wg sync.WaitGroup
		results := make([]string, goroutines)
		wg.Add(goroutines)
		for i := range goroutines {
			go func() {
				defer wg.Done()
				results[i] = tm.getMySQLVersionString(t.Context())
			}()
		}
		wg.Wait()

		for _, r := range results {
			require.Equal(t, "Ver 8.0.35", r)
		}
		// Cold-cache burst may fetch more than once, but far fewer than once per caller.
		require.LessOrEqual(t, daemon.calls.Load(), int64(goroutines))
		require.GreaterOrEqual(t, daemon.calls.Load(), int64(1))
	})
}

func TestGetMySQLVersionStringBounded(t *testing.T) {
	t.Run("bounds a slow lookup to half the remaining deadline and returns empty", func(t *testing.T) {
		// A cold-cache lookup that would never finish on its own. With a generous
		// (CI-safe) deadline, the helper must cap it at half the remaining budget,
		// return "" (best-effort), and leave the other half for the caller to return
		// the already-applied mutation. Timings are seconds, not sub-second, to avoid
		// flakiness on starved runners.
		daemon := &countingVersionDaemon{
			FakeMysqlDaemon: newTestMysqlDaemon(t, 1),
			version:         "Ver 8.0.35",
			delay:           time.Hour, // effectively never completes on its own
		}
		tm := &TabletManager{MysqlDaemon: daemon}

		const deadline = 8 * time.Second
		ctx, cancel := context.WithTimeout(t.Context(), deadline)
		defer cancel()

		start := time.Now()
		got := tm.getMySQLVersionStringBounded(ctx)
		elapsed := time.Since(start)

		require.Empty(t, got, "a lookup that outruns its budget degrades to empty version")
		// The lookup is capped at min(deadline/2, 2s) = 2s here, so it must return
		// comfortably before the caller's full deadline, leaving budget to respond.
		require.Less(t, elapsed, deadline, "lookup must not consume the whole deadline")
		require.NoError(t, ctx.Err(), "caller deadline must not be exhausted by the version lookup")
	})

	t.Run("caps the lookup at maxVersionLookupBudget on a large deadline", func(t *testing.T) {
		// With a large remaining deadline, half of it (e.g. 15s) far exceeds the 2s
		// absolute cap, so the min(..., maxVersionLookupBudget) arm must bound
		// the hung lookup near 2s rather than ~15s.
		daemon := &countingVersionDaemon{
			FakeMysqlDaemon: newTestMysqlDaemon(t, 1),
			version:         "Ver 8.0.35",
			delay:           time.Hour,
		}
		tm := &TabletManager{MysqlDaemon: daemon}

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		start := time.Now()
		got := tm.getMySQLVersionStringBounded(ctx)
		elapsed := time.Since(start)

		require.Empty(t, got)
		// Must be bounded by the 2s cap, not remaining/2 (~15s). Generous upper bound
		// (10s) keeps it CI-safe while still proving the cap — not remaining/2 — applied.
		require.Less(t, elapsed, 10*time.Second, "lookup must be capped near maxVersionLookupBudget, not remaining/2")
	})

	t.Run("returns the version on a fast lookup", func(t *testing.T) {
		daemon := &countingVersionDaemon{
			FakeMysqlDaemon: newTestMysqlDaemon(t, 1),
			version:         "Ver 8.0.35",
		}
		tm := &TabletManager{MysqlDaemon: daemon}

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

		require.Equal(t, "Ver 8.0.35", tm.getMySQLVersionStringBounded(ctx))
	})

	t.Run("no deadline still returns the version on a fast lookup", func(t *testing.T) {
		daemon := &countingVersionDaemon{
			FakeMysqlDaemon: newTestMysqlDaemon(t, 1),
			version:         "Ver 8.0.35",
		}
		tm := &TabletManager{MysqlDaemon: daemon}

		require.Equal(t, "Ver 8.0.35", tm.getMySQLVersionStringBounded(context.Background()))
	})

	t.Run("no deadline is still capped at maxVersionLookupBudget", func(t *testing.T) {
		// A deadline-less caller (e.g. an in-process DemotePrimary) must not let a hung
		// cold-cache lookup hold the action lock forever: the absolute cap applies even
		// without a deadline.
		daemon := &countingVersionDaemon{
			FakeMysqlDaemon: newTestMysqlDaemon(t, 1),
			version:         "Ver 8.0.35",
			delay:           time.Hour,
		}
		tm := &TabletManager{MysqlDaemon: daemon}

		start := time.Now()
		got := tm.getMySQLVersionStringBounded(context.Background())
		elapsed := time.Since(start)

		require.Empty(t, got)
		// Bounded near the 2s cap; generous upper bound keeps it CI-safe while still
		// proving the lookup did not run unbounded.
		require.Less(t, elapsed, 10*time.Second, "deadline-less lookup must still be capped")
	})
}

func TestShardPeerHealthSnapshot(t *testing.T) {
	// Without a monitor configured, FullStatus gets a nil snapshot and must not panic.
	tm := &TabletManager{}
	assert.Nil(t, tm.shardPeerHealthSnapshot(), "no monitor configured -> nil snapshot, no panic")

	// With a monitor, the primary's latest liveness signals are surfaced. The monitor tracks only
	// the shard primary, so the observed peer must be PRIMARY-typed.
	self := &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100}}
	peer := &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: "zone1", Uid: 101}, Keyspace: "ks", Shard: "0", Type: topodatapb.TabletType_PRIMARY}
	pinger := &fakePinger{fail: true}
	m := newShardHealthMonitor(pinger, staticLister(self, peer), staticPrimaryAlias(peer), topoproto.TabletAliasString(self.Alias), time.Second, time.Second)
	require.NoError(t, m.refreshPeers(t.Context()))
	m.runPingRound(t.Context())
	assert.Eventually(t, func() bool { return m.inflightCount() == 0 }, 30*time.Second, 5*time.Millisecond)

	tm = &TabletManager{shardHealthMonitor: m}
	snap := tm.shardPeerHealthSnapshot()
	require.Len(t, snap, 1)
	assert.Equal(t, int64(1), snap[0].ConsecutivePingFailures)
}
