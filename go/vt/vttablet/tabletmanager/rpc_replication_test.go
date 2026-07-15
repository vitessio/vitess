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

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/semisyncmonitor"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func newTestReplicationTM(tablet *topodatapb.Tablet, mysqlDaemon *mysqlctl.FakeMysqlDaemon, ts *topo.Server) *TabletManager {
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

type demotePrimaryStallQS struct {
	tabletserver.Controller
	qsWaitChan     chan any
	primaryStalled atomic.Bool
}

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
		"Rpl_semi_sync_source_yes_tx|5"))

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
		"Rpl_semi_sync_source_yes_tx|5"))
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
			"Rpl_semi_sync_source_yes_tx|5"))
	}
	// Next calls: waiting sessions present, but ackedTrxs=6 (progress!).
	for range 10 {
		fakeDb.AddQuery("SELECT /*+ MAX_EXECUTION_TIME(1000) */ variable_name, variable_value FROM performance_schema.global_status WHERE REGEXP_LIKE(variable_name, 'Rpl_semi_sync_(source|master)_(wait_sessions|yes_tx)')", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
			"Rpl_semi_sync_source_wait_sessions|1",
			"Rpl_semi_sync_source_yes_tx|6"))
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
		"Rpl_semi_sync_source_yes_tx|5"))
	// Second and subsequent calls: no waiting sessions (unblocked!).
	for range 10 {
		fakeDb.AddQuery("SELECT /*+ MAX_EXECUTION_TIME(1000) */ variable_name, variable_value FROM performance_schema.global_status WHERE REGEXP_LIKE(variable_name, 'Rpl_semi_sync_(source|master)_(wait_sessions|yes_tx)')", sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("variable_name|variable_value", "varchar|varchar"),
			"Rpl_semi_sync_source_wait_sessions|0",
			"Rpl_semi_sync_source_yes_tx|5"))
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
			require.Equal(t, "Ver 8.0.35", resp.Status.Before.ServerVersion)
			if resp.Status.After != nil {
				require.Equal(t, "Ver 8.0.35", resp.Status.After.ServerVersion)
			}
		})
	}
}

// countingVersionDaemon wraps a FakeMysqlDaemon to count GetVersionString calls
// and optionally return an error, so we can assert the version cache behavior.
type countingVersionDaemon struct {
	*mysqlctl.FakeMysqlDaemon
	calls   atomic.Int64
	version string
	err     error
}

func (d *countingVersionDaemon) GetVersionString(ctx context.Context) (string, error) {
	d.calls.Add(1)
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
