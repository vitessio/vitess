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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

<<<<<<< HEAD
||||||| parent of 271e8bd3bf (VTTablet: Preserve semi-sync durability during demotion, draining and promotion (#21409))
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
=======
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/mysql/replication"
>>>>>>> 271e8bd3bf (VTTablet: Preserve semi-sync durability during demotion, draining and promotion (#21409))
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
<<<<<<< HEAD
||||||| parent of 271e8bd3bf (VTTablet: Preserve semi-sync durability during demotion, draining and promotion (#21409))
	"vitess.io/vitess/go/vt/topo/topoproto"
=======
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
>>>>>>> 271e8bd3bf (VTTablet: Preserve semi-sync durability during demotion, draining and promotion (#21409))
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/semisyncmonitor"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := tm.waitForGrantsToHaveApplied(ctx)
	require.ErrorContains(t, err, "deadline exceeded")

	err = tm.waitForDBAGrants(nil, 0)
	require.NoError(t, err)

	secondContext, secondCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
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
		tm.demotePrimary(context.Background(), false /* revertPartialFailure */, false /* force */)
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
		require.NoError(t, err)
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
		require.NoError(t, err)
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
		require.NoError(t, err)
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	require.EqualValues(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
	// Verify that the tablet record says the tablet should be a primary.
	require.EqualValues(t, topodatapb.TabletType_PRIMARY, ti.Type)

	err = tm.UndoDemotePrimary(ctx, false)
	require.NoError(t, err)
	require.EqualValues(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	require.EqualValues(t, ti.PrimaryTermStartTime, tm.Tablet().PrimaryTermStartTime)
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
// Only flavors without receiver commands restart replication as a whole.
func TestFixSemiSyncAndReplicationRecoversFromRecoverableReplicationInitializationError(t *testing.T) {
	fakeMysqlDaemon := newTestMysqlDaemon(t, 1)
	fakeMysqlDaemon.Replicating = true
	fakeMysqlDaemon.RestartReplicationReceiverError = mysqlctl.ErrReceiverRestartUnsupported
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
<<<<<<< HEAD
||||||| parent of 271e8bd3bf (VTTablet: Preserve semi-sync durability during demotion, draining and promotion (#21409))

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

// TestChangeTypeReportsReplicationStatusError checks that a type change reports
// a replication status read that fails for a reason other than the tablet not
// being a replica, including one cut short by its context, after publishing the
// requested type.
func TestChangeTypeReportsReplicationStatusError(t *testing.T) {
	for _, tc := range []struct {
		tabletType topodatapb.TabletType
		statusErr  error
	}{
		{tabletType: topodatapb.TabletType_REPLICA, statusErr: errors.New("injected replication status failure")},
		{tabletType: topodatapb.TabletType_RDONLY, statusErr: errors.New("injected replication status failure")},
		{tabletType: topodatapb.TabletType_REPLICA, statusErr: context.DeadlineExceeded},
		{tabletType: topodatapb.TabletType_REPLICA, statusErr: context.Canceled},
		{tabletType: topodatapb.TabletType_REPLICA, statusErr: mysql.ErrNotReplica},
	} {
		t.Run(fmt.Sprintf("%v/%v", tc.tabletType, tc.statusErr), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			t.Cleanup(cancel)
			ts := memorytopo.NewServer(ctx, "cell1")
			tm := newTestTM(t, ts, 1, "ks", "0", nil)
			t.Cleanup(tm.Stop)
			fake := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
			fake.DB().SetNeverFail(true)
			require.NoError(t, tm.ChangeType(ctx, topodatapb.TabletType_SPARE, false))
			fake.ReplicationStatusError = tc.statusErr

			err := tm.ChangeType(ctx, tc.tabletType, true)
			if errors.Is(tc.statusErr, mysql.ErrNotReplica) {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.statusErr.Error())
				require.ErrorContains(t, err, "failed to read replication status")
			}
			assert.Equal(t, tc.tabletType, tm.Tablet().Type)
			ti, err := ts.GetTablet(ctx, tm.Tablet().Alias)
			require.NoError(t, err)
			assert.Equal(t, tc.tabletType, ti.Type)
		})
	}
}
=======

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

// semiSyncOrderMysqlDaemon records, when source-side semi-sync is disabled, whether the query
// service was still serving: disabling it completes the commits that wait for an ACK.
type semiSyncOrderMysqlDaemon struct {
	*mysqlctl.FakeMysqlDaemon
	tm                     *TabletManager
	disabledSourceSemiSync bool
	servingWhenDisabled    bool
	// sourceSemiSyncAtSuperReadOnly records source-side semi-sync when
	// super_read_only is first enabled.
	sourceSemiSyncAtSuperReadOnly *bool
	// releasedPosition, if set, becomes the executed position when source-side
	// semi-sync is disabled, as the commits blocked on ACKs complete.
	releasedPosition replication.Position
}

func (d *semiSyncOrderMysqlDaemon) SetSuperReadOnly(ctx context.Context, on bool, opts ...mysqlctl.SetSuperReadOnlyOption) (mysqlctl.ResetSuperReadOnlyFunc, error) {
	if on && d.sourceSemiSyncAtSuperReadOnly == nil {
		enabled := d.SemiSyncPrimaryEnabled
		d.sourceSemiSyncAtSuperReadOnly = &enabled
	}
	return d.FakeMysqlDaemon.SetSuperReadOnly(ctx, on, opts...)
}

func (d *semiSyncOrderMysqlDaemon) IsSemiSyncBlocked(context.Context) (bool, error) {
	return d.SemiSyncPrimaryEnabled, nil
}

func (d *semiSyncOrderMysqlDaemon) SetSemiSyncEnabled(ctx context.Context, primary, replica bool) error {
	disabling := d.SemiSyncPrimaryEnabled && !primary && !d.disabledSourceSemiSync
	if disabling {
		d.servingWhenDisabled = d.tm.QueryServiceControl.IsServing()
	}
	if err := d.FakeMysqlDaemon.SetSemiSyncEnabled(ctx, primary, replica); err != nil {
		return err
	}
	if disabling {
		d.disabledSourceSemiSync = true
		if !d.releasedPosition.IsZero() {
			d.SetPrimaryPositionLocked(d.releasedPosition)
		}
	}
	return nil
}

// newBlockedPrimaryTest returns a serving PRIMARY whose writes wait for
// semi-sync ACKs, and a new primary registered in the topo.
func newBlockedPrimaryTest(t *testing.T) (context.Context, *TabletManager, *semiSyncOrderMysqlDaemon, *topodatapb.Tablet) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	t.Cleanup(tm.Stop)
	tm.tmc = newFakeTMClient()

	require.NoError(t, tm.ChangeType(ctx, topodatapb.TabletType_PRIMARY, true))
	fake := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	fake.DB().SetNeverFail(true)
	require.NoError(t, fake.SetReadOnly(ctx, false))
	require.NoError(t, fake.SetSemiSyncEnabled(ctx, true, true))
	// An old primary has no replication configured.
	fake.ReplicationStatusError = mysql.ErrNotReplica
	fake.SetPrimaryPositionLocked(replication.MustParsePosition(gtidFlavor, gtidPosition))
	daemon := &semiSyncOrderMysqlDaemon{FakeMysqlDaemon: fake, tm: tm}
	tm.MysqlDaemon = daemon
	require.True(t, tm.QueryServiceControl.IsServing())

	newPrimary := &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "cell1", Uid: 200},
		Keyspace:      "ks",
		Shard:         "0",
		Type:          topodatapb.TabletType_PRIMARY,
		MysqlHostname: "mysql-new-primary",
		MysqlPort:     3306,
	}
	require.NoError(t, ts.CreateTablet(ctx, newPrimary))
	return ctx, tm, daemon, newPrimary
}

// TestSetReplicationSourceDemotesPrimaryBeforeSemiSync checks that SetReplicationSource on a
// PRIMARY tablet whose writes wait for semi-sync ACKs stops serving (killing those sessions)
// before disabling source-side semi-sync, leaves super_read_only enabled, and reads its
// position only after the blocked commits complete. An ERS sends this RPC to an old primary
// that it could not demote, and the new primary doesn't have those commits.
func TestSetReplicationSourceDemotesPrimaryBeforeSemiSync(t *testing.T) {
	ctx, tm, daemon, newPrimary := newBlockedPrimaryTest(t)
	const releasedGTID = "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1"
	daemon.releasedPosition = replication.MustParsePosition(gtidFlavor, gtidPosition+","+releasedGTID)

	err := tm.SetReplicationSource(ctx, newPrimary.Alias, 0, "", false, true, 0)
	require.ErrorContains(t, err, "Errant GTID detected - "+releasedGTID)

	require.True(t, daemon.disabledSourceSemiSync)
	assert.False(t, daemon.servingWhenDisabled, "source-side semi-sync was disabled while the query service served")
	// A forced demotion turns off semi-sync before super_read_only, which
	// would otherwise wait on commits blocked on ACKs.
	require.NotNil(t, daemon.sourceSemiSyncAtSuperReadOnly)
	assert.False(t, *daemon.sourceSemiSyncAtSuperReadOnly, "super_read_only was enabled before source-side semi-sync was disabled")
	assert.True(t, daemon.SuperReadOnly.Load())
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
}

// TestSetReplicationSourceKeepsPrimaryWhenDemotionFails checks that a PRIMARY
// whose demotion fails keeps its type instead of becoming a REPLICA, and stays
// out of service.
func TestSetReplicationSourceKeepsPrimaryWhenDemotionFails(t *testing.T) {
	ctx, tm, daemon, newPrimary := newBlockedPrimaryTest(t)
	injectedErr := errors.New("injected semi-sync failure")
	daemon.SetSemiSyncEnabledFunc = func(primary, replica bool) error {
		if !primary {
			return injectedErr
		}
		return nil
	}
	require.True(t, tm.QueryServiceControl.IsServing())

	err := tm.SetReplicationSource(ctx, newPrimary.Alias, 0, "", false, true, 0)
	require.ErrorContains(t, err, "failed to demote the primary before repointing it: "+injectedErr.Error())
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	ti, err := tm.TopoServer.GetTablet(ctx, tm.Tablet().Alias)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, ti.Type)
	assert.False(t, tm.QueryServiceControl.IsServing())
}

// TestChangeTypeReportsReplicationStatusError checks that a type change reports
// a replication status read that fails for a reason other than the tablet not
// being a replica, including one cut short by its context, after publishing the
// requested type.
func TestChangeTypeReportsReplicationStatusError(t *testing.T) {
	for _, tc := range []struct {
		tabletType topodatapb.TabletType
		statusErr  error
	}{
		{tabletType: topodatapb.TabletType_REPLICA, statusErr: errors.New("injected replication status failure")},
		{tabletType: topodatapb.TabletType_RDONLY, statusErr: errors.New("injected replication status failure")},
		{tabletType: topodatapb.TabletType_REPLICA, statusErr: context.DeadlineExceeded},
		{tabletType: topodatapb.TabletType_REPLICA, statusErr: context.Canceled},
		{tabletType: topodatapb.TabletType_REPLICA, statusErr: mysql.ErrNotReplica},
	} {
		t.Run(fmt.Sprintf("%v/%v", tc.tabletType, tc.statusErr), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			t.Cleanup(cancel)
			ts := memorytopo.NewServer(ctx, "cell1")
			tm := newTestTM(t, ts, 1, "ks", "0", nil)
			t.Cleanup(tm.Stop)
			fake := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
			fake.DB().SetNeverFail(true)
			require.NoError(t, tm.ChangeType(ctx, topodatapb.TabletType_SPARE, false))
			fake.ReplicationStatusError = tc.statusErr

			err := tm.ChangeType(ctx, tc.tabletType, true)
			if errors.Is(tc.statusErr, mysql.ErrNotReplica) {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.statusErr.Error())
				require.ErrorContains(t, err, "failed to read replication status")
			}
			assert.Equal(t, tc.tabletType, tm.Tablet().Type)
			ti, err := ts.GetTablet(ctx, tm.Tablet().Alias)
			require.NoError(t, err)
			assert.Equal(t, tc.tabletType, ti.Type)
		})
	}
}

// TestChangeTypeDrainedStopsSemiSyncAcks checks that a tablet changed to DRAINED stops sending
// semi-sync ACKs even when the caller asks for them (VTOrc's errant GTID recovery before v25).
func TestChangeTypeDrainedStopsSemiSyncAcks(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	t.Cleanup(tm.Stop)
	fakeMysqlDaemon := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	fakeMysqlDaemon.DB().SetNeverFail(true)

	require.NoError(t, tm.ChangeType(ctx, topodatapb.TabletType_REPLICA, true))
	require.True(t, fakeMysqlDaemon.SemiSyncReplicaEnabled)

	require.NoError(t, tm.ChangeType(ctx, topodatapb.TabletType_DRAINED, true))
	assert.Equal(t, topodatapb.TabletType_DRAINED, tm.Tablet().Type)
	assert.False(t, fakeMysqlDaemon.SemiSyncReplicaEnabled)
}

// A stale replication repair must not re-enable ACKs on a DRAINED tablet.
func TestSetReplicationSourceDrainedRejectsStaleSemiSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	t.Cleanup(tm.Stop)
	daemon := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	daemon.DB().SetNeverFail(true)
	require.NoError(t, tm.ChangeType(ctx, topodatapb.TabletType_REPLICA, true))
	// Older tablets published DRAINED before disabling ACKs.
	require.NoError(t, tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_DRAINED, DBActionNone))
	require.True(t, daemon.SemiSyncReplicaEnabled)

	err := tm.SetReplicationSource(ctx, &topodatapb.TabletAlias{Cell: "cell1", Uid: 200}, 0, "", false, true, 0)
	require.Error(t, err, "the parent is absent")
	assert.Equal(t, topodatapb.TabletType_DRAINED, tm.Tablet().Type)
	assert.False(t, daemon.SemiSyncReplicaEnabled)
}

type drainSemiSyncMysqlDaemon struct {
	*mysqlctl.FakeMysqlDaemon
	tm            *TabletManager
	setError      error
	restartError  error
	acking        bool
	typeAtRevoke  topodatapb.TabletType
	typeAtRestart topodatapb.TabletType
	// ioState and lastIOError, when ioState is set, replace the fake's receiver state.
	ioState     replication.ReplicationState
	lastIOError string
}

func (d *drainSemiSyncMysqlDaemon) ReplicationStatus(ctx context.Context) (replication.ReplicationStatus, error) {
	status, err := d.FakeMysqlDaemon.ReplicationStatus(ctx)
	if err == nil && d.ioState != replication.ReplicationStateUnknown {
		status.IOState, status.LastIOError = d.ioState, d.lastIOError
	}
	return status, err
}

func (d *drainSemiSyncMysqlDaemon) SetSemiSyncEnabled(ctx context.Context, primary, replica bool) error {
	if !replica {
		d.typeAtRevoke = d.tm.Tablet().Type
		if d.setError != nil {
			return d.setError
		}
	}
	return d.FakeMysqlDaemon.SetSemiSyncEnabled(ctx, primary, replica)
}

func (d *drainSemiSyncMysqlDaemon) SemiSyncReplicationStatus(ctx context.Context) (bool, error) {
	// The existing connection still ACKs until replication is restarted.
	return d.acking, nil
}

func (d *drainSemiSyncMysqlDaemon) RestartReplicationReceiver(ctx context.Context, hookExtraEnv map[string]string) error {
	if d.restartError != nil {
		return d.restartError
	}
	if err := d.FakeMysqlDaemon.RestartReplicationReceiver(ctx, hookExtraEnv); err != nil {
		return err
	}
	d.typeAtRestart = d.tm.Tablet().Type
	d.acking = d.SemiSyncReplicaEnabled
	return nil
}

func TestChangeTypeDrainedRevokesSemiSyncBeforePublishing(t *testing.T) {
	for _, stage := range []string{"semi-sync update", "status read", "status cancellation", "receiver restart"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			t.Cleanup(cancel)
			ts := memorytopo.NewServer(ctx, "cell1")
			tm := newTestTM(t, ts, 1, "ks", "0", nil)
			t.Cleanup(tm.Stop)
			fake := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
			fake.DB().SetNeverFail(true)
			require.NoError(t, tm.ChangeType(ctx, topodatapb.TabletType_REPLICA, true))
			daemon := &drainSemiSyncMysqlDaemon{FakeMysqlDaemon: fake, tm: tm}
			tm.MysqlDaemon = daemon
			injectedErr := errors.New("injected ACK revocation failure")
			if stage == "semi-sync update" {
				daemon.setError = injectedErr
			} else {
				fake.Replicating = true
				fake.IOThreadRunning = true
				fake.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA IO_THREAD", "START REPLICA IO_THREAD"}
				daemon.acking = true
				switch stage {
				case "status read":
					fake.ReplicationStatusError = injectedErr
				case "status cancellation":
					injectedErr = context.Canceled
					fake.ReplicationStatusError = injectedErr
				case "receiver restart":
					daemon.restartError = injectedErr
				}
			}

			require.ErrorContains(t, tm.ChangeType(ctx, topodatapb.TabletType_DRAINED, true), injectedErr.Error())
			assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
			ti, err := ts.GetTablet(ctx, tm.Tablet().Alias)
			require.NoError(t, err)
			assert.Equal(t, topodatapb.TabletType_REPLICA, ti.Type)
			if stage != "semi-sync update" {
				assert.True(t, daemon.acking)
				assert.Zero(t, fake.ExpectedExecuteSuperQueryCurrent)
			}

			daemon.setError, daemon.restartError = nil, nil
			fake.ReplicationStatusError = nil
			require.NoError(t, tm.ChangeType(ctx, topodatapb.TabletType_DRAINED, true))
			assert.Equal(t, topodatapb.TabletType_REPLICA, daemon.typeAtRevoke)
			assert.Equal(t, topodatapb.TabletType_DRAINED, tm.Tablet().Type)
			assert.False(t, fake.SemiSyncReplicaEnabled)
			if stage != "semi-sync update" {
				assert.Equal(t, topodatapb.TabletType_REPLICA, daemon.typeAtRestart)
				assert.False(t, daemon.acking)
				require.NoError(t, fake.CheckSuperQueryList())
			}
		})
	}
}

// TestChangeTypeDrainedRestartsReceiverUnlessStopped checks that draining restarts
// every receiver that isn't stopped, including one reconnecting after an error,
// which would otherwise stop with error 13117 when it reconnects.
func TestChangeTypeDrainedRestartsReceiverUnlessStopped(t *testing.T) {
	tests := []struct {
		name        string
		ioState     replication.ReplicationState
		lastIOError string
		restart     bool
	}{
		{name: "stopped", ioState: replication.ReplicationStateStopped},
		{name: "running", ioState: replication.ReplicationStateRunning, restart: true},
		{name: "connecting", ioState: replication.ReplicationStateConnecting, restart: true},
		{name: "connecting after an error", ioState: replication.ReplicationStateConnecting, lastIOError: "error reconnecting to source", restart: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			t.Cleanup(cancel)
			ts := memorytopo.NewServer(ctx, "cell1")
			tm := newTestTM(t, ts, 1, "ks", "0", nil)
			t.Cleanup(tm.Stop)
			fake := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
			fake.DB().SetNeverFail(true)
			require.NoError(t, tm.ChangeType(ctx, topodatapb.TabletType_REPLICA, true))
			daemon := &drainSemiSyncMysqlDaemon{FakeMysqlDaemon: fake, tm: tm, acking: true, ioState: tt.ioState, lastIOError: tt.lastIOError}
			tm.MysqlDaemon = daemon
			fake.Replicating = true
			if tt.restart {
				fake.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA IO_THREAD", "START REPLICA IO_THREAD"}
			}

			require.NoError(t, tm.ChangeType(ctx, topodatapb.TabletType_DRAINED, true))
			assert.Equal(t, topodatapb.TabletType_DRAINED, tm.Tablet().Type)
			assert.False(t, fake.SemiSyncReplicaEnabled)
			require.NoError(t, fake.CheckSuperQueryList())
			if tt.restart {
				assert.Equal(t, topodatapb.TabletType_REPLICA, daemon.typeAtRestart)
				assert.False(t, daemon.acking)
			} else {
				assert.Zero(t, fake.ExpectedExecuteSuperQueryCurrent)
			}
		})
	}
}

func TestChangeTypeDrainedWithoutReplication(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	t.Cleanup(tm.Stop)
	daemon := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	daemon.DB().SetNeverFail(true)
	require.NoError(t, tm.ChangeType(ctx, topodatapb.TabletType_REPLICA, true))
	daemon.ReplicationStatusError = mysql.ErrNotReplica

	require.NoError(t, tm.ChangeType(ctx, topodatapb.TabletType_DRAINED, true))
	assert.Equal(t, topodatapb.TabletType_DRAINED, tm.Tablet().Type)
	assert.False(t, daemon.SemiSyncReplicaEnabled)
	assert.Zero(t, daemon.ExpectedExecuteSuperQueryCurrent)
}

func TestChangeTypeDrainedRepairsAcknowledgementsWithoutReclaiming(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	t.Cleanup(tm.Stop)
	daemon := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	daemon.DB().SetNeverFail(true)
	require.NoError(t, tm.ChangeType(ctx, topodatapb.TabletType_REPLICA, true))
	// Older tablets published DRAINED before disabling ACKs.
	require.NoError(t, tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_DRAINED, DBActionNone))
	require.True(t, daemon.SemiSyncReplicaEnabled)

	require.ErrorContains(t, tm.ChangeType(ctx, topodatapb.TabletType_DRAINED, true), "already drained")
	assert.Equal(t, topodatapb.TabletType_DRAINED, tm.Tablet().Type)
	assert.False(t, daemon.SemiSyncReplicaEnabled)
}

// TestChangeTypeDrainedRejectsPrimary checks that draining a primary is rejected
// before changing its tablet type, serving state, or MySQL durability settings.
func TestChangeTypeDrainedRejectsPrimary(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	t.Cleanup(tm.Stop)
	require.NoError(t, tm.ChangeType(ctx, topodatapb.TabletType_PRIMARY, true))
	fake := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	fake.DB().SetNeverFail(true)
	require.NoError(t, fake.SetReadOnly(ctx, false))
	require.NoError(t, fake.SetSemiSyncEnabled(ctx, true, true))
	daemon := &semiSyncOrderMysqlDaemon{FakeMysqlDaemon: fake, tm: tm}
	tm.MysqlDaemon = daemon
	require.True(t, tm.QueryServiceControl.IsServing())

	err := tm.ChangeType(ctx, topodatapb.TabletType_DRAINED, true)
	require.ErrorContains(t, err, "reparent the shard first")
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	assert.False(t, daemon.disabledSourceSemiSync)
	assert.True(t, tm.QueryServiceControl.IsServing())
	assert.False(t, fake.SuperReadOnly.Load())
	assert.True(t, fake.SemiSyncPrimaryEnabled)
	assert.True(t, fake.SemiSyncReplicaEnabled)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
}
>>>>>>> 271e8bd3bf (VTTablet: Preserve semi-sync durability during demotion, draining and promotion (#21409))
