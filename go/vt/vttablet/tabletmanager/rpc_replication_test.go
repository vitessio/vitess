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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/semisyncmonitor"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
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
)

func (d *demotePrimaryStallQS) SetDemotePrimaryStalled(val bool) {
	d.primaryStalled.Store(val)
}

func (d *demotePrimaryStallQS) IsServing() bool {
	<-d.qsWaitChan
	return false
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

// guardedReplicationRPCs are the RPCs an unmanaged tablet must refuse, because each one changes
// replication on a MySQL we don't manage, or promotes or demotes it. Read-only RPCs and ChangeType
// (which external reparents rely on) are deliberately absent.
func guardedReplicationRPCs(tm *TabletManager, ctx context.Context) map[string]func() error {
	return map[string]func() error{
		// Backs both the SetReadOnly and SetReadWrite RPCs, which vtadmin exposes as a button.
		"SetReadOnly":            func() error { return tm.SetReadOnly(ctx, true) },
		"SetReadWrite":           func() error { return tm.SetReadOnly(ctx, false) },
		"StopReplication":        func() error { return tm.StopReplication(ctx) },
		"StopReplicationMinimum": func() error { _, err := tm.StopReplicationMinimum(ctx, "", 0); return err },
		"StartReplication":       func() error { return tm.StartReplication(ctx, false) },
		"RestartReplication":     func() error { return tm.RestartReplication(ctx, false) },
		"StartReplicationUntilAfter": func() error {
			return tm.StartReplicationUntilAfter(ctx, "", 0)
		},
		"ResetReplication": func() error { return tm.ResetReplication(ctx) },
		"InitPrimary":      func() error { _, err := tm.InitPrimary(ctx, false); return err },
		"PopulateReparentJournal": func() error {
			return tm.PopulateReparentJournal(ctx, 0, "action", nil, "")
		},
		"InitReplica":                func() error { return tm.InitReplica(ctx, nil, "", 0, false) },
		"DemotePrimary":              func() error { _, err := tm.DemotePrimary(ctx, false); return err },
		"UndoDemotePrimary":          func() error { return tm.UndoDemotePrimary(ctx, false) },
		"ResetReplicationParameters": func() error { return tm.ResetReplicationParameters(ctx) },
		"SetReplicationSource": func() error {
			return tm.SetReplicationSource(ctx, nil, 0, "", false, false, 0)
		},
		"StopReplicationAndGetStatus": func() error {
			_, err := tm.StopReplicationAndGetStatus(ctx, replicationdatapb.StopReplicationMode_IOANDSQLTHREAD)
			return err
		},
		"PromoteReplica": func() error { _, err := tm.PromoteReplica(ctx, false); return err },
	}
}

// newUnstartedReplicationTM builds a TabletManager whose grants have deliberately NOT been
// applied, so a guarded RPC that gets past the unmanaged check stalls on the context instead of
// doing real work. It still gets a fake MysqlDaemon, because not every guarded RPC waits for
// grants -- SetReadOnly goes straight for MySQL, and a nil daemon would panic rather than fail.
func newUnstartedReplicationTM(t *testing.T, mode topodatapb.TabletMode) *TabletManager {
	t.Helper()

	fakeDb := newTestMysqlDaemon(t, 1)
	tm := &TabletManager{
		actionSema:             semaphore.NewWeighted(1),
		MysqlDaemon:            fakeDb,
		QueryServiceControl:    tabletservermock.NewController(),
		SemiSyncMonitor:        semisyncmonitor.CreateTestSemiSyncMonitor(fakeDb.DB(), exporter),
		_waitForGrantsComplete: make(chan struct{}),
		mode:                   mode,
		tmState: &tmState{
			displayState: displayState{
				tablet: &topodatapb.Tablet{
					Alias: &topodatapb.TabletAlias{Cell: "cell1", Uid: 100},
				},
			},
		},
	}
	return tm
}

// TestUnmanagedTabletRejectsReplicationRPCs checks that a vttablet started with --unmanaged
// refuses every RPC that would reconfigure the replication of its external MySQL, and that it
// refuses before taking the action semaphore or touching MySQL at all.
func TestUnmanagedTabletRejectsReplicationRPCs(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	tm := newUnstartedReplicationTM(t, topodatapb.TabletMode_UNMANAGED)

	for name, call := range guardedReplicationRPCs(tm, ctx) {
		t.Run(name, func(t *testing.T) {
			err := call()
			require.ErrorContains(t, err, "tablet is unmanaged")
			assert.Equal(t, vtrpc.Code_FAILED_PRECONDITION, vterrors.Code(err))

			// The guard runs before tm.lock(), so the action semaphore was never taken.
			require.True(t, tm.actionSema.TryAcquire(1), "%s acquired the action semaphore", name)
			tm.actionSema.Release(1)
		})
	}
}

// TestManagedTabletAllowsReplicationRPCs is the other half of the guard: a managed tablet must be
// left alone. MANAGED is the zero value, which is also what a vttablet too old to set the field
// leaves behind, so this doubles as the check that an upgrade does not stop managing replication.
func TestManagedTabletAllowsReplicationRPCs(t *testing.T) {
	for _, mode := range []topodatapb.TabletMode{
		topodatapb.TabletMode_MANAGED,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			require.Zero(t, mode, "MANAGED must stay the zero value so an absent mode reads as managed")
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()

			tm := newUnstartedReplicationTM(t, mode)

			for name, call := range guardedReplicationRPCs(tm, ctx) {
				t.Run(name, func(t *testing.T) {
					// What each RPC does after the guard differs, and several of them fail for
					// their own reasons against a fake daemon. The only property under test is
					// that the unmanaged guard did not fire.
					err := call()
					if err != nil {
						require.NotContains(t, err.Error(), "tablet is unmanaged",
							"guard fired for mode %v", mode)
					}
				})
			}
		})
	}
}

// TestChangeTypeUnmanagedIsTopoOnly checks that ChangeType stays available on an unmanaged tablet,
// which TabletExternallyReparented needs, but only moves the topo record. With a semi-sync action
// fixSemiSyncAndReplication reconfigures semi-sync and can stop and restart replication on a MySQL
// Vitess does not manage, so an unmanaged tablet must always resolve to SemiSyncActionNone.
func TestChangeTypeUnmanagedIsTopoOnly(t *testing.T) {
	tests := []struct {
		name              string
		mode              topodatapb.TabletMode
		tabletType        topodatapb.TabletType
		wantSemiSyncTouch bool
	}{
		{
			name:              "unmanaged replica-typed change leaves MySQL alone",
			mode:              topodatapb.TabletMode_UNMANAGED,
			tabletType:        topodatapb.TabletType_RDONLY,
			wantSemiSyncTouch: false,
		}, {
			// The external reparent path. It is already safe because fixSemiSyncAndReplication
			// returns early for PRIMARY, so forcing the action to None costs it nothing.
			name:              "unmanaged promotion to primary still works",
			mode:              topodatapb.TabletMode_UNMANAGED,
			tabletType:        topodatapb.TabletType_PRIMARY,
			wantSemiSyncTouch: false,
		}, {
			// Control: without the unmanaged guard the same call does reach MySQL.
			name:              "managed replica-typed change still fixes semi-sync",
			mode:              topodatapb.TabletMode_MANAGED,
			tabletType:        topodatapb.TabletType_RDONLY,
			wantSemiSyncTouch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			ts := memorytopo.NewServer(ctx, "cell1")
			t.Cleanup(func() { ts.Close() })

			tm := newTestTM(t, ts, 1, "ks", "0", nil)
			t.Cleanup(tm.Stop)

			fakeDb, ok := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
			require.True(t, ok)
			fakeDb.SemiSyncPrimaryEnabled = true
			fakeDb.SemiSyncReplicaEnabled = true
			tm.mode = tt.mode

			require.NoError(t, tm.ChangeType(ctx, tt.tabletType, false))
			assert.Equal(t, tt.tabletType, tm.Tablet().Type, "the topo record must always change type")

			semiSyncTouched := !fakeDb.SemiSyncPrimaryEnabled || !fakeDb.SemiSyncReplicaEnabled
			assert.Equal(t, tt.wantSemiSyncTouch, semiSyncTouched,
				"semi-sync state on the external MySQL")
		})
	}
}
