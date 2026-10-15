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

package tabletmanager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	querypb "vitess.io/vitess/go/vt/proto/query"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/reparenttestutil"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	"golang.org/x/sync/semaphore"
)

// The client lets tests hold initialization after acquisition, without sleeps.
type initializationPrimaryClient struct {
	tmclient.TabletManagerClient
	entered chan context.Context
	release chan struct{}
}

func (c *initializationPrimaryClient) PrimaryStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
	c.entered <- ctx
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.release:
		return c.TabletManagerClient.PrimaryStatus(ctx, tablet)
	}
}

func newReplicationInitializationTM(t *testing.T) (*TabletManager, *mysqlctl.FakeMysqlDaemon, *topo.Server, *memorytopo.Factory) {
	t.Helper()
	ctx := t.Context()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "cell1")
	t.Cleanup(ts.Close)
	tm, md := newReplicationInitializationTMWithServer(t, ts)
	return tm, md, ts, factory
}

func newReplicationInitializationTMWithServer(t *testing.T, ts *topo.Server) (*TabletManager, *mysqlctl.FakeMysqlDaemon) {
	t.Helper()
	ctx := t.Context()
	tablet := newTestTablet(t, 1, "ks", "0", nil)
	md := newTestMysqlDaemon(t, 3307)
	md.ServerUUID = serverUUID
	if md.FetchSuperQueryMap == nil {
		md.FetchSuperQueryMap = map[string]*sqltypes.Result{}
	}
	md.FetchSuperQueryMap["SHOW DATABASES"] = sqltypes.MakeTestResult(sqltypes.MakeTestFields("Database", "varchar"), "vt_ks")
	tm := &TabletManager{
		BatchCtx: ctx, TopoServer: ts, MysqlDaemon: md, tmc: newFakeTMClient(), tabletAlias: tablet.Alias,
		actionSema: semaphore.NewWeighted(1),
	}
	tm.tmState = newTMState(tm, tablet)
	t.Cleanup(tm.tmState.cancel)
	require.NoError(t, ts.CreateTablet(ctx, tablet))
	_, err := ts.GetOrCreateShard(ctx, "ks", "0")
	require.NoError(t, err)
	reparenttestutil.SetKeyspaceDurability(ctx, t, ts, "ks", policy.DurabilitySemiSync)
	primary := newTestTablet(t, 2, "ks", "0", nil)
	primary.Type = topodatapb.TabletType_PRIMARY
	primary.MysqlHostname, primary.MysqlPort = "old-primary", 3306
	require.NoError(t, ts.CreateTablet(ctx, primary))
	_, err = ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = primary.Alias
		return nil
	})
	require.NoError(t, err)
	pos, err := replication.ParsePosition(gtidFlavor, gtidPosition)
	require.NoError(t, err)
	md.SetPrimaryPositionLocked(pos)
	md.SetReplicationPositionPos = pos
	md.SetReplicationSourceInputs = []string{"old-primary:3306"}
	md.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "FAKE SET SOURCE", "START REPLICA"}
	return tm, md
}

func holdInitializationShardLock(t *testing.T, ts *topo.Server) func() {
	t.Helper()
	_, unlock, err := ts.LockShard(t.Context(), "ks", "0", "reparent has revoked receiver")
	require.NoError(t, err)
	release := func() {
		if unlock == nil {
			return
		}
		var unlockErr error
		unlock(&unlockErr)
		unlock = nil
		assert.NoError(t, unlockErr)
	}
	t.Cleanup(release)
	return release
}

func TestInitializeReplicationDoesNotReviveReceiverDuringReparent(t *testing.T) {
	tm, md, ts, _ := newReplicationInitializationTM(t)
	release := holdInitializationShardLock(t, ts)

	// memorytopo's TryLock waits for a held lock, so acquisition times out.
	_, err := tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
	require.ErrorContains(t, err, "cannot initialize replication for ks/0")
	_, unavailable := errors.AsType[*shardLockUnavailableError](err)
	assert.True(t, unavailable)
	assert.Equal(t, vtrpcpb.Code_DEADLINE_EXCEEDED, vterrors.Code(err))
	assert.False(t, md.Replicating)
	assert.False(t, md.SemiSyncReplicaEnabled)
	assert.Empty(t, md.CurrentSourceHost)
	assert.Zero(t, md.ExpectedExecuteSuperQueryCurrent, "revoked replication must remain untouched")

	// A subsequent initialization can succeed once the reparent releases its lock.
	release()
	pos, err := tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
	require.NoError(t, err)
	assert.Equal(t, gtidFlavor+"/"+gtidPosition, pos)
	assert.True(t, md.Replicating)
	assert.True(t, md.SemiSyncReplicaEnabled)
	require.NoError(t, md.CheckSuperQueryList())
}

func TestInitializeReplicationHoldsShardLockThroughReceiverStart(t *testing.T) {
	tm, md, ts, _ := newReplicationInitializationTM(t)
	checks := 0
	md.ExecuteSuperQueryListCallback = func() {
		checks++
		// This runs for both source configuration and the final START REPLICA.
		_, unlock, err := ts.LockShard(t.Context(), "ks", "0", "concurrent reparent", topo.WithAcquireTimeout(100*time.Millisecond))
		if unlock != nil {
			var unlockErr error
			unlock(&unlockErr)
			require.NoError(t, unlockErr)
		}
		assert.True(t, topo.IsErrType(err, topo.Timeout), "reparent must wait until initialization finishes: %v", err)
	}
	_, err := tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
	require.NoError(t, err)
	assert.Equal(t, 2, checks)
	require.NoError(t, md.CheckSuperQueryList())
	// Initialization releases its lock after the receiver has started.
	_, unlock, err := ts.LockShard(t.Context(), "ks", "0", "after initialization")
	require.NoError(t, err)
	unlock(&err)
	require.NoError(t, err)
}

func TestInitializeReplicationValidationFailurePreservesSemiSync(t *testing.T) {
	tm, md, _, _ := newReplicationInitializationTM(t)
	md.PrimaryPositionError = errors.New("cannot read executed position")
	_, err := tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
	require.ErrorContains(t, err, "cannot read executed position")
	assert.False(t, md.SemiSyncReplicaEnabled)
	assert.Zero(t, md.ExpectedExecuteSuperQueryCurrent)
}

func TestInitializeReplicationReadOnlyCasesDoNotLockShard(t *testing.T) {
	for _, tc := range []struct {
		name string
		self bool
	}{{name: "no primary"}, {name: "self primary", self: true}} {
		t.Run(tc.name, func(t *testing.T) {
			self := tc.self
			tm, md, ts, factory := newReplicationInitializationTM(t)
			_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error {
				si.PrimaryAlias = nil
				if self {
					si.PrimaryAlias = tm.Tablet().Alias
				}
				return nil
			})
			require.NoError(t, err)
			holdInitializationShardLock(t, ts)
			before := factory.GetCallStats().Counts()["Lock"]
			_, err = tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
			require.NoError(t, err)
			assert.Equal(t, before, factory.GetCallStats().Counts()["Lock"])
			assert.Zero(t, md.ExpectedExecuteSuperQueryCurrent)
		})
	}
}

func TestInitializeReplicationEmptyPrimaryHostnamePreservesSemiSync(t *testing.T) {
	tm, md, ts, _ := newReplicationInitializationTM(t)
	_, err := ts.UpdateTabletFields(t.Context(), &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}, func(tablet *topodatapb.Tablet) error { tablet.MysqlHostname = ""; return nil })
	require.NoError(t, err)
	_, err = tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
	require.NoError(t, err)
	assert.False(t, md.SemiSyncReplicaEnabled)
	assert.Zero(t, md.ExpectedExecuteSuperQueryCurrent)
}

func TestReplicationInitializationMapsBackendContentionError(t *testing.T) {
	tm, md, _, factory := newReplicationInitializationTM(t)
	factory.AddOperationError(memorytopo.TryLock, ".*", topo.NewError(topo.NodeExists, "shard"))
	_, err := tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_UNAVAILABLE, vterrors.Code(err))
	assert.Zero(t, md.ExpectedExecuteSuperQueryCurrent)
}

func TestInitializeReplicationRereadsPrimaryAfterAcquiringLock(t *testing.T) {
	tm, md, ts, factory := newReplicationInitializationTM(t)
	release := holdInitializationShardLock(t, ts)
	before := factory.GetCallStats().Counts()["TryLock"]
	done := make(chan error, 1)
	go func() { _, err := tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA); done <- err }()
	// Publish the new primary and release the lock while TryLock waits for it.
	require.Eventually(t, func() bool { return factory.GetCallStats().Counts()["TryLock"] > before }, 30*time.Second, time.Millisecond)
	primary := newTestTablet(t, 3, "ks", "0", nil)
	primary.Type = topodatapb.TabletType_PRIMARY
	primary.MysqlHostname, primary.MysqlPort = "new-primary", 3306
	require.NoError(t, ts.CreateTablet(t.Context(), primary))
	_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error { si.PrimaryAlias = primary.Alias; return nil })
	require.NoError(t, err)
	md.SetReplicationSourceInputs = []string{"new-primary:3306"}
	release()
	require.NoError(t, <-done)
	assert.Equal(t, "new-primary", md.CurrentSourceHost)
	require.NoError(t, md.CheckSuperQueryList())
}

func TestInitializationPrimaryStatusHasShortDeadline(t *testing.T) {
	tm, md, ts, _ := newReplicationInitializationTM(t)
	client := &initializationPrimaryClient{TabletManagerClient: tm.tmc, entered: make(chan context.Context, 1), release: make(chan struct{})}
	tm.tmc = client
	t.Cleanup(func() { close(client.release) })
	done := make(chan error, 1)
	start := time.Now()
	go func() { _, err := tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA); done <- err }()
	require.Eventually(t, func() bool { return len(done) == 1 }, 30*time.Second, time.Millisecond)
	require.ErrorIs(t, <-done, context.DeadlineExceeded)
	end := time.Now()
	// The probe must not wait for RemoteOperationTimeout.
	assert.Less(t, end.Sub(start), topo.RemoteOperationTimeout)
	rpcCtx := <-client.entered
	deadline, ok := rpcCtx.Deadline()
	require.True(t, ok)
	assert.False(t, deadline.Before(start.Add(replicationInitializationPrimaryStatusTimeout)))
	assert.False(t, deadline.After(end.Add(replicationInitializationPrimaryStatusTimeout)))
	_, unlock, err := ts.LockShard(t.Context(), "ks", "0", "failover after short probe")
	require.NoError(t, err)
	unlock(&err)
	require.NoError(t, err)
	assert.Zero(t, md.ExpectedExecuteSuperQueryCurrent)
}

func TestRestoreFinalizationShardLockFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		// tryLockErr is returned by TryLock. Without it, memorytopo's TryLock
		// blocks on the held lock, like a lock taken between check and acquire.
		tryLockErr error
		// cancelWhileLocking cancels the restore's context while TryLock waits.
		cancelWhileLocking bool
		deferred           bool
	}{
		{name: "lock held", tryLockErr: topo.NewError(topo.NodeExists, "shard"), deferred: true},
		{name: "acquisition timeout", deferred: true},
		{name: "backend error", tryLockErr: errors.New("topology authorization denied")},
		{name: "parent context canceled", cancelWhileLocking: true},
	} {
		for actionName, action := range map[string]replicationAction{"start": replicationActionStart, "initialize": replicationActionInitialize} {
			t.Run(tc.name+"/"+actionName, func(t *testing.T) {
				tm, md, ts, factory := newReplicationInitializationTM(t)
				holdInitializationShardLock(t, ts)
				// Stop the background retry before the lock is released.
				t.Cleanup(tm.tmState.cancel)
				if tc.tryLockErr != nil {
					factory.AddOperationError(memorytopo.TryLock, ".*", tc.tryLockErr)
				}
				require.NoError(t, tm.tmState.ChangeTabletType(t.Context(), topodatapb.TabletType_RESTORE, DBActionNone))
				md.ExpectedExecuteSuperQueryList = nil
				replCmd := replicationCommand{action: action}
				if action == replicationActionStart {
					// Restore already set the GTID position, so finalization must not reset it.
					md.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "FAKE RESET REPLICA ALL"}
					pos := md.GetPrimaryPositionLocked()
					replCmd.position = &pos
				}
				ctx := t.Context()
				if tc.cancelWhileLocking {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					t.Cleanup(cancel)
					before := factory.GetCallStats().Counts()["TryLock"]
					go func() {
						assert.Eventually(t, func() bool { return factory.GetCallStats().Counts()["TryLock"] > before }, 30*time.Second, time.Millisecond)
						cancel()
					}()
				}

				logger := logutil.NewMemoryLogger()
				rsm := tm.newRestoreStateManager(logger, false)
				rsm.state = restoreStarted
				rsm.nextTabletType = topodatapb.TabletType_REPLICA
				// Restore holds the tablet action lock, which the shard lock holder
				// may need, so it must not wait long for the shard lock.
				start := time.Now()
				err := rsm.finish(ctx, replCmd)
				assert.Less(t, time.Since(start), replicationInitializationLockAcquireTimeout+5*time.Second)

				require.NoError(t, md.CheckSuperQueryList())
				assert.False(t, md.Replicating)
				assert.False(t, md.SemiSyncReplicaEnabled)
				assert.Empty(t, md.CurrentSourceHost)
				if tc.deferred {
					require.NoError(t, err)
					// The tablet becomes a replica type, and initialization is retried in the background.
					assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
					assert.Contains(t, logger.String(), "deferring replication initialization")
					return
				}
				require.ErrorContains(t, err, "cannot initialize replication for ks/0")
				assert.Equal(t, topodatapb.TabletType_RESTORE, tm.Tablet().Type)
				assert.NotContains(t, logger.String(), "deferring replication initialization")
			})
		}
	}
}

func TestStartDoesNotWaitForHeldShardLock(t *testing.T) {
	for _, tc := range []struct {
		name       string
		tryLockErr error
	}{
		{name: "lock held", tryLockErr: topo.NewError(topo.NodeExists, "shard")},
		{name: "acquisition timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
			t.Cleanup(ts.Close)
			_, err := ts.GetOrCreateShard(t.Context(), "ks", "0")
			require.NoError(t, err)
			primary := newTestTablet(t, 2, "ks", "0", nil)
			primary.Type = topodatapb.TabletType_PRIMARY
			primary.MysqlHostname, primary.MysqlPort = "old-primary", 3306
			require.NoError(t, ts.CreateTablet(t.Context(), primary))
			_, err = ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error { si.PrimaryAlias = primary.Alias; return nil })
			require.NoError(t, err)
			holdInitializationShardLock(t, ts)
			if tc.tryLockErr != nil {
				factory.AddOperationError(memorytopo.TryLock, ".*", tc.tryLockErr)
			}

			// The shard lock holder may need RPCs to this tablet, which it does
			// not serve until Start returns.
			start := time.Now()
			tm := newTestTM(t, ts, 1, "ks", "0", nil)
			t.Cleanup(tm.Stop)
			assert.Less(t, time.Since(start), replicationInitializationLockAcquireTimeout+5*time.Second)

			md := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
			assert.False(t, tm.QueryServiceControl.(*tabletservermock.Controller).IsServing())
			assert.False(t, md.Replicating)
			assert.False(t, md.SemiSyncReplicaEnabled)
			assert.Empty(t, md.CurrentSourceHost)
			assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
		})
	}
}

func TestDeferredReplicationInitializationRetriesAfterLockRelease(t *testing.T) {
	tm, md, ts, _ := newReplicationInitializationTM(t)
	qsc := tabletservermock.NewController()
	require.NoError(t, qsc.InitDBConfig(&querypb.Target{Keyspace: "ks", Shard: "0"}, nil, md))
	tm.QueryServiceControl = qsc
	tm.tmState.Open()
	require.True(t, qsc.IsServing())
	release := holdInitializationShardLock(t, ts)
	started := make(chan struct{}, 2)
	// Runs for source configuration and START REPLICA.
	md.ExecuteSuperQueryListCallback = func() { started <- struct{}{} }

	logger := logutil.NewMemoryLogger()
	_, err := tm.initializeReplicationUnlessShardLocked(t.Context(), logger, topodatapb.TabletType_REPLICA)
	require.NoError(t, err)
	assert.Contains(t, logger.String(), "deferring replication initialization")
	assert.Empty(t, started, "deferral must not change replication")
	assert.False(t, qsc.IsServing(), "a replica must not serve before replication is initialized")

	release()
	for range 2 {
		select {
		case <-started:
		case <-time.After(30 * time.Second):
			require.FailNow(t, "deferred replication initialization was not retried")
		}
	}
	// The retry releases the shard lock after it has changed replication.
	_, unlock, err := ts.LockShard(t.Context(), "ks", "0", "after retry")
	require.NoError(t, err)
	unlock(&err)
	require.NoError(t, err)
	assert.Equal(t, "old-primary", md.CurrentSourceHost)
	require.NoError(t, md.CheckSuperQueryList())
	// The retry opens query service after it releases the shard lock.
	assert.Eventually(t, qsc.IsServing, 30*time.Second, 10*time.Millisecond)
}

func TestPendingReplicationOnlyWithholdsNonPrimaryService(t *testing.T) {
	tm, _, _, _ := newReplicationInitializationTM(t)
	tm.tmState.pendingReplicationInit = 1
	assert.Equal(t, "replication is not initialized", tm.tmState.canServe(topodatapb.TabletType_REPLICA))
	assert.Equal(t, "replication is not initialized", tm.tmState.canServe(topodatapb.TabletType_RDONLY))
	assert.Empty(t, tm.tmState.canServe(topodatapb.TabletType_PRIMARY))
}

func newPendingReplicationTM(t *testing.T) (*TabletManager, *mysqlctl.FakeMysqlDaemon, *topo.Server, *tabletservermock.Controller, uint64) {
	t.Helper()
	tm, md, ts, _ := newReplicationInitializationTM(t)
	qsc := tabletservermock.NewController()
	require.NoError(t, qsc.InitDBConfig(&querypb.Target{Keyspace: "ks", Shard: "0"}, nil, md))
	tm.QueryServiceControl = qsc
	tm.tmState.Open()
	id := tm.tmState.deferReplicationInit(t.Context())
	require.False(t, qsc.IsServing())
	return tm, md, ts, qsc, id
}

func TestDeferredReplicationInitializationWaitsForUsablePrimary(t *testing.T) {
	for _, tc := range []struct {
		name   string
		update func(t *testing.T, tm *TabletManager, ts *topo.Server)
	}{{
		name: "no primary",
		update: func(t *testing.T, tm *TabletManager, ts *topo.Server) {
			_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error { si.PrimaryAlias = nil; return nil })
			require.NoError(t, err)
		},
	}, {
		name: "shard names this tablet",
		update: func(t *testing.T, tm *TabletManager, ts *topo.Server) {
			_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error { si.PrimaryAlias = tm.Tablet().Alias; return nil })
			require.NoError(t, err)
		},
	}, {
		name: "primary has no MySQL hostname",
		update: func(t *testing.T, tm *TabletManager, ts *topo.Server) {
			_, err := ts.UpdateTabletFields(t.Context(), &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}, func(tablet *topodatapb.Tablet) error { tablet.MysqlHostname = ""; return nil })
			require.NoError(t, err)
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			tm, md, ts, qsc, id := newPendingReplicationTM(t)
			tc.update(t, tm, ts)
			md.ExpectedExecuteSuperQueryList = nil

			require.Error(t, tm.retryReplicationInitializationOnce(t.Context(), id))
			assert.False(t, md.Replicating)
			assert.False(t, qsc.IsServing(), "a replica must not serve until replication is configured")
			require.NoError(t, md.CheckSuperQueryList())
		})
	}
}

func TestDeferredReplicationInitializationCompletesOnPromotion(t *testing.T) {
	tm, md, _, qsc, id := newPendingReplicationTM(t)
	md.ExpectedExecuteSuperQueryList = nil
	require.NoError(t, tm.tmState.ChangeTabletType(t.Context(), topodatapb.TabletType_PRIMARY, DBActionNone))

	require.NoError(t, tm.retryReplicationInitializationOnce(t.Context(), id))
	assert.True(t, qsc.IsServing())
	require.NoError(t, md.CheckSuperQueryList())
}

func TestSupersededReplicationInitializationDoesNotChangeReplication(t *testing.T) {
	t.Run("later deferral", func(t *testing.T) {
		tm, md, _, qsc, first := newPendingReplicationTM(t)
		md.ExpectedExecuteSuperQueryList = nil
		tm.tmState.deferReplicationInit(t.Context())

		require.NoError(t, tm.retryReplicationInitializationOnce(t.Context(), first))
		assert.False(t, md.Replicating)
		assert.False(t, qsc.IsServing(), "the later deferral is still pending")
		require.NoError(t, md.CheckSuperQueryList())
	})
	t.Run("completed", func(t *testing.T) {
		tm, md, _, qsc, id := newPendingReplicationTM(t)
		require.NoError(t, tm.retryReplicationInitializationOnce(t.Context(), id))
		require.NoError(t, md.CheckSuperQueryList())
		require.True(t, qsc.IsServing())

		md.ExpectedExecuteSuperQueryList = nil
		md.ExpectedExecuteSuperQueryCurrent = 0
		require.NoError(t, tm.retryReplicationInitializationOnce(t.Context(), id))
		require.NoError(t, md.CheckSuperQueryList())
	})
	t.Run("point-in-time restore disables replication", func(t *testing.T) {
		tm, md, _, _, id := newPendingReplicationTM(t)
		require.NoError(t, tm.tmState.ChangeTabletType(t.Context(), topodatapb.TabletType_RESTORE, DBActionNone))
		require.Error(t, tm.retryReplicationInitializationOnce(t.Context(), id), "a retry must wait while the restore runs")

		md.SetReplicationSourceInputs = []string{"//:0"}
		// disableReplication points replication at an impossible source.
		md.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "FAKE RESET REPLICA ALL", "FAKE SET SOURCE", "START REPLICA"}
		rsm := tm.newRestoreStateManager(logutil.NewMemoryLogger(), false)
		rsm.state = restoreStarted
		rsm.nextTabletType = topodatapb.TabletType_DRAINED
		require.NoError(t, rsm.finish(t.Context(), replicationCommand{action: replicationActionDisable}))

		require.NoError(t, tm.retryReplicationInitializationOnce(t.Context(), id))
		assert.Equal(t, "//", md.CurrentSourceHost)
		require.NoError(t, md.CheckSuperQueryList())
	})
	t.Run("dry-run restore", func(t *testing.T) {
		tm, md, _, qsc, id := newPendingReplicationTM(t)
		require.NoError(t, tm.tmState.ChangeTabletType(t.Context(), topodatapb.TabletType_RESTORE, DBActionNone))
		rsm := tm.newRestoreStateManager(logutil.NewMemoryLogger(), false)
		rsm.dryRun = true
		rsm.state = restoreStarted
		rsm.nextTabletType = topodatapb.TabletType_REPLICA
		require.NoError(t, rsm.finish(t.Context(), replicationCommand{action: replicationActionNone}))
		assert.False(t, qsc.IsServing(), "a dry run does not initialize replication")

		// The data did not change, so the earlier initialization still applies.
		require.NoError(t, tm.retryReplicationInitializationOnce(t.Context(), id))
		assert.Equal(t, "old-primary", md.CurrentSourceHost)
		assert.True(t, qsc.IsServing())
		require.NoError(t, md.CheckSuperQueryList())
	})
	t.Run("aborted restore", func(t *testing.T) {
		tm, md, _, qsc, id := newPendingReplicationTM(t)
		require.NoError(t, tm.tmState.ChangeTabletType(t.Context(), topodatapb.TabletType_RESTORE, DBActionNone))
		rsm := tm.newRestoreStateManager(logutil.NewMemoryLogger(), false)
		rsm.state = restoreStarted
		rsm.prevTabletType = topodatapb.TabletType_REPLICA
		require.NoError(t, rsm.abort())

		// The data did not change, so the earlier initialization still applies.
		require.NoError(t, tm.retryReplicationInitializationOnce(t.Context(), id))
		assert.Equal(t, "old-primary", md.CurrentSourceHost)
		assert.True(t, qsc.IsServing())
		require.NoError(t, md.CheckSuperQueryList())
	})
}
