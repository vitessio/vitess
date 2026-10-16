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
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql"
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

// deadlineRecordingDaemon records the deadlines of the contexts passed to its
// replication reads.
type deadlineRecordingDaemon struct {
	*mysqlctl.FakeMysqlDaemon
	deadlines []time.Time
}

func (d *deadlineRecordingDaemon) record(ctx context.Context) {
	deadline, _ := ctx.Deadline()
	d.deadlines = append(d.deadlines, deadline)
}

func (d *deadlineRecordingDaemon) ReplicationStatus(ctx context.Context) (replication.ReplicationStatus, error) {
	d.record(ctx)
	return d.FakeMysqlDaemon.ReplicationStatus(ctx)
}

func (d *deadlineRecordingDaemon) PrimaryPosition(ctx context.Context) (replication.Position, error) {
	d.record(ctx)
	return d.FakeMysqlDaemon.PrimaryPosition(ctx)
}

// blockingServingController blocks the transition that enables query service
// until released.
type blockingServingController struct {
	*tabletservermock.Controller
	enabling chan struct{}
	release  chan struct{}
}

func (c *blockingServingController) SetServingType(tabletType topodatapb.TabletType, ptsTime time.Time, serving bool, reason string) error {
	if serving {
		c.enabling <- struct{}{}
		<-c.release
	}
	return c.Controller.SetServingType(tabletType, ptsTime, serving, reason)
}

// failingPrimaryStatusClient fails PrimaryStatus for one tablet.
type failingPrimaryStatusClient struct {
	tmclient.TabletManagerClient
	failing *topodatapb.TabletAlias
}

func (c *failingPrimaryStatusClient) PrimaryStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
	if proto.Equal(tablet.Alias, c.failing) {
		return nil, errors.New("primary status unavailable")
	}
	return c.TabletManagerClient.PrimaryStatus(ctx, tablet)
}

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
	tm, md := newReplicationInitializationTMWithServer(t, ts, 1)
	return tm, md, ts, factory
}

// newReplicationInitializationTMWithServer adds replica uid to shard ks/0,
// whose primary is tablet 2. The first call creates the shard and its primary.
func newReplicationInitializationTMWithServer(t *testing.T, ts *topo.Server, uid int) (*TabletManager, *mysqlctl.FakeMysqlDaemon) {
	t.Helper()
	ctx := t.Context()
	tablet := newTestTablet(t, uid, "ks", "0", nil)
	md := newTestMysqlDaemon(t, int32(3306+uid))
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
	primary := newTestTablet(t, 2, "ks", "0", nil)
	if _, err := ts.GetTablet(ctx, primary.Alias); topo.IsErrType(err, topo.NoNode) {
		_, err := ts.GetOrCreateShard(ctx, "ks", "0")
		require.NoError(t, err)
		reparenttestutil.SetKeyspaceDurability(ctx, t, ts, "ks", policy.DurabilitySemiSync)
		primary.Type = topodatapb.TabletType_PRIMARY
		primary.MysqlHostname, primary.MysqlPort = "old-primary", 3306
		require.NoError(t, ts.CreateTablet(ctx, primary))
		_, err = ts.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
			si.PrimaryAlias = primary.Alias
			return nil
		})
		require.NoError(t, err)
	}
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
	id := tm.tmState.deferReplicationInit(t.Context(), nil)
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
		tm.tmState.deferReplicationInit(t.Context(), nil)

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

func TestDeferredReplicationInitializationKeepsExistingReplication(t *testing.T) {
	for _, tc := range []struct {
		name       string
		replicaPos string
		errant     bool
		// notAcking is set when the receiver does not acknowledge, although
		// the semi_sync durability policy requires it.
		notAcking bool
	}{
		{name: "no errant GTID", replicaPos: gtidPosition},
		{name: "errant GTID", replicaPos: gtidPosition + ",8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1", errant: true},
		{name: "receiver not acknowledging", replicaPos: gtidPosition, notAcking: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tm, md, _, qsc, id := newPendingReplicationTM(t)
			// Replication already runs from the current primary, as after a
			// reparent or a vttablet restart that left mysqld replicating.
			md.Replicating, md.IOThreadRunning = true, true
			md.CurrentSourceHost, md.CurrentSourcePort = "old-primary", 3306
			pos, err := replication.ParsePosition(gtidFlavor, tc.replicaPos)
			require.NoError(t, err)
			md.SetPrimaryPositionLocked(pos)
			md.SemiSyncReplicaEnabled = !tc.notAcking
			md.ExpectedExecuteSuperQueryList = nil
			if tc.notAcking {
				// Restart the receiver so it acknowledges, without changing its source.
				md.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}
			}

			err = tm.retryReplicationInitializationOnce(t.Context(), id)
			require.NoError(t, md.CheckSuperQueryList(), "replication that is already running must keep its source, and restart only to acknowledge")
			if tc.errant {
				require.ErrorContains(t, err, "Errant GTID detected")
				assert.False(t, qsc.IsServing(), "a replica with an errant GTID must not serve")
				return
			}
			require.NoError(t, err)
			assert.True(t, md.SemiSyncReplicaEnabled)
			assert.True(t, qsc.IsServing())
		})
	}
}

func TestDeferredReplicationRetryLocksShardBeforeActionLock(t *testing.T) {
	t.Run("does not hold the action lock while acquiring the shard lock", func(t *testing.T) {
		tm, md, ts, factory := newReplicationInitializationTM(t)
		id := tm.tmState.deferReplicationInit(t.Context(), nil)
		md.ExpectedExecuteSuperQueryList = nil
		holdInitializationShardLock(t, ts)
		before := factory.GetCallStats().Counts()["TryLock"]
		done := make(chan error, 1)
		go func() { done <- tm.retryReplicationInitializationOnce(t.Context(), id) }()
		require.Eventually(t, func() bool { return factory.GetCallStats().Counts()["TryLock"] > before }, 30*time.Second, time.Millisecond)

		// memorytopo's TryLock waits for the held lock; an RPC must not wait for it.
		require.True(t, tm.actionSema.TryAcquire(1), "the retry must not hold the action lock while it acquires the shard lock")
		tm.unlock()
		err := <-done
		_, unavailable := errors.AsType[*shardLockUnavailableError](err)
		assert.True(t, unavailable, "%v", err)
		require.NoError(t, md.CheckSuperQueryList())
	})
	t.Run("does not wait for a busy tablet", func(t *testing.T) {
		tm, md, ts, qsc, id := newPendingReplicationTM(t)
		md.ExpectedExecuteSuperQueryList = nil
		require.NoError(t, tm.lock(t.Context()))
		t.Cleanup(tm.unlock)

		// The deadline makes a retry that waits for the action lock fail instead of hang.
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		t.Cleanup(cancel)
		require.ErrorContains(t, tm.retryReplicationInitializationOnce(ctx, id), "another action is running on this tablet")
		assert.False(t, qsc.IsServing())
		require.NoError(t, md.CheckSuperQueryList())
		// The retry released the shard lock.
		_, unlock, err := ts.LockShard(t.Context(), "ks", "0", "after busy retry", topo.WithAcquireTimeout(30*time.Second))
		require.NoError(t, err)
		unlock(&err)
		require.NoError(t, err)
	})
}

func TestPendingReplicationReasonShowsLatestError(t *testing.T) {
	tm, md, ts, factory := newReplicationInitializationTM(t)
	holdInitializationShardLock(t, ts)
	factory.AddOperationError(memorytopo.TryLock, ".*", topo.NewError(topo.NodeExists, "shard"))
	md.ExpectedExecuteSuperQueryList = nil
	_, err := tm.initializeReplicationUnlessShardLocked(t.Context(), logutil.NewMemoryLogger(), topodatapb.TabletType_REPLICA)
	require.NoError(t, err)
	// Stop the background retry, so that only this test changes the reason.
	tm.tmState.cancel()
	assert.Contains(t, tm.tmState.canServe(topodatapb.TabletType_REPLICA), "replication is not initialized: cannot initialize replication for ks/0; replication has not been changed: another operation holds the shard lock")

	id := tm.tmState.pendingReplicationInit
	tm.tmState.setReplicationInitError(t.Context(), id, errors.New("Errant GTID detected"))
	assert.Equal(t, "replication is not initialized: Errant GTID detected", tm.tmState.canServe(topodatapb.TabletType_REPLICA))
	tm.tmState.setReplicationInitError(t.Context(), id+1, errors.New("superseded"))
	assert.Equal(t, "replication is not initialized: Errant GTID detected", tm.tmState.canServe(topodatapb.TabletType_REPLICA))
}

func TestConcurrentlyDeferredReplicasInitializeReplication(t *testing.T) {
	ts, _ := memorytopo.NewServerAndFactory(t.Context(), "cell1")
	t.Cleanup(ts.Close)
	type replica struct {
		tm  *TabletManager
		md  *mysqlctl.FakeMysqlDaemon
		qsc *tabletservermock.Controller
	}
	var replicas []replica
	for _, uid := range []int{1, 3} {
		tm, md := newReplicationInitializationTMWithServer(t, ts, uid)
		qsc := tabletservermock.NewController()
		require.NoError(t, qsc.InitDBConfig(&querypb.Target{Keyspace: "ks", Shard: "0"}, nil, md))
		tm.QueryServiceControl = qsc
		tm.tmState.Open()
		replicas = append(replicas, replica{tm, md, qsc})
	}

	release := holdInitializationShardLock(t, ts)
	for _, r := range replicas {
		_, err := r.tm.initializeReplicationUnlessShardLocked(t.Context(), logutil.NewMemoryLogger(), topodatapb.TabletType_REPLICA)
		require.NoError(t, err)
		require.False(t, r.qsc.IsServing())
	}
	release()

	// Both retries contend for the shard lock, and each eventually takes it.
	for _, r := range replicas {
		assert.Eventually(t, r.qsc.IsServing, 30*time.Second, 10*time.Millisecond)
		assert.Equal(t, "old-primary", r.md.CurrentSourceHost)
		require.NoError(t, r.md.CheckSuperQueryList())
	}
}

func TestDeferredReplicationRetrySkipsShardLockWithNothingToDo(t *testing.T) {
	for _, tc := range []struct {
		name       string
		tabletType topodatapb.TabletType
		supersede  bool
		wantErr    string
		pending    bool
	}{
		{name: "promoted", tabletType: topodatapb.TabletType_PRIMARY},
		{name: "superseded", tabletType: topodatapb.TabletType_REPLICA, supersede: true, pending: true},
		{name: "restore running", tabletType: topodatapb.TabletType_RESTORE, wantErr: "waiting for the restore to finish", pending: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tm, md, ts, factory := newReplicationInitializationTM(t)
			id := tm.tmState.deferReplicationInit(t.Context(), nil)
			if tc.supersede {
				tm.tmState.deferReplicationInit(t.Context(), nil)
			}
			require.NoError(t, tm.tmState.ChangeTabletType(t.Context(), tc.tabletType, DBActionNone))
			md.ExpectedExecuteSuperQueryList = nil
			holdInitializationShardLock(t, ts)
			before := factory.GetCallStats().Counts()["TryLock"]

			err := tm.retryReplicationInitializationOnce(t.Context(), id)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, before, factory.GetCallStats().Counts()["TryLock"], "the retry must not take the shard lock")
			assert.Equal(t, tc.pending, tm.tmState.pendingReplicationInit != 0)
			require.NoError(t, md.CheckSuperQueryList())
		})
	}
}

func TestDeferredReplicationRetryBoundsReadsUnderLocks(t *testing.T) {
	tm, md, _, _, id := newPendingReplicationTM(t)
	// Take the "already replicating" path, which reads replication status and
	// the executed position while holding both locks.
	md.Replicating, md.IOThreadRunning = true, true
	md.CurrentSourceHost, md.CurrentSourcePort = "old-primary", 3306
	md.SemiSyncReplicaEnabled = true
	md.ExpectedExecuteSuperQueryList = nil
	daemon := &deadlineRecordingDaemon{FakeMysqlDaemon: md}
	tm.MysqlDaemon = daemon

	require.NoError(t, tm.retryReplicationInitializationOnce(t.Context(), id))
	require.Len(t, daemon.deadlines, 2)
	latest := time.Now().Add(topo.RemoteOperationTimeout)
	for _, deadline := range daemon.deadlines {
		require.False(t, deadline.IsZero(), "reads under the locks must have a deadline")
		assert.False(t, deadline.After(latest))
	}
}

func TestReparentSourceEndsDeferredReplicationInitialization(t *testing.T) {
	tm, md, ts, qsc, id := newPendingReplicationTM(t)
	// A reparent promoted tablet 3, but the shard record still names the old
	// primary, because the new primary has not published itself yet.
	newPrimary := newTestTablet(t, 3, "ks", "0", nil)
	newPrimary.Type = topodatapb.TabletType_PRIMARY
	newPrimary.MysqlHostname, newPrimary.MysqlPort = "new-primary", 3306
	require.NoError(t, ts.CreateTablet(t.Context(), newPrimary))
	md.SetReplicationSourceInputs = []string{"new-primary:3306"}
	md.ExpectedExecuteSuperQueryList = []string{"FAKE SET SOURCE", "START REPLICA"}
	// RPCs wait for the DBA grants, which this fixture does not apply.
	tm._waitForGrantsComplete = make(chan struct{})
	close(tm._waitForGrantsComplete)
	// The RPC must not enable query service itself: that can stall on MySQL
	// while the RPC holds the action lock.
	ctrl := &blockingServingController{Controller: qsc, enabling: make(chan struct{}, 1), release: make(chan struct{})}
	tm.QueryServiceControl = ctrl

	done := make(chan error, 1)
	go func() { done <- tm.SetReplicationSource(t.Context(), newPrimary.Alias, 0, "", true, true, 0) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctrl.enabling:
		close(ctrl.release)
		require.FailNow(t, "SetReplicationSource enabled query service")
	case <-time.After(30 * time.Second):
		require.FailNow(t, "SetReplicationSource did not return")
	}
	require.NoError(t, md.CheckSuperQueryList())
	assert.Equal(t, "new-primary", md.CurrentSourceHost)
	assert.False(t, qsc.IsServing())

	// The retry keeps the source the reparent chose, ends the deferral, and
	// enables query service.
	close(ctrl.release)
	md.Replicating, md.IOThreadRunning = true, true
	md.ExpectedExecuteSuperQueryList = nil
	md.ExpectedExecuteSuperQueryCurrent = 0
	require.NoError(t, tm.retryReplicationInitializationOnce(t.Context(), id))
	require.NoError(t, md.CheckSuperQueryList())
	assert.Equal(t, "new-primary", md.CurrentSourceHost)
	assert.True(t, qsc.IsServing())
}

func TestSourceOnlyRPCStartsDeferredReplication(t *testing.T) {
	for _, tc := range []struct {
		name     string
		startErr error
	}{{name: "replication starts"}, {name: "starting replication fails", startErr: errors.New("cannot start replication")}} {
		t.Run(tc.name, func(t *testing.T) {
			tm, md, _, qsc, id := newPendingReplicationTM(t)
			md.StartReplicationError = tc.startErr
			md.ExpectedExecuteSuperQueryList = []string{"FAKE SET SOURCE", "START REPLICA"}
			if tc.startErr != nil {
				md.ExpectedExecuteSuperQueryList = []string{"FAKE SET SOURCE"}
			}
			tm._waitForGrantsComplete = make(chan struct{})
			close(tm._waitForGrantsComplete)

			// PRS, and ERS for a replica it found stopped, do not force replication
			// to start, but replication that was never initialized must start.
			err := tm.SetReplicationSource(t.Context(), &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}, 0, "", false, true, 0)
			require.NoError(t, md.CheckSuperQueryList())
			if tc.startErr != nil {
				require.ErrorContains(t, err, "cannot start replication")
				require.ErrorContains(t, tm.retryReplicationInitializationOnce(t.Context(), id), "(old-primary:3306), which a reparent or VTOrc chose")
				assert.False(t, qsc.IsServing(), "a replica whose replication is stopped must not serve")
				return
			}
			require.NoError(t, err)
			md.IOThreadRunning = true
			require.NoError(t, tm.retryReplicationInitializationOnce(t.Context(), id))
			assert.True(t, qsc.IsServing())
		})
	}
}

func TestSourceOnlyRPCLeavesStoppedReplicationStoppedWithoutDeferral(t *testing.T) {
	tm, md, _, _, _ := newPendingReplicationTM(t)
	tm.tmState.cancelReplicationInit(t.Context())
	md.ExpectedExecuteSuperQueryList = []string{"FAKE SET SOURCE"}
	tm._waitForGrantsComplete = make(chan struct{})
	close(tm._waitForGrantsComplete)

	require.NoError(t, tm.SetReplicationSource(t.Context(), &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}, 0, "", false, true, 0))
	require.NoError(t, md.CheckSuperQueryList())
	assert.False(t, md.Replicating)
}

func TestReparentSourceSurvivesFailedReplicationStart(t *testing.T) {
	for _, tc := range []struct {
		name    string
		queries []string
		rpc     func(ctx context.Context, tm *TabletManager, primary *topodatapb.TabletAlias) error
	}{{
		name:    "SetReplicationSource",
		queries: []string{"FAKE SET SOURCE"},
		rpc: func(ctx context.Context, tm *TabletManager, primary *topodatapb.TabletAlias) error {
			return tm.SetReplicationSource(ctx, primary, 0, "", true, true, 0)
		},
	}, {
		name:    "InitReplica",
		queries: []string{"FAKE RESET BINARY LOGS AND GTIDS", "FAKE SET GLOBAL gtid_purged", "FAKE SET SOURCE"},
		rpc: func(ctx context.Context, tm *TabletManager, primary *topodatapb.TabletAlias) error {
			return tm.InitReplica(ctx, primary, gtidFlavor+"/"+gtidPosition, 0, true)
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			tm, md, ts, qsc, id := newPendingReplicationTM(t)
			// The shard record still names the old primary.
			newPrimary := newTestTablet(t, 3, "ks", "0", nil)
			newPrimary.Type = topodatapb.TabletType_PRIMARY
			newPrimary.MysqlHostname, newPrimary.MysqlPort = "new-primary", 3306
			require.NoError(t, ts.CreateTablet(t.Context(), newPrimary))
			md.SetReplicationSourceInputs = []string{"new-primary:3306"}
			md.ExpectedExecuteSuperQueryList = tc.queries
			md.StartReplicationError = errors.New("cannot start replication")
			tm._waitForGrantsComplete = make(chan struct{})
			close(tm._waitForGrantsComplete)

			// The RPC installs its source, but replication does not start.
			require.ErrorContains(t, tc.rpc(t.Context(), tm, newPrimary.Alias), "cannot start replication")
			require.NoError(t, md.CheckSuperQueryList())
			assert.False(t, qsc.IsServing(), "replication is not running")

			// The retry keeps the source the RPC chose, and keeps the replica
			// out of rotation until replication runs.
			md.ExpectedExecuteSuperQueryList = nil
			md.ExpectedExecuteSuperQueryCurrent = 0
			require.ErrorContains(t, tm.retryReplicationInitializationOnce(t.Context(), id), "waiting for replication")
			require.NoError(t, md.CheckSuperQueryList())
			assert.Equal(t, "new-primary", md.CurrentSourceHost)
			assert.False(t, qsc.IsServing())

			// Once replication runs, for example after VTOrc restarts it, the
			// deferral ends.
			md.Replicating, md.IOThreadRunning = true, true
			require.NoError(t, tm.retryReplicationInitializationOnce(t.Context(), id))
			assert.Equal(t, "new-primary", md.CurrentSourceHost)
			assert.True(t, qsc.IsServing())
		})
	}
}

func TestDeferredReplicationRetryEnablesServingAfterReleasingLocks(t *testing.T) {
	tm, md, ts, qsc, id := newPendingReplicationTM(t)
	ctrl := &blockingServingController{Controller: qsc, enabling: make(chan struct{}, 1), release: make(chan struct{})}
	tm.QueryServiceControl = ctrl
	done := make(chan error, 1)
	go func() { done <- tm.retryReplicationInitializationOnce(t.Context(), id) }()
	select {
	case <-ctrl.enabling:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the retry did not enable query service")
	}

	// Enabling query service can stall on MySQL; it must not hold either lock.
	require.True(t, tm.actionSema.TryAcquire(1), "the retry must release the action lock before enabling query service")
	tm.unlock()
	_, unlock, err := ts.LockShard(t.Context(), "ks", "0", "reparent", topo.WithAcquireTimeout(30*time.Second))
	require.NoError(t, err, "the retry must release the shard lock before enabling query service")
	unlock(&err)
	require.NoError(t, err)

	close(ctrl.release)
	require.NoError(t, <-done)
	require.NoError(t, md.CheckSuperQueryList())
	assert.True(t, qsc.IsServing())
}

func TestFailedReparentSourceKeepsReplicaOutOfRotation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(md *mysqlctl.FakeMysqlDaemon)
		rpc   func(ctx context.Context, tm *TabletManager, primary *topodatapb.TabletAlias) error
	}{{
		name:  "SetReplicationSource cannot install the source",
		setup: func(md *mysqlctl.FakeMysqlDaemon) { md.SetReplicationSourceError = errors.New("cannot change source") },
		rpc: func(ctx context.Context, tm *TabletManager, primary *topodatapb.TabletAlias) error {
			return tm.SetReplicationSource(ctx, primary, 0, "", true, true, 0)
		},
	}, {
		name:  "InitReplica cannot set the position",
		setup: func(md *mysqlctl.FakeMysqlDaemon) {},
		rpc: func(ctx context.Context, tm *TabletManager, primary *topodatapb.TabletAlias) error {
			// A position other than the fake's expected one fails to apply.
			return tm.InitReplica(ctx, primary, gtidFlavor+"/"+serverUUID+":1-100", 0, true)
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			tm, md, ts, qsc, id := newPendingReplicationTM(t)
			newPrimary := newTestTablet(t, 3, "ks", "0", nil)
			newPrimary.Type = topodatapb.TabletType_PRIMARY
			newPrimary.MysqlHostname, newPrimary.MysqlPort = "new-primary", 3306
			require.NoError(t, ts.CreateTablet(t.Context(), newPrimary))
			// The new primary is published, but the replica still replicates
			// from the old one, which the failed RPC did not replace.
			_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error { si.PrimaryAlias = newPrimary.Alias; return nil })
			require.NoError(t, err)
			md.Replicating, md.IOThreadRunning, md.SemiSyncReplicaEnabled = true, true, true
			md.CurrentSourceHost, md.CurrentSourcePort = "old-primary", 3306
			md.SetReplicationSourceInputs = []string{"new-primary:3306"}
			md.ExpectedExecuteSuperQueryList = nil
			tc.setup(md)
			tm._waitForGrantsComplete = make(chan struct{})
			close(tm._waitForGrantsComplete)

			require.Error(t, tc.rpc(t.Context(), tm, newPrimary.Alias))
			assert.Equal(t, "old-primary", md.CurrentSourceHost)

			// Replication runs, but not from the source the RPC chose.
			md.ExpectedExecuteSuperQueryList = nil
			md.ExpectedExecuteSuperQueryCurrent = 0
			require.ErrorContains(t, tm.retryReplicationInitializationOnce(t.Context(), id), "(new-primary:3306), which a reparent or VTOrc chose")
			require.NoError(t, md.CheckSuperQueryList())
			assert.False(t, qsc.IsServing(), "a replica must not serve from a source other than the one the RPC chose")
		})
	}
}

func TestChosenReplicationSourceCompletion(t *testing.T) {
	setup := func(t *testing.T) (*TabletManager, *mysqlctl.FakeMysqlDaemon, *tabletservermock.Controller, uint64) {
		tm, md, ts, qsc, id := newPendingReplicationTM(t)
		newPrimary := newTestTablet(t, 3, "ks", "0", nil)
		newPrimary.Type = topodatapb.TabletType_PRIMARY
		newPrimary.MysqlHostname, newPrimary.MysqlPort = "new-primary", 3306
		require.NoError(t, ts.CreateTablet(t.Context(), newPrimary))
		// An RPC chose the new primary, and replication runs from it,
		// acknowledging as the semi_sync durability policy requires.
		tm.tmState.replicationSourceChosen(newPrimary.Alias)
		md.Replicating, md.IOThreadRunning, md.SemiSyncReplicaEnabled = true, true, true
		md.CurrentSourceHost, md.CurrentSourcePort = "new-primary", 3306
		md.ExpectedExecuteSuperQueryList = nil
		return tm, md, qsc, id
	}
	t.Run("waits for a running action", func(t *testing.T) {
		tm, _, qsc, id := setup(t)
		// An RPC that may still change replication holds the action lock.
		require.NoError(t, tm.lock(t.Context()))
		require.ErrorContains(t, tm.retryReplicationInitializationOnce(t.Context(), id), "another action is running")
		assert.False(t, qsc.IsServing())
		tm.unlock()

		require.NoError(t, tm.retryReplicationInitializationOnce(t.Context(), id))
		assert.True(t, qsc.IsServing())
	})
	t.Run("checks errant GTIDs", func(t *testing.T) {
		tm, md, qsc, id := setup(t)
		pos, err := replication.ParsePosition(gtidFlavor, gtidPosition+",8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1")
		require.NoError(t, err)
		md.SetPrimaryPositionLocked(pos)

		require.ErrorContains(t, tm.retryReplicationInitializationOnce(t.Context(), id), "Errant GTID detected")
		assert.False(t, qsc.IsServing(), "a replica with an errant GTID must not serve")
		require.NoError(t, md.CheckSuperQueryList())
	})
}

func TestReparentSourceSurvivesFailedValidation(t *testing.T) {
	tm, md, ts, qsc, id := newPendingReplicationTM(t)
	// The shard record still names the old primary.
	newPrimary := newTestTablet(t, 3, "ks", "0", nil)
	newPrimary.Type = topodatapb.TabletType_PRIMARY
	newPrimary.MysqlHostname, newPrimary.MysqlPort = "new-primary", 3306
	require.NoError(t, ts.CreateTablet(t.Context(), newPrimary))
	tm.tmc = &failingPrimaryStatusClient{TabletManagerClient: tm.tmc, failing: newPrimary.Alias}
	md.ExpectedExecuteSuperQueryList = nil
	tm._waitForGrantsComplete = make(chan struct{})
	close(tm._waitForGrantsComplete)

	// The RPC fails before it changes the source.
	require.ErrorContains(t, tm.SetReplicationSource(t.Context(), newPrimary.Alias, 0, "", true, true, 0), "primary status unavailable")

	// The retry must not fall back to the stale shard record.
	require.ErrorContains(t, tm.retryReplicationInitializationOnce(t.Context(), id), "(new-primary:3306), which a reparent or VTOrc chose")
	require.NoError(t, md.CheckSuperQueryList())
	assert.False(t, qsc.IsServing())
}

type failingSemiSyncProbeDaemon struct {
	*mysqlctl.FakeMysqlDaemon
}

func (d *failingSemiSyncProbeDaemon) SemiSyncExtensionLoaded(context.Context) (mysql.SemiSyncType, error) {
	return mysql.SemiSyncTypeUnknown, errors.New("semi-sync plugin probe failed")
}

func TestReparentSourceSurvivesEarlyRPCFailure(t *testing.T) {
	for _, rpc := range []struct {
		name string
		call func(context.Context, *TabletManager, *topodatapb.TabletAlias) error
	}{
		{name: "SetReplicationSource", call: func(ctx context.Context, tm *TabletManager, alias *topodatapb.TabletAlias) error {
			return tm.SetReplicationSource(ctx, alias, 0, "", true, true, 0)
		}},
		{name: "InitReplica", call: func(ctx context.Context, tm *TabletManager, alias *topodatapb.TabletAlias) error {
			return tm.InitReplica(ctx, alias, gtidFlavor+"/"+gtidPosition, 0, true)
		}},
	} {
		for _, failure := range []struct {
			name string
			err  string
			// fail makes the RPC fail early, and returns the context to call it
			// with and a function that undoes the failure.
			fail func(t *testing.T, tm *TabletManager, md *mysqlctl.FakeMysqlDaemon) (context.Context, func())
		}{
			{name: "semi-sync plugin probe", err: "semi-sync plugin probe failed", fail: func(t *testing.T, tm *TabletManager, md *mysqlctl.FakeMysqlDaemon) (context.Context, func()) {
				tm.MysqlDaemon = &failingSemiSyncProbeDaemon{FakeMysqlDaemon: md}
				return t.Context(), func() { tm.MysqlDaemon = md }
			}},
			{name: "busy action lock", err: "deadline exceeded", fail: func(t *testing.T, tm *TabletManager, md *mysqlctl.FakeMysqlDaemon) (context.Context, func()) {
				require.NoError(t, tm.lock(t.Context()))
				ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
				t.Cleanup(cancel)
				return ctx, tm.unlock
			}},
		} {
			t.Run(rpc.name+"/"+failure.name, func(t *testing.T) {
				tm, md, ts, qsc, id := newPendingReplicationTM(t)
				newPrimary := newTestTablet(t, 3, "ks", "0", nil)
				newPrimary.Type = topodatapb.TabletType_PRIMARY
				newPrimary.MysqlHostname, newPrimary.MysqlPort = "new-primary", 3306
				require.NoError(t, ts.CreateTablet(t.Context(), newPrimary))
				md.ExpectedExecuteSuperQueryList = nil
				tm._waitForGrantsComplete = make(chan struct{})
				close(tm._waitForGrantsComplete)

				ctx, undo := failure.fail(t, tm, md)
				require.ErrorContains(t, rpc.call(ctx, tm, newPrimary.Alias), failure.err)
				undo()

				// The shard record still names the old primary. The retry must
				// keep the RPC's target.
				require.ErrorContains(t, tm.retryReplicationInitializationOnce(t.Context(), id), "(new-primary:3306), which a reparent or VTOrc chose")
				require.NoError(t, md.CheckSuperQueryList())
				assert.Empty(t, md.CurrentSourceHost)
				assert.False(t, qsc.IsServing())
			})
		}
	}
}

// blockedCommitDaemon is a former primary with a commit waiting for semi-sync
// acknowledgements. Disabling source-side semi-sync releases the commit, which
// reaches gtid_executed only when it finishes. Enabling super_read_only waits
// for it to finish, and until ctx is done while it still waits for
// acknowledgements or while stuck is set.
type blockedCommitDaemon struct {
	*mysqlctl.FakeMysqlDaemon
	committed replication.Position
	released  bool
	stuck     bool
	// disableErr is returned, after disabling source-side semi-sync, once.
	disableErr error
}

func (d *blockedCommitDaemon) SetSemiSyncEnabled(ctx context.Context, primary, replica bool) error {
	disabling := d.SemiSyncPrimaryEnabled && !primary
	d.released = d.released || disabling
	if err := d.FakeMysqlDaemon.SetSemiSyncEnabled(ctx, primary, replica); err != nil {
		return err
	}
	if err := d.disableErr; disabling && err != nil {
		d.disableErr = nil
		return err
	}
	return nil
}

func (d *blockedCommitDaemon) SetSuperReadOnly(ctx context.Context, on bool, opts ...mysqlctl.SetSuperReadOnlyOption) (mysqlctl.ResetSuperReadOnlyFunc, error) {
	if on && !d.SuperReadOnly.Load() {
		if !d.released || d.stuck {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		d.SetPrimaryPositionLocked(d.committed)
	}
	return d.FakeMysqlDaemon.SetSuperReadOnly(ctx, on, opts...)
}

// formerPrimaryInitializations are the paths that initialize replication on a
// former primary: startup, and a retry that configures the source or finds it
// already running.
var formerPrimaryInitializations = []struct {
	name    string
	running bool
	retry   bool
}{
	{name: "startup"},
	{name: "retry configures source", retry: true},
	{name: "retry already running", running: true, retry: true},
}

func newFormerPrimaryTM(t *testing.T, running bool, commitGTID string) (*TabletManager, *blockedCommitDaemon, *tabletservermock.Controller, uint64) {
	t.Helper()
	tm, md, _, qsc, id := newPendingReplicationTM(t)
	md.SemiSyncPrimaryEnabled = true
	if running {
		md.Replicating, md.IOThreadRunning, md.SemiSyncReplicaEnabled = true, true, true
		md.CurrentSourceHost, md.CurrentSourcePort = "old-primary", 3306
		md.ExpectedExecuteSuperQueryList = nil
	}
	committed, err := replication.ParsePosition(gtidFlavor, gtidPosition+commitGTID)
	require.NoError(t, err)
	d := &blockedCommitDaemon{FakeMysqlDaemon: md, committed: committed}
	tm.MysqlDaemon = d
	return tm, d, qsc, id
}

func initializeFormerPrimary(ctx context.Context, tm *TabletManager, retry bool, id uint64) error {
	if retry {
		return tm.retryReplicationInitializationOnce(ctx, id)
	}
	_, err := tm.initializeReplication(ctx, topodatapb.TabletType_REPLICA)
	return err
}

func TestFormerPrimaryIsFencedBeforeReplicating(t *testing.T) {
	for _, path := range formerPrimaryInitializations {
		for _, tc := range []struct {
			name       string
			commitGTID string
		}{
			{name: "released commit is errant", commitGTID: ",8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1"},
			{name: "nothing to release"},
		} {
			t.Run(path.name+"/"+tc.name, func(t *testing.T) {
				tm, md, qsc, id := newFormerPrimaryTM(t, path.running, tc.commitGTID)
				if tc.commitGTID != "" {
					md.ExpectedExecuteSuperQueryList = nil
				}

				err := initializeFormerPrimary(t.Context(), tm, path.retry, id)
				assert.True(t, md.SuperReadOnly.Load(), "a former primary must not accept writes once it replicates")
				assert.False(t, md.SemiSyncPrimaryEnabled)
				require.NoError(t, md.CheckSuperQueryList())
				if tc.commitGTID != "" {
					require.ErrorContains(t, err, "Errant GTID detected", "a commit released after the first check must be validated")
					assert.False(t, qsc.IsServing())
					return
				}
				require.NoError(t, err)
				assert.True(t, md.Replicating)
				assert.Equal(t, path.retry, qsc.IsServing())
			})
		}
	}
}

func TestFailedFenceIsRepeatedByTheNextAttempt(t *testing.T) {
	for _, path := range formerPrimaryInitializations {
		for _, tc := range []struct {
			name  string
			setup func(*blockedCommitDaemon)
			err   string
		}{
			{name: "super_read_only times out", setup: func(d *blockedCommitDaemon) { d.stuck = true }, err: "deadline exceeded"},
			{name: "disabling semi-sync fails", setup: func(d *blockedCommitDaemon) { d.disableErr = errors.New("connection lost") }, err: "connection lost"},
			{name: "reading semi-sync fails", setup: func(d *blockedCommitDaemon) { d.ReadSemiSyncEnabledError = errors.New("variables unreadable") }, err: "variables unreadable"},
		} {
			t.Run(path.name+"/"+tc.name, func(t *testing.T) {
				tm, md, qsc, id := newFormerPrimaryTM(t, path.running, ",8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1")
				md.ExpectedExecuteSuperQueryList = nil
				tc.setup(md)

				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				require.ErrorContains(t, initializeFormerPrimary(ctx, tm, path.retry, id), tc.err)
				assert.True(t, md.SemiSyncPrimaryEnabled, "the next attempt must find the former primary unfenced")
				assert.False(t, qsc.IsServing())

				// Released commits may still be finishing. The next attempt
				// must fence again rather than validate at once.
				md.stuck, md.ReadSemiSyncEnabledError = false, nil
				require.ErrorContains(t, initializeFormerPrimary(t.Context(), tm, path.retry, id), "Errant GTID detected")
				assert.True(t, md.SuperReadOnly.Load())
				assert.False(t, qsc.IsServing())
				require.NoError(t, md.CheckSuperQueryList())
			})
		}
	}
}

func TestReplicaWithoutSourceSemiSyncIsNotFenced(t *testing.T) {
	tm, md, _, _ := newReplicationInitializationTM(t)
	_, err := tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
	require.NoError(t, err)
	assert.False(t, md.SuperReadOnly.Load(), "a replica without source-side semi-sync keeps its read-only setting")
	require.NoError(t, md.CheckSuperQueryList())
}

// failRPCBeforeSemiSync makes SetReplicationSource to the shard primary fail
// at its first replication status read, after it has recorded its source but
// before it changes semi-sync. The retry then completes on that source with
// replication as the RPC found it.
func failRPCBeforeSemiSync(t *testing.T, tm *TabletManager, md *mysqlctl.FakeMysqlDaemon) {
	t.Helper()
	tm._waitForGrantsComplete = make(chan struct{})
	close(tm._waitForGrantsComplete)
	md.ReplicationStatusError = errors.New("status read failed")
	require.ErrorContains(t, tm.SetReplicationSource(t.Context(), &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}, 0, "", true, true, 0), "status read failed")
	md.ReplicationStatusError = nil
}

func TestChosenSourceCompletionChecksWhatInitializationChecks(t *testing.T) {
	t.Run("former primary is fenced", func(t *testing.T) {
		tm, md, qsc, id := newFormerPrimaryTM(t, true, ",8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1")
		failRPCBeforeSemiSync(t, tm, md.FakeMysqlDaemon)

		require.ErrorContains(t, tm.retryReplicationInitializationOnce(t.Context(), id), "Errant GTID detected")
		assert.True(t, md.SuperReadOnly.Load())
		assert.False(t, md.SemiSyncPrimaryEnabled)
		assert.False(t, qsc.IsServing())
		require.NoError(t, md.CheckSuperQueryList())
	})
	t.Run("receiver is restarted to acknowledge under the shard lock", func(t *testing.T) {
		tm, md, ts, qsc, id := newPendingReplicationTM(t)
		md.Replicating, md.IOThreadRunning = true, true
		md.CurrentSourceHost, md.CurrentSourcePort = "old-primary", 3306
		md.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "START REPLICA"}
		md.ExecuteSuperQueryListCallback = func() {
			_, unlock, err := ts.LockShard(t.Context(), "ks", "0", "concurrent reparent", topo.WithAcquireTimeout(100*time.Millisecond))
			if unlock != nil {
				var unlockErr error
				unlock(&unlockErr)
			}
			assert.True(t, topo.IsErrType(err, topo.Timeout), "the receiver must restart under the shard lock: %v", err)
		}
		failRPCBeforeSemiSync(t, tm, md)

		require.NoError(t, tm.retryReplicationInitializationOnce(t.Context(), id))
		require.NoError(t, md.CheckSuperQueryList())
		assert.True(t, md.SemiSyncReplicaEnabled)
		assert.Equal(t, "old-primary", md.CurrentSourceHost)
		assert.True(t, qsc.IsServing())
	})
	t.Run("receiver is not restarted without the shard lock", func(t *testing.T) {
		tm, md, ts, qsc, id := newPendingReplicationTM(t)
		md.Replicating, md.IOThreadRunning = true, true
		md.CurrentSourceHost, md.CurrentSourcePort = "old-primary", 3306
		md.ExpectedExecuteSuperQueryList = nil
		failRPCBeforeSemiSync(t, tm, md)
		holdInitializationShardLock(t, ts)

		err := tm.retryReplicationInitializationOnce(t.Context(), id)
		_, unavailable := errors.AsType[*shardLockUnavailableError](err)
		assert.True(t, unavailable, "%v", err)
		require.NoError(t, md.CheckSuperQueryList())
		assert.False(t, qsc.IsServing(), "a replica that does not acknowledge as required must not serve")
	})
}
