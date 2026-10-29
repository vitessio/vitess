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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

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
		// This runs for the stop before the final check, source
		// configuration and the final START REPLICA.
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
	assert.Equal(t, 3, checks)
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
				// Stop the watcher before the lock is released.
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
					// The tablet becomes a replica type, out of rotation until replication runs.
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

func TestPendingReplicationReasonShowsLatestError(t *testing.T) {
	tm, md, ts, factory := newReplicationInitializationTM(t)
	holdInitializationShardLock(t, ts)
	factory.AddOperationError(memorytopo.TryLock, ".*", topo.NewError(topo.NodeExists, "shard"))
	md.ExpectedExecuteSuperQueryList = nil
	_, err := tm.initializeReplicationUnlessShardLocked(t.Context(), logutil.NewMemoryLogger(), topodatapb.TabletType_REPLICA)
	require.NoError(t, err)
	// Stop the watcher, so that only this test changes the reason.
	tm.tmState.cancel()
	assert.Contains(t, tm.tmState.canServe(topodatapb.TabletType_REPLICA), "replication is not initialized: cannot initialize replication for ks/0; replication has not been changed: another operation holds the shard lock")

	id := tm.tmState.pendingReplicationInit
	tm.tmState.setReplicationInitError(t.Context(), id, errors.New("Errant GTID detected"))
	assert.Equal(t, "replication is not initialized: Errant GTID detected", tm.tmState.canServe(topodatapb.TabletType_REPLICA))
	tm.tmState.setReplicationInitError(t.Context(), id+1, errors.New("superseded"))
	assert.Equal(t, "replication is not initialized: Errant GTID detected", tm.tmState.canServe(topodatapb.TabletType_REPLICA))
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

func TestReplicaWithoutSourceSemiSyncIsNotFenced(t *testing.T) {
	tm, md, _, _ := newReplicationInitializationTM(t)
	_, err := tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
	require.NoError(t, err)
	assert.False(t, md.SuperReadOnly.Load(), "a replica without source-side semi-sync keeps its read-only setting")
	require.NoError(t, md.CheckSuperQueryList())
}

// blockingDemotionDaemon is a deposed primary with commits blocked on
// semi-sync. Its demotion waits for release when it enables super_read_only.
type blockingDemotionDaemon struct {
	*mysqlctl.FakeMysqlDaemon
	entered, release chan struct{}
	once             sync.Once
}

func (d *blockingDemotionDaemon) IsSemiSyncBlocked(context.Context) (bool, error) { return true, nil }

func (d *blockingDemotionDaemon) SetSuperReadOnly(ctx context.Context, on bool, opts ...mysqlctl.SetSuperReadOnlyOption) (mysqlctl.ResetSuperReadOnlyFunc, error) {
	d.once.Do(func() {
		close(d.entered)
		<-d.release
	})
	return d.FakeMysqlDaemon.SetSuperReadOnly(ctx, on, opts...)
}

// newReplicatingPendingTM returns a deferred replica whose replication runs
// from the shard primary and acknowledges, with grants applied.
func newReplicatingPendingTM(t *testing.T) (*TabletManager, *mysqlctl.FakeMysqlDaemon, *tabletservermock.Controller, uint64) {
	t.Helper()
	tm, md, _, qsc, id := newPendingReplicationTM(t)
	tm._waitForGrantsComplete = make(chan struct{})
	close(tm._waitForGrantsComplete)
	md.Replicating, md.IOThreadRunning, md.SemiSyncReplicaEnabled = true, true, true
	md.CurrentSourceHost, md.CurrentSourcePort = "old-primary", 3306
	return tm, md, qsc, id
}

// applierCommitDaemon commits a transaction from the old source's relay log
// when replication stops, as STOP REPLICA waits for the applier's
// transaction in progress.
type applierCommitDaemon struct {
	*mysqlctl.FakeMysqlDaemon
	committed replication.Position
}

func (d *applierCommitDaemon) StopReplication(ctx context.Context, hookExtraEnv map[string]string) error {
	if err := d.FakeMysqlDaemon.StopReplication(ctx, hookExtraEnv); err != nil {
		return err
	}
	d.SetPrimaryPositionLocked(d.committed)
	return nil
}

func (d *applierCommitDaemon) SetReplicationSource(ctx context.Context, host string, port int32, heartbeatInterval float64, stopReplicationBefore bool, startReplicationAfter bool) error {
	if stopReplicationBefore {
		if err := d.StopReplication(ctx, nil); err != nil {
			return err
		}
	}
	return d.FakeMysqlDaemon.SetReplicationSource(ctx, host, port, heartbeatInterval, false, startReplicationAfter)
}

func TestUndoDemotePrimaryKeepsDeferredReplicaOutOfRotation(t *testing.T) {
	tm, md, _, qsc, _ := newPendingReplicationTM(t)
	tm._waitForGrantsComplete = make(chan struct{})
	close(tm._waitForGrantsComplete)
	md.ExpectedExecuteSuperQueryList = nil
	require.NoError(t, tm.UndoDemotePrimary(t.Context(), false))
	assert.True(t, tm.tmState.replicationInitPending())
	assert.False(t, qsc.IsServing())
}

// semiSyncBlockedApplier models an applier whose commit waits for semi-sync
// acknowledgements while source-side semi-sync is on. STOP REPLICA waits for
// that commit, as MySQL does.
type semiSyncBlockedApplier struct {
	*mysqlctl.FakeMysqlDaemon
	stoppedWhileBlocked bool
}

func (d *semiSyncBlockedApplier) StopReplication(ctx context.Context, hookExtraEnv map[string]string) error {
	if d.SemiSyncPrimaryEnabled {
		d.stoppedWhileBlocked = true
		<-ctx.Done()
		return ctx.Err()
	}
	return d.FakeMysqlDaemon.StopReplication(ctx, hookExtraEnv)
}

func TestFormerPrimaryIsFencedBeforeReplicating(t *testing.T) {
	for _, tc := range []struct {
		name       string
		commitGTID string
	}{
		{name: "released commit is errant", commitGTID: ",8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1"},
		{name: "nothing to release"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tm, md := newFormerPrimaryTM(t, tc.commitGTID)
			if tc.commitGTID != "" {
				// The final check fails after the fence and the stop.
				md.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA"}
			}

			_, err := tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
			assert.True(t, md.SuperReadOnly.Load(), "a former primary must not accept writes once it replicates")
			assert.False(t, md.SemiSyncPrimaryEnabled)
			require.NoError(t, md.CheckSuperQueryList())
			if tc.commitGTID != "" {
				require.ErrorContains(t, err, "Errant GTID detected", "a commit released after the first check must be validated")
				return
			}
			require.NoError(t, err)
			assert.True(t, md.Replicating)
		})
	}
}

func TestFailedFenceIsRepeatedByTheNextAttempt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*blockedCommitDaemon)
		err   string
	}{
		{name: "super_read_only times out", setup: func(d *blockedCommitDaemon) { d.stuck = true }, err: "deadline exceeded"},
		{name: "disabling semi-sync fails", setup: func(d *blockedCommitDaemon) { d.disableErr = errors.New("connection lost") }, err: "connection lost"},
		{name: "reading semi-sync fails", setup: func(d *blockedCommitDaemon) { d.ReadSemiSyncEnabledError = errors.New("variables unreadable") }, err: "variables unreadable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tm, md := newFormerPrimaryTM(t, ",8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1")
			// Only the second attempt, whose fence succeeds, stops replication.
			md.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA"}
			tc.setup(md)

			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			_, err := tm.initializeReplication(ctx, topodatapb.TabletType_REPLICA)
			require.ErrorContains(t, err, tc.err)
			assert.True(t, md.SemiSyncPrimaryEnabled, "the next attempt must find the former primary unfenced")

			// Released commits may still be finishing. The next attempt
			// must fence again rather than validate at once.
			md.stuck, md.ReadSemiSyncEnabledError = false, nil
			_, err = tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
			require.ErrorContains(t, err, "Errant GTID detected")
			assert.True(t, md.SuperReadOnly.Load())
			require.NoError(t, md.CheckSuperQueryList())
		})
	}
}

func newFormerPrimaryTM(t *testing.T, commitGTID string) (*TabletManager, *blockedCommitDaemon) {
	t.Helper()
	tm, md, _, _ := newReplicationInitializationTM(t)
	md.SemiSyncPrimaryEnabled = true
	committed, err := replication.ParsePosition(gtidFlavor, gtidPosition+commitGTID)
	require.NoError(t, err)
	d := &blockedCommitDaemon{FakeMysqlDaemon: md, committed: committed}
	tm.MysqlDaemon = d
	return tm, d
}

// TestFinalCheckCoversTheOldApplier covers a replica that runs from a source
// other than the primary it is about to replicate from, with an errant
// transaction in its relay log that commits when replication stops.
// Initialization must check after that.
func TestFinalCheckCoversTheOldApplier(t *testing.T) {
	tm, md, _, _ := newReplicationInitializationTM(t)
	errant, err := replication.ParsePosition(gtidFlavor, gtidPosition+",8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1")
	require.NoError(t, err)
	md.CurrentRelayLogPosition = errant
	md.Replicating, md.IOThreadRunning = true, true
	md.CurrentSourceHost, md.CurrentSourcePort = "stale-primary", 3306
	md.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA"}
	tm.MysqlDaemon = &applierCommitDaemon{FakeMysqlDaemon: md, committed: errant}

	_, err = tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
	require.ErrorContains(t, err, "Errant GTID detected")
	require.NoError(t, md.CheckSuperQueryList())
	assert.Equal(t, "stale-primary", md.CurrentSourceHost)
}

// TestFenceReleasesTheApplierBeforeStopping covers a replica with source-side
// semi-sync on, as a stale UndoDemotePrimary leaves it, whose applier waits
// for acknowledgements. Initialization must disable source-side semi-sync
// before it waits for the applier to stop.
func TestFenceReleasesTheApplierBeforeStopping(t *testing.T) {
	tm, md, _, _, _ := newPendingReplicationTM(t)
	tm.tmState.cancelReplicationInit(t.Context())
	tm._waitForGrantsComplete = make(chan struct{})
	close(tm._waitForGrantsComplete)
	md.Replicating, md.IOThreadRunning, md.SemiSyncReplicaEnabled = true, true, true
	md.CurrentSourceHost, md.CurrentSourcePort = "stale-primary", 3306
	md.ExpectedExecuteSuperQueryList = nil
	require.NoError(t, tm.UndoDemotePrimary(t.Context(), true))
	require.True(t, md.SemiSyncPrimaryEnabled)
	daemon := &semiSyncBlockedApplier{FakeMysqlDaemon: md}
	tm.MysqlDaemon = daemon
	md.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "FAKE SET SOURCE", "START REPLICA"}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, err := tm.initializeReplication(ctx, topodatapb.TabletType_REPLICA)
	require.NoError(t, err)
	assert.False(t, daemon.stoppedWhileBlocked, "replication was stopped while its applier waited for acknowledgements")
	assert.False(t, md.SemiSyncPrimaryEnabled)
	assert.True(t, md.SuperReadOnly.Load())
	assert.True(t, md.Replicating)
	require.NoError(t, md.CheckSuperQueryList())
}

// threadStateDaemon reports the receiver (io) and the applier (sql) in the
// given states.
type threadStateDaemon struct {
	*mysqlctl.FakeMysqlDaemon
	io, sql replication.ReplicationState
}

func (d *threadStateDaemon) ReplicationStatus(ctx context.Context) (replication.ReplicationStatus, error) {
	status, err := d.FakeMysqlDaemon.ReplicationStatus(ctx)
	status.IOState, status.SQLState = d.io, d.sql
	return status, err
}

// statusHookDaemon runs hook after each replication status read.
type statusHookDaemon struct {
	*mysqlctl.FakeMysqlDaemon
	hook func()
}

func (d *statusHookDaemon) ReplicationStatus(ctx context.Context) (replication.ReplicationStatus, error) {
	status, err := d.FakeMysqlDaemon.ReplicationStatus(ctx)
	d.hook()
	return status, err
}

func TestDeferredReplicaIsAdmittedWhenReplicatingFromShardPrimary(t *testing.T) {
	tm, md, qsc, id := newReplicatingPendingTM(t)
	md.ExpectedExecuteSuperQueryList = nil
	// The watcher takes no lock, so a held one does not keep it waiting.
	holdInitializationShardLock(t, tm.TopoServer)

	ended, err := tm.admitDeferredReplicationInit(t.Context(), id)
	require.NoError(t, err)
	assert.True(t, ended)
	assert.False(t, tm.tmState.replicationInitPending())
	assert.True(t, qsc.IsServing())
	require.NoError(t, md.CheckSuperQueryList(), "admission must not change replication")
}

func TestDeferredReplicaIsNotAdmittedWithoutReplicationFromShardPrimary(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, tm *TabletManager, md *mysqlctl.FakeMysqlDaemon, ts *topo.Server)
		err   string
	}{{
		name: "replication stopped",
		setup: func(t *testing.T, tm *TabletManager, md *mysqlctl.FakeMysqlDaemon, ts *topo.Server) {
			md.Replicating = false
		},
		err: "replication is not running",
	}, {
		name: "receiver stopped",
		setup: func(t *testing.T, tm *TabletManager, md *mysqlctl.FakeMysqlDaemon, ts *topo.Server) {
			tm.MysqlDaemon = &threadStateDaemon{FakeMysqlDaemon: md, io: replication.ReplicationStateStopped, sql: replication.ReplicationStateRunning}
		},
		err: "replication is not running",
	}, {
		name: "applier stopped",
		setup: func(t *testing.T, tm *TabletManager, md *mysqlctl.FakeMysqlDaemon, ts *topo.Server) {
			tm.MysqlDaemon = &threadStateDaemon{FakeMysqlDaemon: md, io: replication.ReplicationStateRunning, sql: replication.ReplicationStateStopped}
		},
		err: "replication is not running",
	}, {
		name: "receiver connecting",
		setup: func(t *testing.T, tm *TabletManager, md *mysqlctl.FakeMysqlDaemon, ts *topo.Server) {
			tm.MysqlDaemon = &threadStateDaemon{FakeMysqlDaemon: md, io: replication.ReplicationStateConnecting, sql: replication.ReplicationStateRunning}
		},
		err: "replication is not running",
	}, {
		name: "replicating from another source",
		setup: func(t *testing.T, tm *TabletManager, md *mysqlctl.FakeMysqlDaemon, ts *topo.Server) {
			md.CurrentSourceHost = "stale-primary"
		},
		err: "not from the shard primary",
	}, {
		name: "no shard primary",
		setup: func(t *testing.T, tm *TabletManager, md *mysqlctl.FakeMysqlDaemon, ts *topo.Server) {
			_, err := ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error { si.PrimaryAlias = nil; return nil })
			require.NoError(t, err)
		},
		err: "no primary",
	}, {
		name: "status unreadable",
		setup: func(t *testing.T, tm *TabletManager, md *mysqlctl.FakeMysqlDaemon, ts *topo.Server) {
			md.ReplicationStatusError = errors.New("status read failed")
		},
		err: "status read failed",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			tm, md, qsc, id := newReplicatingPendingTM(t)
			md.ExpectedExecuteSuperQueryList = nil
			tc.setup(t, tm, md, tm.TopoServer)

			ended, err := tm.admitDeferredReplicationInit(t.Context(), id)
			require.ErrorContains(t, err, tc.err)
			assert.False(t, ended)
			assert.True(t, tm.tmState.isPendingReplicationInit(id))
			assert.False(t, qsc.IsServing())
			require.NoError(t, md.CheckSuperQueryList(), "the watcher must not change replication")
		})
	}
}

func TestDeferredReplicaWaitsForRestoreAndBackup(t *testing.T) {
	for _, tabletType := range []topodatapb.TabletType{topodatapb.TabletType_RESTORE, topodatapb.TabletType_BACKUP} {
		t.Run(tabletType.String(), func(t *testing.T) {
			tm, md, _, id := newReplicatingPendingTM(t)
			md.ExpectedExecuteSuperQueryList = nil
			require.NoError(t, tm.tmState.ChangeTabletType(t.Context(), tabletType, DBActionNone))

			ended, err := tm.admitDeferredReplicationInit(t.Context(), id)
			require.ErrorContains(t, err, "waiting for the")
			assert.False(t, ended)
			assert.True(t, tm.tmState.isPendingReplicationInit(id))
			require.NoError(t, md.CheckSuperQueryList())
		})
	}
}

func TestDeferredReplicationInitializationEndsOnPromotion(t *testing.T) {
	tm, md, _, qsc, id := newPendingReplicationTM(t)
	md.ExpectedExecuteSuperQueryList = nil
	require.NoError(t, tm.tmState.ChangeTabletType(t.Context(), topodatapb.TabletType_PRIMARY, DBActionNone))
	assert.True(t, qsc.IsServing(), "a primary is not gated")

	ended, err := tm.admitDeferredReplicationInit(t.Context(), id)
	require.NoError(t, err)
	assert.True(t, ended)
	assert.False(t, tm.tmState.replicationInitPending())
	require.NoError(t, md.CheckSuperQueryList())
}

// TestPrimaryAdmissionChecksTheCurrentType covers a deferred tablet that was
// promoted and then demoted by SetReplicationSource before it configured
// replication. A watcher that saw it as the primary must not end the deferral.
func TestPrimaryAdmissionChecksTheCurrentType(t *testing.T) {
	tm, md, _, qsc, id := newPendingReplicationTM(t)
	md.ExpectedExecuteSuperQueryList = nil
	require.NoError(t, tm.tmState.ChangeTabletType(t.Context(), topodatapb.TabletType_PRIMARY, DBActionNone))
	require.NoError(t, tm.tmState.ChangeTabletType(t.Context(), topodatapb.TabletType_REPLICA, DBActionNone))

	assert.False(t, tm.tmState.endReplicationInit(t.Context(), id, true), "a demoted primary ended the deferral")
	ended, err := tm.admitDeferredReplicationInit(t.Context(), id)
	require.ErrorContains(t, err, "replication is not running")
	assert.False(t, ended)
	assert.True(t, tm.tmState.isPendingReplicationInit(id))
	assert.False(t, qsc.IsServing())
	require.NoError(t, md.CheckSuperQueryList())
}

// TestStaleWatcherDoesNotEndNewerDeferral covers a watcher that read healthy
// replication for one deferral while a restore started and deferred again.
func TestStaleWatcherDoesNotEndNewerDeferral(t *testing.T) {
	for _, tc := range []struct {
		name string
		// during runs after the watcher's replication status read.
		during func(t *testing.T, tm *TabletManager)
	}{{
		name: "restore running",
		during: func(t *testing.T, tm *TabletManager) {
			require.NoError(t, tm.tmState.ChangeTabletType(t.Context(), topodatapb.TabletType_RESTORE, DBActionNone))
		},
	}, {
		name: "restore deferred again",
		during: func(t *testing.T, tm *TabletManager) {
			tm.tmState.cancelReplicationInit(t.Context())
			tm.tmState.deferReplicationInit(t.Context(), nil)
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			tm, md, qsc, id := newReplicatingPendingTM(t)
			md.ExpectedExecuteSuperQueryList = nil
			once := sync.Once{}
			tm.MysqlDaemon = &statusHookDaemon{FakeMysqlDaemon: md, hook: func() { once.Do(func() { tc.during(t, tm) }) }}

			_, _ = tm.admitDeferredReplicationInit(t.Context(), id)
			assert.True(t, tm.tmState.replicationInitPending(), "a stale watcher ended a newer deferral")
			assert.False(t, qsc.IsServing())
		})
	}
	t.Run("superseded before the read", func(t *testing.T) {
		tm, md, qsc, id := newReplicatingPendingTM(t)
		md.ExpectedExecuteSuperQueryList = nil
		tm.tmState.deferReplicationInit(t.Context(), nil)

		ended, err := tm.admitDeferredReplicationInit(t.Context(), id)
		require.NoError(t, err)
		assert.True(t, ended, "the watcher of a superseded deferral stops")
		assert.True(t, tm.tmState.replicationInitPending())
		assert.False(t, qsc.IsServing())
	})
}

// TestSetReplicationSourceDoesNotStartDeferredReplica covers the follow-up
// SetReplicationSource that vtctld sends after a restore, and PRS's to a
// replica it found stopped: without force, replication that is stopped stays
// stopped, and the replica stays out of rotation until something starts it.
func TestSetReplicationSourceDoesNotStartDeferredReplica(t *testing.T) {
	for _, tc := range []struct {
		name    string
		queries []string
		start   func(t *testing.T, tm *TabletManager)
	}{{
		name:    "StartReplication",
		queries: []string{"START REPLICA"},
		start: func(t *testing.T, tm *TabletManager) {
			require.NoError(t, tm.StartReplication(t.Context(), true))
		},
	}, {
		name: "SetReplicationSource with force, as VTOrc repairs",
		// The source is already configured, so only replication restarts.
		queries: []string{"STOP REPLICA", "START REPLICA"},
		start: func(t *testing.T, tm *TabletManager) {
			require.NoError(t, tm.SetReplicationSource(t.Context(), &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}, 0, "", true, true, 0))
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			tm, md, _, qsc, id := newPendingReplicationTM(t)
			tm._waitForGrantsComplete = make(chan struct{})
			close(tm._waitForGrantsComplete)
			md.ExpectedExecuteSuperQueryList = []string{"FAKE SET SOURCE"}

			require.NoError(t, tm.SetReplicationSource(t.Context(), &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}, 0, "", false, true, 0))
			require.NoError(t, md.CheckSuperQueryList())
			assert.False(t, md.Replicating, "a deferred replica's stopped replication was started without force")
			ended, err := tm.admitDeferredReplicationInit(t.Context(), id)
			require.Error(t, err)
			assert.False(t, ended)
			assert.False(t, qsc.IsServing())

			md.IOThreadRunning = true
			md.ExpectedExecuteSuperQueryList = tc.queries
			md.ExpectedExecuteSuperQueryCurrent = 0
			tc.start(t, tm)
			require.NoError(t, md.CheckSuperQueryList())
			require.True(t, md.Replicating)
			ended, err = tm.admitDeferredReplicationInit(t.Context(), id)
			require.NoError(t, err)
			assert.True(t, ended)
			assert.True(t, qsc.IsServing())
		})
	}
}

func TestRestoreFinalizationAndDeferredReplication(t *testing.T) {
	t.Run("dry run keeps the deferral", func(t *testing.T) {
		tm, _, _, qsc, id := newPendingReplicationTM(t)
		require.NoError(t, tm.tmState.ChangeTabletType(t.Context(), topodatapb.TabletType_RESTORE, DBActionNone))
		rsm := tm.newRestoreStateManager(logutil.NewMemoryLogger(), false)
		rsm.dryRun = true
		rsm.state = restoreStarted
		rsm.nextTabletType = topodatapb.TabletType_REPLICA
		require.NoError(t, rsm.finish(t.Context(), replicationCommand{action: replicationActionNone}))
		assert.True(t, tm.tmState.isPendingReplicationInit(id), "a dry run changes no data")
		assert.False(t, qsc.IsServing())
	})
	t.Run("aborted restore keeps the deferral", func(t *testing.T) {
		tm, _, _, qsc, id := newPendingReplicationTM(t)
		require.NoError(t, tm.tmState.ChangeTabletType(t.Context(), topodatapb.TabletType_RESTORE, DBActionNone))
		rsm := tm.newRestoreStateManager(logutil.NewMemoryLogger(), false)
		rsm.state = restoreStarted
		rsm.prevTabletType = topodatapb.TabletType_REPLICA
		require.NoError(t, rsm.abort())
		assert.True(t, tm.tmState.isPendingReplicationInit(id), "an aborted restore leaves the data unchanged")
		assert.False(t, qsc.IsServing())
	})
	t.Run("point-in-time restore ends the deferral", func(t *testing.T) {
		tm, md, _, _, _ := newPendingReplicationTM(t)
		require.NoError(t, tm.tmState.ChangeTabletType(t.Context(), topodatapb.TabletType_RESTORE, DBActionNone))
		md.SetReplicationSourceInputs = []string{"//:0"}
		// disableReplication points replication at an impossible source.
		md.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "FAKE RESET REPLICA ALL", "FAKE SET SOURCE", "START REPLICA"}
		rsm := tm.newRestoreStateManager(logutil.NewMemoryLogger(), false)
		rsm.state = restoreStarted
		rsm.nextTabletType = topodatapb.TabletType_DRAINED
		require.NoError(t, rsm.finish(t.Context(), replicationCommand{action: replicationActionDisable}))
		assert.False(t, tm.tmState.replicationInitPending())
		require.NoError(t, md.CheckSuperQueryList())
	})
}

// startableDaemon reports replication as stopped until started is set, so that
// a test can start it while another goroutine reads the status.
type startableDaemon struct {
	*mysqlctl.FakeMysqlDaemon
	started atomic.Bool
}

func (d *startableDaemon) ReplicationStatus(ctx context.Context) (replication.ReplicationStatus, error) {
	status, err := d.FakeMysqlDaemon.ReplicationStatus(ctx)
	if !d.started.Load() {
		status.IOState, status.SQLState = replication.ReplicationStateStopped, replication.ReplicationStateStopped
	}
	return status, err
}

// TestDeferredReplicationWatcher covers the watcher's loop: once the shard
// lock is released, it still changes no replication, and it admits the
// replica once something else starts replication from the shard primary.
func TestDeferredReplicationWatcher(t *testing.T) {
	tm, md, ts, _ := newReplicationInitializationTM(t)
	qsc := tabletservermock.NewController()
	require.NoError(t, qsc.InitDBConfig(&querypb.Target{Keyspace: "ks", Shard: "0"}, nil, md))
	tm.QueryServiceControl = qsc
	tm.tmState.Open()
	release := holdInitializationShardLock(t, ts)
	md.Replicating, md.IOThreadRunning = true, true
	md.CurrentSourceHost, md.CurrentSourcePort = "old-primary", 3306
	daemon := &startableDaemon{FakeMysqlDaemon: md}
	tm.MysqlDaemon = daemon

	logger := logutil.NewMemoryLogger()
	_, err := tm.initializeReplicationUnlessShardLocked(t.Context(), logger, topodatapb.TabletType_REPLICA)
	require.NoError(t, err)
	assert.Contains(t, logger.String(), "deferring replication initialization")
	assert.False(t, qsc.IsServing(), "a replica must not serve before replication is initialized")

	release()
	assert.Never(t, qsc.IsServing, 3*replicationInitWatchInterval, 10*time.Millisecond,
		"the watcher must leave replication to VTOrc, a reparent or an operator")
	assert.Contains(t, tm.tmState.canServe(topodatapb.TabletType_REPLICA), "replication is not running")

	// VTOrc starts replication from the shard primary.
	daemon.started.Store(true)
	assert.Eventually(t, qsc.IsServing, 30*time.Second, 10*time.Millisecond)
	assert.False(t, tm.tmState.replicationInitPending())
	assert.Zero(t, md.ExpectedExecuteSuperQueryCurrent, "the watcher changed replication")
}

func TestDeferredReplicationWatcherBoundsItsReads(t *testing.T) {
	tm, md, _, id := newReplicatingPendingTM(t)
	md.ExpectedExecuteSuperQueryList = nil
	daemon := &deadlineRecordingDaemon{FakeMysqlDaemon: md}
	tm.MysqlDaemon = daemon

	_, err := tm.admitDeferredReplicationInit(t.Context(), id)
	require.NoError(t, err)
	require.NotEmpty(t, daemon.deadlines)
	latest := time.Now().Add(topo.RemoteOperationTimeout)
	for _, deadline := range daemon.deadlines {
		require.False(t, deadline.IsZero(), "the watcher's reads must have a deadline")
		assert.False(t, deadline.After(latest))
	}
}

// TestDeferredReplicaStaysOutOfServiceDuringDemotion covers a watcher that
// checks a deferral that was still pending when the tablet was promoted, while
// a forced demotion holds the action lock, after it disabled query service and
// source-side semi-sync and before super_read_only.
func TestDeferredReplicaStaysOutOfServiceDuringDemotion(t *testing.T) {
	tm, md, qsc, id := newReplicatingPendingTM(t)
	md.ExpectedExecuteSuperQueryList = nil
	require.NoError(t, tm.tmState.ChangeTabletType(t.Context(), topodatapb.TabletType_PRIMARY, DBActionNone))
	require.True(t, qsc.IsServing())
	md.ReadOnly = false
	md.SemiSyncPrimaryEnabled = true
	d := &blockingDemotionDaemon{FakeMysqlDaemon: md, entered: make(chan struct{}), release: make(chan struct{})}
	tm.MysqlDaemon = d
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(d.release) }) }
	t.Cleanup(release)
	finished := make(chan error, 1)
	go func() {
		_, err := tm.DemotePrimary(t.Context(), true)
		finished <- err
	}()
	<-d.entered
	require.False(t, qsc.IsServing())

	ended, err := tm.admitDeferredReplicationInit(t.Context(), id)
	require.NoError(t, err)
	assert.True(t, ended)
	assert.False(t, qsc.IsServing(), "query service was re-enabled during demotion")

	release()
	require.NoError(t, <-finished)
}
