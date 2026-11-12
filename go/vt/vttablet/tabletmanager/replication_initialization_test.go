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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	querypb "vitess.io/vitess/go/vt/proto/query"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/reparenttestutil"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/semisyncmonitor"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
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
	t.Cleanup(tm.tmState.Close)
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

	// Acquisition waits for the held lock and times out.
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

// slowLockFactory serves a memorytopo whose TryLock takes delay before it
// starts, as a remote topology server's round trips do. An attempt whose
// context expires first fails with topo.Timeout, as etcd's does.
type slowLockFactory struct {
	*memorytopo.Factory
	delay time.Duration
}

func (f *slowLockFactory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	conn, err := f.Factory.Create(cell, serverAddr, root)
	if err != nil {
		return nil, err
	}
	return &slowLockConn{Conn: conn, delay: f.delay}, nil
}

type slowLockConn struct {
	topo.Conn
	delay time.Duration
}

func (c *slowLockConn) TryLock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, topo.NewError(topo.Timeout, dirPath)
		}
		return nil, ctx.Err()
	case <-time.After(c.delay):
	}
	return c.Conn.TryLock(ctx, dirPath, contents)
}

// TestInitializeReplicationTakesFreeShardLockFromSlowTopo checks that a free
// shard lock whose acquisition outlasts the wait for a held lock is taken,
// not treated as held.
func TestInitializeReplicationTakesFreeShardLockFromSlowTopo(t *testing.T) {
	_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
	ts, err := topo.NewWithFactory(&slowLockFactory{Factory: factory, delay: replicationInitializationLockWait + 500*time.Millisecond}, "", "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)
	tm, md := newReplicationInitializationTMWithServer(t, ts, 1)

	_, err = tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
	require.NoError(t, err)
	assert.True(t, md.Replicating)
	require.NoError(t, md.CheckSuperQueryList())
}

// TestInitializeReplicationFailsWhenItsContextExpiresWhileLocking checks that
// an acquisition that times out because the caller's context expired fails
// initialization instead of deferring it as an unavailable lock.
func TestInitializeReplicationFailsWhenItsContextExpiresWhileLocking(t *testing.T) {
	_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
	ts, err := topo.NewWithFactory(&slowLockFactory{Factory: factory, delay: 30 * time.Second}, "", "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)
	tm, md := newReplicationInitializationTMWithServer(t, ts, 1)
	md.ExpectedExecuteSuperQueryList = nil

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err = tm.initializeReplication(ctx, topodatapb.TabletType_REPLICA)
	require.ErrorContains(t, err, "cannot initialize replication for ks/0")
	_, unavailable := errors.AsType[*shardLockUnavailableError](err)
	assert.False(t, unavailable, "an expired context must not defer initialization")
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

// TestInitializeReplicationStopsWhenShardLockIsLost loses the lock while
// initialization waits for the applier to stop, after its other checks.
func TestInitializeReplicationStopsWhenShardLockIsLost(t *testing.T) {
	tm, md, _, factory := newReplicationInitializationTM(t)
	md.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA"}
	md.ExecuteSuperQueryListCallback = func() {
		factory.AddOperationError(memorytopo.CheckLock, ".*", errors.New("lock lost"))
	}

	_, err := tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
	require.ErrorContains(t, err, "lock lost")
	require.NoError(t, md.CheckSuperQueryList())
	assert.False(t, md.Replicating)
	assert.False(t, md.SemiSyncReplicaEnabled)
	assert.Empty(t, md.CurrentSourceHost)
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
			before := factory.GetCallStats().Counts()["TryLock"]
			_, err = tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
			require.NoError(t, err)
			assert.Equal(t, before, factory.GetCallStats().Counts()["TryLock"])
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

func TestInitializeReplicationRereadsPrimaryAfterAcquiringLock(t *testing.T) {
	tm, md, ts, factory := newReplicationInitializationTM(t)
	release := holdInitializationShardLock(t, ts)
	before := factory.GetCallStats().Counts()["TryLock"]
	done := make(chan error, 1)
	go func() { _, err := tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA); done <- err }()
	// Publish the new primary and release the lock while acquisition waits for it.
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
		// lockErr is returned by the acquisition. Without it, the acquisition
		// waits for the held lock and times out.
		lockErr error
		// cancelWhileLocking cancels the restore's context while acquisition waits.
		cancelWhileLocking bool
		deferred           bool
	}{
		{name: "acquisition timeout", deferred: true},
		{name: "topology timeout", lockErr: topo.NewError(topo.Timeout, "ks/0"), deferred: true},
		{name: "backend error", lockErr: errors.New("topology authorization denied")},
		{name: "parent context canceled", cancelWhileLocking: true},
	} {
		for actionName, action := range map[string]replicationAction{"start": replicationActionStart, "initialize": replicationActionInitialize} {
			t.Run(tc.name+"/"+actionName, func(t *testing.T) {
				tm, md, ts, factory := newReplicationInitializationTM(t)
				holdInitializationShardLock(t, ts)
				// Stop the watcher before the lock is released.
				t.Cleanup(tm.tmState.Close)
				if tc.lockErr != nil {
					factory.AddOperationError(memorytopo.TryLock, ".*", tc.lockErr)
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
				// may need, so it waits for the shard lock only briefly.
				start := time.Now()
				err := rsm.finish(ctx, replCmd)
				assert.Less(t, time.Since(start), replicationInitializationLockWait+5*time.Second)

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

// TestStartWaitsBrieflyForHeldShardLock checks that Start waits for a held shard
// lock only for the acquisition budget, then defers.
func TestStartWaitsBrieflyForHeldShardLock(t *testing.T) {
	ts := memorytopo.NewServer(t.Context(), "cell1")
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

	// The shard lock holder may need RPCs to this tablet, which it does
	// not serve until Start returns.
	start := time.Now()
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	t.Cleanup(tm.Stop)
	assert.Less(t, time.Since(start), replicationInitializationLockWait+5*time.Second)

	md := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	assert.False(t, tm.QueryServiceControl.(*tabletservermock.Controller).IsServing())
	assert.False(t, md.Replicating)
	assert.False(t, md.SemiSyncReplicaEnabled)
	assert.Empty(t, md.CurrentSourceHost)
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
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
	id := tm.tmState.deferReplicationInit(t.Context(), nil, nil)
	require.False(t, qsc.IsServing())
	return tm, md, ts, qsc, id
}

func TestPendingReplicationReasonShowsLatestError(t *testing.T) {
	tm, md, ts, _ := newReplicationInitializationTM(t)
	holdInitializationShardLock(t, ts)
	md.ExpectedExecuteSuperQueryList = nil
	_, err := tm.initializeReplicationUnlessShardLocked(t.Context(), logutil.NewMemoryLogger(), topodatapb.TabletType_REPLICA, nil)
	require.NoError(t, err)
	// Stop the watcher, so that only this test changes the reason.
	tm.tmState.Close()
	assert.Contains(t, tm.tmState.canServe(topodatapb.TabletType_REPLICA), "replication is not initialized: cannot initialize replication for ks/0; replication has not been changed: timed out acquiring the shard lock")

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
				// The first check, after the fence, fails.
				md.ExpectedExecuteSuperQueryList = nil
			}

			_, err := tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
			assert.True(t, md.SuperReadOnly.Load(), "a former primary must not accept writes once it replicates")
			assert.False(t, md.SemiSyncPrimaryEnabled)
			require.NoError(t, md.CheckSuperQueryList())
			if tc.commitGTID != "" {
				require.ErrorContains(t, err, "Errant GTID detected", "a commit that the fence releases must be validated")
				return
			}
			require.NoError(t, err)
			assert.True(t, md.Replicating)
		})
	}
}

// TestFormerPrimaryIsFencedWhenInitializationFails covers a former primary
// whose initialization fails before replicating: it must still release the
// waiting commit and reject writes, as a deferral does, and leave replication
// unchanged.
func TestFormerPrimaryIsFencedWhenInitializationFails(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*TabletManager, *blockedCommitDaemon)
		err   string
	}{
		{
			name: "primary does not answer",
			setup: func(tm *TabletManager, _ *blockedCommitDaemon) {
				tm.tmc = &failingPrimaryStatusClient{TabletManagerClient: tm.tmc, failing: &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}}
			},
			err: "primary status unavailable",
		},
		{
			name:  "errant GTID already visible",
			setup: func(_ *TabletManager, d *blockedCommitDaemon) { d.SetPrimaryPositionLocked(d.committed) },
			err:   "Errant GTID detected",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tm, md := newFormerPrimaryTM(t, ",8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1")
			md.ExpectedExecuteSuperQueryList = nil
			tc.setup(tm, md)

			_, err := tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
			require.ErrorContains(t, err, tc.err)
			assert.False(t, md.SemiSyncPrimaryEnabled, "the waiting commit must be released")
			assert.True(t, md.SuperReadOnly.Load(), "a former primary must not accept writes")
			require.NoError(t, md.CheckSuperQueryList(), "replication must not change")
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
			// Neither attempt changes replication: the second one's fence
			// succeeds and its first check finds the released commit.
			md.ExpectedExecuteSuperQueryList = nil
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

// TestFormerPrimaryIsFencedWhenDeferring covers a former primary that restarts
// under a held shard lock with a commit waiting for semi-sync acknowledgements.
// It must release the commit and reject writes before it defers, without
// changing replication.
func TestFormerPrimaryIsFencedWhenDeferring(t *testing.T) {
	tm, md := newFormerPrimaryTM(t, ",8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1")
	holdInitializationShardLock(t, tm.TopoServer)
	md.ExpectedExecuteSuperQueryList = nil

	_, err := tm.initializeReplicationUnlessShardLocked(t.Context(), logutil.NewMemoryLogger(), topodatapb.TabletType_REPLICA, nil)
	require.NoError(t, err)
	assert.True(t, tm.tmState.replicationInitPending())
	assert.False(t, md.SemiSyncPrimaryEnabled, "the waiting commit must be released")
	assert.True(t, md.SuperReadOnly.Load(), "a deferred former primary must not accept writes")
	assert.Equal(t, md.committed, md.CurrentPrimaryPosition, "the released commit must finish")
	require.NoError(t, md.CheckSuperQueryList(), "replication must not change")
}

// TestFailedFenceFailsDeferredInitialization covers a former primary that
// restarts under a held shard lock and fails to fence. Initialization must fail
// instead of deferring: a later repoint's errant GTID check would miss the
// commit that the repoint releases.
func TestFailedFenceFailsDeferredInitialization(t *testing.T) {
	for _, tc := range []struct {
		name     string
		setup    func(*blockedCommitDaemon)
		err      string
		reverted bool
	}{
		{name: "reading semi-sync fails", setup: func(d *blockedCommitDaemon) { d.ReadSemiSyncEnabledError = errors.New("variables unreadable") }, err: "variables unreadable"},
		{name: "disabling semi-sync fails", setup: func(d *blockedCommitDaemon) { d.disableErr = errors.New("connection lost") }, err: "connection lost", reverted: true},
		{name: "super_read_only times out", setup: func(d *blockedCommitDaemon) { d.stuck = true }, err: "deadline exceeded", reverted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tm, md := newFormerPrimaryTM(t, ",8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1")
			holdInitializationShardLock(t, tm.TopoServer)
			md.ExpectedExecuteSuperQueryList = nil
			tc.setup(md)

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			_, err := tm.initializeReplicationUnlessShardLocked(ctx, logutil.NewMemoryLogger(), topodatapb.TabletType_REPLICA, nil)
			require.ErrorContains(t, err, "cannot fence a former primary before deferring replication initialization")
			require.ErrorContains(t, err, tc.err)
			assert.False(t, tm.tmState.replicationInitPending(), "a tablet that failed to fence must not defer")
			assert.True(t, md.SemiSyncPrimaryEnabled, "the next attempt must find the former primary unfenced")
			if tc.reverted {
				assert.True(t, md.released, "the fence disabled source-side semi-sync before failing")
			}
			require.NoError(t, md.CheckSuperQueryList(), "replication must not change")
		})
	}
}

// TestBackupLeavesDeferredReplicationAlone covers an offline backup of a replica
// whose replication initialization is deferred. Its cleanup, which does not hold
// the shard lock, must leave replication to the deferral's recovery, but restore
// the semi-sync role that entering BACKUP turned off, whether replication is
// configured or not and whether the backup succeeds or not.
func TestBackupLeavesDeferredReplicationAlone(t *testing.T) {
	for _, tc := range []struct {
		name         string
		unconfigured bool
		backupErr    error
	}{
		{name: "unconfigured, backup succeeds", unconfigured: true},
		{name: "unconfigured, backup fails", unconfigured: true, backupErr: errors.New("backup failed")},
		{name: "configured and stopped, backup succeeds"},
		{name: "configured and stopped, backup fails", backupErr: errors.New("backup failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tm, md, ts, _, _ := newPendingReplicationTM(t)
			tm.Cnf = &mysqlctl.Mycnf{}
			md.DB().SetNeverFail(true)
			md.Replicating, md.IOThreadRunning = false, false
			if tc.unconfigured {
				md.ReplicationStatusError = mysql.ErrNotReplica
			}
			md.ExpectedExecuteSuperQueryList = nil
			var repointed atomic.Bool
			md.SetReplicationSourceFunc = func(context.Context, string, int32, float64, bool, bool) error {
				repointed.Store(true)
				return nil
			}
			useFakeBackupStorage(t)
			const engine = "deferred-replica-drain"
			res := mysqlctl.BackupUsable
			if tc.backupErr != nil {
				res = mysqlctl.BackupUnusable
			}
			mysqlctl.BackupRestoreEngineMap[engine] = &mysqlctl.FakeBackupEngine{
				ShouldDrainForBackupReturn: true,
				ExecuteBackupReturn:        mysqlctl.FakeBackupEngineExecuteBackupReturn{Res: res, Err: tc.backupErr},
			}
			t.Cleanup(func() { delete(mysqlctl.BackupRestoreEngineMap, engine) })
			holdInitializationShardLock(t, ts)

			err := tm.Backup(t.Context(), logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{BackupEngine: new(engine)})
			if tc.backupErr != nil {
				require.ErrorContains(t, err, "backup failed")
			} else {
				require.NoError(t, err)
			}
			assert.False(t, repointed.Load(), "backup cleanup set the replication source under another operation's shard lock")
			assert.True(t, md.SemiSyncReplicaEnabled, "backup cleanup did not restore replica-side semi-sync")
			require.NoError(t, md.CheckSuperQueryList())
			assert.False(t, md.Replicating)
			assert.True(t, tm.tmState.replicationInitPending())
		})
	}
}

// statusErrorAfterBackupDaemon changes the second replication status read of
// a backup, the cleanup's (the first is entering BACKUP): it fails with err if
// set, or does not return until its context ends if hang is set. It reports
// the receiver as retrying a failed connection if connecting is set.
type statusErrorAfterBackupDaemon struct {
	*mysqlctl.FakeMysqlDaemon
	err        error
	hang       bool
	connecting bool
	reads      atomic.Int32
}

func (d *statusErrorAfterBackupDaemon) ReplicationStatus(ctx context.Context) (replication.ReplicationStatus, error) {
	if d.reads.Add(1) == 2 {
		if d.err != nil {
			return replication.ReplicationStatus{}, d.err
		}
		if d.hang {
			<-ctx.Done()
			return replication.ReplicationStatus{}, ctx.Err()
		}
	}
	status, err := d.FakeMysqlDaemon.ReplicationStatus(ctx)
	if d.connecting {
		status.IOState = replication.ReplicationStateConnecting
		status.LastIOError = "error connecting to source"
	}
	return status, err
}

// TestBackupOfRunningDeferredReplica covers an offline backup of a deferred
// replica that replicates again after the backup. Entering BACKUP turns off
// semi-sync. While initialization is deferred, the backup's cleanup must turn
// the replica's semi-sync role back on without setting the source, but only if
// replication runs from the shard primary: a replica that replicates, or
// connects, to another source, such as a primary a reparent replaces, or whose
// status cannot be read, must not acknowledge. A status read that does not
// return must not keep the backup's action lock.
func TestBackupOfRunningDeferredReplica(t *testing.T) {
	for _, tc := range []struct {
		name             string
		source           string
		port             int32
		statusErr        error
		hang, connecting bool
		semiSync         bool
	}{
		{name: "from the shard primary", source: "old-primary", semiSync: true},
		{name: "from another source", source: "stale-primary"},
		// Tablets on one host differ only by port.
		{name: "from another port on the shard primary's host", source: "old-primary", port: 3307},
		{name: "retrying another source", source: "stale-primary", connecting: true},
		{name: "status unreadable", source: "stale-primary", statusErr: errors.New("status read failed")},
		{name: "status read hangs", source: "old-primary", hang: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tm, md, qsc, id := newReplicatingPendingTM(t)
			md.CurrentSourceHost = tc.source
			if tc.port != 0 {
				md.CurrentSourcePort = tc.port
			}
			tm.Cnf = &mysqlctl.Mycnf{}
			md.DB().SetNeverFail(true)
			md.ExpectedExecuteSuperQueryList = nil
			var repointed atomic.Bool
			md.SetReplicationSourceFunc = func(context.Context, string, int32, float64, bool, bool) error {
				repointed.Store(true)
				return nil
			}
			useFakeBackupStorage(t)
			const engine = "running-deferred-replica-drain"
			mysqlctl.BackupRestoreEngineMap[engine] = &mysqlctl.FakeBackupEngine{
				ShouldDrainForBackupReturn: true,
				ExecuteBackupReturn:        mysqlctl.FakeBackupEngineExecuteBackupReturn{Res: mysqlctl.BackupUsable},
			}
			t.Cleanup(func() { delete(mysqlctl.BackupRestoreEngineMap, engine) })
			holdInitializationShardLock(t, tm.TopoServer)

			tm.MysqlDaemon = &statusErrorAfterBackupDaemon{FakeMysqlDaemon: md, err: tc.statusErr, hang: tc.hang, connecting: tc.connecting}
			if tc.hang {
				previous := topo.RemoteOperationTimeout
				topo.RemoteOperationTimeout = time.Second
				t.Cleanup(func() { topo.RemoteOperationTimeout = previous })
			}
			// The backup holds the action lock until its cleanup returns.
			returned := make(chan error, 1)
			go func() {
				returned <- tm.Backup(t.Context(), logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{BackupEngine: new(engine)})
			}()
			var err error
			require.Eventually(t, func() bool {
				select {
				case err = <-returned:
					return true
				default:
					return false
				}
			}, 30*time.Second, 10*time.Millisecond, "the backup's cleanup did not return")
			require.NoError(t, err)
			assert.False(t, repointed.Load(), "backup cleanup set the replication source under another operation's shard lock")
			require.NoError(t, md.CheckSuperQueryList(), "backup cleanup restarted replication")
			assert.Equal(t, tc.semiSync, md.SemiSyncReplicaEnabled)
			assert.True(t, md.Replicating)
			assert.Equal(t, tc.source, md.CurrentSourceHost)
			assert.True(t, tm.tmState.isPendingReplicationInit(id))
			assert.False(t, qsc.IsServing())
		})
	}
}

// advancingPrimaryTMC reports a primary position that advances with every call,
// as on a primary taking writes.
type advancingPrimaryTMC struct {
	*fakeTMClient
	calls atomic.Int32
}

func (c *advancingPrimaryTMC) PrimaryStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
	pos := fmt.Sprintf("%s/%s:1-%d", gtidFlavor, serverUUID, 220+c.calls.Add(1))
	return &replicationdatapb.PrimaryStatus{Position: pos, FilePosition: pos, ServerUuid: serverUUID}, nil
}

// TestDeferredReplicationWatcherReportsErrantGTIDsOnce covers a deferred replica
// with an errant GTID on a primary taking writes. The error message includes
// the primary's position, which changes on every check; the watcher must report
// the same errant GTIDs once instead of on every check.
func TestDeferredReplicationWatcherReportsErrantGTIDsOnce(t *testing.T) {
	tm, md, _, id := newReplicatingPendingTM(t)
	md.ExpectedExecuteSuperQueryList = nil
	tmc := &advancingPrimaryTMC{fakeTMClient: newFakeTMClient()}
	tm.tmc = tmc
	errant, err := replication.ParsePosition(gtidFlavor, gtidPosition+",8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1")
	require.NoError(t, err)
	md.SetPrimaryPositionLocked(errant)

	// The watcher updates the reason concurrently.
	reason := func() string {
		tm.tmState.mu.Lock()
		defer tm.tmState.mu.Unlock()
		return tm.tmState.canServe(topodatapb.TabletType_REPLICA)
	}
	go tm.watchReplicationInit(tm.tmState.ctx, id)
	var first string
	require.Eventually(t, func() bool {
		first = reason()
		return strings.Contains(first, "Errant GTID detected")
	}, 30*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return tmc.calls.Load() >= 4 }, 30*time.Second, 10*time.Millisecond)
	assert.Equal(t, first, reason(), "the watcher reported the same errant GTIDs again")
	assert.True(t, tm.tmState.isPendingReplicationInit(id))
}

// TestDeferredRestoreWaitsForReplicationPastRestoredPosition covers a restore
// that deferred replication initialization. As a restore that initializes
// replication does, the replica serves only once replication applies past the
// restored position, unless the primary is still at that position.
func TestDeferredRestoreWaitsForReplicationPastRestoredPosition(t *testing.T) {
	position := func(t *testing.T, gtids string) replication.Position {
		pos, err := replication.ParsePosition(gtidFlavor, gtids)
		require.NoError(t, err)
		return pos
	}
	t.Run("behind the primary", func(t *testing.T) {
		tm, md, qsc, _ := newReplicatingPendingTM(t)
		md.ExpectedExecuteSuperQueryList = nil
		restored := position(t, serverUUID+":1-200")
		md.SetPrimaryPositionLocked(restored)
		id := tm.tmState.deferReplicationInit(t.Context(), nil, &restored)

		ended, err := tm.admitDeferredReplicationInit(t.Context(), id)
		require.ErrorContains(t, err, "waiting for replication to apply past the restored position")
		assert.False(t, ended)
		assert.False(t, qsc.IsServing())

		md.SetPrimaryPositionLocked(position(t, serverUUID+":1-210"))
		ended, err = tm.admitDeferredReplicationInit(t.Context(), id)
		require.NoError(t, err)
		assert.True(t, ended)
		assert.True(t, qsc.IsServing())
	})
	t.Run("empty restored position", func(t *testing.T) {
		tm, md, qsc, _ := newReplicatingPendingTM(t)
		md.ExpectedExecuteSuperQueryList = nil
		var restored replication.Position
		md.SetPrimaryPositionLocked(restored)
		id := tm.tmState.deferReplicationInit(t.Context(), nil, &restored)

		ended, err := tm.admitDeferredReplicationInit(t.Context(), id)
		require.ErrorContains(t, err, "waiting for replication to apply past the restored position")
		assert.False(t, ended)
		assert.False(t, qsc.IsServing())
	})
	t.Run("primary at the restored position", func(t *testing.T) {
		tm, md, qsc, _ := newReplicatingPendingTM(t)
		md.ExpectedExecuteSuperQueryList = nil
		restored := position(t, gtidPosition)
		id := tm.tmState.deferReplicationInit(t.Context(), nil, &restored)

		ended, err := tm.admitDeferredReplicationInit(t.Context(), id)
		require.NoError(t, err)
		assert.True(t, ended)
		assert.True(t, qsc.IsServing())
	})
}

// TestReplicationInitDeferredGauge checks that the ReplicationInitDeferred
// gauge reports a pending deferral, for operators without VTOrc to alert on,
// until the deferral ends.
func TestReplicationInitDeferredGauge(t *testing.T) {
	t.Run("deferral and cancellation", func(t *testing.T) {
		tm, md, ts, _ := newReplicationInitializationTM(t)
		holdInitializationShardLock(t, ts)
		md.ExpectedExecuteSuperQueryList = nil

		_, err := tm.initializeReplicationUnlessShardLocked(t.Context(), logutil.NewMemoryLogger(), topodatapb.TabletType_REPLICA, nil)
		require.NoError(t, err)
		assert.EqualValues(t, 1, statsReplicationInitDeferred.Get())

		tm.tmState.cancelReplicationInit(t.Context())
		assert.EqualValues(t, 0, statsReplicationInitDeferred.Get())
	})
	t.Run("admission", func(t *testing.T) {
		tm, md, _, _ := newReplicatingPendingTM(t)
		md.ExpectedExecuteSuperQueryList = nil
		restored, err := replication.ParsePosition(gtidFlavor, gtidPosition)
		require.NoError(t, err)
		superseded := tm.tmState.deferReplicationInit(t.Context(), nil, &restored)
		id := tm.tmState.deferReplicationInit(t.Context(), nil, &restored)

		// Ending a superseded deferral leaves the gauge set.
		assert.False(t, tm.tmState.endReplicationInit(t.Context(), superseded, false))
		assert.EqualValues(t, 1, statsReplicationInitDeferred.Get())

		ended, err := tm.admitDeferredReplicationInit(t.Context(), id)
		require.NoError(t, err)
		assert.True(t, ended)
		assert.EqualValues(t, 0, statsReplicationInitDeferred.Get())
	})
}

// TestDeferredRestoreRecordsRestoredPosition covers a restore whose replication
// initialization defers: the deferral must record the restored position, which
// the watcher then waits for replication to move past.
func TestDeferredRestoreRecordsRestoredPosition(t *testing.T) {
	tm, md, ts, _ := newReplicationInitializationTM(t)
	holdInitializationShardLock(t, ts)
	md.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA", "FAKE RESET REPLICA ALL"}
	restored, err := replication.ParsePosition(gtidFlavor, serverUUID+":1-200")
	require.NoError(t, err)

	require.NoError(t, tm.startReplication(t.Context(), logutil.NewMemoryLogger(), restored, topodatapb.TabletType_REPLICA))
	require.NoError(t, md.CheckSuperQueryList())
	tm.tmState.mu.Lock()
	id := tm.tmState.pendingReplicationInit
	tm.tmState.mu.Unlock()
	got := tm.tmState.replicationInitRestoredPosition(id)
	require.NotNil(t, got, "the deferral did not record the restored position")
	assert.True(t, got.Equal(restored))
}

// TestRestartAfterRestoreInitializesReplication covers a vttablet with a
// restore flag that restarts after a restore, finding data. It does not
// restore again, and must initialize replication only when none is
// configured, as a restore that deferred replication initialization leaves it.
// Configured replication, including one that a point-in-time restore disabled,
// must stay as it is, without the shard lock. A start that finds data runs the
// restore hook, as on main, with the error if it fails.
func TestRestartAfterRestoreInitializesReplication(t *testing.T) {
	for _, tc := range []struct {
		name          string
		notConfigured bool
		locked        bool
		pointInTime   bool
		disabled      bool
		statusErr     error
		initializes   bool
	}{
		{name: "replication not configured", notConfigured: true, initializes: true},
		{name: "replication not configured, shard lock held", notConfigured: true, locked: true},
		{name: "replication configured", locked: true},
		{name: "point-in-time restore", pointInTime: true},
		{name: "replication disabled by a point-in-time restore", disabled: true, locked: true},
		{name: "replication status unreadable", statusErr: errors.New("status read failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tm, md, ts, _ := newReplicationInitializationTM(t)
			tm.Cnf = &mysqlctl.Mycnf{DataDir: filepath.Join(t.TempDir(), "data")}
			if tc.locked {
				holdInitializationShardLock(t, ts)
			}
			if !tc.initializes {
				md.ExpectedExecuteSuperQueryList = nil
			}
			md.Replicating, md.IOThreadRunning = false, false
			if tc.disabled {
				md.CurrentSourceHost, md.CurrentSourcePort = "//", 0
			}
			if tc.pointInTime {
				previous := restoreToPos
				restoreToPos = "MySQL56/" + gtidPosition
				t.Cleanup(func() { restoreToPos = previous })
			}
			md.ReplicationStatusError = tc.statusErr
			if tc.notConfigured {
				md.ReplicationStatusError = mysql.ErrNotReplica
			}

			hookOutput := setupHookDir(t)
			err := tm.restoreAtStartup(t.Context(), logutil.NewMemoryLogger())
			if !tc.pointInTime {
				// As a restore that finds data does, it runs the restore hook.
				hookEnv := readHookOutput(t, hookOutput)
				assert.Contains(t, hookEnv, "TM_RESTORE_DATA_START_TS=")
				if tc.statusErr != nil {
					assert.Contains(t, hookEnv, "TM_RESTORE_DATA_ERROR=cannot read the replication status")
				} else {
					assert.NotContains(t, hookEnv, "TM_RESTORE_DATA_ERROR=")
				}
			}
			if tc.statusErr != nil {
				// Replication may be configured.
				require.ErrorContains(t, err, "status read failed")
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, md.CheckSuperQueryList())
			assert.Equal(t, tc.initializes, md.Replicating)
			// Only an attempt to initialize finds the shard lock held.
			assert.Equal(t, tc.notConfigured && tc.locked, tm.tmState.replicationInitPending())
		})
	}
}

// TestRestartAfterRestoreHookReportsInitializationError covers a restart with a
// restore flag that finds data and no replication, and fails to initialize it:
// the restore hook must receive that error.
// hungStatusDaemon is a mysqld that does not answer SHOW REPLICA STATUS.
type hungStatusDaemon struct {
	*mysqlctl.FakeMysqlDaemon
}

func (hungStatusDaemon) ReplicationStatus(ctx context.Context) (replication.ReplicationStatus, error) {
	<-ctx.Done()
	return replication.ReplicationStatus{}, ctx.Err()
}

// TestRestartAfterRestoreBoundsReplicationStatus checks that a restart that finds
// data fails, rather than holding the action lock, when mysqld does not answer
// the replication status read.
func TestRestartAfterRestoreBoundsReplicationStatus(t *testing.T) {
	previous := topo.RemoteOperationTimeout
	topo.RemoteOperationTimeout = time.Second
	t.Cleanup(func() { topo.RemoteOperationTimeout = previous })
	tm, md, _, _ := newReplicationInitializationTM(t)
	tm.Cnf = &mysqlctl.Mycnf{DataDir: filepath.Join(t.TempDir(), "data")}
	tm.MysqlDaemon = hungStatusDaemon{md}
	setupHookDir(t)

	done := make(chan error, 1)
	go func() { done <- tm.restoreAtStartup(t.Context(), logutil.NewMemoryLogger()) }()
	select {
	case err := <-done:
		require.ErrorContains(t, err, "cannot read the replication status")
		require.ErrorContains(t, err, context.DeadlineExceeded.Error())
	case <-time.After(30 * time.Second):
		require.FailNow(t, "restoreAtStartup did not bound the replication status read")
	}
}

func TestRestartAfterRestoreHookReportsInitializationError(t *testing.T) {
	tm, md, _, _ := newReplicationInitializationTM(t)
	tm.Cnf = &mysqlctl.Mycnf{DataDir: filepath.Join(t.TempDir(), "data")}
	md.ReplicationStatusError = mysql.ErrNotReplica
	md.ExpectedExecuteSuperQueryList = nil
	tm.tmc = &failingPrimaryStatusClient{TabletManagerClient: tm.tmc, failing: &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}}
	hookOutput := setupHookDir(t)

	err := tm.restoreAtStartup(t.Context(), logutil.NewMemoryLogger())
	require.ErrorContains(t, err, "primary status unavailable")
	assert.Contains(t, readHookOutput(t, hookOutput), "TM_RESTORE_DATA_ERROR=failed to initialize replication")
	require.NoError(t, md.CheckSuperQueryList())
}

// TestRestartAfterRestoreRejectsBadBackupTimestamp checks that a restart that
// finds data still fails on an unparsable --restore-from-backup-ts, as on main,
// before changing replication.
func TestRestartAfterRestoreRejectsBadBackupTimestamp(t *testing.T) {
	tm, md, _, _ := newReplicationInitializationTM(t)
	tm.Cnf = &mysqlctl.Mycnf{DataDir: filepath.Join(t.TempDir(), "data")}
	md.ExpectedExecuteSuperQueryList = nil
	previousRestore, previousTs := restoreFromBackup, restoreFromBackupTsStr
	restoreFromBackup, restoreFromBackupTsStr = true, "not-a-timestamp"
	t.Cleanup(func() { restoreFromBackup, restoreFromBackupTsStr = previousRestore, previousTs })

	err := tm.restoreAtStartup(t.Context(), logutil.NewMemoryLogger())
	require.ErrorContains(t, err, "unable to parse the backup timestamp")
	assert.False(t, md.Replicating)
}

// TestFormerPrimaryIsFencedWithoutPrimaryHostname covers a former primary whose
// shard primary has no MySQL hostname yet: initialization cannot replicate,
// but must still fence it.
func TestFormerPrimaryIsFencedWithoutPrimaryHostname(t *testing.T) {
	tm, md := newFormerPrimaryTM(t, "")
	md.ExpectedExecuteSuperQueryList = nil
	_, err := tm.TopoServer.UpdateTabletFields(t.Context(), &topodatapb.TabletAlias{Cell: "cell1", Uid: 2}, func(tablet *topodatapb.Tablet) error {
		tablet.MysqlHostname = ""
		return nil
	})
	require.NoError(t, err)

	_, err = tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
	require.NoError(t, err)
	assert.False(t, md.SemiSyncPrimaryEnabled, "a former primary must not wait for acknowledgements")
	assert.True(t, md.SuperReadOnly.Load(), "a former primary must not accept writes")
	require.NoError(t, md.CheckSuperQueryList(), "replication must not change")
}

// TestRestartAfterRestoreTakesTheActionLock covers the restart case of
// TestRestartAfterRestoreInitializesReplication, which runs once the tablet may
// serve RPCs: it must not change replication while an action holds the lock.
func TestRestartAfterRestoreTakesTheActionLock(t *testing.T) {
	tm, md, _, _ := newReplicationInitializationTM(t)
	tm.Cnf = &mysqlctl.Mycnf{DataDir: filepath.Join(t.TempDir(), "data")}
	md.ReplicationStatusError = mysql.ErrNotReplica
	require.NoError(t, tm.lock(t.Context()))
	returned := make(chan error, 1)
	go func() { returned <- tm.restoreAtStartup(t.Context(), logutil.NewMemoryLogger()) }()

	assert.Never(t, func() bool { return md.ExpectedExecuteSuperQueryCurrent > 0 || len(returned) > 0 }, time.Second, 10*time.Millisecond,
		"replication changed while an action held the lock")
	tm.unlock()
	var err error
	require.Eventually(t, func() bool {
		select {
		case err = <-returned:
			return true
		default:
			return false
		}
	}, 30*time.Second, 10*time.Millisecond, "initialization did not finish after the action released the lock")
	require.NoError(t, err)
	require.NoError(t, md.CheckSuperQueryList())
	assert.True(t, md.Replicating)
}

// endingDeferralBackupEngine ends the tablet's deferred replication
// initialization during the backup, as the watcher can from a sample taken
// before the backup started.
type endingDeferralBackupEngine struct {
	*mysqlctl.FakeBackupEngine
	tm *TabletManager
}

func (e *endingDeferralBackupEngine) ExecuteBackup(ctx context.Context, params mysqlctl.BackupParams, bh backupstorage.BackupHandle) (mysqlctl.BackupResult, error) {
	e.tm.tmState.cancelReplicationInit(ctx)
	return e.FakeBackupEngine.ExecuteBackup(ctx, params, bh)
}

// TestBackupKeepsTheDeferralItStartedWith covers a deferral that ends during an
// offline backup. The backup's cleanup, which does not hold the shard lock,
// must still leave the source alone, as for a deferred replica.
func TestBackupKeepsTheDeferralItStartedWith(t *testing.T) {
	tm, md, _, _ := newReplicatingPendingTM(t)
	tm.Cnf = &mysqlctl.Mycnf{}
	md.DB().SetNeverFail(true)
	md.ExpectedExecuteSuperQueryList = nil
	// A repoint starts with STOP REPLICA, before the source changes.
	var replicationQueries atomic.Int32
	md.ExecuteSuperQueryListCallback = func() { replicationQueries.Add(1) }
	useFakeBackupStorage(t)
	const engine = "ending-deferral-drain"
	mysqlctl.BackupRestoreEngineMap[engine] = &endingDeferralBackupEngine{
		FakeBackupEngine: &mysqlctl.FakeBackupEngine{
			ShouldDrainForBackupReturn: true,
			ExecuteBackupReturn:        mysqlctl.FakeBackupEngineExecuteBackupReturn{Res: mysqlctl.BackupUsable},
		},
		tm: tm,
	}
	t.Cleanup(func() { delete(mysqlctl.BackupRestoreEngineMap, engine) })
	holdInitializationShardLock(t, tm.TopoServer)

	require.NoError(t, tm.Backup(t.Context(), logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{BackupEngine: new(engine)}))
	assert.Zero(t, replicationQueries.Load(), "backup cleanup changed replication under another operation's shard lock")
	assert.True(t, md.Replicating)
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
		// Tablets on one host differ only by port.
		name: "replicating from another port on the shard primary's host",
		setup: func(t *testing.T, tm *TabletManager, md *mysqlctl.FakeMysqlDaemon, ts *topo.Server) {
			md.CurrentSourcePort = 3307
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
		name: "errant GTID",
		setup: func(t *testing.T, tm *TabletManager, md *mysqlctl.FakeMysqlDaemon, ts *topo.Server) {
			errant, err := replication.ParsePosition(gtidFlavor, gtidPosition+",8bc65c84-3fe4-11ed-a912-257f0fcdd6c9:1")
			require.NoError(t, err)
			md.SetPrimaryPositionLocked(errant)
		},
		err: "Errant GTID detected",
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
			tm.tmState.deferReplicationInit(t.Context(), nil, nil)
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
		tm.tmState.deferReplicationInit(t.Context(), nil, nil)

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

// TestDryRunRestoreFromBackupKeepsDeferral runs a dry-run RestoreFromBackup on a
// replica whose replication initialization is deferred. A dry run changes no
// data, so the replica must stay pending and out of rotation.
func TestDryRunRestoreFromBackupKeepsDeferral(t *testing.T) {
	tm, _, _, qsc, id := newPendingReplicationTM(t)
	tm.Cnf = &mysqlctl.Mycnf{DataDir: filepath.Join(t.TempDir(), "data")}
	manifest, err := json.Marshal(mysqlctl.BackupManifest{BackupMethod: "builtin", Position: replication.Position{}})
	require.NoError(t, err)
	handle := &mysqlctl.FakeBackupHandle{
		NameV: "2026-11-12.000000.zone1-0000000002", ReadOnly: true,
		ReadFileReturnF: func(context.Context, string) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(manifest)), nil
		},
	}
	storage := &mysqlctl.FakeBackupStorage{ListBackupsReturn: mysqlctl.FakeBackupStorageListBackupsReturn{BackupHandles: []backupstorage.BackupHandle{handle}}}
	storage.WithParamsReturn = storage
	const storageName = "fake-dry-run-restore"
	previous := backupstorage.BackupStorageImplementation
	backupstorage.BackupStorageMap[storageName] = storage
	backupstorage.BackupStorageImplementation = storageName
	t.Cleanup(func() {
		backupstorage.BackupStorageImplementation = previous
		delete(backupstorage.BackupStorageMap, storageName)
	})

	logger := logutil.NewMemoryLogger()
	require.NoError(t, tm.RestoreFromBackup(t.Context(), logger, &tabletmanagerdatapb.RestoreFromBackupRequest{DryRun: true}))
	require.Contains(t, logger.String(), "Dry run. No changes made")
	assert.True(t, tm.tmState.isPendingReplicationInit(id), "a dry run changes no data")
	assert.False(t, qsc.IsServing())
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
}

func TestRestoreFinalizationAndDeferredReplication(t *testing.T) {
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
	_, err := tm.initializeReplicationUnlessShardLocked(t.Context(), logger, topodatapb.TabletType_REPLICA, nil)
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

// watcherBlockingDaemon fails the DBA grants wait, the last step of Start, once
// the watcher is inside ReplicationStatus, which then returns only on release.
type watcherBlockingDaemon struct {
	*mysqlctl.FakeMysqlDaemon
	armed             atomic.Bool
	entered, released chan struct{}
}

func (d *watcherBlockingDaemon) WaitForDBAGrants(ctx context.Context, _ time.Duration) error {
	d.armed.Store(true)
	select {
	case <-d.entered:
		return errors.New("no DBA grants")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *watcherBlockingDaemon) ReplicationStatus(ctx context.Context) (replication.ReplicationStatus, error) {
	if d.armed.CompareAndSwap(true, false) {
		close(d.entered)
		<-d.released
	}
	return d.FakeMysqlDaemon.ReplicationStatus(ctx)
}

// TestStartFailureStopsReplicationInitWatcher checks that a Start failing after
// it deferred replication initialization returns only once the watcher has
// exited: the watcher uses the topo server, which the caller may close once
// Start has returned.
func TestStartFailureStopsReplicationInitWatcher(t *testing.T) {
	ts := memorytopo.NewServer(t.Context(), "cell1")
	t.Cleanup(ts.Close)
	_, err := ts.GetOrCreateShard(t.Context(), "ks", "0")
	require.NoError(t, err)
	primary := newTestTablet(t, 2, "ks", "0", nil)
	primary.Type = topodatapb.TabletType_PRIMARY
	require.NoError(t, ts.CreateTablet(t.Context(), primary))
	_, err = ts.UpdateShardFields(t.Context(), "ks", "0", func(si *topo.ShardInfo) error { si.PrimaryAlias = primary.Alias; return nil })
	require.NoError(t, err)
	holdInitializationShardLock(t, ts)

	fakeDb := newTestMysqlDaemon(t, 1)
	md := &watcherBlockingDaemon{FakeMysqlDaemon: fakeDb, entered: make(chan struct{}), released: make(chan struct{})}
	tm := &TabletManager{
		BatchCtx:            t.Context(),
		TopoServer:          ts,
		MysqlDaemon:         md,
		DBConfigs:           &dbconfigs.DBConfigs{},
		SemiSyncMonitor:     semisyncmonitor.CreateTestSemiSyncMonitor(fakeDb.DB(), exporter),
		QueryServiceControl: tabletservermock.NewController(),
	}
	t.Cleanup(tm.stopShardSync)
	config := tabletenv.NewDefaultConfig()
	config.DB = &dbconfigs.DBConfigs{}
	startErr := make(chan error, 1)
	tablet := newTestTablet(t, 1, "ks", "0", nil)
	go func() { startErr <- tm.Start(tablet, config) }()

	select {
	case <-md.entered:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the watcher did not run")
	}
	select {
	case <-startErr:
		assert.Fail(t, "Start returned while its watcher was running")
	case <-time.After(200 * time.Millisecond):
	}
	close(md.released)
	select {
	case err := <-startErr:
		require.ErrorContains(t, err, "no DBA grants")
	case <-time.After(30 * time.Second):
		require.FailNow(t, "Start did not return once its watcher could exit")
	}
}

// closeOrderFactory records, when a tablet record is updated, whether the
// tablet manager's state was still open.
type closeOrderFactory struct {
	topo.Factory
	openDuringWrite func()
}

func (f closeOrderFactory) Create(cell, addr, root string) (topo.Conn, error) {
	c, err := f.Factory.Create(cell, addr, root)
	if err != nil {
		return nil, err
	}
	return closeOrderConn{Conn: c, openDuringWrite: f.openDuringWrite}, nil
}

type closeOrderConn struct {
	topo.Conn
	openDuringWrite func()
}

func (c closeOrderConn) Update(ctx context.Context, path string, contents []byte, version topo.Version) (topo.Version, error) {
	if strings.HasSuffix(path, "/Tablet") {
		c.openDuringWrite()
	}
	return c.Conn.Update(ctx, path, contents, version)
}

// TestCloseStopsWatchersBeforeItsTopoWrite checks that Close stops a deferred
// replication initialization's watcher before it clears the tablet record,
// which can take topo.RemoteOperationTimeout: vttablet stops the query service
// and closes MySQL in OnClose hooks that run concurrently with Close.
func TestCloseStopsWatchersBeforeItsTopoWrite(t *testing.T) {
	_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
	var tm *TabletManager
	var writes, openWrites atomic.Int32
	// Open publishes the tablet record under the state's lock.
	var armed atomic.Bool
	ts, err := topo.NewWithFactory(closeOrderFactory{Factory: factory, openDuringWrite: func() {
		if !armed.Load() {
			return
		}
		writes.Add(1)
		tm.tmState.mu.Lock()
		defer tm.tmState.mu.Unlock()
		if tm.tmState.isOpen {
			openWrites.Add(1)
		}
	}}, "", "")
	require.NoError(t, err)
	t.Cleanup(ts.Close)
	tm, md := newReplicationInitializationTMWithServer(t, ts, 1)
	qsc := tabletservermock.NewController()
	require.NoError(t, qsc.InitDBConfig(&querypb.Target{Keyspace: "ks", Shard: "0"}, nil, md))
	tm.QueryServiceControl = qsc
	tm.tmState.Open()
	armed.Store(true)

	tm.Close()
	require.NotZero(t, writes.Load(), "Close did not update the tablet record")
	assert.Zero(t, openWrites.Load(), "Close updated the tablet record with its state still open")
}

// TestCanceledWatcherLeavesReplicationInitError checks that a watcher whose
// check is canceled by Close does not record the cancellation as the reason
// the replica does not serve.
func TestCanceledWatcherLeavesReplicationInitError(t *testing.T) {
	tm, md, _, _, id := newPendingReplicationTM(t)
	blocked := &blockingStatusDaemon{FakeMysqlDaemon: md, entered: make(chan struct{})}
	tm.MysqlDaemon = blocked
	tm.tmState.setReplicationInitError(t.Context(), id, errors.New("replication is not running"))
	tm.tmState.goWatch(func(ctx context.Context) { tm.watchReplicationInit(ctx, id) })
	select {
	case <-blocked.entered:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the watcher did not check replication")
	}

	tm.tmState.Close()
	tm.tmState.mu.Lock()
	defer tm.tmState.mu.Unlock()
	assert.Equal(t, "replication is not running", tm.tmState.replicationInitError)
}

// blockingStatusDaemon answers SHOW REPLICA STATUS only once ctx is done.
type blockingStatusDaemon struct {
	*mysqlctl.FakeMysqlDaemon
	entered chan struct{}
	once    sync.Once
}

func (d *blockingStatusDaemon) ReplicationStatus(ctx context.Context) (replication.ReplicationStatus, error) {
	d.once.Do(func() { close(d.entered) })
	<-ctx.Done()
	return replication.ReplicationStatus{}, ctx.Err()
}
