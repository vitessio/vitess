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
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/reparenttestutil"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

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

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	t.Cleanup(cancel)
	_, err := tm.initializeReplication(ctx, topodatapb.TabletType_REPLICA)
	require.ErrorContains(t, err, "cannot initialize replication for ks/0")
	assert.Equal(t, vtrpcpb.Code_DEADLINE_EXCEEDED, vterrors.Code(err))
	require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
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
		ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
		defer cancel()
		_, unlock, err := ts.LockShard(ctx, "ks", "0", "concurrent reparent")
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
	factory.AddOperationError(memorytopo.Lock, ".*", topo.NewError(topo.NodeExists, "shard"))
	_, err := tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA)
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_UNAVAILABLE, vterrors.Code(err))
	assert.Zero(t, md.ExpectedExecuteSuperQueryCurrent)
}

func TestInitializeReplicationRereadsPrimaryAfterAcquiringLock(t *testing.T) {
	tm, md, ts, factory := newReplicationInitializationTM(t)
	release := holdInitializationShardLock(t, ts)
	before := factory.GetCallStats().Counts()["Lock"]
	done := make(chan error, 1)
	go func() { _, err := tm.initializeReplication(t.Context(), topodatapb.TabletType_REPLICA); done <- err }()
	require.Eventually(t, func() bool { return factory.GetCallStats().Counts()["Lock"] > before }, 30*time.Second, time.Millisecond)
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
	assert.Less(t, time.Since(start), 5*time.Second)
	rpcCtx := <-client.entered
	deadline, ok := rpcCtx.Deadline()
	require.True(t, ok)
	assert.WithinDuration(t, start.Add(replicationInitializationPrimaryStatusTimeout), deadline, 500*time.Millisecond)
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
		ctxTimeout time.Duration
		deferred   bool
	}{
		{name: "lock held", tryLockErr: topo.NewError(topo.NodeExists, "shard"), deferred: true},
		{name: "acquisition timeout", deferred: true},
		{name: "backend error", tryLockErr: errors.New("topology authorization denied")},
		{name: "parent context expires", ctxTimeout: 100 * time.Millisecond},
	} {
		for actionName, action := range map[string]replicationAction{"start": replicationActionStart, "initialize": replicationActionInitialize} {
			t.Run(tc.name+"/"+actionName, func(t *testing.T) {
				tm, md, ts, factory := newReplicationInitializationTM(t)
				holdInitializationShardLock(t, ts)
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
				if tc.ctxTimeout != 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, tc.ctxTimeout)
					t.Cleanup(cancel)
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
					// The tablet becomes a replica type so that VTOrc repairs its replication.
					assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
					assert.Contains(t, logger.String(), "leaving replication unchanged for VTOrc to repair")
					return
				}
				require.ErrorContains(t, err, "cannot initialize replication for ks/0")
				assert.Equal(t, topodatapb.TabletType_RESTORE, tm.Tablet().Type)
				assert.NotContains(t, logger.String(), "leaving replication unchanged")
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
			assert.False(t, md.Replicating)
			assert.False(t, md.SemiSyncReplicaEnabled)
			assert.Empty(t, md.CurrentSourceHost)
			assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
		})
	}
}
