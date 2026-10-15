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

package buffer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// moveTablesErr is the error the source primary returns for a write to a
// table that the traffic switcher has denied while it switches writes.
var moveTablesErr = vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION,
	"vttablet: rpc error: code = FailedPrecondition desc = disallowed due to rule: enforce denied tables (CallerID: userData1)")

// moveTablesBufferEnv is a buffer wired to a real keyspace event watcher, the
// way the tablet gateway wires them, for the source keyspace ("ks1/0") of a
// MoveTables of table t1 to the keyspace "target".
type moveTablesBufferEnv struct {
	ts      *topo.Server
	hc      *discovery.FakeHealthCheck
	kew     *discovery.KeyspaceEventWatcher
	buf     *Buffer
	primary *topodatapb.Tablet
	// handled counts the keyspace events the buffer has handled.
	handled *atomic.Int32
}

const moveTablesCell = "cell1"

// moveTablesRoutingRules returns the routing rules MoveTables maintains for
// table t1, with writes routed to toKeyspace.
func moveTablesRoutingRules(toKeyspace string) *vschemapb.SrvVSchema {
	to := []string{toKeyspace + ".t1"}
	return &vschemapb.SrvVSchema{
		RoutingRules: &vschemapb.RoutingRules{
			Rules: []*vschemapb.RoutingRule{
				{FromTable: "t1", ToTables: to},
				{FromTable: keyspace + ".t1", ToTables: to},
				{FromTable: "target.t1", ToTables: to},
			},
		},
	}
}

func newMoveTablesBufferEnv(t *testing.T) *moveTablesBufferEnv {
	t.Helper()
	ctx := t.Context()

	resetVariables()
	t.Cleanup(func() { checkVariables(t) })

	// The server is not closed: the watcher may still be reading shard
	// records in the background when the test ends, and closing the server
	// races with those reads. Its watches end with the test's context.
	ts := memorytopo.NewServer(ctx, moveTablesCell)
	require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}))
	require.NoError(t, ts.CreateShard(ctx, keyspace, shard))
	require.NoError(t, ts.UpdateSrvKeyspace(ctx, moveTablesCell, keyspace, &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{{
			ServedType:      topodatapb.TabletType_PRIMARY,
			ShardReferences: []*topodatapb.ShardReference{{Name: shard}},
		}},
	}))
	// MoveTables has been created: the rules exist, writes still go to the source.
	require.NoError(t, ts.UpdateSrvVSchema(ctx, moveTablesCell, moveTablesRoutingRules(keyspace)))

	srvTopo := srvtopo.NewResilientServer(ctx, ts, stats.NewCountersWithSingleLabel("", "", "type"))
	hc := discovery.NewFakeHealthCheck(make(chan *discovery.TabletHealth))
	t.Cleanup(func() { _ = hc.Close() })
	kew := discovery.NewKeyspaceEventWatcher(ctx, srvTopo, hc, moveTablesCell)

	cfg := NewDefaultConfig()
	cfg.Enabled = true
	// Only the keyspace event watcher may end buffering in these tests.
	cfg.Window = 10 * time.Minute
	cfg.MaxFailoverDuration = 10 * time.Minute
	buf := New(cfg)
	t.Cleanup(buf.Shutdown)

	// Forward keyspace events to the buffer, as the tablet gateway does.
	events := kew.Subscribe()
	handled := &atomic.Int32{}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-events:
				buf.HandleKeyspaceEvent(ev)
				handled.Add(1)
			}
		}
	}()

	hc.AddTestTablet(moveTablesCell, "1.1.1.1", 1001, keyspace, shard, topodatapb.TabletType_PRIMARY, true, 10, nil)
	primary := hc.GetPrimaryTablet()
	require.NotNil(t, primary)
	hc.Broadcast(primary)

	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	require.NoError(t, kew.WaitForConsistentKeyspaces(waitCtx, []string{keyspace}))
	// The keyspace became consistent once, and that event must reach the
	// buffer before buffering starts, or it would end the buffering.
	require.Eventually(t, func() bool { return handled.Load() == 1 }, 30*time.Second, time.Millisecond)

	return &moveTablesBufferEnv{ts: ts, hc: hc, kew: kew, buf: buf, primary: primary, handled: handled}
}

// stopSourceWrites denies t1 on the source shard, as the first step of
// SwitchWrites does. It does not rebuild the SrvVSchema.
func (env *moveTablesBufferEnv) stopSourceWrites(t *testing.T) {
	t.Helper()
	_, err := env.ts.UpdateShardFields(t.Context(), keyspace, shard, func(si *topo.ShardInfo) error {
		si.TabletControls = []*topodatapb.Shard_TabletControl{{
			TabletType:   topodatapb.TabletType_PRIMARY,
			DeniedTables: []string{"t1"},
		}}
		return nil
	})
	require.NoError(t, err)
}

// cancelSwitch allows t1 on the source shard again, as cancelling a switch
// does. If rebuildSrvVSchema is set, it then rewrites the unchanged SrvVSchema,
// as a vtctld that rebuilds it on cancel does.
func (env *moveTablesBufferEnv) cancelSwitch(t *testing.T, rebuildSrvVSchema bool) {
	t.Helper()
	_, err := env.ts.UpdateShardFields(t.Context(), keyspace, shard, func(si *topo.ShardInfo) error {
		si.TabletControls = nil
		return nil
	})
	require.NoError(t, err)
	if rebuildSrvVSchema {
		require.NoError(t, env.ts.UpdateSrvVSchema(t.Context(), moveTablesCell, moveTablesRoutingRules(keyspace)))
	}
}

// switchWriteRoute points the routing rules at the target keyspace, which is
// when SwitchWrites rebuilds the SrvVSchema.
func (env *moveTablesBufferEnv) switchWriteRoute(t *testing.T) {
	t.Helper()
	require.NoError(t, env.ts.UpdateSrvVSchema(t.Context(), moveTablesCell, moveTablesRoutingRules("target")))
}

// bufferWrite issues a write that failed with the denied tables error and
// waits until the buffer holds it.
func (env *moveTablesBufferEnv) bufferWrite(t *testing.T) chan error {
	t.Helper()
	done := env.issueWrite(t)
	require.NoError(t, waitForRequestsInFlight(env.buf, 1))
	return done
}

// issueWrite issues a write that failed with the denied tables error.
func (env *moveTablesBufferEnv) issueWrite(t *testing.T) chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		retryDone, err := env.buf.WaitForFailoverEnd(t.Context(), keyspace, shard, env.kew, moveTablesErr)
		if retryDone != nil {
			retryDone()
		}
		done <- err
	}()
	return done
}

func waitForRelease(t *testing.T, done chan error) {
	t.Helper()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the buffered write was not released")
	}
}

// TestMoveTablesBufferingIgnoresPrimaryHealthUpdate verifies that a health
// update from the still-serving source primary does not end the buffering a
// MoveTables switch started. The primary broadcasts its health periodically and
// after a schema reload, and the watcher has not seen any SrvVSchema with the
// switch in it yet when that update arrives.
func TestMoveTablesBufferingIgnoresPrimaryHealthUpdate(t *testing.T) {
	env := newMoveTablesBufferEnv(t)

	env.stopSourceWrites(t)
	done := env.bufferWrite(t)

	// The source primary is still serving and says so. Broadcasting twice
	// returns only after the watcher has processed the first update.
	env.hc.Broadcast(env.primary)
	env.hc.Broadcast(env.primary)

	env.switchWriteRoute(t)
	waitForRelease(t, done)

	stopCounts := stops.Counts()
	require.Equal(t, int64(0), stopCounts[statsKeyJoinedFailoverEndDetected],
		"buffering ended on a health update before writes were switched")
	require.Equal(t, int64(1), stopCounts[statsKeyJoined+"."+string(stopMoveTablesSwitchingTraffic)])
}

// TestMoveTablesBufferingEndsWhenWritesSwitch verifies that buffering started
// by a MoveTables switch ends when the watcher sees the switched routing rules,
// without waiting for another health update from the source primary.
func TestMoveTablesBufferingEndsWhenWritesSwitch(t *testing.T) {
	env := newMoveTablesBufferEnv(t)

	env.stopSourceWrites(t)
	done := env.bufferWrite(t)

	env.switchWriteRoute(t)
	waitForRelease(t, done)

	require.Equal(t, int64(1), stops.Counts()[statsKeyJoined+"."+string(stopMoveTablesSwitchingTraffic)])
}

// TestMoveTablesBufferingEndsWhenSwitchCancelled verifies that buffering
// started by a MoveTables switch ends promptly when the switch is cancelled,
// which allows the tables again without switching the routing rules: on the
// SrvVSchema rebuild that cancelling triggers, and, for a vtctld that does not
// rebuild it, on the next health update from the source primary.
func TestMoveTablesBufferingEndsWhenSwitchCancelled(t *testing.T) {
	for _, tc := range []struct {
		name    string
		rebuild bool
	}{
		{name: "SrvVSchema rebuild", rebuild: true},
		{name: "health update", rebuild: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newMoveTablesBufferEnv(t)

			env.stopSourceWrites(t)
			done := env.bufferWrite(t)

			env.cancelSwitch(t, tc.rebuild)
			if !tc.rebuild {
				env.hc.Broadcast(env.primary)
			}
			waitForRelease(t, done)

			stopCounts := stops.Counts()
			require.Equal(t, int64(1), stopCounts[statsKeyJoined+"."+string(stopMoveTablesSwitchCancelled)])
			require.Equal(t, int64(0), stopCounts[statsKeyJoinedFailoverEndDetected])
		})
	}
}

// TestMoveTablesBufferingStartedAfterSwitchEnds verifies that buffering that a
// stale denied tables error starts after the watcher has already seen the
// writes switched ends at once, rather than waiting for an event that already
// happened.
func TestMoveTablesBufferingStartedAfterSwitchEnds(t *testing.T) {
	env := newMoveTablesBufferEnv(t)

	env.stopSourceWrites(t)
	env.switchWriteRoute(t)
	// The watcher reports the switch; nothing is buffered yet.
	require.Eventually(t, func() bool { return env.handled.Load() == 2 }, 30*time.Second, time.Millisecond)

	// The buffer may release the write before it could be seen in the queue.
	done := env.issueWrite(t)
	waitForRelease(t, done)

	require.Equal(t, int64(1), starts.Counts()[statsKeyJoined])
	require.Equal(t, int64(1), stops.Counts()[statsKeyJoined+"."+string(stopMoveTablesSwitchingTraffic)])
}
