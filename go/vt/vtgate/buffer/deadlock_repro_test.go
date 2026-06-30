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
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/srvtopo/srvtopotest"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TestStartBufferingDeadlockWithKeyspaceEvents reproduces a deadlock cycle
// between the shard buffer and the keyspace event watcher:
//
//   - Thread A (vtgate request): holds shardBuffer.mu in waitForFailoverEnd ->
//     startBufferingLocked -> kev.MarkShardNotServing, which blocks acquiring
//     the keyspaceState.mu.
//   - Thread X (KeyspaceEventWatcher healthcheck goroutine): holds
//     keyspaceState.mu in onHealthCheck -> ensureConsistentLocked ->
//     broadcast, which blocks sending to the full (cap 10) subscriber channel.
//   - Thread C (the subscriber consumer, TabletGateway.setupBuffering's
//     goroutine in production): blocks acquiring shardBuffer.mu in
//     HandleKeyspaceEvent -> recordKeyspaceEvent, so it never returns to
//     drain the subscriber channel.
//
// A waits on X, X waits on C, C waits on A. Each blocking step below is
// verified via goroutine stacks before the next thread is engaged, so the
// test deterministically wedges if the cycle exists and passes once any
// edge of the cycle is broken.
func TestStartBufferingDeadlockWithKeyspaceEvents(t *testing.T) {
	const (
		cell     = "cell1"
		keyspace = "ks1"
		shard    = "0"
	)

	ctx := t.Context()

	srvTopo := srvtopotest.NewPassthroughSrvTopoServer()
	srvTopo.SrvKeyspaceNames = []string{keyspace}
	srvTopo.SrvKeyspace = &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{{
			ServedType:      topodatapb.TabletType_PRIMARY,
			ShardReferences: []*topodatapb.ShardReference{{Name: shard}},
		}},
	}

	fhc := discovery.NewFakeHealthCheck(make(chan *discovery.TabletHealth))
	defer fhc.Close()

	kev := discovery.NewKeyspaceEventWatcher(ctx, srvTopo, fhc, cell)
	subscriber := kev.Subscribe()

	cfg := NewDefaultConfig()
	cfg.Enabled = true
	cfg.Window = 1 * time.Second
	cfg.MaxFailoverDuration = 2 * time.Second
	// NOTE: no buf.Shutdown() — when the deadlock engages it would block on
	// shardBuffer.mu forever and hang the test binary instead of failing.
	buf := New(cfg)

	// Prime the keyspace event watcher: a serving PRIMARY healthcheck creates
	// the keyspaceState and shardState, makes the keyspace consistent and
	// broadcasts one event into the subscriber channel.
	conn := fhc.AddTestTablet(cell, "1.1.1.1", 1, keyspace, shard, topodatapb.TabletType_PRIMARY, true, 0, nil)
	tablet := conn.Tablet()
	fhc.Broadcast(tablet)
	require.Eventually(t, func() bool {
		return len(subscriber) == 1
	}, 30*time.Second, 10*time.Millisecond, "priming keyspace event was not broadcast")

	// Fill the remaining 9 slots of the cap-10 subscriber channel. In
	// production these are events from other keyspaces backing up while the
	// consumer goroutine is busy.
	for range 9 {
		subscriber <- &discovery.KeyspaceEvent{}
	}

	// Thread X: flip the primary not-serving -> serving. Processing the
	// second healthcheck makes the keyspace consistent again, so the
	// healthcheck goroutine broadcasts while holding keyspaceState.mu and
	// blocks on the full subscriber channel.
	fhc.SetServing(tablet, false)
	fhc.Broadcast(tablet)
	fhc.SetServing(tablet, true)
	fhc.Broadcast(tablet)
	waitForGoroutineIn(t, "discovery.(*KeyspaceEventWatcher).broadcast")

	// Thread A: a request sees a reparent error, takes shardBuffer.mu, tries
	// to start buffering and blocks on keyspaceState.mu inside
	// MarkShardNotServing.
	aDone := make(chan struct{})
	go func() {
		defer close(aDone)
		reparentErr := vterrors.New(vtrpcpb.Code_CLUSTER_EVENT, ClusterEventReparentInProgress)
		retryDone, _ := buf.WaitForFailoverEnd(ctx, keyspace, shard, kev, reparentErr)
		if retryDone != nil {
			retryDone()
		}
	}()
	waitForGoroutineIn(t, "discovery.(*KeyspaceEventWatcher).MarkShardNotServing")

	// Thread C: the subscriber consumer processes an event it received
	// earlier (before the channel backed up) and blocks on shardBuffer.mu in
	// recordKeyspaceEvent. Only after that would it return to draining the
	// subscriber channel, exactly like TabletGateway's consumer goroutine.
	go func() {
		buf.HandleKeyspaceEvent(&discovery.KeyspaceEvent{
			Cell:     cell,
			Keyspace: keyspace,
			Shards: []discovery.ShardEvent{{
				Tablet:  tablet.Alias,
				Target:  &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_PRIMARY},
				Serving: true,
			}},
		})
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-subscriber:
				if ev == nil {
					return
				}
				buf.HandleKeyspaceEvent(ev)
			}
		}
	}()

	// If the cycle exists, thread A never returns: it waits on
	// keyspaceState.mu (held by X), X waits on the subscriber channel
	// (drained only by C), and C waits on shardBuffer.mu (held by A).
	// Without the deadlock, C unblocks, drains the channel, X's broadcast
	// completes and A's buffered request finishes well within the window
	// (1s) / max failover duration (2s).
	select {
	case <-aDone:
	case <-time.After(20 * time.Second):
		stacks := allStacks()
		require.Failf(t, "deadlock", "WaitForFailoverEnd is deadlocked: sb.mu -> kss.mu -> subscriber channel -> sb.mu\n%s", stacks)
	}
}

// waitForGoroutineIn blocks until some goroutine has the given function on
// its stack, i.e. it is executing or blocked inside it.
func waitForGoroutineIn(t *testing.T, fn string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return strings.Contains(allStacks(), fn)
	}, 30*time.Second, 10*time.Millisecond, "no goroutine reached %s", fn)
}

func allStacks() string {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return string(buf[:n])
}
