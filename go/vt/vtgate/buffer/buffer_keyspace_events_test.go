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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestBufferingIgnoresKeyspaceEventsSentBeforeItStarted verifies that a
// keyspace event that was sent before buffering started, but was still queued
// when it started, does not end the buffering. Only an event sent after the
// disruption was marked can report its end.
func TestBufferingIgnoresKeyspaceEventsSentBeforeItStarted(t *testing.T) {
	ctx := utils.LeakCheckContext(t)

	ts := memorytopo.NewServer(ctx, singleShardCell)
	t.Cleanup(ts.Close)
	require.NoError(t, ts.CreateKeyspace(ctx, keyspace, &topodatapb.Keyspace{}))
	require.NoError(t, ts.CreateShard(ctx, keyspace, shard))

	healthChecks := make(chan *discovery.TabletHealth, 10)
	kew := discovery.NewKeyspaceEventWatcher(ctx, &singleShardSrvTopo{ts: ts}, discovery.NewFakeHealthCheck(healthChecks), singleShardCell)
	events := kew.Subscribe()

	cfg := NewDefaultConfig()
	cfg.Enabled = true
	// No timeout may end the buffering during the test.
	cfg.Window = time.Minute
	cfg.MaxFailoverDuration = time.Minute
	b := New(cfg)
	t.Cleanup(b.Shutdown)

	nextEvent := func() *discovery.KeyspaceEvent {
		select {
		case ev := <-events:
			return ev
		case <-time.After(30 * time.Second):
			require.FailNow(t, "no keyspace event")
			return nil
		}
	}
	primaryHealth := func(primaryTermStartTime int64) *discovery.TabletHealth {
		return &discovery.TabletHealth{
			Tablet: &topodatapb.Tablet{
				Alias:    &topodatapb.TabletAlias{Cell: singleShardCell, Uid: 100},
				Keyspace: keyspace,
				Shard:    shard,
				Type:     topodatapb.TabletType_PRIMARY,
			},
			Target:               &querypb.Target{Keyspace: keyspace, Shard: shard, TabletType: topodatapb.TabletType_PRIMARY},
			Serving:              true,
			PrimaryTermStartTime: primaryTermStartTime,
		}
	}

	// The watcher reports the keyspace as consistent once it sees its
	// primary. The event is still queued: the buffer has not seen it yet.
	healthChecks <- primaryHealth(1)
	queued := nextEvent()
	require.Equal(t, keyspace, queued.Keyspace)

	// A failover starts and a request is buffered.
	released := make(chan error, 1)
	go func() {
		retryDone, err := b.WaitForFailoverEnd(ctx, keyspace, shard, kew, failoverErr)
		if retryDone != nil {
			retryDone()
		}
		released <- err
	}()
	require.NoError(t, waitForRequestsInFlight(b, 1))

	// The queued event arrives. It was sent before the failover started.
	b.HandleKeyspaceEvent(queued)
	require.Equal(t, stateBuffering, b.getOrCreateBuffer(keyspace, shard).testGetState(),
		"a keyspace event sent before buffering started ended the buffering")

	// The new primary reports itself as serving, which ends the failover.
	healthChecks <- primaryHealth(2)
	b.HandleKeyspaceEvent(nextEvent())
	select {
	case err := <-released:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the buffered request was not released after the failover ended")
	}
	require.NoError(t, waitForState(b, stateIdle))
}
