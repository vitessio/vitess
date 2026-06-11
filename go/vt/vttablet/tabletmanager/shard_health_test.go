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

	"vitess.io/vitess/go/protoutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// fakePinger is a test tabletPinger whose behavior is controlled per call.
type fakePinger struct {
	mu       sync.Mutex
	fail     bool
	block    chan struct{} // when non-nil, Ping blocks until it is closed or ctx is done
	inflight atomic.Int64
	calls    atomic.Int64
}

func (f *fakePinger) Ping(ctx context.Context, tablet *topodatapb.Tablet) error {
	f.inflight.Add(1)
	defer f.inflight.Add(-1)
	f.calls.Add(1)
	f.mu.Lock()
	block := f.block
	fail := f.fail
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if fail {
		return errors.New("ping failed")
	}
	return nil
}

// fakePooledPinger is a fakePinger that also offers a pooled ping, like grpctmclient.Client.
type fakePooledPinger struct {
	fakePinger
	pooledCalls atomic.Int64
}

func (f *fakePooledPinger) PingPooled(ctx context.Context, tablet *topodatapb.Tablet) error {
	f.pooledCalls.Add(1)
	return nil
}

func peerTablet(cell string, uid uint32) *topodatapb.Tablet {
	return &topodatapb.Tablet{Alias: &topodatapb.TabletAlias{Cell: cell, Uid: uid}, Keyspace: "ks", Shard: "0"}
}

func staticLister(tablets ...*topodatapb.Tablet) func(context.Context) (map[string]*topo.TabletInfo, error) {
	return func(context.Context) (map[string]*topo.TabletInfo, error) {
		m := make(map[string]*topo.TabletInfo, len(tablets))
		for _, t := range tablets {
			m[topoproto.TabletAliasString(t.Alias)] = &topo.TabletInfo{Tablet: t}
		}
		return m, nil
	}
}

func TestShardHealthMonitor_PingHealthAccounting(t *testing.T) {
	self := peerTablet("zone1", 100)
	peer := peerTablet("zone1", 101)
	pinger := &fakePinger{fail: true}
	m := newShardHealthMonitor(pinger, staticLister(self, peer), topoproto.TabletAliasString(self.Alias), time.Second, time.Second)

	fixed := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	m.now = func() time.Time { return fixed }

	require.NoError(t, m.refreshPeers(t.Context()))

	// Three failing rounds -> consecutiveFailures == 3, the attempt is stamped with this
	// tablet's clock, and no success is recorded.
	for range 3 {
		m.runPingRound(t.Context())
		assert.Eventually(t, func() bool { return m.inflightCount() == 0 }, 30*time.Second, 5*time.Millisecond)
	}
	snap := m.snapshot()
	require.Len(t, snap, 1)
	assert.Equal(t, int64(3), snap[0].ConsecutivePingFailures)
	require.NotNil(t, snap[0].LastAttemptedPing)
	assert.Equal(t, fixed, protoutil.TimeFromProto(snap[0].LastAttemptedPing).UTC())
	assert.Nil(t, snap[0].LastSuccessfulPing, "no successful ping yet")

	// A success resets the counter to zero and stamps the success time.
	pinger.mu.Lock()
	pinger.fail = false
	pinger.mu.Unlock()
	m.runPingRound(t.Context())
	assert.Eventually(t, func() bool { return m.inflightCount() == 0 }, 30*time.Second, 5*time.Millisecond)
	snap = m.snapshot()
	require.Len(t, snap, 1)
	assert.Equal(t, int64(0), snap[0].ConsecutivePingFailures)
	require.NotNil(t, snap[0].LastSuccessfulPing)
	assert.Equal(t, fixed, protoutil.TimeFromProto(snap[0].LastSuccessfulPing).UTC())

	// The reported ping age is measured with this tablet's clock at snapshot time, so
	// consumers never have to compare our timestamps against their own clock.
	m.now = func() time.Time { return fixed.Add(3 * time.Second) }
	snap = m.snapshot()
	require.Len(t, snap, 1)
	age, ok, err := protoutil.DurationFromProto(snap[0].TimeSinceLastAttemptedPing)
	require.NoError(t, err)
	require.True(t, ok, "snapshot must report the ping age")
	assert.Equal(t, 3*time.Second, age)

	// Self is in the peer list but must never be pinged: 4 rounds x 1 actual peer = 4 calls.
	assert.Equal(t, int64(4), pinger.calls.Load(), "monitor must not ping itself")
}

func TestShardHealthMonitor_BackPressureSingleFlight(t *testing.T) {
	self := peerTablet("zone1", 100)
	peer := peerTablet("zone1", 101)
	pinger := &fakePinger{block: make(chan struct{})}
	m := newShardHealthMonitor(pinger, staticLister(self, peer), topoproto.TabletAliasString(self.Alias), time.Second, 30*time.Second)
	require.NoError(t, m.refreshPeers(t.Context()))

	// Fire several rounds while the first ping is still blocked.
	for range 5 {
		m.runPingRound(t.Context())
	}
	// Exactly one ping is in flight despite five rounds (single-flight per peer).
	assert.Eventually(t, func() bool { return pinger.inflight.Load() == 1 }, 30*time.Second, 5*time.Millisecond)
	assert.Never(t, func() bool { return pinger.inflight.Load() > 1 }, time.Second, 10*time.Millisecond)

	// Unblock; the in-flight ping resolves and the slot frees.
	close(pinger.block)
	assert.Eventually(t, func() bool { return m.inflightCount() == 0 }, 30*time.Second, 5*time.Millisecond)
}

func TestShardHealthMonitor_CancelUnblocksInflight(t *testing.T) {
	self := peerTablet("zone1", 100)
	peer := peerTablet("zone1", 101)
	pinger := &fakePinger{block: make(chan struct{})} // never closed
	m := newShardHealthMonitor(pinger, staticLister(self, peer), topoproto.TabletAliasString(self.Alias), time.Second, 30*time.Second)
	require.NoError(t, m.refreshPeers(t.Context()))

	ctx, cancel := context.WithCancel(t.Context())
	m.runPingRound(ctx)
	assert.Eventually(t, func() bool { return pinger.inflight.Load() == 1 }, 30*time.Second, 5*time.Millisecond)
	cancel() // ping's derived ctx is cancelled -> Ping returns ctx.Err()
	assert.Eventually(t, func() bool { return m.inflightCount() == 0 }, 30*time.Second, 5*time.Millisecond)
	snap := m.snapshot()
	require.Len(t, snap, 1)
	assert.Equal(t, int64(1), snap[0].ConsecutivePingFailures, "cancelled ping counts as a failure")
}

func TestShardHealthMonitor_RefreshPrunesRemovedPeers(t *testing.T) {
	self := peerTablet("zone1", 100)
	peer1 := peerTablet("zone1", 101)
	peer2 := peerTablet("zone1", 102)
	pinger := &fakePinger{fail: true}
	lister := func() func(context.Context) (map[string]*topo.TabletInfo, error) {
		full := staticLister(self, peer1, peer2)
		shrunk := staticLister(self, peer1)
		first := true
		return func(ctx context.Context) (map[string]*topo.TabletInfo, error) {
			if first {
				first = false
				return full(ctx)
			}
			return shrunk(ctx)
		}
	}()
	m := newShardHealthMonitor(pinger, lister, topoproto.TabletAliasString(self.Alias), time.Second, time.Second)

	require.NoError(t, m.refreshPeers(t.Context()))
	m.runPingRound(t.Context())
	assert.Eventually(t, func() bool { return m.inflightCount() == 0 }, 30*time.Second, 5*time.Millisecond)
	require.Len(t, m.snapshot(), 2)

	require.NoError(t, m.refreshPeers(t.Context())) // peer2 disappears
	assert.Len(t, m.snapshot(), 1, "health for removed peer must be pruned")
}

func TestShardHealthMonitor_StartStopDrainsInflight(t *testing.T) {
	self := peerTablet("zone1", 100)
	peer := peerTablet("zone1", 101)
	pinger := &fakePinger{block: make(chan struct{})} // pings block until closed
	m := newShardHealthMonitor(pinger, staticLister(self, peer), topoproto.TabletAliasString(self.Alias), 10*time.Millisecond, 30*time.Second)

	m.Start(t.Context())
	// A ping should become inflight via the ticker.
	assert.Eventually(t, func() bool { return pinger.inflight.Load() == 1 }, 30*time.Second, 5*time.Millisecond)

	close(pinger.block) // let pings complete
	done := make(chan struct{})
	go func() { m.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		assert.Fail(t, "Stop did not return promptly")
	}
	assert.Eventually(t, func() bool { return m.inflightCount() == 0 }, 30*time.Second, 5*time.Millisecond)
}

func TestShardHealthMonitor_StartRejectsNonPositiveTiming(t *testing.T) {
	tests := []struct {
		name        string
		interval    time.Duration
		pingTimeout time.Duration
	}{
		{name: "zero interval", interval: 0, pingTimeout: time.Second},
		{name: "negative interval", interval: -time.Second, pingTimeout: time.Second},
		{name: "zero timeout", interval: time.Second, pingTimeout: 0},
		{name: "negative timeout", interval: time.Second, pingTimeout: -time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var listCalls atomic.Int64
			lister := func(context.Context) (map[string]*topo.TabletInfo, error) {
				listCalls.Add(1)
				return nil, nil
			}
			m := newShardHealthMonitor(&fakePinger{}, lister, "zone1-0000000100", tt.interval, tt.pingTimeout)

			require.NotPanics(t, func() {
				m.Start(t.Context())
				m.Stop()
			})
			assert.Equal(t, int64(0), listCalls.Load())
		})
	}
}

func TestMonitorPinger(t *testing.T) {
	ctx := t.Context()
	peer := peerTablet("zone1", 101)

	// A tmclient that offers a pooled ping is preferred, so the monitor's per-interval
	// probes do not dial a fresh connection each time.
	pooled := &fakePooledPinger{}
	require.NoError(t, monitorPinger(pooled).Ping(ctx, peer))
	assert.Equal(t, int64(1), pooled.pooledCalls.Load(), "must route through PingPooled")
	assert.Equal(t, int64(0), pooled.calls.Load(), "must not use the dial-per-call Ping")

	// A tmclient without a pooled ping is used as-is.
	plain := &fakePinger{}
	require.NoError(t, monitorPinger(plain).Ping(ctx, peer))
	assert.Equal(t, int64(1), plain.calls.Load())
}

func TestTabletManagerStopShardHealthMonitor(t *testing.T) {
	self := peerTablet("zone1", 100)
	m := newShardHealthMonitor(&fakePinger{}, staticLister(self), topoproto.TabletAliasString(self.Alias), time.Second, time.Second)
	m.Start(t.Context())
	tm := &TabletManager{shardHealthMonitor: m}

	// FullStatus reads tm.shardHealthMonitor without synchronization and can run concurrently
	// with Close()/Stop() during lame-duck, so stopping must not mutate the field (write-once).
	// This goroutine races with the stop below under -race if the field is ever rewritten.
	var wg sync.WaitGroup
	wg.Add(1)
	stopReading := make(chan struct{})
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopReading:
				return
			default:
				_ = tm.shardPeerHealthSnapshot()
			}
		}
	}()

	tm.stopShardHealthMonitor()
	tm.stopShardHealthMonitor() // idempotent: both Close() and Stop() call it

	close(stopReading)
	wg.Wait()

	assert.NotNil(t, tm.shardHealthMonitor, "the monitor field is write-once; FullStatus may read it during lame-duck")
	assert.Nil(t, tm.shardPeerHealthSnapshot(), "monitor never pinged anyone, so the snapshot stays empty")
}
