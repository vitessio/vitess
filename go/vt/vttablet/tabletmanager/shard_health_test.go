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
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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

// primaryTablet is a tablet with a PRIMARY record type, for realistic shard-primary fixtures. Note
// that the monitor selects the probe target by the shard record's PrimaryAlias (see
// staticPrimaryAlias), not by this record type — so a target's type is incidental, not the selector.
func primaryTablet(cell string, uid uint32) *topodatapb.Tablet {
	t := peerTablet(cell, uid)
	t.Type = topodatapb.TabletType_PRIMARY
	return t
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

// staticPrimaryAlias returns a shardPrimaryAlias func reporting the given tablet's alias (or nil)
// as the shard record's authoritative primary.
func staticPrimaryAlias(primary *topodatapb.Tablet) func(context.Context) (*topodatapb.TabletAlias, error) {
	return func(context.Context) (*topodatapb.TabletAlias, error) {
		if primary == nil {
			return nil, nil
		}
		return primary.Alias, nil
	}
}

func TestShardHealthMonitor_PingHealthAccounting(t *testing.T) {
	self := peerTablet("zone1", 100)
	peer := primaryTablet("zone1", 101)
	pinger := &fakePinger{fail: true}
	m := newShardHealthMonitor(pinger, staticLister(self, peer), staticPrimaryAlias(peer), topoproto.TabletAliasString(self.Alias), time.Second, time.Second)

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
	peer := primaryTablet("zone1", 101)
	pinger := &fakePinger{block: make(chan struct{})}
	m := newShardHealthMonitor(pinger, staticLister(self, peer), staticPrimaryAlias(peer), topoproto.TabletAliasString(self.Alias), time.Second, 30*time.Second)
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
	peer := primaryTablet("zone1", 101)
	pinger := &fakePinger{block: make(chan struct{})} // never closed
	m := newShardHealthMonitor(pinger, staticLister(self, peer), staticPrimaryAlias(peer), topoproto.TabletAliasString(self.Alias), time.Second, 30*time.Second)
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

func TestShardHealthMonitor_RefreshPrunesDepartedPrimary(t *testing.T) {
	self := peerTablet("zone1", 100)
	oldPrimary := primaryTablet("zone1", 101)
	// After a reparent the old primary (101) is a replica and 102 is the new primary.
	demotedOld := peerTablet("zone1", 101)
	newPrimary := primaryTablet("zone1", 102)
	pinger := &fakePinger{fail: true}
	lister := func() func(context.Context) (map[string]*topo.TabletInfo, error) {
		before := staticLister(self, oldPrimary)
		after := staticLister(self, demotedOld, newPrimary)
		first := true
		return func(ctx context.Context) (map[string]*topo.TabletInfo, error) {
			if first {
				first = false
				return before(ctx)
			}
			return after(ctx)
		}
	}()
	// The shard record's primary moves from 101 to 102 on the second refresh, mirroring the lister.
	primaryAlias := func() func(context.Context) (*topodatapb.TabletAlias, error) {
		first := true
		return func(context.Context) (*topodatapb.TabletAlias, error) {
			if first {
				first = false
				return oldPrimary.Alias, nil
			}
			return newPrimary.Alias, nil
		}
	}()
	m := newShardHealthMonitor(pinger, lister, primaryAlias, topoproto.TabletAliasString(self.Alias), time.Second, time.Second)
	var evicted []string
	m.evictPooledConn = func(tablet *topodatapb.Tablet) {
		evicted = append(evicted, topoproto.TabletAliasString(tablet.Alias))
	}

	// Only the primary (101) is tracked and pinged.
	require.NoError(t, m.refreshPeers(t.Context()))
	m.runPingRound(t.Context())
	assert.Eventually(t, func() bool { return m.inflightCount() == 0 }, 30*time.Second, 5*time.Millisecond)
	snap := m.snapshot()
	require.Len(t, snap, 1)
	assert.Equal(t, "zone1-0000000101", topoproto.TabletAliasString(snap[0].TabletAlias))
	assert.Empty(t, evicted, "no pooled connection may be evicted while the peer is still tracked")

	// The primary moves to 102; the old primary's health must be pruned (it is no longer tracked)
	// and its pooled ping connection released — nothing will ever ping 101 again, so nothing else
	// would close it.
	// 102 has not been pinged yet, so the snapshot is empty rather than carrying stale 101 health.
	require.NoError(t, m.refreshPeers(t.Context()))
	assert.Empty(t, m.snapshot(), "health for the departed primary must be pruned")
	assert.Equal(t, []string{"zone1-0000000101"}, evicted, "the departed primary's pooled connection must be evicted")
}

// TestShardHealthMonitor_InFlightPingDoesNotResurrectPrunedPeer covers the race the prune test
// above does not: a ping still in flight when a refresh prunes its target (the primary moved) must
// not re-insert a health entry for that departed peer when it finally completes.
func TestShardHealthMonitor_InFlightPingDoesNotResurrectPrunedPeer(t *testing.T) {
	self := peerTablet("zone1", 100)
	oldPrimary := primaryTablet("zone1", 101)
	newPrimary := primaryTablet("zone1", 102)

	block := make(chan struct{})
	pinger := &fakePinger{block: block}
	lister := staticLister(self, oldPrimary, newPrimary)

	current := oldPrimary.Alias
	var primaryMu sync.Mutex
	primaryAlias := func(context.Context) (*topodatapb.TabletAlias, error) {
		primaryMu.Lock()
		defer primaryMu.Unlock()
		return current, nil
	}
	m := newShardHealthMonitor(pinger, lister, primaryAlias, topoproto.TabletAliasString(self.Alias), time.Second, 30*time.Second)
	var evictedMu sync.Mutex
	var evicted []string
	m.evictPooledConn = func(tablet *topodatapb.Tablet) {
		evictedMu.Lock()
		defer evictedMu.Unlock()
		evicted = append(evicted, topoproto.TabletAliasString(tablet.Alias))
	}

	// Track only the old primary (101), then dispatch a ping that blocks while in flight.
	require.NoError(t, m.refreshPeers(t.Context()))
	go m.runPingRound(t.Context())
	require.Eventually(t, func() bool { return pinger.inflight.Load() == 1 }, 30*time.Second, time.Millisecond)

	// While 101's ping is in flight, the primary moves to 102; the refresh prunes 101 from peers.
	primaryMu.Lock()
	current = newPrimary.Alias
	primaryMu.Unlock()
	require.NoError(t, m.refreshPeers(t.Context()))

	// Let the in-flight ping complete. It must not resurrect health for the pruned 101, and it
	// must re-evict 101's pooled connection: the stale ping may have (re)opened it after the
	// refresh's eviction, and no future refresh will see 101 depart again.
	close(block)
	assert.Eventually(t, func() bool { return m.inflightCount() == 0 }, 30*time.Second, 5*time.Millisecond)
	assert.Empty(t, m.snapshot(), "an in-flight ping must not re-insert health for a peer pruned during the refresh")
	evictedMu.Lock()
	defer evictedMu.Unlock()
	assert.Equal(t, []string{"zone1-0000000101", "zone1-0000000101"}, evicted,
		"the departed primary must be evicted by the refresh and re-evicted by the stale ping's dropped-result path")
}

// TestShardHealthMonitor_PingsOnlyThePrimary pins that the monitor probes and reports only the
// current shard primary — the single tablet the quorum recovery observes — and not other
// replica/rdonly peers. This keeps the feature O(N) per shard instead of all-to-all O(N^2).
func TestShardHealthMonitor_PingsOnlyThePrimary(t *testing.T) {
	self := peerTablet("zone1", 100)         // this observer (a replica)
	primary := primaryTablet("zone1", 101)   // the primary — must be pinged
	otherReplica := peerTablet("zone1", 102) // another replica — must NOT be pinged
	pinger := &fakePinger{}
	m := newShardHealthMonitor(pinger, staticLister(self, primary, otherReplica), staticPrimaryAlias(primary), topoproto.TabletAliasString(self.Alias), time.Second, time.Second)

	require.NoError(t, m.refreshPeers(t.Context()))
	m.runPingRound(t.Context())
	assert.Eventually(t, func() bool { return m.inflightCount() == 0 }, 30*time.Second, 5*time.Millisecond)

	assert.Equal(t, int64(1), pinger.calls.Load(), "only the primary is pinged, not the other replica or self")
	snap := m.snapshot()
	require.Len(t, snap, 1, "only the primary's health is reported")
	assert.Equal(t, "zone1-0000000101", topoproto.TabletAliasString(snap[0].TabletAlias))
}

// TestShardHealthMonitor_SelectsByShardPrimaryAliasNotType pins that the probed tablet is chosen by
// the shard record's authoritative PrimaryAlias — the alias VTOrc evaluates quorum for — not by the
// tablet record's Type. Here the shard primary (101) still has a stale REPLICA record type (type
// lags during a promotion) while a different tablet (102) momentarily has a PRIMARY record type.
// The monitor must probe 101 (the shard PrimaryAlias), or observers would report an alias VTOrc
// never queries and quorum would silently fail closed.
func TestShardHealthMonitor_SelectsByShardPrimaryAliasNotType(t *testing.T) {
	self := peerTablet("zone1", 100)
	shardPrimary := peerTablet("zone1", 101)        // PrimaryAlias points here, but record type is REPLICA
	staleTypePrimary := primaryTablet("zone1", 102) // record type PRIMARY, but NOT the shard primary
	pinger := &fakePinger{}
	m := newShardHealthMonitor(pinger, staticLister(self, shardPrimary, staleTypePrimary), staticPrimaryAlias(shardPrimary), topoproto.TabletAliasString(self.Alias), time.Second, time.Second)

	require.NoError(t, m.refreshPeers(t.Context()))
	m.runPingRound(t.Context())
	assert.Eventually(t, func() bool { return m.inflightCount() == 0 }, 30*time.Second, 5*time.Millisecond)

	assert.Equal(t, int64(1), pinger.calls.Load(), "exactly one tablet probed")
	snap := m.snapshot()
	require.Len(t, snap, 1)
	assert.Equal(t, "zone1-0000000101", topoproto.TabletAliasString(snap[0].TabletAlias),
		"must probe the shard PrimaryAlias (101), not the tablet whose record type is PRIMARY (102)")
}

func TestShardHealthMonitor_StartStopDrainsInflight(t *testing.T) {
	self := peerTablet("zone1", 100)
	peer := primaryTablet("zone1", 101)
	pinger := &fakePinger{block: make(chan struct{})} // pings block until closed
	m := newShardHealthMonitor(pinger, staticLister(self, peer), staticPrimaryAlias(peer), topoproto.TabletAliasString(self.Alias), 10*time.Millisecond, 30*time.Second)

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

// TestShardHealthMonitor_StartStopConcurrent exercises the shutdown-during-startup race: Start
// and Stop run as concurrently as possible while the initial peer refresh is held open. Start
// writes the cancel func and increments the WaitGroup; Stop reads the cancel func and waits. On
// the unsynchronized version this races on cancel (flagged under -race); with the fix neither
// call hangs or leaks the loops regardless of which wins.
func TestShardHealthMonitor_StartStopConcurrent(t *testing.T) {
	self := peerTablet("zone1", 100)
	// Hold the initial refresh open so Start spends real time between installing cancel and
	// launching the loops, maximizing the overlap with a concurrent Stop.
	releaseRefresh := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRefresh) }) }
	t.Cleanup(release)
	lister := func(ctx context.Context) (map[string]*topo.TabletInfo, error) {
		select {
		case <-releaseRefresh:
		case <-ctx.Done():
		}
		return map[string]*topo.TabletInfo{}, nil
	}
	m := newShardHealthMonitor(&fakePinger{}, lister, staticPrimaryAlias(nil), topoproto.TabletAliasString(self.Alias), 10*time.Millisecond, 30*time.Second)

	barrier := make(chan struct{})
	var loops sync.WaitGroup
	loops.Add(2)
	go func() { defer loops.Done(); <-barrier; m.Start(t.Context()) }()
	go func() { defer loops.Done(); <-barrier; m.Stop() }()
	close(barrier) // release both as simultaneously as possible
	release()      // let any in-flight refresh return so Start can complete even if Stop ran first

	finished := make(chan struct{})
	go func() { loops.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		require.Fail(t, "concurrent Start/Stop did not return")
	}

	// Whichever ordering won, a final (idempotent) Stop must drain the loops.
	m.Stop()
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
			m := newShardHealthMonitor(&fakePinger{}, lister, staticPrimaryAlias(nil), "zone1-0000000100", tt.interval, tt.pingTimeout)

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
	m := newShardHealthMonitor(&fakePinger{}, staticLister(self), staticPrimaryAlias(nil), topoproto.TabletAliasString(self.Alias), time.Second, time.Second)
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
