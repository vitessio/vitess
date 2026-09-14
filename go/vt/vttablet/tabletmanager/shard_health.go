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
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/utils"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	trackShardTabletHealth    = false
	shardTabletHealthInterval = time.Second
)

// shardHealthTopoRefreshInterval is how often the monitor re-reads the shard's
// tablet list from topo. It is intentionally much slower than the ping interval
// to keep topo load low.
const shardHealthTopoRefreshInterval = 10 * time.Second

func init() {
	servenv.OnParseFor("vttablet", registerShardHealthFlags)
}

func registerShardHealthFlags(fs *pflag.FlagSet) {
	utils.SetFlagBoolVar(fs, &trackShardTabletHealth, "track-shard-tablet-health", trackShardTabletHealth,
		"If set, this tablet periodically pings its shard's current primary and reports the primary's vttablet liveness in the FullStatus RPC. Used by VTOrc to form a quorum before failing over an unreachable primary vttablet.")
	utils.SetFlagDurationVar(fs, &shardTabletHealthInterval, "shard-tablet-health-interval", shardTabletHealthInterval,
		"Interval at which this tablet pings its shard's current primary when --track-shard-tablet-health is set. The per-ping timeout is twice this interval.")
}

// tabletPinger is the minimal slice of tmclient.TabletManagerClient the monitor needs.
type tabletPinger interface {
	Ping(ctx context.Context, tablet *topodatapb.Tablet) error
}

// pooledPinger is implemented by tmclients that can ping over a pooled connection
// (e.g. grpctmclient.Client.PingPooled).
type pooledPinger interface {
	PingPooled(ctx context.Context, tablet *topodatapb.Tablet) error
}

// pingerFunc adapts a function to the tabletPinger interface.
type pingerFunc func(ctx context.Context, tablet *topodatapb.Tablet) error

// Ping implements tabletPinger.
func (f pingerFunc) Ping(ctx context.Context, tablet *topodatapb.Tablet) error {
	return f(ctx, tablet)
}

// monitorPinger prefers a pooled ping when the tmclient offers one: the monitor pings the shard
// primary each interval, and dialing a fresh connection per probe would be needless TCP/TLS churn.
// Plain Ping keeps its dial-per-call semantics for all other callers.
func monitorPinger(tmc tabletPinger) tabletPinger {
	if pooled, ok := tmc.(pooledPinger); ok {
		return pingerFunc(pooled.PingPooled)
	}
	// No pooled ping available: the monitor falls back to a fresh dial per probe. Log it once at
	// wiring time so the resulting connection churn (a handshake per interval) is visible rather
	// than silent — e.g. a grpc-cached tmclient does not implement PingPooled.
	log.Warn("shard health monitor: tmclient does not support pooled pings; falling back to dial-per-probe",
		slog.String("tmclient_type", fmt.Sprintf("%T", tmc)))
	return tmc
}

type peerPingHealth struct {
	alias               *topodatapb.TabletAlias
	consecutiveFailures int64
	lastSuccessfulPing  time.Time
	lastAttemptedPing   time.Time
}

// shardHealthMonitor pings this tablet's shard primary and exposes the primary's latest raw
// liveness signals via snapshot(). It is safe for concurrent use.
type shardHealthMonitor struct {
	// evictPooledConn, when set, releases the pooled ping connection to a peer
	// that has left the probe set (e.g. a demoted primary): nothing pings such
	// a peer again, so nothing else would ever close its connection. It must
	// be set before Start and never mutated afterwards.
	evictPooledConn func(tablet *topodatapb.Tablet)

	pinger    tabletPinger
	listPeers func(ctx context.Context) (map[string]*topo.TabletInfo, error)
	// shardPrimaryAlias returns the shard record's authoritative PrimaryAlias (nil when the shard
	// has no primary). The monitor probes exactly this tablet — the one VTOrc evaluates quorum for
	// — rather than whichever tablet record's Type currently says PRIMARY, which can lag or disagree
	// during promotion/demotion.
	shardPrimaryAlias func(ctx context.Context) (*topodatapb.TabletAlias, error)
	selfAlias         string
	interval          time.Duration
	pingTimeout       time.Duration
	now               func() time.Time

	mu       sync.Mutex
	health   map[string]*peerPingHealth
	peers    map[string]*topodatapb.Tablet
	inflight map[string]bool

	refreshing bool
	// cancel, stopped, and the wg increments are all guarded by mu so that Start and a
	// concurrent Stop (e.g. shutdown while the initial peer refresh is still running) cannot
	// race on cancel or misuse the WaitGroup. The monitor is single-use: once stopped it
	// cannot be started again.
	cancel  context.CancelFunc
	stopped bool
	wg      sync.WaitGroup
}

func newShardHealthMonitor(
	pinger tabletPinger,
	listPeers func(ctx context.Context) (map[string]*topo.TabletInfo, error),
	shardPrimaryAlias func(ctx context.Context) (*topodatapb.TabletAlias, error),
	selfAlias string,
	interval, pingTimeout time.Duration,
) *shardHealthMonitor {
	return &shardHealthMonitor{
		pinger:            pinger,
		listPeers:         listPeers,
		shardPrimaryAlias: shardPrimaryAlias,
		selfAlias:         selfAlias,
		interval:          interval,
		pingTimeout:       pingTimeout,
		now:               time.Now,
		health:            make(map[string]*peerPingHealth),
		peers:             make(map[string]*topodatapb.Tablet),
		inflight:          make(map[string]bool),
	}
}

// Start launches the ping and refresh loops. The refresh loop performs the
// initial peer refresh, so Start never blocks on topo and is safe to call on
// the tablet startup path.
func (m *shardHealthMonitor) Start(ctx context.Context) {
	if m.interval <= 0 || m.pingTimeout <= 0 {
		log.Warn("shard health monitor disabled because interval and ping timeout must be positive",
			slog.Duration("interval", m.interval),
			slog.Duration("ping_timeout", m.pingTimeout),
		)
		return
	}

	monitorCtx, cancel := context.WithCancel(ctx)

	m.mu.Lock()
	if m.stopped || m.cancel != nil {
		// Already stopped (Stop won the race during startup) or already started: do not
		// launch a second set of loops, and release the unused context.
		m.mu.Unlock()
		cancel()
		return
	}
	m.cancel = cancel
	// Increment the WaitGroup before launching the loops and while holding mu,
	// so a concurrent Stop either observes the increment before its Wait or
	// installs m.stopped before we get here — never Add-after-Wait.
	m.wg.Add(2)
	m.mu.Unlock()

	go m.pingLoop(monitorCtx)
	go m.refreshLoop(monitorCtx)
}

// Stop cancels the loops and waits for in-flight work to drain. It is idempotent and safe to
// call concurrently with Start.
func (m *shardHealthMonitor) Stop() {
	m.mu.Lock()
	m.stopped = true
	cancel := m.cancel
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	m.wg.Wait()
}

func (m *shardHealthMonitor) pingLoop(ctx context.Context) {
	defer m.wg.Done()
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.runPingRound(ctx)
		}
	}
}

func (m *shardHealthMonitor) refreshLoop(ctx context.Context) {
	defer m.wg.Done()
	// Initial refresh so the first ping round (one interval after start) has a
	// peer list. It runs here, on the loop goroutine, rather than synchronously
	// in Start: a slow or unreachable topo must not stall tablet startup, which
	// calls Start on its critical path. A missed first ping round (topo slower
	// than one ping interval) is harmless — pings begin on the next round.
	if err := m.refreshPeers(ctx); err != nil {
		log.Warn("shard health monitor: initial peer refresh failed", slog.Any("error", err))
	}
	ticker := time.NewTicker(shardHealthTopoRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.refreshPeers(ctx); err != nil {
				log.Warn("shard health monitor: peer refresh failed", slog.Any("error", err))
			}
		}
	}
}

// refreshPeers re-reads the shard's tablet list and prunes health for departed peers.
// It is single-flight: a slow topo read never overlaps the next refresh.
func (m *shardHealthMonitor) refreshPeers(ctx context.Context) error {
	m.mu.Lock()
	if m.refreshing {
		m.mu.Unlock()
		return nil
	}
	m.refreshing = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.refreshing = false
		m.mu.Unlock()
	}()

	refreshCtx, cancel := context.WithTimeout(ctx, shardHealthTopoRefreshInterval)
	defer cancel()
	tablets, err := m.listPeers(refreshCtx)
	if err != nil {
		return err
	}
	primaryAlias, err := m.shardPrimaryAlias(refreshCtx)
	if err != nil {
		return err
	}

	// Track only the current shard primary, not every peer. VTOrc's quorum recovery decision
	// consumes solely each observer's report about the primary (see EvaluatePrimaryQuorum), so
	// pinging and reporting all peers would be all-to-all O(N^2) work per shard for no benefit.
	// Restricting to the primary keeps it O(1) pings per tablet (O(N) per shard) and a single-entry
	// FullStatus payload.
	//
	// Select that primary by the shard record's authoritative PrimaryAlias — the same alias VTOrc
	// evaluates quorum for — and look it up in the tablet map for its address. Selecting by the
	// tablet record's Type instead would diverge from VTOrc whenever a record's type lags during
	// promotion/demotion, so observers would report an alias VTOrc never queries and quorum would
	// silently fail closed. A replica/rdonly probes the primary; the primary itself (self) and any
	// tablet that currently sees no shard primary probe nothing. Broader peer-health probing, if
	// ever needed, should be a separately justified opt-in mode rather than the ERS default.
	peers := make(map[string]*topodatapb.Tablet, 1)
	if primaryAlias != nil {
		if pa := topoproto.TabletAliasString(primaryAlias); pa != m.selfAlias {
			if ti, ok := tablets[pa]; ok {
				peers[pa] = ti.Tablet
			}
		}
	}

	m.mu.Lock()
	var departed []*topodatapb.Tablet
	for alias, tablet := range m.peers {
		if _, ok := peers[alias]; !ok {
			departed = append(departed, tablet)
		}
	}
	m.peers = peers
	for alias := range m.health {
		if _, ok := peers[alias]; !ok {
			delete(m.health, alias)
		}
	}
	m.mu.Unlock()

	// Release the pooled ping connections of peers that left the probe set,
	// outside the mutex: closing a connection may abort an in-flight ping to
	// that peer, whose result is then dropped by pingPeer's membership check.
	if m.evictPooledConn != nil {
		for _, tablet := range departed {
			m.evictPooledConn(tablet)
		}
	}
	return nil
}

// runPingRound dispatches at most one in-flight ping per peer. Ticks that arrive
// while a peer's ping is still outstanding are dropped for that peer, never queued.
func (m *shardHealthMonitor) runPingRound(ctx context.Context) {
	m.mu.Lock()
	peers := make(map[string]*topodatapb.Tablet, len(m.peers))
	maps.Copy(peers, m.peers)
	m.mu.Unlock()

	for alias, tablet := range peers {
		if ctx.Err() != nil {
			return
		}
		if !m.tryAcquire(alias) {
			continue
		}
		// Safe: pingLoop holds a wg count for the duration of this call, so the
		// WaitGroup counter is never zero here and cannot race Stop()'s Wait().
		m.wg.Add(1)
		go func(alias string, tablet *topodatapb.Tablet) {
			defer m.wg.Done()
			defer m.release(alias)
			m.pingPeer(ctx, alias, tablet)
		}(alias, tablet)
	}
}

func (m *shardHealthMonitor) pingPeer(ctx context.Context, alias string, tablet *topodatapb.Tablet) {
	pingCtx, cancel := context.WithTimeout(ctx, m.pingTimeout)
	defer cancel()
	err := m.pinger.Ping(pingCtx, tablet)
	now := m.now()

	m.mu.Lock()
	// If a refresh pruned this peer while the ping was in flight (e.g. the primary moved), drop the
	// result instead of resurrecting a health entry for a tablet that is no longer a tracked peer —
	// otherwise a departed primary would linger in this observer's snapshot until the next refresh.
	if _, ok := m.peers[alias]; !ok {
		m.mu.Unlock()
		// The stale ping may have (re)opened a pooled connection to the
		// departed peer after the refresh already evicted it, and no future
		// refresh will see this alias depart again — so evict here too, or a
		// ping round racing the refresh would leak a connection to a
		// demoted-but-alive primary until the monitor stops. Evicting an
		// absent entry is a no-op, so the common (non-racing) case is free.
		if m.evictPooledConn != nil {
			m.evictPooledConn(tablet)
		}
		return
	}
	defer m.mu.Unlock()
	h := m.health[alias]
	if h == nil {
		h = &peerPingHealth{alias: tablet.Alias}
		m.health[alias] = h
	}
	h.lastAttemptedPing = now
	if err != nil {
		h.consecutiveFailures++
		return
	}
	h.consecutiveFailures = 0
	h.lastSuccessfulPing = now
}

func (m *shardHealthMonitor) tryAcquire(alias string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.inflight[alias] {
		return false
	}
	m.inflight[alias] = true
	return true
}

func (m *shardHealthMonitor) release(alias string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.inflight, alias)
}

// inflightCount reports the number of peers with an in-flight ping. Used by tests
// to observe back-pressure.
func (m *shardHealthMonitor) inflightCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.inflight)
}

// snapshot returns the current raw liveness signals for all known peers.
func (m *shardHealthMonitor) snapshot() []*replicationdatapb.ShardPeerHealth {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.health) == 0 {
		return nil
	}
	now := m.now()
	out := make([]*replicationdatapb.ShardPeerHealth, 0, len(m.health))
	for _, h := range m.health {
		entry := &replicationdatapb.ShardPeerHealth{
			TabletAlias:             h.alias.CloneVT(),
			ConsecutivePingFailures: h.consecutiveFailures,
		}
		if !h.lastSuccessfulPing.IsZero() {
			entry.LastSuccessfulPing = protoutil.TimeToProto(h.lastSuccessfulPing)
		}
		if !h.lastAttemptedPing.IsZero() {
			entry.LastAttemptedPing = protoutil.TimeToProto(h.lastAttemptedPing)
			// The age is measured with this tablet's clock so consumers can judge ping
			// staleness without comparing our timestamp against their own clock.
			entry.TimeSinceLastAttemptedPing = protoutil.DurationToProto(now.Sub(h.lastAttemptedPing))
		}
		out = append(out, entry)
	}
	return out
}
