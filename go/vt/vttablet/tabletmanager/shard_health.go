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
	"log/slog"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
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
		"If set, this tablet periodically pings the other tablets in its shard and reports their liveness in the FullStatus RPC. Used by VTOrc to form a quorum before failing over an unreachable primary vttablet.")
	utils.SetFlagDurationVar(fs, &shardTabletHealthInterval, "shard-tablet-health-interval", shardTabletHealthInterval,
		"Interval at which this tablet pings its shard peers when --track-shard-tablet-health is set. Also used as the per-ping timeout.")
}

// tabletPinger is the minimal slice of tmclient.TabletManagerClient the monitor needs.
type tabletPinger interface {
	Ping(ctx context.Context, tablet *topodatapb.Tablet) error
}

type peerPingHealth struct {
	alias               *topodatapb.TabletAlias
	consecutiveFailures int64
	lastSuccessfulPing  time.Time
	lastAttemptedPing   time.Time
}

// shardHealthMonitor pings the other tablets in this tablet's shard and exposes the
// latest raw liveness signals via snapshot(). It is safe for concurrent use.
type shardHealthMonitor struct {
	pinger      tabletPinger
	listPeers   func(ctx context.Context) (map[string]*topo.TabletInfo, error)
	selfAlias   string
	interval    time.Duration
	pingTimeout time.Duration
	now         func() time.Time

	mu       sync.Mutex
	health   map[string]*peerPingHealth
	peers    map[string]*topodatapb.Tablet
	inflight map[string]bool

	refreshing bool
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

func newShardHealthMonitor(
	pinger tabletPinger,
	listPeers func(ctx context.Context) (map[string]*topo.TabletInfo, error),
	selfAlias string,
	interval, pingTimeout time.Duration,
) *shardHealthMonitor {
	return &shardHealthMonitor{
		pinger:      pinger,
		listPeers:   listPeers,
		selfAlias:   selfAlias,
		interval:    interval,
		pingTimeout: pingTimeout,
		now:         time.Now,
		health:      make(map[string]*peerPingHealth),
		peers:       make(map[string]*topodatapb.Tablet),
		inflight:    make(map[string]bool),
	}
}

// Start launches the ping and refresh loops. It performs one synchronous peer
// refresh first so the first ping round has a peer list.
func (m *shardHealthMonitor) Start(ctx context.Context) {
	monitorCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel

	if err := m.refreshPeers(monitorCtx); err != nil {
		log.Warn("shard health monitor: initial peer refresh failed", slog.Any("error", err))
	}

	m.wg.Add(2)
	go m.pingLoop(monitorCtx)
	go m.refreshLoop(monitorCtx)
}

// Stop cancels the loops and waits for in-flight work to drain.
func (m *shardHealthMonitor) Stop() {
	if m.cancel != nil {
		m.cancel()
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

	peers := make(map[string]*topodatapb.Tablet, len(tablets))
	for alias, ti := range tablets {
		if alias == m.selfAlias {
			continue
		}
		peers[alias] = ti.Tablet
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.peers = peers
	for alias := range m.health {
		if _, ok := peers[alias]; !ok {
			delete(m.health, alias)
		}
	}
	return nil
}

// runPingRound dispatches at most one in-flight ping per peer. Ticks that arrive
// while a peer's ping is still outstanding are dropped for that peer, never queued.
func (m *shardHealthMonitor) runPingRound(ctx context.Context) {
	m.mu.Lock()
	peers := make(map[string]*topodatapb.Tablet, len(m.peers))
	for alias, tablet := range m.peers {
		peers[alias] = tablet
	}
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
		}
		out = append(out, entry)
	}
	return out
}
