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

package logic

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/gossip"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vttablet/grpctmclient"

	gossippb "vitess.io/vitess/go/vt/proto/gossip"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	gossipOnce    sync.Once
	gossipStateMu sync.RWMutex
	gossipAgent   *gossip.Gossip
	gossipCancel  context.CancelFunc

	gossipRPCMu       sync.Mutex
	gossipRPCServer   *grpc.Server
	gossipRPCListener net.Listener
	gossipWatchCount  atomic.Int32
	// gossipPollInterval governs how often we rescan all keyspaces
	// looking for the first one with gossip enabled. The poll only
	// runs while no keyspace is enabled yet — once one shows up we
	// upgrade to a SrvKeyspace watch with no further polling, and
	// startGossip itself is gated on --gossip-listen-addr so opt-out
	// admins never hit this path at all. 1s is what makes a fresh
	// `UpdateGossipConfig --enable` visible to VTOrc within seconds,
	// which the issue's manual test relies on.
	gossipPollInterval = time.Second
)

// currentGossipAgent returns the process-wide gossip agent, if any.
// Used by the debug handler, the gRPC service, and the quorum analysis
// loop — all of which need the latest agent across enable/disable
// cycles.
func currentGossipAgent() *gossip.Gossip {
	gossipStateMu.RLock()
	defer gossipStateMu.RUnlock()
	return gossipAgent
}

// setGossipCancel installs the cancel func that tears down the
// SrvKeyspace watcher goroutines spawned by startGossip. Stored under
// the same mutex as gossipAgent so Stop() can drain both atomically.
func setGossipCancel(cancel context.CancelFunc) {
	gossipStateMu.Lock()
	defer gossipStateMu.Unlock()
	gossipCancel = cancel
}

// clearGossipState swaps out both the agent and the watcher-cancel
// func, returning them to the caller for shutdown. One critical
// section so readers never see a half-cleared state.
func clearGossipState() (*gossip.Gossip, context.CancelFunc) {
	gossipStateMu.Lock()
	defer gossipStateMu.Unlock()

	agent := gossipAgent
	cancel := gossipCancel
	gossipAgent = nil
	gossipCancel = nil
	return agent, cancel
}

// clearGossipAgent swaps out just the agent (leaving the watcher
// running). Used by reconcileGossipConfig when gossip gets disabled at
// the keyspace level but VTOrc itself is still running and should keep
// watching for a future re-enable.
func clearGossipAgent() *gossip.Gossip {
	gossipStateMu.Lock()
	defer gossipStateMu.Unlock()

	agent := gossipAgent
	gossipAgent = nil
	return agent
}

// startGossip is the one-shot process-wide entry point (guarded by
// gossipOnce). It registers the debug endpoint + gossip gRPC server,
// does a first reconcile pass, and kicks off either a SrvKeyspace
// watcher (if a keyspace already has gossip enabled) or a poller (to
// wait for the first enable). Called from ContinuousDiscovery so it
// runs as VTOrc boots.
//
// When --gossip-listen-addr is empty the operator has not opted in:
// the agent could never receive peer traffic anyway, so we skip every
// piece of ongoing work (keyspace scans, watch goroutines, the debug
// endpoint, the gRPC listener) instead of paying for them in vain.
// This is the common case while gossip rolls out — most installs will
// have it disabled for a long time.
func startGossip() {
	if config.GossipListenAddr() == "" {
		return
	}
	gossipOnce.Do(func() {
		// Register the debug endpoint once — it safely returns empty
		// when gossipAgent is nil.
		servenv.HTTPHandleFunc("/debug/gossip", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			agent := currentGossipAgent()
			if agent != nil {
				_ = json.NewEncoder(w).Encode(agent.Debug())
			} else {
				_, _ = w.Write([]byte("null\n"))
			}
		})

		startGossipRPCServer()

		reconcileGossipConfig(nil)

		var gossipCtx context.Context
		var cancel context.CancelFunc
		gossipCtx, cancel = context.WithCancel(context.Background())
		setGossipCancel(cancel)
		if !watchExistingGossipKeyspaces(gossipCtx) {
			go pollForGossipKeyspace(gossipCtx)
		}
	})
}

// startGossipRPCServer stands up VTOrc's dedicated gossip gRPC listener
// bound to --gossip-listen-addr. VTOrc doesn't reuse the main vtctld
// gRPC server because it exists to serve gossip traffic specifically,
// which needs to remain available regardless of operator-side service
// map toggles on the primary gRPC server.
func startGossipRPCServer() {
	listenAddr := config.GossipListenAddr()
	if listenAddr == "" {
		return
	}

	gossipRPCMu.Lock()
	defer gossipRPCMu.Unlock()
	if gossipRPCServer != nil {
		return
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Error("failed to listen for gossip gRPC server",
			slog.String("addr", listenAddr),
			slog.Any("error", err))
		return
	}

	server := servenv.NewGRPCServer()
	gossippb.RegisterGossipServer(server, &gossip.Service{
		GetAgent: currentGossipAgent,
	})

	gossipRPCServer = server
	gossipRPCListener = listener

	log.Info("gossip: starting gRPC server", slog.String("addr", listenAddr))
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Error("gossip gRPC server exited",
				slog.String("addr", listenAddr),
				slog.Any("error", err))
		}
	}()
}

// stopGossipRPCServer tears down the listener + gRPC server spun up by
// startGossipRPCServer. Invoked from stopGossip at process termination.
func stopGossipRPCServer() {
	gossipRPCMu.Lock()
	server := gossipRPCServer
	listener := gossipRPCListener
	gossipRPCServer = nil
	gossipRPCListener = nil
	gossipRPCMu.Unlock()

	if server != nil {
		log.Info("gossip: stopping gRPC server")
		server.Stop()
	}
	if listener != nil {
		_ = listener.Close()
	}
}

// startGossipAgent creates and starts the gossip agent from the given config.
func startGossipAgent(cfg *topodatapb.GossipConfig) {
	seeds := discoverGossipSeeds()
	transport := gossip.NewGRPCTransport(&gossip.GRPCDialer{SecureDialOption: grpctmclient.SecureDialOption})
	tuning := effectiveGossipTuning(cfg)

	agent := gossip.New(gossip.Config{
		NodeID:       gossip.NodeID(config.GossipNodeID()),
		BindAddr:     config.GossipListenAddr(),
		Seeds:        seeds,
		PhiThreshold: tuning.PhiThreshold,
		PingInterval: tuning.PingInterval,
		ProbeTimeout: 500 * time.Millisecond,
		MaxUpdateAge: tuning.MaxUpdateAge,
	}, transport, nil)

	if agent == nil {
		return
	}

	gossipStateMu.Lock()
	defer gossipStateMu.Unlock()
	if gossipAgent != nil {
		return
	}
	if err := agent.Start(context.Background()); err != nil {
		log.Error("failed to start gossip", slog.Any("error", err))
		return
	}
	log.Info("gossip: agent started",
		slog.String("node_id", config.GossipNodeID()),
		slog.String("bind_addr", config.GossipListenAddr()),
		slog.Int("seeds", len(seeds)),
		slog.Float64("phi_threshold", tuning.PhiThreshold),
		slog.Duration("ping_interval", tuning.PingInterval),
		slog.Duration("max_update_age", tuning.MaxUpdateAge))
	gossipAgent = agent
}

// findGossipConfig scans all keyspaces for an enabled GossipConfig.
// Returns the config and the keyspace name it was found in (for watching).
// If multiple keyspaces have gossip enabled with differing configs,
// it returns conflict=true so callers can fail closed.
func findGossipConfig() (*topodatapb.GossipConfig, string, bool) {
	cfg, enabledKeyspaces, conflict := findGossipConfigState()
	if conflict {
		return nil, "", true
	}
	if len(enabledKeyspaces) == 0 {
		return cfg, "", false
	}
	// Sort so the pick is deterministic across calls and log the
	// selection so operators can correlate with the watcher they see.
	sort.Strings(enabledKeyspaces)
	selected := enabledKeyspaces[0]
	if len(enabledKeyspaces) > 1 {
		log.Info("multiple keyspaces have gossip enabled; selecting deterministic watch target",
			slog.String("selected", selected),
			slog.Int("total_enabled", len(enabledKeyspaces)))
	}
	return cfg, selected, false
}

// findGossipConfigState scans every Keyspace for an enabled GossipConfig
// and returns the first one found, the list of all enabled keyspaces,
// and whether they conflict on effective tuning. findGossipConfig wraps
// this into the "which keyspace do I watch" decision; this helper is
// separate so tests and other callers can see the full picture.
func findGossipConfigState() (*topodatapb.GossipConfig, []string, bool) {
	topoServer := ts
	if topoServer == nil {
		return nil, nil, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
	defer cancel()

	keyspaces, err := topoServer.GetKeyspaces(ctx)
	if err != nil {
		return nil, nil, false
	}
	var found *topodatapb.GossipConfig
	var enabledKeyspaces []string
	for _, ksName := range keyspaces {
		ki, err := topoServer.GetKeyspace(ctx, ksName)
		if err != nil {
			continue
		}
		if ki.GossipConfig == nil || !ki.GossipConfig.Enabled {
			continue
		}
		enabledKeyspaces = append(enabledKeyspaces, ksName)
		if found == nil {
			found = ki.GossipConfig
			continue
		}
		if effectiveGossipTuning(found) != effectiveGossipTuning(ki.GossipConfig) {
			log.Error("refusing to start gossip: multiple keyspaces have conflicting configs",
				slog.String("keyspace1", enabledKeyspaces[0]),
				slog.String("keyspace2", ksName))
			return nil, enabledKeyspaces, true
		}
	}
	return found, enabledKeyspaces, false
}

// gossipTuning is the normalized shape used for conflict detection and
// agent startup. It's separate from topodatapb.GossipConfig because the
// proto allows zero/empty values meaning "use default", and we want to
// compare post-normalization values so two configs with equivalent
// effective settings are not flagged as conflicting.
type gossipTuning struct {
	PhiThreshold float64
	PingInterval time.Duration
	MaxUpdateAge time.Duration
}

// effectiveGossipTuning normalizes a GossipConfig to its runtime values,
// substituting defaults for unset/invalid fields. Used both for
// conflict detection between keyspaces and for driving agent startup /
// Reconfigure calls.
func effectiveGossipTuning(cfg *topodatapb.GossipConfig) gossipTuning {
	if cfg == nil {
		return gossipTuning{}
	}

	phiThreshold := cfg.PhiThreshold
	if phiThreshold <= 0 {
		phiThreshold = 4
	}

	return gossipTuning{
		PhiThreshold: phiThreshold,
		PingInterval: parseDurationVTOrc(cfg.PingInterval, 1*time.Second),
		MaxUpdateAge: parseDurationVTOrc(cfg.MaxUpdateAge, 5*time.Second),
	}
}

// parseDurationVTOrc parses a duration string with a safe fallback for
// missing/invalid/non-positive values. VTOrc has its own copy (rather
// than sharing with the vttablet one) just so the two binaries don't
// have to import each other's packages.
func parseDurationVTOrc(s string, fallback time.Duration) time.Duration {
	if s == "" {
		return fallback
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

// stopGossip is the process termination hook — wired into servenv.OnTerm
// from ContinuousDiscovery. Drains watchers, stops the agent, and tears
// down the gRPC server in one deterministic order.
func stopGossip() {
	agent, cancel := clearGossipState()
	if cancel != nil {
		cancel()
	}
	if agent != nil {
		agent.Stop()
	}
	stopGossipRPCServer()
}

// reconcileGossipConfig reconciles the process-wide agent against the
// current topology: start an agent if any keyspace has gossip enabled,
// Reconfigure it if settings changed, or clear it if nothing is enabled
// or keyspaces conflict. The one place responsible for deciding the
// agent's lifecycle based on topology state, which makes the
// enable/disable/re-enable/conflict semantics easier to reason about.
func reconcileGossipConfig(localCfg *topodatapb.GossipConfig) {
	cfg, _, conflict := findGossipConfig()
	if !conflict && cfg == nil && localCfg != nil && localCfg.Enabled {
		cfg = localCfg
	}
	if conflict || cfg == nil || !cfg.Enabled {
		if agent := clearGossipAgent(); agent != nil {
			if conflict {
				log.Warn("stopping gossip agent due to conflicting keyspace configs (fail-closed)")
			} else {
				log.Info("stopping gossip agent: no keyspace has gossip enabled")
			}
			agent.Stop()
		}
		return
	}

	agent := currentGossipAgent()
	if agent == nil {
		startGossipAgent(cfg)
		return
	}

	tuning := effectiveGossipTuning(cfg)
	agent.Reconfigure(gossip.Config{
		PhiThreshold: tuning.PhiThreshold,
		PingInterval: tuning.PingInterval,
		MaxUpdateAge: tuning.MaxUpdateAge,
	})
}

// watchExistingGossipKeyspaces picks a single keyspace with gossip
// enabled (if any) and starts watching its SrvKeyspace for config
// changes. Only one watcher runs at a time — watching every enabled
// keyspace would create O(N) long-lived watches in large installs,
// and VTOrc only needs one driver keyspace because the gossip agent
// is process-global.
func watchExistingGossipKeyspaces(ctx context.Context) bool {
	_, selected, _ := findGossipConfig()
	if selected == "" {
		return false
	}
	go watchGossipConfig(ctx, selected)
	return true
}

// watchGossipConfig watches SrvKeyspace for gossip config changes and
// manages the gossip agent lifecycle. Handles cold-enable, disable
// (stopping and clearing the agent), and tuning changes. Returns true
// if a watch was successfully established.
func watchGossipConfig(ctx context.Context, keyspace string) bool {
	gossipWatchCount.Add(1)
	defer gossipWatchCount.Add(-1)

	topoServer := ts
	if topoServer == nil || keyspace == "" {
		return false
	}
	retryTicker := time.NewTicker(100 * time.Millisecond)
	defer retryTicker.Stop()

	for {
		watchKeyspace := keyspace
		cells, err := topoServer.GetCellInfoNames(ctx)
		if err != nil || len(cells) == 0 {
			if ctx.Err() != nil {
				return false
			}
			select {
			case <-ctx.Done():
				return false
			case <-retryTicker.C:
			}
			continue
		}

		// Try each cell until we find one serving this keyspace.
		var initial *topo.WatchSrvKeyspaceData
		var changes <-chan *topo.WatchSrvKeyspaceData
		watchCtx, watchCancel := context.WithCancel(ctx)
		for _, cell := range cells {
			initial, changes, err = topoServer.WatchSrvKeyspace(watchCtx, cell, watchKeyspace)
			if err == nil {
				break
			}
		}
		if changes == nil {
			watchCancel()
			if ctx.Err() != nil {
				return false
			}
			select {
			case <-ctx.Done():
				return false
			case <-retryTicker.C:
			}
			continue
		}

		if initial != nil && initial.Value != nil {
			reconcileGossipConfig(initial.Value.GossipConfig)
		}

		restart := false
		for change := range changes {
			if change.Err != nil {
				if ctx.Err() != nil || topo.IsErrType(change.Err, topo.Interrupted) {
					watchCancel()
					return true
				}
				if !topo.IsErrType(change.Err, topo.NoNode) {
					log.Error("gossip SrvKeyspace watch error", slog.Any("error", change.Err))
				}
				reconcileGossipConfig(nil)
				if _, fallbackKeyspace, conflict := findGossipConfig(); !conflict && fallbackKeyspace != "" && fallbackKeyspace != watchKeyspace {
					keyspace = fallbackKeyspace
				}
				restart = true
				break
			}
			if change.Value != nil {
				cfg := change.Value.GossipConfig
				reconcileGossipConfig(cfg)
				if cfg == nil || !cfg.Enabled {
					_, fallbackKeyspace, conflict := findGossipConfig()
					if !conflict && fallbackKeyspace != "" && fallbackKeyspace != watchKeyspace {
						keyspace = fallbackKeyspace
						restart = true
						break
					}
					// No enabled fallback keyspace. Re-reconcile with
					// nil to guarantee the agent is cleared even when
					// reconcileGossipConfig above reconfigured it
					// based on a keyspace that was disabled after we
					// read its state (i.e., racing disables in
					// multiple keyspaces).
					if conflict || fallbackKeyspace == "" {
						reconcileGossipConfig(nil)
					}
				}
			}
		}
		watchCancel()
		if !restart {
			return true
		}

		select {
		case <-ctx.Done():
			return false
		case <-retryTicker.C:
		}
	}
}

// applyGossipConfigChange processes a SrvKeyspace change for gossip config.
// It handles enable, disable, and tuning updates.
func applyGossipConfigChange(srvKs *topodatapb.SrvKeyspace) {
	cfg := srvKs.GossipConfig
	if cfg == nil || !cfg.Enabled {
		reconcileGossipConfig(nil)
		return
	}
	reconcileGossipConfig(cfg)
}

// pollForGossipKeyspace periodically rescans topo for a keyspace with
// gossip enabled. Once found, it starts the gossip agent and transitions
// to the normal SrvKeyspace watcher. Only stops polling after a watch
// is successfully established.
func pollForGossipKeyspace(ctx context.Context) {
	ticker := time.NewTicker(gossipPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if watchExistingGossipKeyspaces(ctx) {
				return
			}
		}
	}
}

// discoverGossipSeeds queries VTOrc's tablet DB for all known tablets
// and returns their gRPC addresses as gossip seeds.
func discoverGossipSeeds() []gossip.Member {
	query := `SELECT info FROM vitess_tablet`
	var seeds []gossip.Member
	opts := prototext.UnmarshalOptions{DiscardUnknown: true}
	_ = db.QueryVTOrc(query, nil, func(row sqlutils.RowMap) error {
		tablet := &topodatapb.Tablet{}
		if err := opts.Unmarshal([]byte(row.GetString("info")), tablet); err != nil {
			return nil
		}
		grpcPort, ok := tablet.PortMap["grpc"]
		if !ok || grpcPort == 0 || tablet.Hostname == "" {
			return nil
		}
		alias := topoproto.TabletAliasString(tablet.Alias)
		if alias == "" {
			return nil
		}
		// netutil.JoinHostPort handles IPv6 literal bracketing so we
		// don't produce targets like "::1:16100".
		addr := netutil.JoinHostPort(tablet.Hostname, grpcPort)
		seeds = append(seeds, gossip.Member{
			// Vttablets publish their tablet alias as the gossip node ID, so VTOrc
			// must seed them with that same identity. Using the address here can
			// collapse a tablet into VTOrc's own node ID when the listen address
			// matches a tablet gRPC port, which makes quorum analysis drop that peer.
			ID:   gossip.NodeID(alias),
			Addr: addr,
			Meta: map[string]string{
				gossip.MetaKeyKeyspace:    tablet.Keyspace,
				gossip.MetaKeyShard:       tablet.Shard,
				gossip.MetaKeyTabletAlias: alias,
			},
		})
		return nil
	})
	return seeds
}
