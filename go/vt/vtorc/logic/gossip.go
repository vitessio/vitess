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
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/gossip"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"

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
)

func currentGossipAgent() *gossip.Gossip {
	gossipStateMu.RLock()
	defer gossipStateMu.RUnlock()
	return gossipAgent
}

func setGossipCancel(cancel context.CancelFunc) {
	gossipStateMu.Lock()
	defer gossipStateMu.Unlock()
	gossipCancel = cancel
}

func clearGossipState() (*gossip.Gossip, context.CancelFunc) {
	gossipStateMu.Lock()
	defer gossipStateMu.Unlock()

	agent := gossipAgent
	cancel := gossipCancel
	gossipAgent = nil
	gossipCancel = nil
	return agent, cancel
}

func clearGossipAgent() *gossip.Gossip {
	gossipStateMu.Lock()
	defer gossipStateMu.Unlock()

	agent := gossipAgent
	gossipAgent = nil
	return agent
}

func startGossip() {
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

	server := grpc.NewServer()
	gossippb.RegisterGossipServer(server, &gossip.Service{
		GetAgent: currentGossipAgent,
	})

	gossipRPCServer = server
	gossipRPCListener = listener

	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Error("gossip gRPC server exited",
				slog.String("addr", listenAddr),
				slog.Any("error", err))
		}
	}()
}

func stopGossipRPCServer() {
	gossipRPCMu.Lock()
	server := gossipRPCServer
	listener := gossipRPCListener
	gossipRPCServer = nil
	gossipRPCListener = nil
	gossipRPCMu.Unlock()

	if server != nil {
		server.Stop()
	}
	if listener != nil {
		_ = listener.Close()
	}
}

// startGossipAgent creates and starts the gossip agent from the given config.
func startGossipAgent(cfg *topodatapb.GossipConfig) {
	seeds := discoverGossipSeeds()
	transport := gossip.NewGRPCTransport(gossip.GRPCDialer{})

	pingInterval := parseDurationVTOrc(cfg.PingInterval, 1*time.Second)
	maxUpdateAge := parseDurationVTOrc(cfg.MaxUpdateAge, 5*time.Second)
	phiThreshold := cfg.PhiThreshold
	if phiThreshold <= 0 {
		phiThreshold = 4
	}

	agent := gossip.New(gossip.Config{
		NodeID:       gossip.NodeID(config.GossipNodeID()),
		BindAddr:     config.GossipListenAddr(),
		Seeds:        seeds,
		PhiThreshold: phiThreshold,
		PingInterval: pingInterval,
		ProbeTimeout: 500 * time.Millisecond,
		MaxUpdateAge: maxUpdateAge,
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
	gossipAgent = agent
}

// findGossipConfig scans all keyspaces for an enabled GossipConfig.
// Returns the config and the keyspace name it was found in (for watching).
// If multiple keyspaces have gossip enabled with differing configs,
// the first one found is used and a warning is logged.
func findGossipConfig() (*topodatapb.GossipConfig, string) {
	topoServer := ts
	if topoServer == nil {
		return nil, ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
	defer cancel()

	keyspaces, err := topoServer.GetKeyspaces(ctx)
	if err != nil {
		return nil, ""
	}
	var found *topodatapb.GossipConfig
	var foundKs string
	for _, ksName := range keyspaces {
		ki, err := topoServer.GetKeyspace(ctx, ksName)
		if err != nil {
			continue
		}
		if ki.GossipConfig == nil || !ki.GossipConfig.Enabled {
			continue
		}
		if found == nil {
			found = ki.GossipConfig
			foundKs = ksName
			continue
		}
		if found.PhiThreshold != ki.GossipConfig.PhiThreshold ||
			found.PingInterval != ki.GossipConfig.PingInterval ||
			found.MaxUpdateAge != ki.GossipConfig.MaxUpdateAge {
			log.Error("refusing to start gossip: multiple keyspaces have conflicting configs",
				slog.String("keyspace1", foundKs),
				slog.String("keyspace2", ksName))
			return nil, ""
		}
	}
	return found, foundKs
}

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

func reconcileGossipConfig(localCfg *topodatapb.GossipConfig) {
	cfg, _ := findGossipConfig()
	if cfg == nil && localCfg != nil && localCfg.Enabled {
		cfg = localCfg
	}
	if cfg == nil || !cfg.Enabled {
		if agent := clearGossipAgent(); agent != nil {
			agent.Stop()
		}
		return
	}

	agent := currentGossipAgent()
	if agent == nil {
		startGossipAgent(cfg)
		return
	}

	agent.Reconfigure(gossip.Config{
		PhiThreshold: cfg.PhiThreshold,
		PingInterval: parseDurationVTOrc(cfg.PingInterval, 0),
		MaxUpdateAge: parseDurationVTOrc(cfg.MaxUpdateAge, 0),
	})
}

func watchExistingGossipKeyspaces(ctx context.Context) bool {
	topoServer := ts
	if topoServer == nil {
		return false
	}

	keyspaces, err := topoServer.GetKeyspaces(ctx)
	if err != nil || len(keyspaces) == 0 {
		return false
	}

	for _, ksName := range keyspaces {
		ksName := ksName
		go watchGossipConfig(ctx, ksName)
	}

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
				if _, fallbackKeyspace := findGossipConfig(); fallbackKeyspace != "" && fallbackKeyspace != watchKeyspace {
					keyspace = fallbackKeyspace
				}
				restart = true
				break
			}
			if change.Value != nil {
				cfg := change.Value.GossipConfig
				reconcileGossipConfig(cfg)
				if cfg == nil || !cfg.Enabled {
					if _, fallbackKeyspace := findGossipConfig(); fallbackKeyspace != "" && fallbackKeyspace != watchKeyspace {
						keyspace = fallbackKeyspace
						restart = true
						break
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
	ticker := time.NewTicker(30 * time.Second)
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
		addr := fmt.Sprintf("%s:%d", tablet.Hostname, grpcPort)
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
