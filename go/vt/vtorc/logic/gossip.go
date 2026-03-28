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
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/gossip"
	"vitess.io/vitess/go/vt/grpcclient"
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

		cfg, ksName := findGossipConfig()
		if cfg != nil && cfg.Enabled {
			startGossipAgent(cfg)
		}

		// Always start the watcher so runtime enable works even if
		// gossip was initially disabled. If no keyspace has gossip
		// enabled yet, pollForGossipKeyspace rescans periodically
		// until one is found.
		var gossipCtx context.Context
		var cancel context.CancelFunc
		gossipCtx, cancel = context.WithCancel(context.Background())
		setGossipCancel(cancel)
		if ksName != "" {
			go watchGossipConfig(gossipCtx, ksName)
		} else {
			go pollForGossipKeyspace(gossipCtx)
		}
	})
}

// startGossipAgent creates and starts the gossip agent from the given config.
func startGossipAgent(cfg *topodatapb.GossipConfig) {
	seeds := discoverGossipSeeds()
	transport := gossip.NewGRPCTransport(gossipDialer{})

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
	if ts == nil {
		return nil, ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
	defer cancel()

	keyspaces, err := ts.GetKeyspaces(ctx)
	if err != nil {
		return nil, ""
	}
	var found *topodatapb.GossipConfig
	var foundKs string
	for _, ksName := range keyspaces {
		ki, err := ts.GetKeyspace(ctx, ksName)
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
}

// watchGossipConfig watches SrvKeyspace for gossip config changes and
// manages the gossip agent lifecycle. Handles cold-enable, disable
// (stopping and clearing the agent), and tuning changes. Returns true
// if a watch was successfully established.
func watchGossipConfig(ctx context.Context, keyspace string) bool {
	if ts == nil || keyspace == "" {
		return false
	}
	retryTicker := time.NewTicker(100 * time.Millisecond)
	defer retryTicker.Stop()

	for {
		watchKeyspace := keyspace
		cells, err := ts.GetCellInfoNames(ctx)
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
			initial, changes, err = ts.WatchSrvKeyspace(watchCtx, cell, watchKeyspace)
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

		// Apply the initial value so we don't miss config set before the watch.
		if initial != nil && initial.Value != nil {
			applyGossipConfigChange(initial.Value)
		}

		restart := false
		for change := range changes {
			if change.Err != nil {
				if ctx.Err() != nil || topo.IsErrType(change.Err, topo.Interrupted) {
					watchCancel()
					return true
				}
				if topo.IsErrType(change.Err, topo.NoNode) {
					if agent := clearGossipAgent(); agent != nil {
						agent.Stop()
					}
				} else {
					log.Error("gossip SrvKeyspace watch error", slog.Any("error", change.Err))
				}
				restart = true
				break
			}
			if change.Value != nil {
				cfg := change.Value.GossipConfig
				if cfg == nil || !cfg.Enabled {
					fallbackCfg, fallbackKeyspace := findGossipConfig()
					if fallbackCfg != nil && fallbackCfg.Enabled && fallbackKeyspace != "" && fallbackKeyspace != watchKeyspace {
						keyspace = fallbackKeyspace
						restart = true
						break
					}
				}
				applyGossipConfigChange(change.Value)
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
		// Disable: stop agent and clear it so stale state isn't analyzed.
		if agent := clearGossipAgent(); agent != nil {
			agent.Stop()
		}
		return
	}
	// Enable or reconfigure.
	agent := currentGossipAgent()
	if agent == nil {
		// Cold-enable: create and start a new agent.
		startGossipAgent(cfg)
		return
	}
	// Tuning update on running agent.
	agent.Reconfigure(gossip.Config{
		PhiThreshold: cfg.PhiThreshold,
		PingInterval: parseDurationVTOrc(cfg.PingInterval, 0),
		MaxUpdateAge: parseDurationVTOrc(cfg.MaxUpdateAge, 0),
	})
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
			cfg, ksName := findGossipConfig()
			if cfg == nil || !cfg.Enabled || ksName == "" {
				continue
			}
			if currentGossipAgent() == nil {
				startGossipAgent(cfg)
			}
			if watchGossipConfig(ctx, ksName) {
				return
			}
			// Watch failed to attach — keep polling.
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
		addr := fmt.Sprintf("%s:%d", tablet.Hostname, grpcPort)
		seeds = append(seeds, gossip.Member{
			ID:   gossip.NodeID(addr),
			Addr: addr,
			Meta: map[string]string{
				gossip.MetaKeyKeyspace:    tablet.Keyspace,
				gossip.MetaKeyShard:       tablet.Shard,
				gossip.MetaKeyTabletAlias: topoproto.TabletAliasString(tablet.Alias),
			},
		})
		return nil
	})
	return seeds
}

type gossipDialer struct{}

func (d gossipDialer) Dial(ctx context.Context, target string) (gossippb.GossipClient, error) {
	if target == "" {
		return nil, nil
	}
	conn, err := grpcclient.DialContext(ctx, target, grpcclient.FailFast(false))
	if err != nil {
		return nil, err
	}
	client := gossippb.NewGossipClient(conn)
	return &gossipClient{client: client, closer: conn}, nil
}

type gossipClient struct {
	client gossippb.GossipClient
	closer io.Closer
}

func (c *gossipClient) PushPull(ctx context.Context, in *gossippb.GossipMessage, opts ...grpc.CallOption) (*gossippb.GossipMessage, error) {
	defer c.closer.Close()
	return c.client.PushPull(ctx, in, opts...)
}

func (c *gossipClient) Join(ctx context.Context, in *gossippb.GossipJoinRequest, opts ...grpc.CallOption) (*gossippb.GossipJoinResponse, error) {
	defer c.closer.Close()
	return c.client.Join(ctx, in, opts...)
}
