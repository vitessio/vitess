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
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"

	gossippb "vitess.io/vitess/go/vt/proto/gossip"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	gossipOnce  sync.Once
	gossipAgent *gossip.Gossip
)

func startGossip() {
	gossipOnce.Do(func() {
		cfg, ksName := findGossipConfig()
		if cfg == nil || !cfg.Enabled {
			return
		}

		seeds := discoverGossipSeeds()
		transport := gossip.NewGRPCTransport(gossipDialer{})

		pingInterval := parseDurationVTOrc(cfg.PingInterval, 1*time.Second)
		maxUpdateAge := parseDurationVTOrc(cfg.MaxUpdateAge, 5*time.Second)
		phiThreshold := cfg.PhiThreshold
		if phiThreshold <= 0 {
			phiThreshold = 4
		}

		gossipAgent = gossip.New(gossip.Config{
			NodeID:       gossip.NodeID(config.GossipNodeID()),
			BindAddr:     config.GossipListenAddr(),
			Seeds:        seeds,
			PhiThreshold: phiThreshold,
			PingInterval: pingInterval,
			ProbeTimeout: 500 * time.Millisecond,
			MaxUpdateAge: maxUpdateAge,
		}, transport, nil)

		if gossipAgent == nil {
			return
		}

		if err := gossipAgent.Start(context.Background()); err != nil {
			log.Error("failed to start gossip", slog.Any("error", err))
			return
		}

		servenv.OnTerm(gossipAgent.Stop)

		servenv.HTTPHandleFunc("/debug/gossip", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(gossipAgent.Debug())
		})

		go watchGossipConfig(ksName)
	})
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
			log.Warn("multiple keyspaces have gossip enabled with different configs",
				slog.String("using", foundKs),
				slog.String("conflict", ksName))
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
	if gossipAgent == nil {
		return
	}
	gossipAgent.Stop()
}

// watchGossipConfig watches SrvKeyspace for gossip config changes and
// applies them to the running gossip agent. Uses the first available
// cell for the watch.
func watchGossipConfig(keyspace string) {
	if ts == nil || keyspace == "" {
		return
	}
	ctx := context.Background()
	cells, err := ts.GetCellInfoNames(ctx)
	if err != nil || len(cells) == 0 {
		return
	}
	_, changes, err := ts.WatchSrvKeyspace(ctx, cells[0], keyspace)
	if err != nil {
		return
	}
	for change := range changes {
		if change.Err != nil {
			if !topo.IsErrType(change.Err, topo.Interrupted) {
				log.Error("gossip SrvKeyspace watch error", slog.Any("error", change.Err))
			}
			return
		}
		if change.Value == nil {
			continue
		}
		cfg := change.Value.GossipConfig
		if cfg == nil {
			continue
		}
		if !cfg.Enabled && gossipAgent != nil {
			gossipAgent.Stop()
			continue
		}
		if cfg.Enabled && gossipAgent != nil {
			gossipAgent.Reconfigure(gossip.Config{
				PhiThreshold: cfg.PhiThreshold,
				PingInterval: parseDurationVTOrc(cfg.PingInterval, 0),
				MaxUpdateAge: parseDurationVTOrc(cfg.MaxUpdateAge, 0),
			})
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
