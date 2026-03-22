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
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/gossip"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"

	gossippb "vitess.io/vitess/go/vt/proto/gossip"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	gossipOnce  sync.Once
	gossipAgent *gossip.Gossip
)

func startGossip() {
	gossipOnce.Do(func() {
		cfg := findGossipConfig()
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
	})
}

func findGossipConfig() *topodatapb.GossipConfig {
	query := `SELECT keyspace FROM vitess_keyspace`
	var cfg *topodatapb.GossipConfig
	_ = db.QueryVTOrc(query, nil, func(row sqlutils.RowMap) error {
		ks := row.GetString("keyspace")
		ksInfo, err := inst.ReadKeyspace(ks)
		if err != nil || ksInfo == nil {
			return nil
		}
		ksCfg := ksInfo.GetGossipConfig()
		if ksCfg != nil && ksCfg.Enabled {
			cfg = ksCfg
		}
		return nil
	})
	return cfg
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
