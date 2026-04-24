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
	"time"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/vt/gossip"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// GossipSecureDialOption allows a caller (typically the vttablet main
// entry point) to supply a TLS-aware gRPC dial option so that gossip
// traffic inherits the same transport security as other tablet-to-tablet
// RPCs. It is a function to avoid an import cycle with grpctmclient
// (whose test files import tabletmanager). Leave nil to fall back to
// insecure, which is only appropriate for local testing.
var GossipSecureDialOption func() (grpc.DialOption, error)

// newGossipAgent creates a gossip agent from the keyspace-level config and tablet identity.
func newGossipAgent(cfg *topodatapb.GossipConfig, tablet *topodatapb.Tablet, ts *topo.Server) (*gossip.Gossip, bool) {
	if cfg == nil || !cfg.Enabled || tablet == nil {
		return nil, false
	}

	port, ok := tablet.PortMap["grpc"]
	if !ok || port == 0 {
		return nil, false
	}

	nodeID := topoproto.TabletAliasString(tablet.Alias)
	// Use netutil.JoinHostPort so IPv6 literals get bracketed correctly
	// (e.g. "[::1]:16100" instead of the invalid "::1:16100").
	grpcAddr := netutil.JoinHostPort(tablet.Hostname, port)

	meta := map[string]string{
		gossip.MetaKeyKeyspace:    tablet.Keyspace,
		gossip.MetaKeyShard:       tablet.Shard,
		gossip.MetaKeyTabletAlias: nodeID,
	}

	seeds := discoverSeeds(tablet, ts)

	bindAddr := grpcAddr

	pingInterval := parseDuration(cfg.PingInterval, 1*time.Second)
	maxUpdateAge := parseDuration(cfg.MaxUpdateAge, 5*time.Second)
	phiThreshold := cfg.PhiThreshold
	if phiThreshold <= 0 {
		phiThreshold = 4
	}

	// Reuse the tablet manager gRPC client's TLS configuration so
	// gossip inherits the same certs/CA/peer-name as every other
	// tablet-to-tablet RPC. GossipSecureDialOption is wired from the
	// vttablet main entry point (see plugin_grpctmclient.go) to avoid
	// an import cycle with grpctmclient's tests.
	transport := gossip.NewGRPCTransport(&gossip.GRPCDialer{
		SecureDialOption: GossipSecureDialOption,
	})
	agent := gossip.New(gossip.Config{
		NodeID:       gossip.NodeID(nodeID),
		BindAddr:     bindAddr,
		Seeds:        seeds,
		Meta:         meta,
		PhiThreshold: phiThreshold,
		PingInterval: pingInterval,
		ProbeTimeout: 500 * time.Millisecond,
		MaxUpdateAge: maxUpdateAge,
	}, transport, nil)

	return agent, agent != nil
}

// discoverSeeds queries topo for other tablets in the same shard to use
// as gossip seeds. This replaces the static --gossip-seed-addrs flag.
func discoverSeeds(self *topodatapb.Tablet, ts *topo.Server) []gossip.Member {
	if ts == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
	defer cancel()

	tablets, err := ts.GetTabletMapForShard(ctx, self.Keyspace, self.Shard)
	if err != nil {
		return nil
	}

	selfAlias := topoproto.TabletAliasString(self.Alias)
	var seeds []gossip.Member
	for alias, ti := range tablets {
		if alias == selfAlias {
			continue
		}
		t := ti.Tablet
		grpcPort, ok := t.PortMap["grpc"]
		if !ok || grpcPort == 0 {
			continue
		}
		addr := netutil.JoinHostPort(t.Hostname, grpcPort)
		seeds = append(seeds, gossip.Member{
			ID:   gossip.NodeID(alias),
			Addr: addr,
		})
	}
	return seeds
}

// parseDuration parses a duration string with a fallback for empty or invalid values.
func parseDuration(s string, fallback time.Duration) time.Duration {
	if s == "" {
		return fallback
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}
