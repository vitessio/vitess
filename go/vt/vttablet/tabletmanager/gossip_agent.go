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
	"strconv"

	"vitess.io/vitess/go/vt/gossip"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func newGossipAgent(cfg gossipConfig, tablet *topodatapb.Tablet, ts *topo.Server) (*gossip.Gossip, bool) {
	if !cfg.enabled || tablet == nil {
		return nil, false
	}

	port, ok := tablet.PortMap["grpc"]
	if !ok || port == 0 {
		return nil, false
	}

	nodeID := topoproto.TabletAliasString(tablet.Alias)
	grpcAddr := tablet.Hostname + ":" + strconv.Itoa(int(port))

	meta := map[string]string{
		gossip.MetaKeyKeyspace:    tablet.Keyspace,
		gossip.MetaKeyShard:       tablet.Shard,
		gossip.MetaKeyTabletAlias: nodeID,
	}

	seeds := discoverSeeds(tablet, ts)

	agent := cfg.agent(nodeID, grpcAddr, meta, seeds)
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
		addr := fmt.Sprintf("%s:%d", t.Hostname, grpcPort)
		seeds = append(seeds, gossip.Member{
			ID:   gossip.NodeID(alias),
			Addr: addr,
		})
	}
	return seeds
}
