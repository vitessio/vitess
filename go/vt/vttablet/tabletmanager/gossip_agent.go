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
	"strconv"

	"vitess.io/vitess/go/vt/gossip"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func newGossipAgent(cfg gossipConfig, tablet *topodatapb.Tablet) (*gossip.Gossip, bool) {
	if !cfg.enabled || tablet == nil {
		return nil, false
	}

	nodeID := topoproto.TabletAliasString(tablet.Alias)
	grpcAddr := tablet.Hostname
	if port, ok := tablet.PortMap["grpc"]; ok {
		grpcAddr = grpcAddr + ":" + strconv.Itoa(int(port))
	}

	agent := cfg.agent(nodeID, grpcAddr)
	return agent, agent != nil
}
