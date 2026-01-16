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
