package tabletmanager

import (
	"vitess.io/vitess/go/vt/gossip"
	"vitess.io/vitess/go/vt/servenv"

	gossippb "vitess.io/vitess/go/vt/proto/gossip"
)

// SetGossip sets the gossip agent used by the tablet manager.
func (tm *TabletManager) SetGossip(agent *gossip.Gossip, enabled bool) {
	tm.Gossip = agent
	tm.GossipEnabled = enabled
}

func registerGossipService(tm *TabletManager) {
	if tm == nil || tm.Gossip == nil || !tm.GossipEnabled {
		return
	}

	if servenv.GRPCCheckServiceMap("gossip") {
		gossippb.RegisterGossipServer(servenv.GRPCServer, &gossip.Service{Agent: tm.Gossip})
	}
}
