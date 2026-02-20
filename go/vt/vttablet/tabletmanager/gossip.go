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
