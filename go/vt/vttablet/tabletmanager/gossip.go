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
	"encoding/json"
	"net/http"
	"sync"

	"vitess.io/vitess/go/vt/gossip"
	"vitess.io/vitess/go/vt/servenv"

	gossippb "vitess.io/vitess/go/vt/proto/gossip"
)

// SetGossip sets the gossip agent used by the tablet manager.
func (tm *TabletManager) SetGossip(agent *gossip.Gossip, enabled bool) {
	tm.Gossip = agent
	tm.GossipEnabled = enabled
}

var gossipServiceOnce sync.Once

// registerGossipService registers the gossip gRPC service and debug HTTP
// endpoint. It is safe to call multiple times — registration happens only
// once. The handlers are nil-safe so they remain valid across
// enable/disable transitions.
func registerGossipService(tm *TabletManager) {
	if tm == nil {
		return
	}

	gossipServiceOnce.Do(func() {
		if servenv.GRPCCheckServiceMap("gossip") {
			gossippb.RegisterGossipServer(servenv.GRPCServer, &gossip.Service{
				GetAgent: func() *gossip.Gossip { return tm.Gossip },
			})
		}

		servenv.HTTPHandleFunc("/debug/gossip", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if tm.Gossip != nil {
				_ = json.NewEncoder(w).Encode(tm.Gossip.Debug())
			} else {
				_, _ = w.Write([]byte("null\n"))
			}
		})
	})
}
