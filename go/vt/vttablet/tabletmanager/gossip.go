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
	"encoding/json"
	"net/http"
	"sync"

	"vitess.io/vitess/go/vt/gossip"
	"vitess.io/vitess/go/vt/servenv"

	gossippb "vitess.io/vitess/go/vt/proto/gossip"
)

// SetGossip sets the gossip agent used by the tablet manager.
func (tm *TabletManager) SetGossip(agent *gossip.Gossip, enabled bool) {
	tm.gossipMu.Lock()
	defer tm.gossipMu.Unlock()
	tm.Gossip = agent
	tm.GossipEnabled = enabled
}

func (tm *TabletManager) currentGossipAgent() *gossip.Gossip {
	if tm == nil {
		return nil
	}
	tm.gossipMu.RLock()
	defer tm.gossipMu.RUnlock()
	return tm.Gossip
}

func (tm *TabletManager) currentGossipState() (*gossip.Gossip, bool) {
	if tm == nil {
		return nil, false
	}
	tm.gossipMu.RLock()
	defer tm.gossipMu.RUnlock()
	return tm.Gossip, tm.GossipEnabled
}

func (tm *TabletManager) setGossipCancel(cancel context.CancelFunc) {
	tm.gossipMu.Lock()
	defer tm.gossipMu.Unlock()
	tm.gossipCancel = cancel
}

func (tm *TabletManager) stopGossipAgent() {
	if tm == nil {
		return
	}
	tm.gossipMu.Lock()
	agent := tm.Gossip
	tm.Gossip = nil
	tm.GossipEnabled = false
	tm.gossipMu.Unlock()
	if agent != nil {
		agent.Stop()
	}
}

func (tm *TabletManager) stopGossipLifecycle() {
	if tm == nil {
		return
	}
	tm.gossipMu.Lock()
	cancel := tm.gossipCancel
	tm.gossipCancel = nil
	agent := tm.Gossip
	tm.Gossip = nil
	tm.GossipEnabled = false
	tm.gossipMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if agent != nil {
		agent.Stop()
	}
}

var (
	gossipDebugHandlerOnce sync.Once
	gossipGRPCServiceOnce  sync.Once
)

// registerGossipService registers the gossip gRPC service and debug HTTP
// endpoint. It is safe to call multiple times — registration happens only
// once. The handlers are nil-safe so they remain valid across
// enable/disable transitions.
func registerGossipService(tm *TabletManager) {
	if tm == nil {
		return
	}

	gossipDebugHandlerOnce.Do(func() {
		servenv.HTTPHandleFunc("/debug/gossip", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			agent := tm.currentGossipAgent()
			if agent != nil {
				_ = json.NewEncoder(w).Encode(agent.Debug())
			} else {
				_, _ = w.Write([]byte("null\n"))
			}
		})
	})

	if servenv.GRPCServer == nil {
		return
	}

	gossipGRPCServiceOnce.Do(func() {
		gossippb.RegisterGossipServer(servenv.GRPCServer, &gossip.Service{
			GetAgent: func() *gossip.Gossip { return tm.currentGossipAgent() },
		})
	})
}
