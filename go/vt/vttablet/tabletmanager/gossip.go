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
	"sync/atomic"

	"vitess.io/vitess/go/vt/gossip"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"

	gossippb "vitess.io/vitess/go/vt/proto/gossip"
)

// SetGossip installs (or clears) the gossip agent on the TabletManager.
// Used by the watcher goroutine when SrvKeyspace transitions enable or
// disable gossip; kept exported so out-of-tree callers can inject a
// pre-built agent for testing.
func (tm *TabletManager) SetGossip(agent *gossip.Gossip, enabled bool) {
	tm.gossipMu.Lock()
	defer tm.gossipMu.Unlock()
	tm.Gossip = agent
	tm.GossipEnabled = enabled
}

// currentGossipAgent returns the agent currently installed on the TM,
// or nil if gossip is disabled. The gRPC service and /debug/gossip
// handler both route through here so they automatically follow
// enable/disable transitions.
func (tm *TabletManager) currentGossipAgent() *gossip.Gossip {
	if tm == nil {
		return nil
	}
	tm.gossipMu.RLock()
	defer tm.gossipMu.RUnlock()
	return tm.Gossip
}

// currentGossipState returns both the agent and whether gossip is
// currently enabled. Used by the watcher so it can distinguish
// "no agent because disabled" from "no agent yet" during startup.
func (tm *TabletManager) currentGossipState() (*gossip.Gossip, bool) {
	if tm == nil {
		return nil, false
	}
	tm.gossipMu.RLock()
	defer tm.gossipMu.RUnlock()
	return tm.Gossip, tm.GossipEnabled
}

// setGossipCancel stores the cancel func for the SrvKeyspace watcher
// goroutine so TabletManager.Stop / Close can tear it down deterministically.
func (tm *TabletManager) setGossipCancel(cancel context.CancelFunc) {
	tm.gossipMu.Lock()
	defer tm.gossipMu.Unlock()
	tm.gossipCancel = cancel
}

// stopGossipAgent tears down just the agent (keeping the watcher
// running). Used when SrvKeyspace flips gossip off but the watcher
// should stay live to catch a future re-enable.
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
		log.Info("gossip: stopping agent (config disabled)")
		agent.Stop()
	}
}

// stopGossipLifecycle tears down everything gossip-related for this
// TabletManager: the watcher context and the agent. Wired into
// TabletManager.Close/Stop so a shutting-down tablet doesn't leak
// goroutines or gossip connections.
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
		log.Info("gossip: stopping agent (lifecycle shutdown)")
		agent.Stop()
	}
}

var (
	gossipDebugHandlerOnce sync.Once
	gossipGRPCServiceOnce  sync.Once
	processGossipManager   atomic.Pointer[TabletManager]
)

// setProcessGossipManager atomically publishes the TM whose agent the
// shared /debug/gossip handler and gRPC service should route to. In
// vtcombo (multiple TMs in one process) the last-registered TM wins —
// a documented limitation; the indirection exists so later TMs are not
// shadowed by the sync.Once-captured first one.
func setProcessGossipManager(tm *TabletManager) {
	if tm == nil {
		return
	}
	processGossipManager.Store(tm)
}

// currentProcessGossipAgent returns the agent for whichever TM was most
// recently published via setProcessGossipManager. Called from the
// gossip gRPC service and /debug/gossip handler on every request so
// they pick up enable/disable transitions without re-registration.
func currentProcessGossipAgent() *gossip.Gossip {
	tm := processGossipManager.Load()
	if tm == nil {
		return nil
	}
	return tm.currentGossipAgent()
}

// registerGossipService registers the gossip gRPC service and debug HTTP
// endpoint. It is safe to call multiple times — registration happens only
// once. The handlers are nil-safe so they remain valid across
// enable/disable transitions.
func registerGossipService(tm *TabletManager) {
	if tm == nil {
		return
	}
	setProcessGossipManager(tm)

	gossipDebugHandlerOnce.Do(func() {
		servenv.HTTPHandleFunc("/debug/gossip", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			agent := currentProcessGossipAgent()
			if agent != nil {
				_ = json.NewEncoder(w).Encode(agent.Debug())
			} else {
				_, _ = w.Write([]byte("null\n"))
			}
		})
	})

	if servenv.GRPCServer == nil || !servenv.GRPCCheckServiceMap("gossip") {
		return
	}

	gossipGRPCServiceOnce.Do(func() {
		log.Info("gossip: registering gRPC service on shared vttablet gRPC server")
		gossippb.RegisterGossipServer(servenv.GRPCServer, &gossip.Service{
			GetAgent: currentProcessGossipAgent,
		})
	})
}
