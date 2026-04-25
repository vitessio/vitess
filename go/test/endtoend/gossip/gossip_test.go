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

package gossip

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/gossip"
	"vitess.io/vitess/go/vt/vtorc/inst"
	"vitess.io/vitess/go/vt/vtorc/logic"
)

type localTransport struct {
	mu      sync.Mutex
	peers   map[string]*gossip.Gossip
	offline map[string]bool
}

func newLocalTransport() *localTransport {
	return &localTransport{
		peers:   make(map[string]*gossip.Gossip),
		offline: make(map[string]bool),
	}
}

func (t *localTransport) Register(addr string, agent *gossip.Gossip) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[addr] = agent
}

func (t *localTransport) SetOffline(addr string, offline bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.offline[addr] = offline
}

func (t *localTransport) PushPull(ctx context.Context, addr string, msg *gossip.Message) (*gossip.Message, error) {
	t.mu.Lock()
	peer := t.peers[addr]
	offline := t.offline[addr]
	t.mu.Unlock()
	if offline || peer == nil {
		return nil, errors.New("peer unavailable")
	}
	return peer.HandlePushPull(msg), nil
}

func (t *localTransport) Join(ctx context.Context, addr string, req *gossip.JoinRequest) (*gossip.JoinResponse, error) {
	t.mu.Lock()
	peer := t.peers[addr]
	offline := t.offline[addr]
	t.mu.Unlock()
	if offline || peer == nil {
		return nil, errors.New("peer unavailable")
	}
	return peer.HandleJoin(req)
}

// Close is a no-op for the in-process test transport.
func (t *localTransport) Close() {}

func newAgent(id string, seeds []gossip.Member, transport *localTransport, meta map[string]string) *gossip.Gossip {
	agent := gossip.New(gossip.Config{
		NodeID:       gossip.NodeID(id),
		BindAddr:     id,
		Seeds:        seeds,
		Meta:         meta,
		PhiThreshold: 4,
		PingInterval: 100 * time.Millisecond,
		ProbeTimeout: 100 * time.Millisecond,
		MaxUpdateAge: 2 * time.Second,
	}, transport, nil)
	transport.Register(id, agent)
	return agent
}

func newAgentSimple(id string, seeds []gossip.Member, transport *localTransport) *gossip.Gossip {
	return newAgent(id, seeds, transport, nil)
}

func seedsFor(ids ...string) []gossip.Member {
	members := make([]gossip.Member, len(ids))
	for i, id := range ids {
		members[i] = gossip.Member{ID: gossip.NodeID(id), Addr: id}
	}
	return members
}

func metaFor(keyspace, shard, alias string) map[string]string {
	return map[string]string{
		gossip.MetaKeyKeyspace:    keyspace,
		gossip.MetaKeyShard:       shard,
		gossip.MetaKeyTabletAlias: alias,
	}
}

func waitStatus(t *testing.T, observer *gossip.Gossip, nodeID string, expected gossip.Status, timeout time.Duration) {
	t.Helper()
	require.Eventuallyf(t, func() bool {
		return observer.Snapshot()[gossip.NodeID(nodeID)].Status == expected
	}, timeout, 10*time.Millisecond, "expected %s to reach status %d", nodeID, expected)
}

// waitFullConvergence waits until the observer has the expected number of
// members AND all members have metadata populated (non-empty shard).
func waitFullConvergence(t *testing.T, observer *gossip.Gossip, expectedMembers int, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		debug := observer.Debug()
		if len(debug.Members) < expectedMembers {
			return false
		}
		for _, m := range debug.Members {
			if m.Meta[gossip.MetaKeyShard] == "" {
				return false
			}
		}
		return true
	}, timeout, 50*time.Millisecond, "metadata did not converge on all %d members", expectedMembers)
}

func assertDebugStatus(t *testing.T, agent *gossip.Gossip, nodeID string, expected gossip.Status) {
	t.Helper()
	debug := agent.Debug()
	entry, ok := debug.States[gossip.NodeID(nodeID)]
	require.True(t, ok, "node %s not found in debug states", nodeID)
	assert.Equal(t, expected, entry.Status, "node %s status mismatch", nodeID)
}

// agentStateProvider wraps a gossip.Gossip as a logic.GossipStateProvider.
type agentStateProvider struct{ agent *gossip.Gossip }

func (p *agentStateProvider) Members() []gossip.Member                 { return p.agent.Members() }
func (p *agentStateProvider) Snapshot() map[gossip.NodeID]gossip.State { return p.agent.Snapshot() }

func quorumAnalysis(agent *gossip.Gossip, primaries map[string]string, vtorcView map[string]bool) []*inst.DetectionAnalysis {
	var view *logic.VTOrcView
	if vtorcView != nil {
		view = &logic.VTOrcView{HealthCheckFailed: vtorcView}
	}
	return logic.AnalyzeGossipQuorum(&agentStateProvider{agent: agent}, primaries, nil, view)
}

// TestGossipPrimaryTabletUnreachableByQuorum is the single top-level e2e test for the gossip protocol.

func TestGossipPrimaryTabletUnreachableByQuorum(t *testing.T) {
	t.Run("ClusterFormation", func(t *testing.T) {
		transport := newLocalTransport()
		g1 := newAgentSimple("node1", seedsFor("node2", "node3"), transport)
		g2 := newAgentSimple("node2", seedsFor("node1"), transport)
		g3 := newAgentSimple("node3", seedsFor("node1"), transport)

		ctx := t.Context()
		require.NoError(t, g1.Start(ctx))
		require.NoError(t, g2.Start(ctx))
		require.NoError(t, g3.Start(ctx))
		defer g1.Stop()
		defer g2.Stop()
		defer g3.Stop()

		g1.UpdateLocal(gossip.HealthSnapshot{NodeID: "node1"})
		g2.UpdateLocal(gossip.HealthSnapshot{NodeID: "node2"})
		g3.UpdateLocal(gossip.HealthSnapshot{NodeID: "node3"})

		// All nodes converge to Alive.
		waitStatus(t, g1, "node2", gossip.StatusAlive, 10*time.Second)
		waitStatus(t, g1, "node3", gossip.StatusAlive, 10*time.Second)
		waitStatus(t, g2, "node1", gossip.StatusAlive, 10*time.Second)
		waitStatus(t, g2, "node3", gossip.StatusAlive, 10*time.Second)
		waitStatus(t, g3, "node1", gossip.StatusAlive, 10*time.Second)
		waitStatus(t, g3, "node2", gossip.StatusAlive, 10*time.Second)

		// Debug endpoint reflects converged state.
		debug := g1.Debug()
		assert.Len(t, debug.Members, 3)
		assert.Equal(t, gossip.NodeID("node1"), debug.NodeID)
		assert.True(t, debug.Epoch > 0)
		assertDebugStatus(t, g1, "node2", gossip.StatusAlive)
		assertDebugStatus(t, g1, "node3", gossip.StatusAlive)
	})

	t.Run("NodeFailureDetection", func(t *testing.T) {
		transport := newLocalTransport()
		g1 := newAgentSimple("node1", seedsFor("node2", "node3"), transport)
		g2 := newAgentSimple("node2", seedsFor("node1"), transport)
		g3 := newAgentSimple("node3", seedsFor("node1"), transport)

		ctx := t.Context()
		require.NoError(t, g1.Start(ctx))
		require.NoError(t, g2.Start(ctx))
		require.NoError(t, g3.Start(ctx))
		defer g1.Stop()
		defer g2.Stop()

		g1.UpdateLocal(gossip.HealthSnapshot{NodeID: "node1"})
		g2.UpdateLocal(gossip.HealthSnapshot{NodeID: "node2"})
		g3.UpdateLocal(gossip.HealthSnapshot{NodeID: "node3"})

		waitStatus(t, g1, "node3", gossip.StatusAlive, 10*time.Second)

		// Kill node3.
		g3.Stop()
		transport.SetOffline("node3", true)

		waitStatus(t, g1, "node3", gossip.StatusDown, 10*time.Second)
		waitStatus(t, g2, "node3", gossip.StatusDown, 10*time.Second)

		assertDebugStatus(t, g1, "node3", gossip.StatusDown)
		// Surviving nodes still see each other as alive.
		assertDebugStatus(t, g1, "node2", gossip.StatusAlive)
		assertDebugStatus(t, g2, "node1", gossip.StatusAlive)
	})

	t.Run("NodeRecovery", func(t *testing.T) {
		transport := newLocalTransport()
		g1 := newAgentSimple("node1", seedsFor("node2", "node3"), transport)
		g2 := newAgentSimple("node2", seedsFor("node1"), transport)
		g3 := newAgentSimple("node3", seedsFor("node1"), transport)

		ctx := t.Context()
		require.NoError(t, g1.Start(ctx))
		require.NoError(t, g2.Start(ctx))
		require.NoError(t, g3.Start(ctx))
		defer g1.Stop()
		defer g2.Stop()

		g1.UpdateLocal(gossip.HealthSnapshot{NodeID: "node1"})
		g2.UpdateLocal(gossip.HealthSnapshot{NodeID: "node2"})
		g3.UpdateLocal(gossip.HealthSnapshot{NodeID: "node3"})
		waitStatus(t, g1, "node3", gossip.StatusAlive, 10*time.Second)

		// Kill and detect.
		g3.Stop()
		transport.SetOffline("node3", true)
		waitStatus(t, g1, "node3", gossip.StatusDown, 10*time.Second)

		// Bring back.
		transport.SetOffline("node3", false)
		g3New := newAgentSimple("node3", seedsFor("node1"), transport)
		require.NoError(t, g3New.Start(ctx))
		defer g3New.Stop()
		g3New.UpdateLocal(gossip.HealthSnapshot{NodeID: "node3"})

		waitStatus(t, g1, "node3", gossip.StatusAlive, 10*time.Second)
		waitStatus(t, g2, "node3", gossip.StatusAlive, 10*time.Second)
		assertDebugStatus(t, g1, "node3", gossip.StatusAlive)
	})

	t.Run("JoinProtocol", func(t *testing.T) {
		transport := newLocalTransport()
		seed := newAgentSimple("seed", nil, transport)
		joiner := newAgentSimple("joiner", seedsFor("seed"), transport)
		seed.UpdateLocal(gossip.HealthSnapshot{NodeID: "seed"})

		resp, err := joiner.Join(t.Context(), "seed")
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Seed now knows the joiner.
		found := false
		for _, m := range seed.Members() {
			if m.ID == "joiner" {
				found = true
			}
		}
		assert.True(t, found, "seed should know joiner after Join")

		// Response includes both.
		ids := make(map[gossip.NodeID]bool)
		for _, m := range resp.Members {
			ids[m.ID] = true
		}
		assert.True(t, ids["seed"])
		assert.True(t, ids["joiner"])
	})

	t.Run("DebugEndpointStructure", func(t *testing.T) {
		transport := newLocalTransport()
		g := newAgent("node1", seedsFor("node2"), transport, metaFor("commerce", "0", "zone1-0000000100"))
		require.NoError(t, g.Start(t.Context()))
		defer g.Stop()
		g.UpdateLocal(gossip.HealthSnapshot{NodeID: "node1"})

		debug := g.Debug()
		assert.Equal(t, gossip.NodeID("node1"), debug.NodeID)
		assert.Equal(t, "node1", debug.BindAddr)

		// Self member has metadata.
		var self *gossip.Member
		for _, m := range debug.Members {
			if m.ID == "node1" {
				self = &m
			}
		}
		require.NotNil(t, self)
		assert.Equal(t, "commerce", self.Meta[gossip.MetaKeyKeyspace])
		assert.Equal(t, "0", self.Meta[gossip.MetaKeyShard])
		assert.Equal(t, "zone1-0000000100", self.Meta[gossip.MetaKeyTabletAlias])

		// Self state is alive, seed is unknown.
		assert.Equal(t, gossip.StatusAlive, debug.States["node1"].Status)
		assert.False(t, debug.States["node1"].LastUpdate.IsZero())
		assert.Equal(t, gossip.StatusUnknown, debug.States["node2"].Status)
	})

	t.Run("EpochAdvances", func(t *testing.T) {
		transport := newLocalTransport()
		g := newAgentSimple("node1", nil, transport)

		epoch0 := g.Debug().Epoch
		g.UpdateLocal(gossip.HealthSnapshot{NodeID: "node1"})
		epoch1 := g.Debug().Epoch
		assert.True(t, epoch1 > epoch0)

		g.HandlePushPull(&gossip.Message{
			Members: []gossip.Member{{ID: "node2", Addr: "node2"}},
			States:  []gossip.StateDigest{{NodeID: "node2", Status: gossip.StatusAlive, LastUpdate: time.Now()}},
		})
		epoch2 := g.Debug().Epoch
		assert.True(t, epoch2 > epoch1)
	})

	t.Run("MultiShardIndependence", func(t *testing.T) {
		transport := newLocalTransport()

		// Two shards, each with 1 primary + 2 replicas. By design,
		// gossip traffic is scoped to a single keyspace/shard — a
		// vttablet only exchanges members within its shard. Peers
		// without shard metadata (e.g., VTOrc) are unrestricted.
		s1P := newAgent("s1-p", seedsFor("s1-r1", "s1-r2"), transport, metaFor("ks", "-80", "zone1-0000000100"))
		s1R1 := newAgent("s1-r1", seedsFor("s1-p"), transport, metaFor("ks", "-80", "zone1-0000000101"))
		s1R2 := newAgent("s1-r2", seedsFor("s1-p"), transport, metaFor("ks", "-80", "zone1-0000000102"))
		s2P := newAgent("s2-p", seedsFor("s2-r1", "s2-r2"), transport, metaFor("ks", "80-", "zone1-0000000200"))
		s2R1 := newAgent("s2-r1", seedsFor("s2-p"), transport, metaFor("ks", "80-", "zone1-0000000201"))
		s2R2 := newAgent("s2-r2", seedsFor("s2-p"), transport, metaFor("ks", "80-", "zone1-0000000202"))

		all := []*gossip.Gossip{s1P, s1R1, s1R2, s2P, s2R1, s2R2}
		ctx := t.Context()
		for _, a := range all {
			require.NoError(t, a.Start(ctx))
			a.UpdateLocal(gossip.HealthSnapshot{})
		}
		defer func() {
			for _, a := range all {
				a.Stop()
			}
		}()

		// Each shard converges independently (3 members per shard).
		waitFullConvergence(t, s1R1, 3, 10*time.Second)
		waitFullConvergence(t, s2R1, 3, 10*time.Second)

		// Verify metadata propagated within a shard.
		debug := s1R1.Debug()
		found := false
		for _, m := range debug.Members {
			if m.ID == "s1-p" {
				assert.Equal(t, "-80", m.Meta[gossip.MetaKeyShard])
				found = true
			}
		}
		assert.True(t, found)

		// Shard-scope isolation: s1-r1 should NOT see any members from
		// shard 80-. Cross-shard observers are the VTOrc's job.
		for _, m := range s1R1.Debug().Members {
			assert.NotEqual(t, "80-", m.Meta[gossip.MetaKeyShard], "shard -80 peer saw shard 80- member %s", m.ID)
		}

		// Kill shard -80 primary.
		s1P.Stop()
		transport.SetOffline("s1-p", true)
		waitStatus(t, s1R1, "s1-p", gossip.StatusDown, 10*time.Second)

		// Shard 80- primary unaffected.
		assertDebugStatus(t, s2R1, "s2-p", gossip.StatusAlive)

		// Quorum analysis detects only shard -80. Small shards (1
		// primary + 2 replicas each) require VTOrc corroboration.
		primaries := map[string]string{"ks/-80": "zone1-0000000100", "ks/80-": "zone1-0000000200"}
		vtorcView := map[string]bool{"ks/-80": true}
		analyses := quorumAnalysis(s1R1, primaries, vtorcView)
		require.Len(t, analyses, 1)
		assert.Equal(t, inst.PrimaryTabletUnreachableByQuorum, analyses[0].Analysis)
		assert.Equal(t, "-80", analyses[0].AnalyzedShard)
	})

	t.Run("QuorumRequiresMajority", func(t *testing.T) {
		transport := newLocalTransport()
		allSeeds := seedsFor("p", "r1", "r2", "r3", "r4")
		p := newAgent("p", allSeeds, transport, metaFor("ks", "0", "zone1-0000000100"))
		r1 := newAgent("r1", allSeeds, transport, metaFor("ks", "0", "zone1-0000000101"))
		r2 := newAgent("r2", allSeeds, transport, metaFor("ks", "0", "zone1-0000000102"))
		r3 := newAgent("r3", allSeeds, transport, metaFor("ks", "0", "zone1-0000000103"))
		r4 := newAgent("r4", allSeeds, transport, metaFor("ks", "0", "zone1-0000000104"))

		all := []*gossip.Gossip{p, r1, r2, r3, r4}
		ctx := t.Context()
		for _, a := range all {
			require.NoError(t, a.Start(ctx))
			a.UpdateLocal(gossip.HealthSnapshot{})
		}
		defer func() {
			for _, a := range all {
				a.Stop()
			}
		}()

		waitFullConvergence(t, r1, 5, 10*time.Second)

		// Kill primary + r3 + r4 → 2 of 4 replicas alive (no majority).
		p.Stop()
		transport.SetOffline("p", true)
		r3.Stop()
		transport.SetOffline("r3", true)
		r4.Stop()
		transport.SetOffline("r4", true)
		waitStatus(t, r1, "p", gossip.StatusDown, 10*time.Second)
		waitStatus(t, r1, "r3", gossip.StatusDown, 10*time.Second)
		waitStatus(t, r1, "r4", gossip.StatusDown, 10*time.Second)

		primaries := map[string]string{"ks/0": "zone1-0000000100"}
		require.Empty(t, quorumAnalysis(r1, primaries, nil), "2/4 is not a majority")

		// Bring r3 back → 3 of 4 alive (majority).
		transport.SetOffline("r3", false)
		r3New := newAgent("r3", seedsFor("r1"), transport, metaFor("ks", "0", "zone1-0000000103"))
		require.NoError(t, r3New.Start(ctx))
		defer r3New.Stop()
		r3New.UpdateLocal(gossip.HealthSnapshot{})
		waitStatus(t, r1, "r3", gossip.StatusAlive, 10*time.Second)
		waitFullConvergence(t, r1, 5, 10*time.Second)

		require.Len(t, quorumAnalysis(r1, primaries, nil), 1, "3/4 is a majority")
	})

	t.Run("TieBreakerScenario", func(t *testing.T) {
		transport := newLocalTransport()
		allSeeds := seedsFor("p", "r1", "r2", "r3", "r4")
		p := newAgent("p", allSeeds, transport, metaFor("ks", "0", "zone1-0000000100"))
		r1 := newAgent("r1", allSeeds, transport, metaFor("ks", "0", "zone1-0000000101"))
		r2 := newAgent("r2", allSeeds, transport, metaFor("ks", "0", "zone1-0000000102"))
		r3 := newAgent("r3", allSeeds, transport, metaFor("ks", "0", "zone1-0000000103"))
		r4 := newAgent("r4", allSeeds, transport, metaFor("ks", "0", "zone1-0000000104"))

		all := []*gossip.Gossip{p, r1, r2, r3, r4}
		ctx := t.Context()
		for _, a := range all {
			require.NoError(t, a.Start(ctx))
			a.UpdateLocal(gossip.HealthSnapshot{})
		}
		defer func() {
			for _, a := range all {
				a.Stop()
			}
		}()

		waitFullConvergence(t, r1, 5, 10*time.Second)

		// Exact tie: kill primary + r3 + r4 → 2 of 4 alive.
		p.Stop()
		transport.SetOffline("p", true)
		r3.Stop()
		transport.SetOffline("r3", true)
		r4.Stop()
		transport.SetOffline("r4", true)
		waitStatus(t, r1, "p", gossip.StatusDown, 10*time.Second)
		waitStatus(t, r1, "r3", gossip.StatusDown, 10*time.Second)
		waitStatus(t, r1, "r4", gossip.StatusDown, 10*time.Second)

		primaries := map[string]string{"ks/0": "zone1-0000000100"}

		// No tiebreaker → no ERS.
		require.Empty(t, quorumAnalysis(r1, primaries, nil))
		// VTOrc says primary down → ERS.
		require.Len(t, quorumAnalysis(r1, primaries, map[string]bool{"ks/0": true}), 1)
		// VTOrc says primary reachable → no ERS.
		require.Empty(t, quorumAnalysis(r1, primaries, map[string]bool{"ks/0": false}))
	})

	t.Run("SingleObserverDownDoesNotPropagate", func(t *testing.T) {
		transport := newLocalTransport()
		primary := newAgentSimple("primary", seedsFor("r1", "r2"), transport)
		r1 := newAgentSimple("r1", seedsFor("primary", "r2"), transport)
		r2 := newAgentSimple("r2", seedsFor("primary", "r1"), transport)

		ctx := t.Context()
		require.NoError(t, primary.Start(ctx))
		require.NoError(t, r1.Start(ctx))
		require.NoError(t, r2.Start(ctx))
		defer primary.Stop()
		defer r1.Stop()
		defer r2.Stop()

		primary.UpdateLocal(gossip.HealthSnapshot{NodeID: "primary"})
		r1.UpdateLocal(gossip.HealthSnapshot{NodeID: "r1"})
		r2.UpdateLocal(gossip.HealthSnapshot{NodeID: "r2"})
		waitStatus(t, r1, "primary", gossip.StatusAlive, 10*time.Second)
		waitStatus(t, r2, "primary", gossip.StatusAlive, 10*time.Second)

		// Inject stale Down state on r1 (simulates r1 partitioned from primary).
		r1.HandlePushPull(&gossip.Message{
			Members: []gossip.Member{{ID: "primary", Addr: "primary"}},
			States: []gossip.StateDigest{{
				NodeID: "primary", Status: gossip.StatusDown, Phi: 10,
				LastUpdate: time.Now().Add(-10 * time.Second),
			}},
		})

		// Primary is still alive and gossiping fresh state.
		// After several gossip rounds, r2 must still see primary as Alive.
		// r1's stale Down must NOT have propagated.
		require.Eventually(t, func() bool {
			// Wait for r1 to have exchanged with r2 at least once.
			return r1.Debug().Epoch > 5
		}, 10*time.Second, 50*time.Millisecond)
		assertDebugStatus(t, r2, "primary", gossip.StatusAlive)
	})
}
