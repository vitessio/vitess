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
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	gossippb "vitess.io/vitess/go/vt/proto/gossip"
)

type localTransport struct {
	mu      sync.Mutex
	peers   map[string]*Gossip
	offline map[string]bool
}

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// advance moves fake time forward. Tests need synchronization here
// because gossip.Start spawns a bootstrapSeeds goroutine that reads
// the clock concurrently with the test goroutine advancing it.
func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newLocalTransport() *localTransport {
	return &localTransport{
		peers:   make(map[string]*Gossip),
		offline: make(map[string]bool),
	}
}

func (t *localTransport) Register(addr string, gossip *Gossip) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if gossip == nil {
		delete(t.peers, addr)
		return
	}
	t.peers[addr] = gossip
}

func (t *localTransport) SetOffline(addr string, offline bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.offline[addr] = offline
}

func (t *localTransport) Dial(ctx context.Context, addr string) (gossippb.GossipClient, error) {
	t.mu.Lock()
	peer := t.peers[addr]
	offline := t.offline[addr]
	t.mu.Unlock()

	if offline || peer == nil {
		return nil, errors.New("peer unavailable")
	}

	return localClient{peer: peer}, nil
}

// Close is a no-op for the in-process test transport; there are no
// network resources to release.
func (t *localTransport) Close() {}

type localClient struct {
	peer *Gossip
}

func (c localClient) PushPull(ctx context.Context, in *gossippb.GossipMessage, opts ...grpc.CallOption) (*gossippb.GossipMessage, error) {
	decoded, err := fromProtoMessage(in)
	if err != nil {
		return nil, err
	}
	return toProtoMessage(c.peer.HandlePushPull(decoded)), nil
}

func (c localClient) Join(ctx context.Context, in *gossippb.GossipJoinRequest, opts ...grpc.CallOption) (*gossippb.GossipJoinResponse, error) {
	resp, err := c.peer.HandleJoin(fromProtoJoinRequest(in))
	if err != nil {
		return nil, err
	}
	return toProtoJoinResponse(resp), nil
}

func newTestGossip(id string, seeds []Member, transport *localTransport, clock Clock, maxUpdateAge time.Duration) *Gossip {
	cfg := Config{
		NodeID:       NodeID(id),
		BindAddr:     id,
		Seeds:        seeds,
		PhiThreshold: 4,
		PingInterval: 10 * time.Millisecond,
		ProbeTimeout: 10 * time.Millisecond,
		MaxUpdateAge: maxUpdateAge,
	}
	transport.Register(id, nil)
	g := New(cfg, NewGRPCTransport(transport), clock)
	transport.Register(id, g)
	return g
}

func scopedMember(id, keyspace, shard string) Member {
	return Member{
		ID:   NodeID(id),
		Addr: id,
		Meta: map[string]string{
			MetaKeyKeyspace: keyspace,
			MetaKeyShard:    shard,
		},
	}
}

func TestGossipConvergesWithSeeds(t *testing.T) {
	transport := newLocalTransport()
	clock := &testClock{now: time.Now()}
	g1 := newTestGossip("node1", []Member{{ID: "node2", Addr: "node2"}}, transport, clock, 100*time.Millisecond)
	g2 := newTestGossip("node2", []Member{{ID: "node1", Addr: "node1"}}, transport, clock, 100*time.Millisecond)

	ctx := t.Context()

	require.NoError(t, g1.Start(ctx))
	require.NoError(t, g2.Start(ctx))
	defer g1.Stop()
	defer g2.Stop()

	g1.UpdateLocal(HealthSnapshot{NodeID: "node1", Timestamp: clock.Now()})
	g2.UpdateLocal(HealthSnapshot{NodeID: "node2", Timestamp: clock.Now()})

	_, err := g1.HandleJoin(&JoinRequest{Member: Member{ID: "node2", Addr: "node2"}})
	require.NoError(t, err)
	_, err = g2.HandleJoin(&JoinRequest{Member: Member{ID: "node1", Addr: "node1"}})
	require.NoError(t, err)
	g1.HandlePushPull(&Message{Members: []Member{{ID: "node2", Addr: "node2"}}, States: []StateDigest{{NodeID: "node2", Status: StatusAlive, LastUpdate: clock.Now()}}})
	g2.HandlePushPull(&Message{Members: []Member{{ID: "node1", Addr: "node1"}}, States: []StateDigest{{NodeID: "node1", Status: StatusAlive, LastUpdate: clock.Now()}}})

	assert.Equal(t, StatusAlive, g1.Snapshot()["node2"].Status)
	assert.Equal(t, StatusAlive, g2.Snapshot()["node1"].Status)
}

func TestPickPeerSkipsDifferentShardMembersForScopedNode(t *testing.T) {
	clock := &testClock{now: time.Unix(0, 0)}
	g := New(Config{
		NodeID:   "node1",
		BindAddr: "node1",
		Meta: map[string]string{
			MetaKeyKeyspace: "ks",
			MetaKeyShard:    "0",
		},
	}, nil, clock)

	g.mu.Lock()
	g.addMemberLocked(scopedMember("same-shard", "ks", "0"))
	g.addMemberLocked(scopedMember("other-shard", "ks", "1"))
	g.mu.Unlock()

	for range 20 {
		peer, _ := g.pickPeer()
		require.NotNil(t, peer)
		assert.Equal(t, "ks", peer.Meta[MetaKeyKeyspace])
		assert.Equal(t, "0", peer.Meta[MetaKeyShard])
	}
}

func TestSnapshotMessageLockedFiltersPeerScope(t *testing.T) {
	clock := &testClock{now: time.Unix(0, 0)}
	g := New(Config{NodeID: "vtorc", BindAddr: "vtorc"}, nil, clock)

	g.mu.Lock()
	g.addMemberLocked(scopedMember("a1", "ks", "0"))
	g.addMemberLocked(scopedMember("a2", "ks", "0"))
	g.addMemberLocked(scopedMember("b1", "ks", "1"))
	g.states["a1"] = State{Status: StatusAlive, LastUpdate: clock.Now()}
	g.states["a2"] = State{Status: StatusAlive, LastUpdate: clock.Now()}
	g.states["b1"] = State{Status: StatusAlive, LastUpdate: clock.Now()}
	msg := g.snapshotMessageLocked("ks/0")
	g.mu.Unlock()

	memberIDs := make(map[NodeID]struct{}, len(msg.Members))
	for _, member := range msg.Members {
		memberIDs[member.ID] = struct{}{}
	}
	assert.Contains(t, memberIDs, NodeID("a1"))
	assert.Contains(t, memberIDs, NodeID("a2"))
	assert.NotContains(t, memberIDs, NodeID("b1"))
	assert.Contains(t, memberIDs, NodeID("vtorc"))

	stateIDs := make(map[NodeID]struct{}, len(msg.States))
	for _, state := range msg.States {
		stateIDs[state.NodeID] = struct{}{}
	}
	assert.Contains(t, stateIDs, NodeID("a1"))
	assert.Contains(t, stateIDs, NodeID("a2"))
	assert.NotContains(t, stateIDs, NodeID("b1"))
	assert.Contains(t, stateIDs, NodeID("vtorc"))
}

func TestGossipIgnoresOutOfScopePushPullData(t *testing.T) {
	tests := []struct {
		name       string
		cfg        Config
		members    []Member
		wantNode   NodeID
		rejectNode NodeID
	}{{
		name: "scoped agent",
		cfg: Config{
			NodeID:   "a1",
			BindAddr: "a1",
			Meta: map[string]string{
				MetaKeyKeyspace: "ks",
				MetaKeyShard:    "0",
			},
		},
		members:    []Member{scopedMember("a2", "ks", "0"), scopedMember("b1", "ks", "1")},
		wantNode:   "a2",
		rejectNode: "b1",
	}, {
		name:       "unscoped agent",
		cfg:        Config{NodeID: "vtorc", BindAddr: "vtorc"},
		members:    []Member{scopedMember("a1", "ks", "0"), scopedMember("b1", "ks", "1")},
		wantNode:   "a1",
		rejectNode: "b1",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := &testClock{now: time.Unix(0, 0)}
			g := New(tt.cfg, nil, clock)
			states := make([]StateDigest, 0, len(tt.members))
			for _, member := range tt.members {
				states = append(states, StateDigest{NodeID: member.ID, Status: StatusAlive, LastUpdate: clock.Now()})
			}

			g.HandlePushPull(&Message{
				Members: tt.members,
				States:  states,
			})

			members := make(map[NodeID]Member)
			for _, member := range g.Members() {
				members[member.ID] = member
			}
			assert.Contains(t, members, tt.wantNode)
			assert.NotContains(t, members, tt.rejectNode)

			snapshot := g.Snapshot()
			assert.Contains(t, snapshot, tt.wantNode)
			assert.NotContains(t, snapshot, tt.rejectNode)
		})
	}
}

func TestGossipMarksDownWhenPeerUnreachable(t *testing.T) {
	transport := newLocalTransport()
	clock := &testClock{now: time.Now()}
	g1 := newTestGossip("node1", []Member{{ID: "node2", Addr: "node2"}}, transport, clock, 40*time.Millisecond)
	g2 := newTestGossip("node2", []Member{{ID: "node1", Addr: "node1"}}, transport, clock, 40*time.Millisecond)

	ctx := t.Context()

	require.NoError(t, g1.Start(ctx))
	require.NoError(t, g2.Start(ctx))
	defer g1.Stop()
	defer g2.Stop()

	g1.UpdateLocal(HealthSnapshot{NodeID: "node1", Timestamp: clock.Now()})
	g2.UpdateLocal(HealthSnapshot{NodeID: "node2", Timestamp: clock.Now()})

	_, err := g1.HandleJoin(&JoinRequest{Member: Member{ID: "node2", Addr: "node2"}})
	require.NoError(t, err)
	_, err = g2.HandleJoin(&JoinRequest{Member: Member{ID: "node1", Addr: "node1"}})
	require.NoError(t, err)
	g1.HandlePushPull(&Message{Members: []Member{{ID: "node2", Addr: "node2"}}, States: []StateDigest{{NodeID: "node2", Status: StatusAlive, LastUpdate: clock.Now()}}})

	assert.Equal(t, StatusAlive, g1.Snapshot()["node2"].Status)

	clock.advance(80 * time.Millisecond)
	g1.updateSuspicion(clock.Now())
	assert.Equal(t, StatusDown, g1.Snapshot()["node2"].Status)
}

func TestGossipUpdateLocalUsesConfigNodeID(t *testing.T) {
	clock := &testClock{now: time.Unix(0, 0)}
	g := New(Config{NodeID: "node1", BindAddr: "node1", PhiThreshold: 4, PingInterval: time.Second}, nil, clock)
	clock.advance(10 * time.Millisecond)

	g.UpdateLocal(HealthSnapshot{Timestamp: clock.Now()})

	state, ok := g.Snapshot()["node1"]
	require.True(t, ok)
	assert.Equal(t, StatusAlive, state.Status)
	assert.True(t, state.LastUpdate.Equal(clock.Now()))

	members := g.Members()
	require.Len(t, members, 1)
	assert.Equal(t, NodeID("node1"), members[0].ID)
}

func TestGossipHandleJoinReturnsSnapshot(t *testing.T) {
	clock := &testClock{now: time.Unix(0, 0)}
	g := New(Config{
		NodeID:       "node1",
		BindAddr:     "node1",
		Seeds:        []Member{{ID: "node3", Addr: "node3"}},
		PhiThreshold: 4,
	}, nil, clock)

	resp, err := g.HandleJoin(&JoinRequest{Member: Member{ID: "node2", Addr: "node2"}})
	require.NoError(t, err)
	require.NotNil(t, resp)

	ids := make(map[NodeID]bool)
	for _, member := range resp.Members {
		ids[member.ID] = true
	}
	assert.True(t, ids["node1"])
	assert.True(t, ids["node2"])
	assert.True(t, ids["node3"])

	states := make(map[NodeID]StateDigest)
	for _, state := range resp.Initial.States {
		states[state.NodeID] = state
	}
	assert.Equal(t, StatusAlive, states["node1"].Status)
	assert.Equal(t, StatusAlive, states["node2"].Status)
	assert.Equal(t, StatusUnknown, states["node3"].Status)
}

func TestGossipHandlePushPullAppliesState(t *testing.T) {
	clock := &testClock{now: time.Unix(0, 0)}
	g := New(Config{NodeID: "node1", BindAddr: "node1", PhiThreshold: 4}, nil, clock)

	msg := &Message{
		Members: []Member{{ID: "node2", Addr: "node2"}},
		States:  []StateDigest{{NodeID: "node2", Status: StatusAlive, LastUpdate: clock.Now()}},
	}
	g.HandlePushPull(msg)

	state, ok := g.Snapshot()["node2"]
	require.True(t, ok)
	assert.Equal(t, StatusAlive, state.Status)

	members := g.Members()
	require.Len(t, members, 2)
}

func TestNewGossipSeedsAndMembers(t *testing.T) {
	clock := &testClock{now: time.Unix(0, 0)}
	g := New(Config{NodeID: "node1", BindAddr: "node1", Seeds: []Member{{ID: "node2", Addr: "node2"}}}, nil, clock)

	members := g.Members()
	require.Len(t, members, 2)

	states := g.Snapshot()
	assert.Equal(t, StatusAlive, states["node1"].Status)
	assert.Equal(t, StatusUnknown, states["node2"].Status)
}

func TestGossipJoinUsesTransport(t *testing.T) {
	transport := newLocalTransport()
	clock := &testClock{now: time.Unix(0, 0)}
	g1 := newTestGossip("node1", []Member{{ID: "node2", Addr: "node2"}}, transport, clock, time.Second)
	g2 := newTestGossip("node2", nil, transport, clock, time.Second)

	resp, err := g1.Join(t.Context(), "node2")
	require.NoError(t, err)
	require.NotNil(t, resp)

	ids := make(map[NodeID]bool)
	for _, member := range resp.Members {
		ids[member.ID] = true
	}
	assert.True(t, ids["node1"])
	assert.True(t, ids["node2"])

	state := g2.Snapshot()["node1"]
	assert.Equal(t, StatusAlive, state.Status)
}

func TestGossipStartBootstrapsSeedsImmediately(t *testing.T) {
	transport := newLocalTransport()
	clock := &testClock{now: time.Unix(0, 0)}

	g1 := New(Config{
		NodeID:       "node1",
		BindAddr:     "node1",
		Seeds:        []Member{{ID: "node2", Addr: "node2"}},
		PhiThreshold: 4,
		PingInterval: time.Hour,
		ProbeTimeout: 10 * time.Millisecond,
		MaxUpdateAge: time.Second,
	}, NewGRPCTransport(transport), clock)
	g2 := New(Config{
		NodeID:       "node2",
		BindAddr:     "node2",
		Seeds:        []Member{{ID: "node1", Addr: "node1"}},
		PhiThreshold: 4,
		PingInterval: time.Hour,
		ProbeTimeout: 10 * time.Millisecond,
		MaxUpdateAge: time.Second,
	}, NewGRPCTransport(transport), clock)

	transport.Register("node1", g1)
	transport.Register("node2", g2)

	require.NoError(t, g1.Start(t.Context()))
	require.NoError(t, g2.Start(t.Context()))
	defer g1.Stop()
	defer g2.Stop()

	require.Eventually(t, func() bool {
		return g1.Snapshot()["node2"].Status == StatusAlive &&
			g2.Snapshot()["node1"].Status == StatusAlive
	}, time.Second, 10*time.Millisecond)
}

func TestGossipOnceMergesState(t *testing.T) {
	transport := newLocalTransport()
	clock := &testClock{now: time.Unix(0, 0)}
	g1 := newTestGossip("node1", []Member{{ID: "node2", Addr: "node2"}}, transport, clock, time.Second)
	g2 := newTestGossip("node2", []Member{{ID: "node1", Addr: "node1"}}, transport, clock, time.Second)

	g1.UpdateLocal(HealthSnapshot{NodeID: "node1", Timestamp: clock.Now()})
	g2.UpdateLocal(HealthSnapshot{NodeID: "node2", Timestamp: clock.Now()})

	g1.gossipOnce(t.Context(), clock.Now())

	state := g1.Snapshot()["node2"]
	assert.Equal(t, StatusAlive, state.Status)
}

func TestGossipUpdateSuspicionMarksSuspect(t *testing.T) {
	clock := &testClock{now: time.Unix(0, 0)}
	g := New(Config{NodeID: "node1", BindAddr: "node1", PhiThreshold: 2}, nil, clock)
	g.addMemberLocked(Member{ID: "node2", Addr: "node2"})
	g.states["node2"] = State{Status: StatusAlive, LastUpdate: clock.Now()}

	g.observeLocked("node2", clock.Now())
	clock.advance(10 * time.Millisecond)
	g.observeLocked("node2", clock.Now())
	clock.advance(10 * time.Millisecond)
	g.observeLocked("node2", clock.Now())

	clock.advance(200 * time.Millisecond)
	g.updateSuspicion(clock.Now())
	g.mu.Lock()
	status := g.states["node2"].Status
	g.mu.Unlock()
	assert.Equal(t, StatusSuspect, status)
}

func TestGossipIgnoresOlderStateUpdate(t *testing.T) {
	clock := &testClock{now: time.Unix(0, 0)}
	g := New(Config{NodeID: "node1", BindAddr: "node1"}, nil, clock)
	g.states["node2"] = State{Status: StatusAlive, LastUpdate: clock.Now()}

	msg := &Message{
		States: []StateDigest{{NodeID: "node2", Status: StatusDown, LastUpdate: clock.Now().Add(-time.Second)}},
	}
	g.HandlePushPull(msg)

	state := g.Snapshot()["node2"]
	assert.Equal(t, StatusAlive, state.Status)
}

func TestNeverObservedSeedsStayUnknown(t *testing.T) {
	clock := &testClock{now: time.Now()}
	g := New(Config{
		NodeID:       "node1",
		BindAddr:     "node1",
		PhiThreshold: 4,
		MaxUpdateAge: 50 * time.Millisecond,
		Seeds:        []Member{{ID: "node2", Addr: "node2"}},
	}, nil, clock)

	// Advance time well past MaxUpdateAge.
	clock.advance(10 * time.Second)
	g.updateSuspicion(clock.Now())

	// node2 was never observed — it should remain Unknown, not Down.
	state := g.Snapshot()["node2"]
	assert.Equal(t, StatusUnknown, state.Status,
		"never-observed seeds must stay Unknown, not age to Down")
}

func TestEqualTimestampMergeRules(t *testing.T) {
	tests := []struct {
		name       string
		first      Status
		second     Status
		wantFirst  Status
		wantSecond Status
	}{{
		name:       "down normalizes to alive before merge",
		first:      StatusDown,
		second:     StatusAlive,
		wantFirst:  StatusAlive,
		wantSecond: StatusAlive,
	}, {
		name:       "down does not override alive",
		first:      StatusAlive,
		second:     StatusDown,
		wantFirst:  StatusAlive,
		wantSecond: StatusAlive,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := &testClock{now: time.Unix(100, 0)}
			g := New(Config{NodeID: "node1", BindAddr: "node1"}, nil, clock)
			ts := clock.Now()

			g.HandlePushPull(&Message{
				Members: []Member{{ID: "node2", Addr: "node2"}},
				States:  []StateDigest{{NodeID: "node2", Status: tt.first, LastUpdate: ts}},
			})
			assert.Equal(t, tt.wantFirst, g.Snapshot()["node2"].Status)

			g.HandlePushPull(&Message{
				Members: []Member{{ID: "node2", Addr: "node2"}},
				States:  []StateDigest{{NodeID: "node2", Status: tt.second, LastUpdate: ts}},
			})
			assert.Equal(t, tt.wantSecond, g.Snapshot()["node2"].Status)
		})
	}
}

func TestProtoConversions(t *testing.T) {
	member := Member{ID: "node1", Addr: "addr", Meta: map[string]string{"role": "seed"}}
	protoMember := toProtoMember(member)
	assert.Equal(t, member.ID, NodeID(protoMember.Id))
	assert.Equal(t, member.Addr, protoMember.Addr)
	assert.Equal(t, member.Meta, protoMember.Meta)

	state := StateDigest{NodeID: "node1", Status: StatusSuspect, Phi: 2.5, LastUpdate: time.Unix(0, 5)}
	protoState := toProtoState(state)
	roundTripped, err := fromProtoState(protoState)
	require.NoError(t, err)
	assert.Equal(t, state.NodeID, roundTripped.NodeID)
	assert.Equal(t, state.Status, roundTripped.Status)
	assert.Equal(t, state.Phi, roundTripped.Phi)
	assert.True(t, state.LastUpdate.Equal(roundTripped.LastUpdate))

	unknownState := StateDigest{NodeID: "node2", Status: StatusUnknown}
	protoUnknownState := toProtoState(unknownState)
	assert.Nil(t, protoUnknownState.LastUpdate, "never-observed peers must serialize as nil so they round-trip to a zero time.Time")
	roundTrippedUnknown, err := fromProtoState(protoUnknownState)
	require.NoError(t, err)
	assert.True(t, roundTrippedUnknown.LastUpdate.IsZero())
}

func TestPhiAccrualReturnsHighPhiForOldHeartbeat(t *testing.T) {
	phi := newPhiAccrual(2)
	base := time.Unix(0, 0)
	phi.Observe(base)
	assert.Equal(t, 0.0, phi.Phi(base))

	phi.Observe(base.Add(10 * time.Millisecond))
	phi.Observe(base.Add(20 * time.Millisecond))
	assert.Equal(t, 100.0, phi.Phi(base.Add(50*time.Millisecond)))
}

func TestGRPCTransportDialFailures(t *testing.T) {
	tests := []struct {
		name   string
		dialer Dialer
	}{{
		name: "nil dialer",
	}, {
		name:   "nil client",
		dialer: nilClientDialer{},
	}, {
		name:   "error propagation",
		dialer: failingDialer{},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := NewGRPCTransport(tt.dialer)

			resp, err := transport.PushPull(t.Context(), "addr", &Message{})
			assert.Error(t, err)
			assert.Nil(t, resp)

			joinResp, err := transport.Join(t.Context(), "addr", &JoinRequest{})
			assert.Error(t, err)
			assert.Nil(t, joinResp)
		})
	}
}

func TestGRPCDialerUsesInsecureTransport(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = lis.Close()
	})

	server := grpc.NewServer()
	t.Cleanup(server.Stop)

	agent := New(Config{NodeID: "node1", BindAddr: lis.Addr().String()}, nil, &testClock{now: time.Unix(0, 0)})
	gossippb.RegisterGossipServer(server, &Service{GetAgent: func() *Gossip { return agent }})

	go func() {
		_ = server.Serve(lis)
	}()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	dialer := &GRPCDialer{}
	t.Cleanup(dialer.Close)
	client, err := dialer.Dial(ctx, lis.Addr().String())
	require.NoError(t, err)

	resp, err := client.Join(ctx, toProtoJoinRequest(&JoinRequest{
		Member: Member{ID: "node2", Addr: "node2"},
	}))
	require.NoError(t, err)
	assert.NotEmpty(t, resp.Members)
}

func TestGRPCDialerUsesConfiguredSecureDialOption(t *testing.T) {
	expectedErr := errors.New("boom")

	dialer := &GRPCDialer{
		SecureDialOption: func() (grpc.DialOption, error) {
			return nil, expectedErr
		},
	}
	t.Cleanup(dialer.Close)
	_, err := dialer.Dial(t.Context(), "127.0.0.1:1")
	require.ErrorIs(t, err, expectedErr)
}

func TestGossipServiceNilAgent(t *testing.T) {
	service := &Service{GetAgent: func() *Gossip { return nil }}

	joinResp, err := service.Join(t.Context(), &gossippb.GossipJoinRequest{})
	require.NoError(t, err)
	assert.Empty(t, joinResp.Members)

	pushResp, err := service.PushPull(t.Context(), &gossippb.GossipMessage{})
	require.NoError(t, err)
	assert.Empty(t, pushResp.Members)
}

func TestSingleObserverDownDoesNotPropagate(t *testing.T) {
	// Regression test: if only one node marks the primary as Down
	// (via local timeout), that verdict must NOT propagate to other
	// nodes through gossip, because updateSuspicion does not bump
	// LastUpdate — so the gossip merge rejects the stale timestamp.
	transport := newLocalTransport()
	clock := &testClock{now: time.Unix(0, 0)}

	primary := newTestGossip("primary", []Member{{ID: "r1", Addr: "r1"}, {ID: "r2", Addr: "r2"}}, transport, clock, time.Second)
	r1 := newTestGossip("r1", []Member{{ID: "primary", Addr: "primary"}, {ID: "r2", Addr: "r2"}}, transport, clock, 40*time.Millisecond)
	r2 := newTestGossip("r2", []Member{{ID: "primary", Addr: "primary"}, {ID: "r1", Addr: "r1"}}, transport, clock, time.Second)

	// All nodes alive and converged.
	primary.UpdateLocal(HealthSnapshot{NodeID: "primary", Timestamp: clock.Now()})
	r1.UpdateLocal(HealthSnapshot{NodeID: "r1", Timestamp: clock.Now()})
	r2.UpdateLocal(HealthSnapshot{NodeID: "r2", Timestamp: clock.Now()})

	_, err := primary.HandleJoin(&JoinRequest{Member: Member{ID: "r1", Addr: "r1"}})
	require.NoError(t, err)
	_, err = primary.HandleJoin(&JoinRequest{Member: Member{ID: "r2", Addr: "r2"}})
	require.NoError(t, err)
	r1.HandlePushPull(&Message{Members: []Member{{ID: "primary", Addr: "primary"}}, States: []StateDigest{{NodeID: "primary", Status: StatusAlive, LastUpdate: clock.Now()}}})
	r2.HandlePushPull(&Message{Members: []Member{{ID: "primary", Addr: "primary"}}, States: []StateDigest{{NodeID: "primary", Status: StatusAlive, LastUpdate: clock.Now()}}})

	// Simulate: r1 alone can't reach primary (partition), but primary is actually alive.
	// Advance time past r1's MaxUpdateAge so r1 locally marks primary as Down.
	transport.SetOffline("primary", true) // block r1 from reaching primary
	clock.advance(80 * time.Millisecond)
	r1.updateSuspicion(clock.Now())
	assert.Equal(t, StatusDown, r1.Snapshot()["primary"].Status, "r1 should locally see primary as Down")

	// Now restore primary and have it gossip with r2 — primary is alive.
	transport.SetOffline("primary", false)
	primary.UpdateLocal(HealthSnapshot{NodeID: "primary", Timestamp: clock.Now()})
	primary.gossipOnce(t.Context(), clock.Now())

	// r1 gossips with r2. r1 sends primary=Down with the OLD timestamp.
	// r2 should NOT adopt r1's Down because r2 already has primary=Alive
	// with a NEWER timestamp from primary's direct gossip.
	r1.gossipOnce(t.Context(), clock.Now())

	// r2 must still see primary as Alive — single observer's Down must not spread.
	assert.Equal(t, StatusAlive, r2.Snapshot()["primary"].Status, "r2 must not adopt r1's single-observer Down verdict")
}

func TestFutureTimestampClamped(t *testing.T) {
	clock := &testClock{now: time.Unix(100, 0)}
	g := New(Config{NodeID: "node1", BindAddr: "node1", PhiThreshold: 4, MaxUpdateAge: time.Second}, nil, clock)

	// Apply a message with a far-future timestamp.
	futureTime := clock.Now().Add(time.Hour)
	g.HandlePushPull(&Message{
		Members: []Member{{ID: "node2", Addr: "node2"}},
		States:  []StateDigest{{NodeID: "node2", Status: StatusAlive, LastUpdate: futureTime}},
	})

	// The timestamp should be clamped to local time, not the future value.
	state := g.Snapshot()["node2"]
	assert.True(t, !state.LastUpdate.After(clock.Now()), "future timestamp should be clamped to local time")
}

func TestHandleJoinRejectsEmptyMember(t *testing.T) {
	clock := &testClock{now: time.Unix(0, 0)}
	g := New(Config{NodeID: "node1", BindAddr: "node1"}, nil, clock)

	resp, err := g.HandleJoin(&JoinRequest{Member: Member{ID: "", Addr: ""}})
	require.Error(t, err, "HandleJoin should reject empty member ID")
	assert.Nil(t, resp)

	// Verify no empty-string key was added to state.
	_, exists := g.Snapshot()[""]
	assert.False(t, exists, "empty node ID should not be in state map")
}

func TestPhiAccrualZeroStddev(t *testing.T) {
	// When all intervals are identical, stddev=0.
	// elapsed <= mean → phi=0, elapsed > mean → phi=100
	p := newPhiAccrual(10)
	base := time.Unix(0, 0)
	p.Observe(base)
	p.Observe(base.Add(10 * time.Millisecond))
	p.Observe(base.Add(20 * time.Millisecond))

	// elapsed == mean (10ms) → 0
	assert.Equal(t, 0.0, p.Phi(base.Add(30*time.Millisecond)))
	// elapsed < mean → 0
	assert.Equal(t, 0.0, p.Phi(base.Add(25*time.Millisecond)))
	// elapsed > mean → 100
	assert.Equal(t, 100.0, p.Phi(base.Add(50*time.Millisecond)))
}

func TestPhiAccrualInsufficientIntervals(t *testing.T) {
	p := newPhiAccrual(10)
	base := time.Unix(0, 0)
	// No observations at all.
	assert.Equal(t, 0.0, p.Phi(base))
	// One observation, no intervals.
	p.Observe(base)
	assert.Equal(t, 0.0, p.Phi(base.Add(time.Second)))
	// Two observations, one interval — still < 2.
	p.Observe(base.Add(10 * time.Millisecond))
	assert.Equal(t, 0.0, p.Phi(base.Add(time.Second)))
}

func TestPhiAccrualNormalRange(t *testing.T) {
	// With some variance, phi should be a reasonable positive number.
	p := newPhiAccrual(10)
	base := time.Unix(0, 0)
	p.Observe(base)
	p.Observe(base.Add(10 * time.Millisecond))
	p.Observe(base.Add(22 * time.Millisecond))
	p.Observe(base.Add(30 * time.Millisecond))

	// Slightly past mean — should be low but positive.
	phi := p.Phi(base.Add(42 * time.Millisecond))
	assert.True(t, phi > 0 && phi < 100, "phi should be in normal range, got %f", phi)
}

func TestObserveLockedCreatesDetector(t *testing.T) {
	clock := &testClock{now: time.Unix(0, 0)}
	g := New(Config{NodeID: "node1", BindAddr: "node1"}, nil, clock)

	// ObserveLocked for a node with no existing detector.
	g.mu.Lock()
	g.observeLocked("unknown-node", clock.Now())
	detector := g.detectors["unknown-node"]
	g.mu.Unlock()

	assert.NotNil(t, detector, "observeLocked should create a detector for unknown nodes")
}

func TestGossipServiceWithAgent(t *testing.T) {
	clock := &testClock{now: time.Unix(0, 0)}
	agent := New(Config{NodeID: "node1", BindAddr: "node1", PhiThreshold: 4}, nil, clock)
	service := &Service{GetAgent: func() *Gossip { return agent }}

	// Join with a real agent.
	joinResp, err := service.Join(t.Context(), toProtoJoinRequest(&JoinRequest{
		Member: Member{ID: "node2", Addr: "node2"},
	}))
	require.NoError(t, err)
	assert.NotEmpty(t, joinResp.Members)

	// PushPull with a real agent.
	pushResp, err := service.PushPull(t.Context(), toProtoMessage(&Message{
		Members: []Member{{ID: "node3", Addr: "node3"}},
		States:  []StateDigest{{NodeID: "node3", Status: StatusAlive, LastUpdate: clock.Now()}},
	}))
	require.NoError(t, err)
	assert.NotEmpty(t, pushResp.Members)
}

func TestGossipServiceRejectsEmptyMember(t *testing.T) {
	clock := &testClock{now: time.Unix(0, 0)}
	agent := New(Config{NodeID: "node1", BindAddr: "node1"}, nil, clock)
	service := &Service{GetAgent: func() *Gossip { return agent }}

	_, err := service.Join(t.Context(), toProtoJoinRequest(&JoinRequest{Member: Member{ID: "", Addr: ""}}))
	assert.Error(t, err)
}

func TestWithProbeTimeout(t *testing.T) {
	tests := []struct {
		name         string
		probeTimeout time.Duration
		wantDeadline bool
	}{{
		name: "zero",
	}, {
		name:         "positive",
		probeTimeout: time.Second,
		wantDeadline: true,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := New(Config{NodeID: "node1", ProbeTimeout: tt.probeTimeout}, nil, nil)
			ctx, cancel := g.withProbeTimeout(t.Context())
			defer cancel()
			_, hasDeadline := ctx.Deadline()
			assert.Equal(t, tt.wantDeadline, hasDeadline)
		})
	}
}

func TestNewPhiAccrualMinSize(t *testing.T) {
	for _, size := range []int{0, -5} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			p := newPhiAccrual(size)
			assert.Equal(t, 1, p.maxSize)
		})
	}
}

func TestStartWithZeroPingInterval(t *testing.T) {
	g := New(Config{NodeID: "node1", PingInterval: 0}, nil, nil)
	err := g.Start(t.Context())
	assert.NoError(t, err)
	// Should be a no-op, no goroutine started.
}

func TestDebugState(t *testing.T) {
	clock := &testClock{now: time.Unix(100, 0)}
	g := New(Config{
		NodeID:   "node1",
		BindAddr: "addr1",
		Meta:     map[string]string{"keyspace": "ks", "shard": "0"},
		Seeds:    []Member{{ID: "node2", Addr: "addr2"}},
	}, nil, clock)
	g.UpdateLocal(HealthSnapshot{NodeID: "node1", Timestamp: clock.Now()})

	debug := g.Debug()
	assert.Equal(t, NodeID("node1"), debug.NodeID)
	assert.Equal(t, "addr1", debug.BindAddr)
	assert.Len(t, debug.Members, 2)
	require.Contains(t, debug.States, NodeID("node1"))
	assert.Equal(t, StatusAlive, debug.States["node1"].Status)
	assert.False(t, debug.States["node1"].LastUpdate.IsZero())
	require.Contains(t, debug.States, NodeID("node2"))
	assert.Equal(t, StatusUnknown, debug.States["node2"].Status)
}

func TestStatusString(t *testing.T) {
	tests := []struct {
		status Status
		want   string
	}{
		{status: StatusAlive, want: "alive"},
		{status: StatusSuspect, want: "suspect"},
		{status: StatusDown, want: "down"},
		{status: StatusUnknown, want: "unknown"},
		{status: Status(99), want: "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, statusString(tt.status))
		})
	}
}

func TestAddMemberLockedEnrichesMetadata(t *testing.T) {
	clock := &testClock{now: time.Unix(0, 0)}
	g := New(Config{
		NodeID:   "node1",
		BindAddr: "addr1",
		Seeds:    []Member{{ID: "node2", Addr: "addr2"}},
	}, nil, clock)

	// node2 starts without metadata (seed entry).
	m := g.Members()
	for _, member := range m {
		if member.ID == "node2" {
			assert.Empty(t, member.Meta)
		}
	}

	// Gossip brings node2 with metadata — should enrich.
	g.HandlePushPull(&Message{
		Members: []Member{{ID: "node2", Addr: "addr2", Meta: map[string]string{"keyspace": "ks"}}},
		States:  []StateDigest{{NodeID: "node2", Status: StatusAlive, LastUpdate: clock.Now()}},
	})

	for _, member := range g.Members() {
		if member.ID == "node2" {
			assert.Equal(t, "ks", member.Meta["keyspace"])
		}
	}
}

func TestNewCopiesMemberMetadata(t *testing.T) {
	selfMeta := map[string]string{MetaKeyKeyspace: "ks", MetaKeyShard: "0"}
	seedMeta := map[string]string{MetaKeyKeyspace: "ks", MetaKeyShard: "0"}
	g := New(Config{
		NodeID:   "node1",
		BindAddr: "addr1",
		Meta:     selfMeta,
		Seeds:    []Member{{ID: "node2", Addr: "addr2", Meta: seedMeta}},
	}, nil, &testClock{now: time.Unix(0, 0)})

	selfMeta[MetaKeyShard] = "changed"
	seedMeta[MetaKeyShard] = "changed"

	members := g.Members()
	require.Len(t, members, 2)
	for _, member := range members {
		assert.Equal(t, "0", member.Meta[MetaKeyShard])
	}
}

func TestMembersReturnsMetadataCopies(t *testing.T) {
	g := New(Config{
		NodeID:   "node1",
		BindAddr: "addr1",
		Meta:     map[string]string{MetaKeyKeyspace: "ks", MetaKeyShard: "0"},
	}, nil, &testClock{now: time.Unix(0, 0)})

	members := g.Members()
	require.Len(t, members, 1)
	members[0].Meta[MetaKeyShard] = "changed"

	require.Len(t, g.Members(), 1)
	assert.Equal(t, "0", g.Members()[0].Meta[MetaKeyShard])
}

func TestSnapshotMessageLockedReturnsMetadataCopies(t *testing.T) {
	g := New(Config{
		NodeID:   "node1",
		BindAddr: "addr1",
		Meta:     map[string]string{MetaKeyKeyspace: "ks", MetaKeyShard: "0"},
	}, nil, &testClock{now: time.Unix(0, 0)})

	g.mu.Lock()
	msg := g.snapshotMessageLocked("")
	g.mu.Unlock()
	require.Len(t, msg.Members, 1)
	msg.Members[0].Meta[MetaKeyShard] = "changed"

	assert.Equal(t, "0", g.Members()[0].Meta[MetaKeyShard])
}

func TestReconfigure(t *testing.T) {
	transport := newLocalTransport()
	g1 := newTestGossip("node1", []Member{{ID: "node2", Addr: "node2"}}, transport, nil, time.Second)
	g2 := newTestGossip("node2", []Member{{ID: "node1", Addr: "node1"}}, transport, nil, time.Second)

	ctx := t.Context()
	require.NoError(t, g1.Start(ctx))
	require.NoError(t, g2.Start(ctx))
	defer g1.Stop()
	defer g2.Stop()

	// Reconfigure with new phi threshold and max update age.
	g1.Reconfigure(Config{
		PhiThreshold: 10,
		MaxUpdateAge: 30 * time.Second,
		PingInterval: 50 * time.Millisecond,
	})

	// Give the gossip loop one tick to process the reconfig.
	require.Eventually(t, func() bool {
		g1.mu.Lock()
		defer g1.mu.Unlock()
		return g1.cfg.PhiThreshold == 10 &&
			g1.cfg.MaxUpdateAge == 30*time.Second &&
			g1.cfg.PingInterval == 50*time.Millisecond
	}, time.Second, 10*time.Millisecond)
}

func TestReconfigurePartialUpdate(t *testing.T) {
	transport := newLocalTransport()
	g := newTestGossip("node1", nil, transport, nil, time.Second)
	require.NoError(t, g.Start(t.Context()))
	defer g.Stop()

	// Only update phi threshold, leave others at zero (no change).
	g.Reconfigure(Config{PhiThreshold: 8})

	require.Eventually(t, func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		return g.cfg.PhiThreshold == 8
	}, time.Second, 10*time.Millisecond)

	// Verify ping interval wasn't changed (still the test default).
	g.mu.Lock()
	assert.Equal(t, 10*time.Millisecond, g.cfg.PingInterval)
	g.mu.Unlock()
}

func TestReconfigureOnStoppedAgent(t *testing.T) {
	g := New(Config{NodeID: "node1", PingInterval: 10 * time.Millisecond}, nil, nil)
	// Reconfigure on a never-started agent should not panic.
	g.Reconfigure(Config{PhiThreshold: 5})
}

type failingDialer struct{}

func (failingDialer) Dial(ctx context.Context, target string) (gossippb.GossipClient, error) {
	return nil, errors.New("dial failed")
}

func (failingDialer) Close() {}

type nilClientDialer struct{}

func (nilClientDialer) Dial(ctx context.Context, target string) (gossippb.GossipClient, error) {
	return nil, nil
}

func (nilClientDialer) Close() {}
