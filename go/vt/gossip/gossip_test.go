package gossip

import (
	"context"
	"errors"
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
	now time.Time
}

func (c *testClock) Now() time.Time {
	return c.now
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

type localClient struct {
	peer *Gossip
}

func (c localClient) PushPull(ctx context.Context, in *gossippb.GossipMessage, opts ...grpc.CallOption) (*gossippb.GossipMessage, error) {
	return toProtoMessage(c.peer.HandlePushPull(fromProtoMessage(in))), nil
}

func (c localClient) Join(ctx context.Context, in *gossippb.GossipJoinRequest, opts ...grpc.CallOption) (*gossippb.GossipJoinResponse, error) {
	return toProtoJoinResponse(c.peer.HandleJoin(fromProtoJoinRequest(in))), nil
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

func TestGossipConvergesWithSeeds(t *testing.T) {
	transport := newLocalTransport()
	clock := &testClock{now: time.Now()}
	g1 := newTestGossip("node1", []Member{{ID: "node2", Addr: "node2"}}, transport, clock, 100*time.Millisecond)
	g2 := newTestGossip("node2", []Member{{ID: "node1", Addr: "node1"}}, transport, clock, 100*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, g1.Start(ctx))
	require.NoError(t, g2.Start(ctx))
	defer g1.Stop()
	defer g2.Stop()

	g1.UpdateLocal(HealthSnapshot{NodeID: "node1", Timestamp: clock.Now()})
	g2.UpdateLocal(HealthSnapshot{NodeID: "node2", Timestamp: clock.Now()})

	g1.HandleJoin(&JoinRequest{Member: Member{ID: "node2", Addr: "node2"}})
	g2.HandleJoin(&JoinRequest{Member: Member{ID: "node1", Addr: "node1"}})
	g1.HandlePushPull(&Message{Members: []Member{{ID: "node2", Addr: "node2"}}, States: []StateDigest{{NodeID: "node2", Status: StatusAlive, LastUpdate: clock.Now()}}})
	g2.HandlePushPull(&Message{Members: []Member{{ID: "node1", Addr: "node1"}}, States: []StateDigest{{NodeID: "node1", Status: StatusAlive, LastUpdate: clock.Now()}}})

	assert.Equal(t, StatusAlive, g1.Snapshot()["node2"].Status)
	assert.Equal(t, StatusAlive, g2.Snapshot()["node1"].Status)
}

func TestGossipMarksDownWhenPeerUnreachable(t *testing.T) {
	transport := newLocalTransport()
	clock := &testClock{now: time.Now()}
	g1 := newTestGossip("node1", []Member{{ID: "node2", Addr: "node2"}}, transport, clock, 40*time.Millisecond)
	g2 := newTestGossip("node2", []Member{{ID: "node1", Addr: "node1"}}, transport, clock, 40*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, g1.Start(ctx))
	require.NoError(t, g2.Start(ctx))
	defer g1.Stop()
	defer g2.Stop()

	g1.UpdateLocal(HealthSnapshot{NodeID: "node1", Timestamp: clock.Now()})
	g2.UpdateLocal(HealthSnapshot{NodeID: "node2", Timestamp: clock.Now()})

	g1.HandleJoin(&JoinRequest{Member: Member{ID: "node2", Addr: "node2"}})
	g2.HandleJoin(&JoinRequest{Member: Member{ID: "node1", Addr: "node1"}})
	g1.HandlePushPull(&Message{Members: []Member{{ID: "node2", Addr: "node2"}}, States: []StateDigest{{NodeID: "node2", Status: StatusAlive, LastUpdate: clock.Now()}}})

	assert.Equal(t, StatusAlive, g1.Snapshot()["node2"].Status)

	clock.now = clock.now.Add(80 * time.Millisecond)
	g1.updateSuspicion(clock.Now())
	assert.Equal(t, StatusDown, g1.Snapshot()["node2"].Status)
}

func TestGossipUpdateLocalUsesConfigNodeID(t *testing.T) {
	clock := &testClock{now: time.Unix(0, 0)}
	g := New(Config{NodeID: "node1", BindAddr: "node1", PhiThreshold: 4, PingInterval: time.Second}, nil, clock)
	clock.now = clock.now.Add(10 * time.Millisecond)

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

	resp := g.HandleJoin(&JoinRequest{Member: Member{ID: "node2", Addr: "node2"}})
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

	resp, err := g1.Join(context.Background(), Member{ID: "node1", Addr: "node1"})
	require.NoError(t, err)
	require.NotNil(t, resp)

	ids := make(map[NodeID]bool)
	for _, member := range resp.Members {
		ids[member.ID] = true
	}
	assert.True(t, ids["node1"])
	assert.True(t, ids["node2"])

	state := g2.Snapshot()["node1"]
	assert.Equal(t, StatusUnknown, state.Status)
}

func TestGossipOnceMergesState(t *testing.T) {
	transport := newLocalTransport()
	clock := &testClock{now: time.Unix(0, 0)}
	g1 := newTestGossip("node1", []Member{{ID: "node2", Addr: "node2"}}, transport, clock, time.Second)
	g2 := newTestGossip("node2", []Member{{ID: "node1", Addr: "node1"}}, transport, clock, time.Second)

	g1.UpdateLocal(HealthSnapshot{NodeID: "node1", Timestamp: clock.Now()})
	g2.UpdateLocal(HealthSnapshot{NodeID: "node2", Timestamp: clock.Now()})

	g1.gossipOnce(context.Background(), clock.Now())

	state := g1.Snapshot()["node2"]
	assert.Equal(t, StatusAlive, state.Status)
}

func TestGossipUpdateSuspicionMarksSuspect(t *testing.T) {
	clock := &testClock{now: time.Unix(0, 0)}
	g := New(Config{NodeID: "node1", BindAddr: "node1", PhiThreshold: 2}, nil, clock)
	g.addMemberLocked(Member{ID: "node2", Addr: "node2"})
	g.states["node2"] = State{Status: StatusAlive, LastUpdate: clock.Now()}

	g.observeLocked("node2", clock.Now())
	clock.now = clock.now.Add(10 * time.Millisecond)
	g.observeLocked("node2", clock.Now())
	clock.now = clock.now.Add(10 * time.Millisecond)
	g.observeLocked("node2", clock.Now())

	clock.now = clock.now.Add(200 * time.Millisecond)
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

func TestProtoConversions(t *testing.T) {
	member := Member{ID: "node1", Addr: "addr", Meta: map[string]string{"role": "seed"}}
	protoMember := toProtoMember(member)
	assert.Equal(t, member.ID, NodeID(protoMember.Id))
	assert.Equal(t, member.Addr, protoMember.Addr)
	assert.Equal(t, member.Meta, protoMember.Meta)

	state := StateDigest{NodeID: "node1", Status: StatusSuspect, Phi: 2.5, LastUpdate: time.Unix(0, 5)}
	protoState := toProtoState(state)
	roundTripped := fromProtoState(protoState)
	assert.Equal(t, state.NodeID, roundTripped.NodeID)
	assert.Equal(t, state.Status, roundTripped.Status)
	assert.Equal(t, state.Phi, roundTripped.Phi)
	assert.True(t, state.LastUpdate.Equal(roundTripped.LastUpdate))
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

func TestGRPCTransportNilDialer(t *testing.T) {
	transport := NewGRPCTransport(nil).(*grpcTransport)

	client, err := transport.dial(context.Background(), "addr")
	assert.NoError(t, err)
	assert.Nil(t, client)
}

func TestGRPCTransportErrorPropagation(t *testing.T) {
	transport := NewGRPCTransport(failingDialer{})

	resp, err := transport.PushPull(context.Background(), "addr", &Message{})
	assert.Error(t, err)
	assert.Nil(t, resp)

	joinResp, err := transport.Join(context.Background(), "addr", &JoinRequest{})
	assert.Error(t, err)
	assert.Nil(t, joinResp)
}

func TestGossipServiceNilAgent(t *testing.T) {
	service := &Service{}

	joinResp, err := service.Join(context.Background(), &gossippb.GossipJoinRequest{})
	require.NoError(t, err)
	assert.Empty(t, joinResp.Members)

	pushResp, err := service.PushPull(context.Background(), &gossippb.GossipMessage{})
	require.NoError(t, err)
	assert.Empty(t, pushResp.Members)
}

type failingDialer struct{}

func (failingDialer) Dial(ctx context.Context, target string) (gossippb.GossipClient, error) {
	return nil, errors.New("dial failed")
}
