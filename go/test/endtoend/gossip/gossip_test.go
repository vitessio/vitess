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

	return peer.HandleJoin(req), nil
}

func newAgent(id string, seeds []gossip.Member, transport *localTransport) *gossip.Gossip {
	cfg := gossip.Config{
		NodeID:       gossip.NodeID(id),
		BindAddr:     id,
		Seeds:        seeds,
		PhiThreshold: 4,
		PingInterval: 25 * time.Millisecond,
		ProbeTimeout: 10 * time.Millisecond,
		MaxUpdateAge: 50 * time.Millisecond,
	}
	agent := gossip.New(cfg, transport, nil)
	transport.Register(id, agent)
	return agent
}

func TestGossipEndToEnd(t *testing.T) {
	transport := newLocalTransport()
	g1 := newAgent("node1", []gossip.Member{{ID: "node2", Addr: "node2"}, {ID: "node3", Addr: "node3"}}, transport)
	g2 := newAgent("node2", []gossip.Member{{ID: "node1", Addr: "node1"}}, transport)
	g3 := newAgent("node3", []gossip.Member{{ID: "node1", Addr: "node1"}}, transport)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, g1.Start(ctx))
	require.NoError(t, g2.Start(ctx))
	require.NoError(t, g3.Start(ctx))
	defer g1.Stop()
	defer g2.Stop()
	defer g3.Stop()

	g1.UpdateLocal(gossip.HealthSnapshot{NodeID: "node1"})
	g2.UpdateLocal(gossip.HealthSnapshot{NodeID: "node2"})
	g3.UpdateLocal(gossip.HealthSnapshot{NodeID: "node3"})

	assert.Eventually(t, func() bool {
		return g1.Snapshot()["node2"].Status == gossip.StatusAlive &&
			g1.Snapshot()["node3"].Status == gossip.StatusAlive
	}, 300*time.Millisecond, 10*time.Millisecond)

	transport.SetOffline("node3", true)
	assert.Eventually(t, func() bool {
		return g1.Snapshot()["node3"].Status == gossip.StatusDown &&
			g2.Snapshot()["node3"].Status == gossip.StatusDown
	}, 400*time.Millisecond, 10*time.Millisecond)
}
