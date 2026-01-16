package logic

import (
	"context"
	"io"
	"sync"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/gossip"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtorc/config"

	gossippb "vitess.io/vitess/go/vt/proto/gossip"
)

var (
	gossipOnce  sync.Once
	gossipAgent *gossip.Gossip
)

func startGossip() {
	gossipOnce.Do(func() {
		if !config.GossipEnabled() {
			return
		}

		dialer := gossipDialer{seeds: config.GossipSeeds()}
		transport := gossip.NewGRPCTransport(dialer)

		gossipAgent = gossip.New(gossip.Config{
			NodeID:       gossip.NodeID(config.GossipNodeID()),
			BindAddr:     config.GossipListenAddr(),
			Seeds:        config.GossipSeeds(),
			PhiThreshold: config.GossipPhiThreshold(),
			PingInterval: config.GossipPingInterval(),
			ProbeTimeout: config.GossipProbeTimeout(),
			MaxUpdateAge: config.GossipMaxUpdateAge(),
		}, transport, nil)

		if gossipAgent == nil {
			return
		}

		if err := gossipAgent.Start(context.Background()); err != nil {
			log.Errorf("failed to start gossip: %v", err)
			return
		}

		servenv.OnTerm(gossipAgent.Stop)
	})
}

func stopGossip() {
	if gossipAgent == nil {
		return
	}
	gossipAgent.Stop()
}

type gossipDialer struct {
	seeds []gossip.Member
}

func (d gossipDialer) Dial(ctx context.Context, target string) (gossippb.GossipClient, error) {
	if target == "" {
		if len(d.seeds) == 0 {
			return nil, nil
		}
		target = d.seeds[0].Addr
	}
	conn, err := grpcclient.DialContext(ctx, target, grpcclient.FailFast(false))
	if err != nil {
		return nil, err
	}
	client := gossippb.NewGossipClient(conn)
	return &gossipClient{client: client, closer: conn}, nil
}

type gossipClient struct {
	client gossippb.GossipClient
	closer io.Closer
}

func (c *gossipClient) PushPull(ctx context.Context, in *gossippb.GossipMessage, opts ...grpc.CallOption) (*gossippb.GossipMessage, error) {
	defer c.closer.Close()
	return c.client.PushPull(ctx, in, opts...)
}

func (c *gossipClient) Join(ctx context.Context, in *gossippb.GossipJoinRequest, opts ...grpc.CallOption) (*gossippb.GossipJoinResponse, error) {
	defer c.closer.Close()
	return c.client.Join(ctx, in, opts...)
}
