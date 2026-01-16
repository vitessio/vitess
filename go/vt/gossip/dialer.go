package gossip

import (
	"context"
	"io"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"

	gossippb "vitess.io/vitess/go/vt/proto/gossip"
)

type GRPCDialer struct{}

func (GRPCDialer) Dial(ctx context.Context, target string) (gossippb.GossipClient, error) {
	conn, err := grpcclient.DialContext(ctx, target, grpcclient.FailFast(false))
	if err != nil {
		log.Errorf("gossip dial failed to %s: %v", target, err)
		return nil, err
	}
	client := gossippb.NewGossipClient(conn)
	return &grpcClient{client: client, closer: conn}, nil
}

type grpcClient struct {
	client gossippb.GossipClient
	closer io.Closer
}

func (c *grpcClient) PushPull(ctx context.Context, in *gossippb.GossipMessage, opts ...grpc.CallOption) (*gossippb.GossipMessage, error) {
	defer c.closer.Close()
	return c.client.PushPull(ctx, in, opts...)
}

func (c *grpcClient) Join(ctx context.Context, in *gossippb.GossipJoinRequest, opts ...grpc.CallOption) (*gossippb.GossipJoinResponse, error) {
	defer c.closer.Close()
	return c.client.Join(ctx, in, opts...)
}
