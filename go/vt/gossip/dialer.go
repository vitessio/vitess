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
	"io"
	"log/slog"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"

	gossippb "vitess.io/vitess/go/vt/proto/gossip"
)

// GRPCDialer implements the Dialer interface using Vitess gRPC client connections.
type GRPCDialer struct{}

// Dial creates a new gRPC connection to the target and returns a gossip client.
func (GRPCDialer) Dial(ctx context.Context, target string) (gossippb.GossipClient, error) {
	conn, err := grpcclient.DialContext(ctx, target, grpcclient.FailFast(false))
	if err != nil {
		log.Error("gossip dial failed", slog.String("target", target), slog.Any("error", err))
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
