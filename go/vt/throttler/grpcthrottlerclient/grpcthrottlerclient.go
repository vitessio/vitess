// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package grpcthrottlerclient contains the gRPC version of the throttler client protocol.
package grpcthrottlerclient

import (
	"flag"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/proto/throttlerdata"
	"github.com/youtube/vitess/go/vt/proto/throttlerservice"
	"github.com/youtube/vitess/go/vt/servenv/grpcutils"
	"github.com/youtube/vitess/go/vt/throttler/throttlerclient"
	"github.com/youtube/vitess/go/vt/vterrors"
	"google.golang.org/grpc"
)

var (
	cert = flag.String("throttler_client_grpc_cert", "", "the cert to use to connect")
	key  = flag.String("throttler_client_grpc_key", "", "the key to use to connect")
	ca   = flag.String("throttler_client_grpc_ca", "", "the server ca to use to validate servers when connecting")
	name = flag.String("throttler_client_grpc_server_name", "", "the server name to use to validate server certificate")
)

type client struct {
	conn       *grpc.ClientConn
	gRPCClient throttlerservice.ThrottlerClient
}

func factory(addr string) (throttlerclient.Client, error) {
	opt, err := grpcutils.ClientSecureDialOption(*cert, *key, *ca, *name)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.Dial(addr, opt)
	if err != nil {
		return nil, err
	}
	gRPCClient := throttlerservice.NewThrottlerClient(conn)

	return &client{conn, gRPCClient}, nil
}

// SetMaxRate is part of the ThrottlerClient interface and sets the rate on all
// throttlers of the server.
func (c *client) SetMaxRate(ctx context.Context, rate int64) ([]string, error) {
	request := &throttlerdata.SetMaxRateRequest{
		Rate: rate,
	}

	response, err := c.gRPCClient.SetMaxRate(ctx, request)
	if err != nil {
		return nil, vterrors.FromGRPCError(err)
	}
	return response.Names, nil
}

// Close is part of the ThrottlerClient interface.
func (c *client) Close() {
	c.conn.Close()
}

func init() {
	throttlerclient.RegisterFactory("grpc", factory)
}
