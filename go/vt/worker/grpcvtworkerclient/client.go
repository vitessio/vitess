/*
Copyright 2017 Google Inc.

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

// Package grpcvtworkerclient contains the gRPC version of the vtworker client protocol.
package grpcvtworkerclient

import (
	"flag"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/servenv/grpcutils"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/worker/vtworkerclient"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	vtworkerdatapb "github.com/youtube/vitess/go/vt/proto/vtworkerdata"
	vtworkerservicepb "github.com/youtube/vitess/go/vt/proto/vtworkerservice"
)

var (
	cert = flag.String("vtworker_client_grpc_cert", "", "the cert to use to connect")
	key  = flag.String("vtworker_client_grpc_key", "", "the key to use to connect")
	ca   = flag.String("vtworker_client_grpc_ca", "", "the server ca to use to validate servers when connecting")
	name = flag.String("vtworker_client_grpc_server_name", "", "the server name to use to validate server certificate")
)

type gRPCVtworkerClient struct {
	cc *grpc.ClientConn
	c  vtworkerservicepb.VtworkerClient
}

func gRPCVtworkerClientFactory(addr string, dialTimeout time.Duration) (vtworkerclient.Client, error) {
	// create the RPC client
	opt, err := grpcutils.ClientSecureDialOption(*cert, *key, *ca, *name)
	if err != nil {
		return nil, err
	}
	cc, err := grpc.Dial(addr, opt, grpc.WithBlock(), grpc.WithTimeout(dialTimeout))
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "grpc.Dial() err: %v", err)
	}
	c := vtworkerservicepb.NewVtworkerClient(cc)

	return &gRPCVtworkerClient{
		cc: cc,
		c:  c,
	}, nil
}

type eventStreamAdapter struct {
	stream vtworkerservicepb.Vtworker_ExecuteVtworkerCommandClient
}

func (e *eventStreamAdapter) Recv() (*logutilpb.Event, error) {
	le, err := e.stream.Recv()
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return le.Event, nil
}

// ExecuteVtworkerCommand is part of the VtworkerClient interface.
func (client *gRPCVtworkerClient) ExecuteVtworkerCommand(ctx context.Context, args []string) (logutil.EventStream, error) {
	query := &vtworkerdatapb.ExecuteVtworkerCommandRequest{
		Args: args,
	}

	stream, err := client.c.ExecuteVtworkerCommand(ctx, query)
	if err != nil {
		return nil, vterrors.FromGRPC(err)
	}
	return &eventStreamAdapter{stream}, nil
}

// Close is part of the VtworkerClient interface.
func (client *gRPCVtworkerClient) Close() {
	client.cc.Close()
}

func init() {
	vtworkerclient.RegisterFactory("grpc", gRPCVtworkerClientFactory)
}
