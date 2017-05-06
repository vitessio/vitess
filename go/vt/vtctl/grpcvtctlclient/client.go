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

// Package grpcvtctlclient contains the gRPC version of the vtctl client protocol
package grpcvtctlclient

import (
	"time"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/vtctl/vtctlclient"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
	vtctldatapb "github.com/youtube/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "github.com/youtube/vitess/go/vt/proto/vtctlservice"
)

type gRPCVtctlClient struct {
	cc *grpc.ClientConn
	c  vtctlservicepb.VtctlClient
}

func gRPCVtctlClientFactory(addr string, dialTimeout time.Duration) (vtctlclient.VtctlClient, error) {
	// create the RPC client
	cc, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(dialTimeout))
	if err != nil {
		return nil, err
	}
	c := vtctlservicepb.NewVtctlClient(cc)

	return &gRPCVtctlClient{
		cc: cc,
		c:  c,
	}, nil
}

type eventStreamAdapter struct {
	stream vtctlservicepb.Vtctl_ExecuteVtctlCommandClient
}

func (e *eventStreamAdapter) Recv() (*logutilpb.Event, error) {
	le, err := e.stream.Recv()
	if err != nil {
		return nil, err
	}
	return le.Event, nil
}

// ExecuteVtctlCommand is part of the VtctlClient interface
func (client *gRPCVtctlClient) ExecuteVtctlCommand(ctx context.Context, args []string, actionTimeout time.Duration) (logutil.EventStream, error) {
	query := &vtctldatapb.ExecuteVtctlCommandRequest{
		Args:          args,
		ActionTimeout: int64(actionTimeout.Nanoseconds()),
	}

	stream, err := client.c.ExecuteVtctlCommand(ctx, query)
	if err != nil {
		return nil, err
	}
	return &eventStreamAdapter{stream}, nil
}

// Close is part of the VtctlClient interface
func (client *gRPCVtctlClient) Close() {
	client.cc.Close()
}

func init() {
	vtctlclient.RegisterFactory("grpc", gRPCVtctlClientFactory)
}
