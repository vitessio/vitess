/*
Copyright 2021 The Vitess Authors.

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

// Package grpcvtctldclient contains the gRPC version of the vtctld client
// protocol.
package grpcvtctldclient

import (
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vtctl/grpcclientcommon"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

const connClosedMsg = "grpc: the client connection is closed"

type gRPCVtctldClient struct {
	cc *grpc.ClientConn
	c  vtctlservicepb.VtctldClient
}

//go:generate -command grpcvtctldclient go run ./codegen
//go:generate grpcvtctldclient -out client_gen.go

func gRPCVtctldClientFactory(addr string) (vtctldclient.VtctldClient, error) {
	opt, err := grpcclientcommon.SecureDialOption()
	if err != nil {
		return nil, err
	}

	conn, err := grpcclient.Dial(addr, grpcclient.FailFast(false), opt)
	if err != nil {
		return nil, err
	}

	return &gRPCVtctldClient{
		cc: conn,
		c:  vtctlservicepb.NewVtctldClient(conn),
	}, nil
}

// NewWithDialOpts returns a vtctldclient.VtctldClient configured with the given
// DialOptions. It is exported for use in vtadmin.
func NewWithDialOpts(addr string, failFast grpcclient.FailFast, opts ...grpc.DialOption) (vtctldclient.VtctldClient, error) {
	conn, err := grpcclient.Dial(addr, failFast, opts...)
	if err != nil {
		return nil, err
	}

	return &gRPCVtctldClient{
		cc: conn,
		c:  vtctlservicepb.NewVtctldClient(conn),
	}, nil
}

func (client *gRPCVtctldClient) Close() error {
	err := client.cc.Close()
	if err == nil {
		client.c = nil
	}

	return err
}

func init() {
	vtctldclient.Register("grpc", gRPCVtctldClientFactory)
}
