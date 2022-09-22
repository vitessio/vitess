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
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vtctl/grpcclientcommon"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

var (
	ErrConnectionShutdown = errors.New("gRPCVtctldClient in a SHUTDOWN state")
	ErrConnectionTimeout  = errors.New("gRPC connection wait time exceeded")
)

const connClosedMsg = "grpc: the client connection is closed"

type gRPCVtctldClient struct {
	cc *grpc.ClientConn
	c  vtctlservicepb.VtctldClient
}

//go:generate -command grpcvtctldclient go run ../vtctldclient/codegen
//go:generate grpcvtctldclient --out client_gen.go

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

// Close is part of the vtctldclient.VtctldClient interface.
func (client *gRPCVtctldClient) Close() error {
	err := client.cc.Close()
	if err == nil {
		client.c = nil
	}

	return err
}

// WaitForReady is part of the vtctldclient.VtctldClient interface.
func (client *gRPCVtctldClient) WaitForReady(ctx context.Context) error {
	// The gRPC implementation of WaitForReady uses the gRPC Connectivity API
	// See https://github.com/grpc/grpc/blob/master/doc/connectivity-semantics-and-api.md
	for {
		select {
		// A READY connection to the vtctld could not be established
		// within the context timeout. The caller should close their
		// existing connection and establish a new one.
		case <-ctx.Done():
			return ErrConnectionTimeout

		// Wait to transition to READY state
		default:
			connState := client.cc.GetState()

			switch connState {
			case connectivity.Ready:
				return nil

			// Per https://github.com/grpc/grpc/blob/master/doc/connectivity-semantics-and-api.md,
			// a client that enters the SHUTDOWN state never leaves this state, and all new RPCs should
			// fail immediately. Further polling is futile, in other words, and so we
			// return an error immediately to indicate that the caller can close the connection.
			case connectivity.Shutdown:
				return ErrConnectionShutdown

			// If the connection is IDLE, CONNECTING, or in a TRANSIENT_FAILURE mode,
			// then we wait to see if it will transition to a READY state.
			default:
				if !client.cc.WaitForStateChange(ctx, connState) {
					// If the client has failed to transition, fail so that the caller can close the connection.
					return fmt.Errorf("failed to transition from state %s", connState)
				}
			}
		}
	}
}

func init() {
	vtctldclient.Register("grpc", gRPCVtctldClientFactory)
}
