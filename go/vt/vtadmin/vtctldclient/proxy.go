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

package vtctldclient

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldclient"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

// Proxy defines the connection interface of a proxied vtctldclient used by
// VTAdmin clusters.
type Proxy interface {
	// Dial opens a gRPC connection to a vtctld in the cluster. If the Proxy
	// already has a valid connection, this is a no-op.
	Dial(ctx context.Context) error

	// Hostname returns the hostname the Proxy is currently connected to.
	Hostname() string

	// Close closes the underlying vtctldclient connection. This is a no-op if
	// the Proxy has no current, valid connection. It is safe to call repeatedly.
	// Users may call Dial on a previously-closed Proxy to create a new
	// connection, but that connection may not be to the same particular vtctld.
	Close() error

	vtctlservicepb.VtctldClient
}

// ClientProxy implements the Proxy interface relying on a discovery.Discovery
// implementation to handle vtctld discovery and connection management.
type ClientProxy struct {
	vtctldclient.VtctldClient // embedded to provide easy implementation of the vtctlservicepb.VtctldClient interface

	cluster   *vtadminpb.Cluster
	creds     *grpcclient.StaticAuthClientCreds
	discovery discovery.Discovery

	// DialFunc is called to open a new vtctdclient connection. In production,
	// this should always be grpcvtctldclient.NewWithDialOpts, but it is
	// exported for testing purposes.
	DialFunc func(addr string, ff grpcclient.FailFast, opts ...grpc.DialOption) (vtctldclient.VtctldClient, error)

	closed bool
	host   string
}

// New returns a ClientProxy to the given cluster. When Dial-ing, it will use
// the given discovery implementation to find a vtctld to connect to, and the
// given creds to dial the underlying gRPC connection, both of which are
// provided by the Config.
//
// It does not open a connection to a vtctld; users must call Dial before first
// use.
func New(cfg *Config) *ClientProxy {
	return &ClientProxy{
		cluster:   cfg.Cluster,
		creds:     cfg.Credentials,
		discovery: cfg.Discovery,
		DialFunc:  grpcvtctldclient.NewWithDialOpts,
	}
}

// Dial is part of the Proxy interface.
func (vtctld *ClientProxy) Dial(ctx context.Context) error {
	span, ctx := trace.NewSpan(ctx, "VtctldClientProxy.Dial")
	defer span.Finish()

	if vtctld.VtctldClient != nil {
		if !vtctld.closed {
			span.Annotate("is_noop", true)

			return nil
		}

		span.Annotate("is_stale", true)

		// close before reopen. this is safe to call on an already-closed client.
		if err := vtctld.Close(); err != nil {
			return fmt.Errorf("error closing possibly-stale connection before re-dialing: %w", err)
		}
	}

	addr, err := vtctld.discovery.DiscoverVtctldAddr(ctx, nil)
	if err != nil {
		return fmt.Errorf("error discovering vtctld to dial: %w", err)
	}

	span.Annotate("vtctld_host", addr)
	span.Annotate("is_using_credentials", vtctld.creds != nil)

	opts := []grpc.DialOption{
		// TODO: make configurable. right now, omitting this and attempting
		// to not use TLS results in:
		//		grpc: no transport security set (use grpc.WithInsecure() explicitly or set credentials)
		grpc.WithInsecure(),
	}

	if vtctld.creds != nil {
		opts = append(opts, grpc.WithPerRPCCredentials(vtctld.creds))
	}

	client, err := vtctld.DialFunc(addr, grpcclient.FailFast(false), opts...)
	if err != nil {
		return err
	}

	vtctld.host = addr
	vtctld.VtctldClient = client
	vtctld.closed = false

	return nil
}

// Hostname is part of the Proxy interface.
func (vtctld *ClientProxy) Hostname() string {
	return vtctld.host
}

// Close is part of the Proxy interface.
func (vtctld *ClientProxy) Close() error {
	if vtctld.VtctldClient == nil {
		vtctld.closed = true

		return nil
	}

	err := vtctld.VtctldClient.Close()
	if err != nil {
		return err
	}

	vtctld.closed = true

	return nil
}
