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
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery"
	"vitess.io/vitess/go/vt/vtadmin/debug"
	"vitess.io/vitess/go/vt/vtadmin/vtadminproto"
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
	cfg       *Config

	// DialFunc is called to open a new vtctdclient connection. In production,
	// this should always be grpcvtctldclient.NewWithDialOpts, but it is
	// exported for testing purposes.
	DialFunc func(addr string, ff grpcclient.FailFast, opts ...grpc.DialOption) (vtctldclient.VtctldClient, error)
	resolver resolver.Builder

	m        sync.Mutex
	closed   bool
	dialedAt time.Time
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
		cfg:       cfg,
		cluster:   cfg.Cluster,
		creds:     cfg.Credentials,
		discovery: cfg.Discovery,
		DialFunc:  grpcvtctldclient.NewWithDialOpts,
		resolver:  cfg.ResolverOptions.NewBuilder(cfg.Cluster.Id, cfg.Discovery),
		closed:    true,
	}
}

// Dial is part of the Proxy interface.
func (vtctld *ClientProxy) Dial(ctx context.Context) error {
	span, _ := trace.NewSpan(ctx, "VtctldClientProxy.Dial")
	defer span.Finish()

	vtadminproto.AnnotateClusterSpan(vtctld.cluster, span)

	vtctld.m.Lock()
	defer vtctld.m.Unlock()

	if vtctld.VtctldClient != nil {
		if !vtctld.closed {
			span.Annotate("is_noop", true)
			return nil
		}

		span.Annotate("is_stale", true)

		if err := vtctld.closeLocked(); err != nil {
			// Even if the client connection does not shut down cleanly, we don't want to block
			// Dial from discovering a new vtctld. This makes VTAdmin's dialer more resilient,
			// but, as a caveat, it _can_ potentially leak improperly-closed gRPC connections.
			log.Errorf("error closing possibly-stale connection before re-dialing: %w", err)
		}
	}

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

	opts = append(opts, grpc.WithResolvers(vtctld.resolver))

	// TODO: update DialFunc to take ctx as first arg.
	client, err := vtctld.DialFunc(vtctld.resolver.Scheme()+"://vtctld/", grpcclient.FailFast(false), append(opts, grpc.WithBlock())...)
	if err != nil {
		return err
	}

	log.Infof("Established gRPC connection to vtctld\n")
	vtctld.dialedAt = time.Now()
	vtctld.VtctldClient = client
	vtctld.closed = false

	return nil
}

// Close is part of the Proxy interface.
func (vtctld *ClientProxy) Close() error {
	vtctld.m.Lock()
	defer vtctld.m.Unlock()

	return vtctld.closeLocked()
}

func (vtctld *ClientProxy) closeLocked() error {
	if vtctld.VtctldClient == nil {
		vtctld.closed = true

		return nil
	}

	err := vtctld.VtctldClient.Close()

	// Mark the vtctld connection as "closed" from the proxy side even if
	// the client connection does not shut down cleanly. This makes VTAdmin's dialer more resilient,
	// but, as a caveat, it _can_ potentially leak improperly-closed gRPC connections.
	vtctld.closed = true

	if err != nil {
		return err
	}

	return nil
}

// Debug implements debug.Debuggable for ClientProxy.
func (vtctld *ClientProxy) Debug() map[string]any {
	vtctld.m.Lock()
	defer vtctld.m.Unlock()

	m := map[string]any{
		"is_connected": !vtctld.closed,
	}

	if vtctld.creds != nil {
		m["credentials"] = map[string]any{
			"source":   vtctld.cfg.CredentialsPath,
			"username": vtctld.creds.Username,
			"password": debug.SanitizeString(vtctld.creds.Password),
		}
	}

	if !vtctld.closed {
		m["dialed_at"] = debug.TimeToString(vtctld.dialedAt)
	}

	if dr, ok := vtctld.resolver.(debug.Debuggable); ok {
		m["resolver"] = dr.Debug()
	}

	return m
}
