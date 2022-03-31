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
	"sync"
	"time"

	"google.golang.org/grpc"

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

	m        sync.Mutex
	closed   bool
	host     string
	lastPing time.Time
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
		closed:    true,
	}
}

// Dial is part of the Proxy interface.
func (vtctld *ClientProxy) Dial(ctx context.Context) error {
	span, ctx := trace.NewSpan(ctx, "VtctldClientProxy.Dial")
	defer span.Finish()

	vtadminproto.AnnotateClusterSpan(vtctld.cluster, span)

	vtctld.m.Lock()
	defer vtctld.m.Unlock()

	if vtctld.VtctldClient != nil {
		if !vtctld.closed {
			waitCtx, waitCancel := context.WithTimeout(ctx, vtctld.cfg.ConnectivityTimeout)
			defer waitCancel()

			if err := vtctld.VtctldClient.WaitForReady(waitCtx); err == nil {
				// Our cached connection is still open and ready, so we're good to go.
				span.Annotate("is_noop", true)
				span.Annotate("vtctld_host", vtctld.host)

				vtctld.lastPing = time.Now()

				return nil
			}
			// If WaitForReady returns an error, that indicates our cached connection
			// is no longer valid. We fall through to close the cached connection,
			// discover a new vtctld, and establish a new connection.
		}

		span.Annotate("is_stale", true)

		// close before reopen. this is safe to call on an already-closed client.
		if err := vtctld.closeLocked(); err != nil {
			// Even if the client connection does not shut down cleanly, we don't want to block
			// Dial from discovering a new vtctld. This makes VTAdmin's dialer more resilient,
			// but, as a caveat, it _can_ potentially leak improperly-closed gRPC connections.
			log.Errorf("error closing possibly-stale connection before re-dialing: %w", err)
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

	waitCtx, waitCancel := context.WithTimeout(ctx, vtctld.cfg.ConnectivityTimeout)
	defer waitCancel()

	if err := client.WaitForReady(waitCtx); err != nil {
		// If the gRPC connection does not transition to a READY state within the context timeout,
		// then return an error. The onus to redial (or not) is on the caller of the Dial function.
		// As an enhancement, we could update this Dial function to try redialing the discovered vtctld
		// a few times with a backoff before giving up.
		log.Infof("Could not transition to READY state for gRPC connection to %s: %s\n", addr, err.Error())
		return err
	}

	log.Infof("Established gRPC connection to vtctld %s\n", addr)
	vtctld.dialedAt = time.Now()
	vtctld.host = addr
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
		"host":         vtctld.host,
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
		m["last_ping"] = debug.TimeToString(vtctld.lastPing)
		m["dialed_at"] = debug.TimeToString(vtctld.dialedAt)
	}

	return m
}
