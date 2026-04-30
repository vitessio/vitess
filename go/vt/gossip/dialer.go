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
	"log/slog"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"

	gossippb "vitess.io/vitess/go/vt/proto/gossip"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// GRPCDialer implements the Dialer interface using Vitess gRPC client
// connections, caching one *grpc.ClientConn per target. Gossip runs
// frequent RPCs (PingInterval is typically 1s), so reusing connections
// avoids a TLS+TCP handshake on every call. The zero value is NOT
// valid — use a pointer so the internal cache is shared across calls.
// Always call Close when done to release cached connections.
type GRPCDialer struct {
	SecureDialOption func() (grpc.DialOption, error)

	mu     sync.Mutex
	conns  map[string]*grpc.ClientConn
	closed bool
}

// ErrDialerClosed is returned from Dial when the dialer has been closed.
var ErrDialerClosed = vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "gossip: dialer closed")

// Dial returns a gossip client bound to target, reusing a cached
// *grpc.ClientConn when available.
func (d *GRPCDialer) Dial(ctx context.Context, target string) (gossippb.GossipClient, error) {
	// Fast path: return cached conn if present and still usable.
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, ErrDialerClosed
	}
	if conn, ok := d.conns[target]; ok {
		if conn.GetState() != connectivity.Shutdown {
			d.mu.Unlock()
			return gossippb.NewGossipClient(conn), nil
		}
		// Replace a shut-down conn.
		_ = conn.Close()
		delete(d.conns, target)
	}
	d.mu.Unlock()

	// Slow path: dial a new connection outside the lock so concurrent
	// callers for other targets aren't blocked by network I/O.
	opt, err := d.secureDialOption()
	if err != nil {
		log.Error("gossip dial failed", slog.String("target", target), slog.Any("error", err))
		return nil, err
	}
	conn, err := grpcclient.DialContext(ctx, target, grpcclient.FailFast(false), opt)
	if err != nil {
		log.Error("gossip dial failed", slog.String("target", target), slog.Any("error", err))
		return nil, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		_ = conn.Close()
		return nil, ErrDialerClosed
	}
	if d.conns == nil {
		d.conns = make(map[string]*grpc.ClientConn)
	}
	// Another goroutine may have dialed in parallel for the same
	// target. Keep whichever landed first and close our duplicate.
	if existing, ok := d.conns[target]; ok && existing.GetState() != connectivity.Shutdown {
		_ = conn.Close()
		return gossippb.NewGossipClient(existing), nil
	}
	d.conns[target] = conn
	return gossippb.NewGossipClient(conn), nil
}

// Close tears down all cached connections. After Close, Dial returns
// ErrDialerClosed. Close is idempotent and safe for concurrent use.
func (d *GRPCDialer) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return
	}
	d.closed = true
	for target, conn := range d.conns {
		_ = conn.Close()
		delete(d.conns, target)
	}
}

// secureDialOption resolves the TLS (or insecure) dial option. Callers
// wire SecureDialOption from their process-specific TLS flags
// (tablet-manager flags for vttablet, VTOrc's own flags for VTOrc) so
// gossip transport security tracks the rest of the deployment. Falls
// back to the equivalent of grpcclient's "no certs configured" path,
// which matches how other Vitess clients behave with missing TLS flags.
func (d *GRPCDialer) secureDialOption() (grpc.DialOption, error) {
	if d.SecureDialOption != nil {
		return d.SecureDialOption()
	}
	return grpcclient.SecureDialOption("", "", "", "", "")
}
