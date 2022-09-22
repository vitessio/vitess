/*
Copyright 2020 The Vitess Authors.

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

package vtsql

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/credentials/insecure"

	"google.golang.org/grpc"
	grpcresolver "google.golang.org/grpc/resolver"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vitessdriver"
	"vitess.io/vitess/go/vt/vtadmin/cluster/resolver"
	"vitess.io/vitess/go/vt/vtadmin/debug"
	"vitess.io/vitess/go/vt/vtadmin/vtadminproto"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// DB defines the connection and query interface of vitess SQL queries used by
// VTAdmin clusters.
type DB interface {
	// ShowTablets executes `SHOW vitess_tablets` and returns the result.
	ShowTablets(ctx context.Context) (*sql.Rows, error)

	// Ping behaves like (*sql.DB).Ping.
	Ping() error
	// PingContext behaves like (*sql.DB).PingContext.
	PingContext(ctx context.Context) error

	// Close closes the underlying database connection. This is a no-op if
	// the DB has no current valid connection. It is safe to call repeatedly.
	//
	// Once closed, a DB is not safe for reuse.
	Close() error
}

// VTGateProxy is a proxy for creating and using database connections to vtgates
// in a Vitess cluster.
type VTGateProxy struct {
	cluster *vtadminpb.Cluster
	creds   Credentials
	cfg     *Config

	// DialFunc is called to open a new database connection. In production this
	// should always be vitessdriver.OpenWithConfiguration, but it is exported
	// for testing purposes.
	dialFunc func(cfg vitessdriver.Configuration) (*sql.DB, error)
	resolver grpcresolver.Builder

	conn *sql.DB

	m        sync.Mutex
	closed   bool
	dialedAt time.Time
}

var _ DB = (*VTGateProxy)(nil)

// New returns a VTGateProxy to the given cluster. When Dial-ing, it will use
// the given discovery implementation to find a vtgate to connect to, and the
// given creds to dial the underlying gRPC connection, both of which are
// provided by the Config.
//
// It does not open a connection to a vtgate; users must call Dial before first
// use.
func New(ctx context.Context, cfg *Config) (*VTGateProxy, error) {
	dialFunc := cfg.dialFunc
	if dialFunc == nil {
		dialFunc = vitessdriver.OpenWithConfiguration
	}

	proxy := VTGateProxy{
		cluster:  cfg.Cluster,
		creds:    cfg.Credentials,
		cfg:      cfg,
		dialFunc: dialFunc,
		resolver: cfg.ResolverOptions.NewBuilder(cfg.Cluster.Id),
	}

	if err := proxy.dial(ctx, ""); err != nil {
		return nil, err
	}

	return &proxy, nil
}

// getQueryContext returns a new context with the correct effective and immediate
// Caller IDs set, so queries do not passed to vttablet as the application RW
// user. All calls to to vtgate.conn should pass a context wrapped with this
// function.
//
// It returns the original context unchanged if the vtgate has no credentials
// configured.
func (vtgate *VTGateProxy) getQueryContext(ctx context.Context) context.Context {
	if vtgate.creds == nil {
		return ctx
	}

	return callerid.NewContext(
		ctx,
		callerid.NewEffectiveCallerID(vtgate.creds.GetEffectiveUsername(), "vtadmin", ""),
		callerid.NewImmediateCallerID(vtgate.creds.GetUsername()),
	)
}

// Dial is part of the DB interface. The proxy's DiscoveryTags can be set to
// narrow the set of possible gates it will connect to.
func (vtgate *VTGateProxy) dial(ctx context.Context, target string, opts ...grpc.DialOption) (err error) {
	span, _ := trace.NewSpan(ctx, "VTGateProxy.Dial")
	defer span.Finish()

	vtadminproto.AnnotateClusterSpan(vtgate.cluster, span)
	span.Annotate("is_using_credentials", vtgate.creds != nil)

	conf := vitessdriver.Configuration{
		Protocol:        fmt.Sprintf("grpc_%s", vtgate.cluster.Id),
		Address:         resolver.DialAddr(vtgate.resolver, "vtgate"),
		Target:          target,
		GRPCDialOptions: append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithResolvers(vtgate.resolver)),
	}

	if vtgate.creds != nil {
		conf.GRPCDialOptions = append([]grpc.DialOption{
			grpc.WithPerRPCCredentials(vtgate.creds),
		}, conf.GRPCDialOptions...)
	}

	vtgate.conn, err = vtgate.dialFunc(conf)
	if err != nil {
		return fmt.Errorf("error dialing vtgate: %w", err)
	}

	log.Infof("Established gRPC connection to vtgate\n")

	vtgate.m.Lock()
	defer vtgate.m.Unlock()

	vtgate.closed = false
	vtgate.dialedAt = time.Now()

	return nil
}

// ShowTablets is part of the DB interface.
func (vtgate *VTGateProxy) ShowTablets(ctx context.Context) (*sql.Rows, error) {
	span, ctx := trace.NewSpan(ctx, "VTGateProxy.ShowTablets")
	defer span.Finish()

	vtadminproto.AnnotateClusterSpan(vtgate.cluster, span)

	return vtgate.conn.QueryContext(vtgate.getQueryContext(ctx), "SHOW vitess_tablets")
}

// Ping is part of the DB interface.
func (vtgate *VTGateProxy) Ping() error {
	return vtgate.pingContext(context.Background())
}

// PingContext is part of the DB interface.
func (vtgate *VTGateProxy) PingContext(ctx context.Context) error {
	span, ctx := trace.NewSpan(ctx, "VTGateProxy.PingContext")
	defer span.Finish()

	vtadminproto.AnnotateClusterSpan(vtgate.cluster, span)

	return vtgate.pingContext(ctx)
}

func (vtgate *VTGateProxy) pingContext(ctx context.Context) error {
	return vtgate.conn.PingContext(vtgate.getQueryContext(ctx))
}

// Close is part of the DB interface and satisfies io.Closer.
func (vtgate *VTGateProxy) Close() error {
	vtgate.m.Lock()
	defer vtgate.m.Unlock()

	if vtgate.closed {
		return nil
	}

	defer func() { vtgate.closed = true }()
	return vtgate.conn.Close()
}

// Debug implements debug.Debuggable for VTGateProxy.
func (vtgate *VTGateProxy) Debug() map[string]any {
	vtgate.m.Lock()
	defer vtgate.m.Unlock()

	m := map[string]any{
		"is_connected": (!vtgate.closed),
	}

	if !vtgate.closed {
		m["dialed_at"] = debug.TimeToString(vtgate.dialedAt)
	}

	if vtgate.creds != nil {
		cmap := map[string]any{
			"source":         vtgate.cfg.CredentialsPath,
			"immediate_user": vtgate.creds.GetUsername(),
			"effective_user": vtgate.creds.GetEffectiveUsername(),
		}

		if creds, ok := vtgate.creds.(*StaticAuthCredentials); ok {
			cmap["password"] = debug.SanitizeString(creds.Password)
		}

		m["credentials"] = cmap
	}

	if dr, ok := vtgate.resolver.(debug.Debuggable); ok {
		m["resolver"] = dr.Debug()
	}

	return m
}
