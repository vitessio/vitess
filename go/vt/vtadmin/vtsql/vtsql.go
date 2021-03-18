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
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vitessdriver"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery"
	"vitess.io/vitess/go/vt/vtadmin/vtadminproto"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// DB defines the connection and query interface of vitess SQL queries used by
// VTAdmin clusters.
type DB interface {
	// ShowTablets executes `SHOW vitess_tablets` and returns the result.
	ShowTablets(ctx context.Context) (*sql.Rows, error)

	// Dial opens a gRPC database connection to a vtgate in the cluster. If the
	// DB already has a valid connection, this is a no-op.
	//
	// target is a Vitess query target, e.g. "", "<keyspace>", "<keyspace>@replica".
	Dial(ctx context.Context, target string, opts ...grpc.DialOption) error

	// Hostname returns the hostname the DB is currently connected to.
	Hostname() string

	// Ping behaves like (*sql.DB).Ping.
	Ping() error
	// PingContext behaves like (*sql.DB).PingContext.
	PingContext(ctx context.Context) error

	// Close closes the currently-held database connection. This is a no-op if
	// the DB has no current valid connection. It is safe to call repeatedly.
	// Users may call Dial on a previously-closed DB to create a new connection,
	// but that connection may not be to the same particular vtgate.
	Close() error
}

// VTGateProxy is a proxy for creating and using database connections to vtgates
// in a Vitess cluster.
type VTGateProxy struct {
	cluster       *vtadminpb.Cluster
	discovery     discovery.Discovery
	discoveryTags []string
	creds         Credentials

	// DialFunc is called to open a new database connection. In production this
	// should always be vitessdriver.OpenWithConfiguration, but it is exported
	// for testing purposes.
	DialFunc        func(cfg vitessdriver.Configuration) (*sql.DB, error)
	dialPingTimeout time.Duration

	host string
	conn *sql.DB
}

var _ DB = (*VTGateProxy)(nil)

// ErrConnClosed is returned when attempting to use a closed connection.
var ErrConnClosed = errors.New("use of closed connection")

// New returns a VTGateProxy to the given cluster. When Dial-ing, it will use
// the given discovery implementation to find a vtgate to connect to, and the
// given creds to dial the underlying gRPC connection, both of which are
// provided by the Config.
//
// It does not open a connection to a vtgate; users must call Dial before first
// use.
func New(cfg *Config) *VTGateProxy {
	discoveryTags := cfg.DiscoveryTags
	if discoveryTags == nil {
		discoveryTags = []string{}
	}

	return &VTGateProxy{
		cluster:         cfg.Cluster,
		discovery:       cfg.Discovery,
		discoveryTags:   discoveryTags,
		creds:           cfg.Credentials,
		DialFunc:        vitessdriver.OpenWithConfiguration,
		dialPingTimeout: cfg.DialPingTimeout,
	}
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
func (vtgate *VTGateProxy) Dial(ctx context.Context, target string, opts ...grpc.DialOption) error {
	span, _ := trace.NewSpan(ctx, "VTGateProxy.Dial")
	defer span.Finish()

	vtgate.annotateSpan(span)

	if vtgate.conn != nil {
		ctx, cancel := context.WithTimeout(ctx, vtgate.dialPingTimeout)
		defer cancel()

		err := vtgate.PingContext(ctx)
		switch err {
		case nil:
			log.Infof("Have valid connection to %s, reusing it.", vtgate.host)
			span.Annotate("is_noop", true)

			return nil
		default:
			log.Warningf("Ping failed on host %s: %s; Rediscovering a vtgate to get new connection", vtgate.host, err)

			if err := vtgate.Close(); err != nil {
				log.Warningf("Error when closing connection to vtgate %s: %s; Continuing anyway ...", vtgate.host, err)
			}
		}
	}

	span.Annotate("is_noop", false)

	if vtgate.host == "" {
		gate, err := vtgate.discovery.DiscoverVTGateAddr(ctx, vtgate.discoveryTags)
		if err != nil {
			return fmt.Errorf("error discovering vtgate to dial: %w", err)
		}

		vtgate.host = gate
		// re-annotate the hostname
		span.Annotate("vtgate_host", gate)
	}

	log.Infof("Dialing %s ...", vtgate.host)

	conf := vitessdriver.Configuration{
		Protocol:        fmt.Sprintf("grpc_%s", vtgate.cluster.Id),
		Address:         vtgate.host,
		Target:          target,
		GRPCDialOptions: append(opts, grpc.WithInsecure()),
	}

	if vtgate.creds != nil {
		conf.GRPCDialOptions = append([]grpc.DialOption{
			grpc.WithPerRPCCredentials(vtgate.creds),
		}, conf.GRPCDialOptions...)
	}

	db, err := vtgate.DialFunc(conf)
	if err != nil {
		return fmt.Errorf("error dialing vtgate %s: %w", vtgate.host, err)
	}

	vtgate.conn = db

	return nil
}

// ShowTablets is part of the DB interface.
func (vtgate *VTGateProxy) ShowTablets(ctx context.Context) (*sql.Rows, error) {
	span, ctx := trace.NewSpan(ctx, "VTGateProxy.ShowTablets")
	defer span.Finish()

	vtgate.annotateSpan(span)

	if vtgate.conn == nil {
		return nil, ErrConnClosed
	}

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

	vtgate.annotateSpan(span)

	return vtgate.pingContext(ctx)
}

func (vtgate *VTGateProxy) pingContext(ctx context.Context) error {
	if vtgate.conn == nil {
		return ErrConnClosed
	}

	return vtgate.conn.PingContext(vtgate.getQueryContext(ctx))
}

// Close is part of the DB interface and satisfies io.Closer.
func (vtgate *VTGateProxy) Close() error {
	if vtgate.conn == nil {
		return nil
	}

	err := vtgate.conn.Close()

	vtgate.host = ""
	vtgate.conn = nil

	return err
}

// Hostname is part of the DB interface.
func (vtgate *VTGateProxy) Hostname() string {
	return vtgate.host
}

func (vtgate *VTGateProxy) annotateSpan(span trace.Span) {
	vtadminproto.AnnotateClusterSpan(vtgate.cluster, span)

	if vtgate.host != "" {
		span.Annotate("vtgate_host", vtgate.host)
	}
}
