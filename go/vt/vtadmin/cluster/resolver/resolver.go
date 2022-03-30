/*
Copyright 2022 The Vitess Authors.

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

// Package resolver provides a discovery-based resolver for VTAdmin clusters.
//
// It uses a discovery.Discovery implementation to dynamically update the set of
// vtctlds and vtgates in a cluster being used by a grpc.ClientConn, allowing
// VTAdmin to transparently reconnect to different vtctlds and vtgates both
// periodically and when hosts are recycled.
//
// Some potential improvements we can add, if desired:
//
// 1. Background refresh. We would take a config flag that governs the refresh
//	  interval and backoff (for when background refresh happens around the same
//	  time as grpc-core calls to ResolveNow) and spin up a goroutine. We would
//	  then have to spin this down when Close is called.
//
// 2. Stats!
package resolver

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"
	grpcresolver "google.golang.org/grpc/resolver"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery"
	"vitess.io/vitess/go/vt/vtadmin/debug"
)

const logPrefix = "[vtadmin.cluster.resolver]"

type builder struct {
	scheme string
	opts   Options

	// for debug.Debuggable
	m         sync.Mutex
	resolvers []*resolver
}

// DialAddr returns the dial address for a resolver scheme and component.
//
// VtctldClientProxy and VTGateProxy should use this to ensure their Dial calls
// use their respective discovery resolvers.
func DialAddr(resolver grpcresolver.Builder, component string) string {
	return fmt.Sprintf("%s://%s/", resolver.Scheme(), component)
}

type Options struct {
	Discovery        discovery.Discovery
	DiscoveryTags    []string
	DiscoveryTimeout time.Duration
}

// NewBuilder returns a gRPC resolver.Builder for the given scheme. For vtadmin,
// the scheme should be a cluster ID.
//
// The target provided to Builder.Build will be used to switch on vtctld or
// vtgate, based on the Authority field of the target. This means that the addr
// passed to Dial should have the form "{clusterID}://{vtctld|vtgate}/". Other
// target Authorities will cause an error.
func (opts *Options) NewBuilder(scheme string) grpcresolver.Builder {
	return &builder{
		scheme: scheme,
		opts:   *opts,
	}
}

// InstallFlags installs the resolver.Options flags on the given flagset. It is
// used by both vtsql and vtctldclient proxies.
func (opts *Options) InstallFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&opts.DiscoveryTimeout, "discovery-timeout", 100*time.Millisecond,
		"Timeout to use when resolving hosts via discovery.")
	fs.StringSliceVar(&opts.DiscoveryTags, "discovery-tags", nil,
		"repeated, comma-separated list of tags to use when discovering hosts to connect to. "+
			"the semantics of the tags may depend on the specific discovery implementation used.")
}

// Build is part of the resolver.Builder interface. See the commentary on
// NewBuilder in this package for more details on this particular
// implementation.
//
// Build is called during grpc.Dial and grpc.DialContext, but a grpc ClientConn
// will not call ResolveNow on the built Resolver until an error occurs or a
// period of time has elapsed. Therefore, we do a first resolution here before
// returning our Resolver back to grpc core. Failing to do this means that our
// first RPC would hang waiting for a resolver update.
func (b *builder) Build(target grpcresolver.Target, cc grpcresolver.ClientConn, opts grpcresolver.BuildOptions) (grpcresolver.Resolver, error) {
	r, err := b.build(target, cc, opts)
	if err != nil {
		return nil, err
	}

	b.m.Lock()
	b.resolvers = append(b.resolvers, r)
	b.m.Unlock()

	r.ResolveNow(grpcresolver.ResolveNowOptions{})

	return r, nil
}

func (b *builder) build(target grpcresolver.Target, cc grpcresolver.ClientConn, opts grpcresolver.BuildOptions) (*resolver, error) {
	var fn func(context.Context, []string) ([]string, error)
	switch target.Authority {
	case "vtctld":
		fn = b.opts.Discovery.DiscoverVtctldAddrs
	case "vtgate":
		fn = b.opts.Discovery.DiscoverVTGateAddrs
	default:
		return nil, fmt.Errorf("%s: unsupported authority %s", logPrefix, target.Authority)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &resolver{
		component:     target.Authority,
		cluster:       target.Scheme,
		discoverAddrs: fn,
		opts:          b.opts,
		cc:            cc,
		ctx:           ctx,
		cancel:        cancel,
		createdAt:     time.Now().UTC(),
	}, nil
}

// Scheme is part of the resolver.Builder interface.
func (b *builder) Scheme() string {
	return b.scheme
}

// Debug implements debug.Debuggable for builder.
func (b *builder) Debug() map[string]any {
	b.m.Lock()
	defer b.m.Unlock()

	resolvers := make([]map[string]any, len(b.resolvers))
	m := map[string]any{
		"scheme":            b.scheme,
		"discovery_tags":    b.opts.DiscoveryTags,
		"discovery_timeout": b.opts.DiscoveryTimeout,
		"resolvers":         resolvers,
	}

	for i, r := range b.resolvers {
		resolvers[i] = r.Debug()
	}

	return m
}

type resolver struct {
	component     string
	cluster       string
	discoverAddrs func(ctx context.Context, tags []string) ([]string, error)
	opts          Options

	cc grpcresolver.ClientConn

	ctx    context.Context
	cancel context.CancelFunc

	// for debug.Debuggable
	// TODO: consider proper exported stats - histograms for timings, error rates, etc.

	m                sync.Mutex
	createdAt        time.Time
	lastResolvedAt   time.Time
	lastResolveError error
	lastAddrs        []grpcresolver.Address
}

func (r *resolver) resolve() (*grpcresolver.State, error) {
	span, ctx := trace.NewSpan(r.ctx, "(vtadmin/cluster/resolver).resolve")
	defer span.Finish()

	span.Annotate("cluster_id", r.cluster)
	span.Annotate("component", r.component)

	log.Infof("%s: resolving %ss (cluster %s)", logPrefix, r.component, r.cluster)

	ctx, cancel := context.WithTimeout(ctx, r.opts.DiscoveryTimeout)
	defer cancel()

	addrs, err := r.discoverAddrs(ctx, r.opts.DiscoveryTags)
	if err != nil {
		return nil, fmt.Errorf("failed to discover %ss (cluster %s): %w", r.component, r.cluster, err)
	}

	span.Annotate("addrs", strings.Join(addrs, ","))

	state := &grpcresolver.State{
		Addresses: make([]grpcresolver.Address, len(addrs)),
	}

	for i, addr := range addrs {
		state.Addresses[i] = grpcresolver.Address{
			Addr: addr,
		}
	}

	return state, nil
}

// ResolveNow is part of the resolver.Resolver interface. It is called by grpc
// ClientConn's when errors occur, as well as periodically to refresh the set of
// addresses a ClientConn can use for SubConns.
func (r *resolver) ResolveNow(o grpcresolver.ResolveNowOptions) {
	r.m.Lock()
	defer r.m.Unlock()

	var (
		state *grpcresolver.State
		err   error
	)

	r.lastResolvedAt = time.Now().UTC()
	defer func() {
		r.lastResolveError = err
		if state != nil {
			r.lastAddrs = state.Addresses
		}
	}()

	state, err = r.resolve()
	if err != nil {
		log.Errorf("%s: failed to resolve new addresses for %s (cluster %s): %s", logPrefix, r.component, r.cluster, err)
		r.cc.ReportError(err)
		return
	}

	switch len(state.Addresses) {
	case 0:
		log.Warningf("%s: found no %ss (cluster %s); updating grpc clientconn state anyway", logPrefix, r.component, r.cluster)
	default:
		log.Infof("%s: found %d %ss (cluster %s)", logPrefix, len(state.Addresses), r.component, r.cluster)
	}

	err = r.cc.UpdateState(*state)
	if err != nil {
		log.Errorf("%s: failed to update %ss addresses for %s (cluster %s): %s", logPrefix, r.component, r.cluster, err)
		r.cc.ReportError(err)
		return
	}
}

// Close is part of the resolver.Resolver interface.
func (r *resolver) Close() {
	r.cancel() // cancel any ongoing call to ResolveNow, and therefore any resultant discovery lookup.
}

// Debug implements debug.Debuggable for resolver.
func (r *resolver) Debug() map[string]any {
	r.m.Lock()
	defer r.m.Unlock()

	m := map[string]any{
		"cluster":    r.cluster,
		"component":  r.component,
		"created_at": debug.TimeToString(r.createdAt),
		"addr_list":  r.lastAddrs,
	}

	if !r.lastResolvedAt.IsZero() {
		m["last_resolved_at"] = debug.TimeToString(r.lastResolvedAt)
	}

	if r.lastResolveError != nil {
		m["error"] = r.lastResolvedAt.String()
	}

	return m
}
