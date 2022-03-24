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
package resolver

import (
	"context"
	"fmt"
	"time"

	grpcresolver "google.golang.org/grpc/resolver"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery"
)

const logPrefix = "[vtadmin.cluster.resolver]"

type builder struct {
	scheme string
	disco  discovery.Discovery
	opts   Options
}

type Options struct {
	ResolveTimeout time.Duration
	DiscoveryTags  []string
}

// NewBuilder returns a gRPC resolver.Builder for the given scheme. For vtadmin,
// the scheme should be a cluster ID.
//
// The target provided to Builder.Build will be used to switch on vtctld or
// vtgate, based on the Authority field of the target. This means that the addr
// passed to Dial should have the form "{clusterID}://{vtctld|vtgate}/". Other
// target Authorities will cause an error.
func NewBuilder(scheme string, disco discovery.Discovery, opts Options) grpcresolver.Builder {
	return &builder{
		scheme: scheme,
		disco:  disco,
		opts:   opts,
	}
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
	r := &resolver{
		disco:  b.disco,
		cc:     cc,
		target: target,
		opts:   b.opts,
	}

	switch r.target.Authority {
	case "vtgate", "vtctld":
	default:
		return nil, fmt.Errorf("%s: unsupported authority %s", logPrefix, r.target.Authority)
	}

	r.resolve()
	return r, nil
}

// Scheme is part of the resolver.Builder interface.
func (b *builder) Scheme() string {
	return b.scheme
}

type resolver struct {
	disco  discovery.Discovery
	cc     grpcresolver.ClientConn
	target grpcresolver.Target
	opts   Options
}

func (r *resolver) resolve() {
	span, ctx := trace.NewSpan(context.Background(), "(vtadmin/cluster/resolver).resolve")
	defer span.Finish()

	span.Annotate("cluster_id", r.target.Scheme)

	span.Annotate("scheme", r.target.Scheme)
	span.Annotate("authority", r.target.Authority)
	span.Annotate("endpoint", r.target.Endpoint)

	log.Infof("%s: resolving %ss (cluster %s)", logPrefix, r.target.Authority, r.target.Scheme)

	ctx, cancel := context.WithTimeout(ctx, r.opts.ResolveTimeout)
	defer cancel()

	var addrs []grpcresolver.Address
	switch r.target.Authority {
	case "vtctld":
		vtctlds, err := r.disco.DiscoverVtctlds(ctx, r.opts.DiscoveryTags)
		if err != nil {
			log.Errorf("%s: failed to discover vtctlds (cluster %s): %s", logPrefix, r.target.Scheme, err)
			return
		}

		addrs = make([]grpcresolver.Address, len(vtctlds))
		for i, vtctld := range vtctlds {
			addrs[i] = grpcresolver.Address{
				Addr: vtctld.Hostname,
			}
		}
	case "vtgate":
		vtgates, err := r.disco.DiscoverVTGates(ctx, r.opts.DiscoveryTags)
		if err != nil {
			log.Errorf("%s: failed to discover vtgates (cluster %s): %s", logPrefix, r.target.Scheme, err)
			return
		}

		addrs = make([]grpcresolver.Address, len(vtgates))
		for i, vtgate := range vtgates {
			addrs[i] = grpcresolver.Address{
				Addr: vtgate.Hostname,
			}
		}
	default:
		// TODO: decide if we should just log error or full-blown panic.
		// this _should_ be impossible, since we checked this in builder.Build()
	}

	if len(addrs) == 0 {
		log.Warningf("%s: found no %ss (cluster %s); not updating grpc clientconn state", logPrefix, r.target.Authority, r.target.Scheme)
		return
	}

	log.Infof("%s: found %d %ss (cluster %s)", logPrefix, len(addrs), r.target.Authority, r.target.Scheme)

	if err := r.cc.UpdateState(grpcresolver.State{Addresses: addrs}); err != nil {
		log.Errorf("%s: failed to update addresses for %s (cluster %s)", logPrefix, err, r.target.Scheme)
	}
}

// ResolveNow is part of the resolver.Resolver interface. It is called by grpc
// ClientConn's when errors occur, as well as periodically to refresh the set of
// addresses a ClientConn can use for SubConns.
func (r *resolver) ResolveNow(o grpcresolver.ResolveNowOptions) {
	r.resolve()
}

// Close is part of the resolver.Resolver interface.
func (r *resolver) Close() {}
