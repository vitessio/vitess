package resolver

import (
	"context"
	"time"

	"google.golang.org/grpc/resolver"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery"
)

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
func NewBuilder(scheme string, disco discovery.Discovery, opts Options) resolver.Builder {
	return &builder{
		scheme: scheme,
		disco:  disco,
		opts:   opts,
	}
}

// Build is part of the resolver.Builder interface. See the commentary on
// NewBuilder in this package for more details on this particular
// implementation.
func (b *builder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r := &Resolver{
		disco: b.disco,
		cc:    cc,
		opts:  b.opts,
	}

	r.resolve()
	return r, nil
}

// Scheme is part of the resolver.Builder interface.
func (b *builder) Scheme() string {
	return b.scheme
}

type Resolver struct {
	disco discovery.Discovery
	cc    resolver.ClientConn
	opts  Options
}

func (r *Resolver) resolve() {
	span, ctx := trace.NewSpan(context.Background(), "(vtadmin/cluster/resolver).resolve")
	defer span.Finish()

	log.Infof("(vtadmin.cluster.Resolver).resolve called")

	// TODO: apply timeout (via config)
	// TODO: support tags from cluster configs
	// TODO: use target.Authority field (in Builder.Build) to switch between
	//		 vtctld/vtgate and use in package vtsql as well.
	ctx, cancel := context.WithTimeout(ctx, r.opts.ResolveTimeout)
	defer cancel()

	vtctlds, err := r.disco.DiscoverVtctlds(ctx, nil)
	if err != nil {
		log.Errorf("error discovering vtctlds: %s", err)
		return
	}

	log.Infof("found %d vtctlds", len(vtctlds))
	addrs := make([]resolver.Address, len(vtctlds))
	for i, vtctld := range vtctlds {
		addrs[i] = resolver.Address{Addr: vtctld.Hostname}
	}

	// TODO: check error and log
	_ = r.cc.UpdateState(resolver.State{
		Addresses: addrs,
	})
}

func (r *Resolver) ResolveNow(o resolver.ResolveNowOptions) {
	r.resolve()
}

func (r *Resolver) Close() {}
