package resolver

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/resolver"

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

type Resolver struct {
	disco  discovery.Discovery
	cc     resolver.ClientConn
	target resolver.Target
	opts   Options
}

func (r *Resolver) resolve() {
	span, ctx := trace.NewSpan(context.Background(), "(vtadmin/cluster/resolver).resolve")
	defer span.Finish()

	span.Annotate("cluster_id", r.target.Scheme)

	span.Annotate("scheme", r.target.Scheme)
	span.Annotate("authority", r.target.Authority)
	span.Annotate("endpoint", r.target.Endpoint)

	log.Infof("%s: resolving %ss (cluster %s)", logPrefix, r.target.Authority, r.target.Scheme)

	ctx, cancel := context.WithTimeout(ctx, r.opts.ResolveTimeout)
	defer cancel()

	var addrs []resolver.Address
	switch r.target.Authority {
	case "vtctld":
		vtctlds, err := r.disco.DiscoverVtctlds(ctx, r.opts.DiscoveryTags)
		if err != nil {
			log.Errorf("%s: failed to discover vtctlds (cluster %s): %s", logPrefix, r.target.Scheme, err)
			return
		}

		addrs = make([]resolver.Address, len(vtctlds))
		for i, vtctld := range vtctlds {
			addrs[i] = resolver.Address{
				Addr: vtctld.Hostname,
			}
		}
	case "vtgate":
		vtgates, err := r.disco.DiscoverVTGates(ctx, r.opts.DiscoveryTags)
		if err != nil {
			log.Errorf("%s: failed to discover vtgates (cluster %s): %s", logPrefix, r.target.Scheme, err)
			return
		}

		addrs = make([]resolver.Address, len(vtgates))
		for i, vtgate := range vtgates {
			addrs[i] = resolver.Address{
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

	if err := r.cc.UpdateState(resolver.State{Addresses: addrs}); err != nil {
		log.Errorf("%s: failed to update addresses for %s (cluster %s)", logPrefix, err, r.target.Scheme)
	}
}

func (r *Resolver) ResolveNow(o resolver.ResolveNowOptions) {
	r.resolve()
}

func (r *Resolver) Close() {}
