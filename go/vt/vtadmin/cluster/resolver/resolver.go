package resolver

import (
	"context"
	"time"

	"google.golang.org/grpc/resolver"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery"
)

type Builder struct {
	scheme string
	disco  discovery.Discovery
}

// TODO: inject options from the cluster config here (to be called by vtctldclient.Proxy and vtsql.VTGateProxy)
func NewBuilder(scheme string, disco discovery.Discovery) *Builder {
	return &Builder{scheme: scheme, disco: disco}
}

func (b *Builder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r := &Resolver{
		disco: b.disco,
		cc:    cc,
	}

	r.resolve()
	return r, nil
}

func (b *Builder) Scheme() string {
	return b.scheme
}

type Resolver struct {
	disco discovery.Discovery
	cc    resolver.ClientConn
}

func (r *Resolver) resolve() {
	span, ctx := trace.NewSpan(context.Background(), "vtadmin.cluster.Resolver.resolve")
	defer span.Finish()

	log.Infof("(vtadmin.cluster.Resolver).resolve called")

	// TODO: apply timeout (via config)
	// TODO: support tags from cluster configs
	// TODO: use target.Authority field (in Builder.Build) to switch between
	//		 vtctld/vtgate and use in package vtsql as well.
	ctx, cancel := context.WithTimeout(ctx, time.Second)
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
