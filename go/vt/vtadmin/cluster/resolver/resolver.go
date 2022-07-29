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
// 1. Stats!
package resolver

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"
	grpcbackoff "google.golang.org/grpc/backoff"
	grpcresolver "google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery"
	"vitess.io/vitess/go/vt/vtadmin/debug"
	"vitess.io/vitess/go/vt/vtadmin/internal/backoff"
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

// BalancerPolicy is an enum for different grpc load balancer policies. It also
// implements the pflag.Value interface to be used as a flagset Var destination.
type BalancerPolicy string

const (
	// PickFirstBalancer
	PickFirstBalancer  BalancerPolicy = "pick_first"
	RoundRobinBalancer BalancerPolicy = "round_robin"
)

var allBalancerPolicies = []string{ // convenience for help/error messages
	string(PickFirstBalancer),
	string(RoundRobinBalancer),
}

// Set is part of the pflag.Value interface.
func (bp *BalancerPolicy) Set(s string) error {
	switch s {
	case string(PickFirstBalancer), string(RoundRobinBalancer):
		*bp = BalancerPolicy(s)
	default:
		return fmt.Errorf("unsupported balancer policy %s; must be one of %s", s, strings.Join(allBalancerPolicies, ", "))
	}

	return nil
}

// String is part of the pflag.Value interface.
func (bp *BalancerPolicy) String() string { return string(*bp) }

// Type is part of the pflag.Value interface.
func (*BalancerPolicy) Type() string { return "resolver.BalancerPolicy" }

// Options defines the configuration options that can produce a resolver.Builder.
//
// A builder may be produced directly from an Options struct, but the intended
// usage is to first initialize an Options struct via opts.InstallFlags, which
// ensures the Options have sensible defaults and both vtctldclient proxy and
// VTGateProxy do.
type Options struct {
	// Discovery is the discovery implementation used to discover host addresses
	// when the ClientConn requests an update from the resolver.
	Discovery        discovery.Discovery
	DiscoveryTags    []string
	DiscoveryTimeout time.Duration

	// MinDiscoveryInterval is the minimum amount of time to wait before re-
	// discovering after a successful discovery resolution. This is useful to
	// avoid spamming the backing discovery service if multiple ResolveNow calls
	// are made in rapid succession.
	//
	// Note that the interval for failed discovery resolutions are governed by
	// the backoff strategy and configuration, not this field.
	MinDiscoveryInterval time.Duration

	// BackoffStrategy specfies the strategy to use when backing off of failed
	// discovery resolution. Permitted values are "exponential", "linear", and
	// "none"; the empty string defaults to "exponential".
	BackoffStrategy string
	// BackoffConfig is the configuration used by the given backoff strategy.
	// For a "none" strategy, this is ignored.
	BackoffConfig grpcbackoff.Config

	// BalancerPolicy, if set, will cause a resolver to provide a ServiceConfig
	// to the resolver's ClientConn with a corresponding loadBalancingConfig.
	// Omitting this option will cause grpc to use its default load balacing
	// policy, which is currently pick_first.
	//
	// For more details, see https://github.com/grpc/grpc/blob/master/doc/service_config.md.
	BalancerPolicy BalancerPolicy
}

// NewBuilder returns a gRPC resolver.Builder for the given scheme. For vtadmin,
// the scheme should be a cluster ID.
//
// The target provided to Builder.Build will be used to switch on vtctld or
// vtgate, based on the URL.Host field of the parsed dial target. This means
// that the addr passed to Dial should have the form
// "{clusterID}://{vtctld|vtgate}/". Other target URL hosts will cause an error.
// To ensure the dial address conforms to this constraint, use this package's
// DialAddr function.
func (opts *Options) NewBuilder(scheme string) grpcresolver.Builder {
	return &builder{
		scheme: scheme,
		opts:   *opts,
	}
}

var defaultBackoffConfig = grpcbackoff.DefaultConfig

// InstallFlags installs the resolver.Options flags on the given flagset. It is
// used by both vtsql and vtctldclient proxies.
func (opts *Options) InstallFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&opts.DiscoveryTimeout, "discovery-timeout", 100*time.Millisecond,
		"Timeout to use when resolving hosts via discovery.")
	fs.StringSliceVar(&opts.DiscoveryTags, "discovery-tags", nil,
		"repeated, comma-separated list of tags to use when discovering hosts to connect to. "+
			"the semantics of the tags may depend on the specific discovery implementation used.")
	fs.Var(&opts.BalancerPolicy, "grpc-balancer-policy",
		fmt.Sprintf("Specify a load balancer policy to use for resolvers built by these options (the default grpc behavior is pick_first). Valid choices are %s",
			strings.Join(allBalancerPolicies, ",")))

	fs.DurationVar(&opts.MinDiscoveryInterval, "min-rediscovery-interval", time.Second*30,
		"Minimum amount of time to wait between successful discovery resolution calls. "+
			"Useful to avoid spamming the backing discovery service if multiple ResolveNow calls are made in rapid succession "+
			"(Note that the interval for failed discovery resolutions are governed by the backoff strategy and configuration, not this flag).")

	fs.StringVar(&opts.BackoffStrategy, "backoff-strategy", "",
		"Backoff strategy to use when backing off of failed discovery resolutions. "+
			`Permitted values are "exponential", "linear", and "none", and the empty string defaults to "exponential".`)
	fs.DurationVar(&opts.BackoffConfig.BaseDelay, "backoff-base-delay", defaultBackoffConfig.BaseDelay,
		"The amount of time to backoff after the first failure.")
	fs.DurationVar(&opts.BackoffConfig.MaxDelay, "backoff-max-delay", defaultBackoffConfig.MaxDelay,
		"The upper bound of time to wait, including backoffs offset by jitter.")
	fs.Float64Var(&opts.BackoffConfig.Multiplier, "backoff-multiplier", defaultBackoffConfig.Multiplier,
		"The factor to multiply (or add, in the case of \"linear\" backoff after a failed retry. "+
			"Ideally should be greater than 1.")
	fs.Float64Var(&opts.BackoffConfig.Jitter, "backoff-jitter", defaultBackoffConfig.Jitter,
		"The factor to which backoffs are randomized.")
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

	return r, nil
}

func (b *builder) build(target grpcresolver.Target, cc grpcresolver.ClientConn, opts grpcresolver.BuildOptions) (*resolver, error) {
	var fn func(context.Context, []string) ([]string, error)
	switch target.URL.Host {
	case "vtctld":
		fn = b.opts.Discovery.DiscoverVtctldAddrs
	case "vtgate":
		fn = b.opts.Discovery.DiscoverVTGateAddrs
	default:
		return nil, fmt.Errorf("%s: unsupported URL host %s", logPrefix, target.URL.Host)
	}

	var sc serviceconfig.Config
	if b.opts.BalancerPolicy != "" {
		// c.f. https://github.com/grpc/grpc/blob/master/doc/service_config.md#example
		scpr := cc.ParseServiceConfig(fmt.Sprintf(`{"loadBalancingConfig": [{ "%s": {} }] }`, b.opts.BalancerPolicy))
		if scpr.Err != nil {
			return nil, fmt.Errorf("failed to initialize service config with load balancer policy %s: %s", b.opts.BalancerPolicy, scpr.Err)
		}

		sc = scpr.Config
	}

	ctx, cancel := context.WithCancel(context.Background())

	r := &resolver{
		component:       target.URL.Host,
		cluster:         target.URL.Scheme,
		discoverAddrs:   fn,
		backoffStrategy: backoff.Get(b.opts.BackoffStrategy, b.opts.BackoffConfig),
		opts:            b.opts,
		cc:              cc,
		sc:              sc,
		rn:              make(chan struct{}, 1),
		ctx:             ctx,
		cancel:          cancel,
		createdAt:       time.Now().UTC(),
	}

	r.wg.Add(1)
	go r.watch()

	return r, nil
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
	component       string
	cluster         string
	discoverAddrs   func(ctx context.Context, tags []string) ([]string, error)
	backoffStrategy backoff.Strategy
	opts            Options

	cc grpcresolver.ClientConn
	sc serviceconfig.Config // optionally used to enforce a balancer policy

	rn     chan struct{} // used to signal that ResolveNow has been called
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// for debug.Debuggable
	// TODO: consider proper exported stats - histograms for timings, error rates, etc.

	m                sync.Mutex
	createdAt        time.Time
	lastResolvedAt   time.Time
	lastResolveError error
	lastAddrs        []grpcresolver.Address
}

func (r *resolver) watch() {
	defer r.wg.Done()

	// Handles calling r.resolve and recording debug metrics appropriately,
	// called in the for loop below.
	resolveOnce := func() (state *grpcresolver.State, err error) {
		lastResolvedAt := time.Now().UTC()
		defer func() {
			r.m.Lock()
			defer r.m.Unlock()

			r.lastResolvedAt = lastResolvedAt
			r.lastResolveError = err
			if state != nil {
				r.lastAddrs = state.Addresses
			}
		}()

		state, err = r.resolve()
		return state, err
	}

	backoffIndex := 1
	for {
		state, err := resolveOnce()
		switch err {
		case nil:
			switch len(state.Addresses) {
			case 0:
				log.Warningf("%s: found no %ss (cluster %s); updating grpc clientconn state anyway", logPrefix, r.component, r.cluster)
			default:
				log.Infof("%s: found %d %ss (cluster %s)", logPrefix, len(state.Addresses), r.component, r.cluster)
			}

			if updateErr := r.cc.UpdateState(*state); updateErr != nil {
				log.Errorf("%s: failed to update %ss addresses for %s (cluster %s): %s", logPrefix, r.component, r.cluster, updateErr)
				err = updateErr
			}
		default:
			log.Errorf("%s: failed to resolve new addresses for %s (cluster %s): %s", logPrefix, r.component, r.cluster, err)
			r.cc.ReportError(err)
		}

		var timer *time.Timer
		if err == nil {
			// Success. Wait for next call to ResolveNow (via r.rn chan), but
			// also at least a minimum time to avoid spamming the backing
			// discovery.
			backoffIndex = 1
			timer = time.NewTimer(r.opts.MinDiscoveryInterval)
			select {
			case <-r.ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}

				return
			case <-r.rn:
			}
		} else {
			timer = time.NewTimer(r.backoffStrategy.Backoff(backoffIndex))
			backoffIndex++
		}

		select {
		case <-r.ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}

			return
		case <-timer.C:
			timer.Stop()
		}
	}
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

	if r.sc != nil {
		span.Annotate("balancer_policy", r.opts.BalancerPolicy)
		state.ServiceConfig = &serviceconfig.ParseResult{
			Config: r.sc,
		}
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
	select {
	case r.rn <- struct{}{}:
	default:
	}
}

// Close is part of the resolver.Resolver interface.
func (r *resolver) Close() {
	// First, cancel any ongoing discovery calls and terminate the watcher loop.
	r.cancel()
	// Then, wait for the watch goroutine to gracefully exit.
	r.wg.Wait()
}

// Debug implements debug.Debuggable for resolver.
func (r *resolver) Debug() map[string]any {
	r.m.Lock()
	defer r.m.Unlock()

	// TODO: add a field for "closed"

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
		m["error"] = r.lastResolveError.Error()
	}

	return m
}
