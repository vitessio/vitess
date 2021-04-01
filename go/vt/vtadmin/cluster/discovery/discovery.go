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

package discovery

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/pflag"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

var (
	// ErrImplementationNotRegistered is returned from discovery.New if it is
	// called with an unknown implementation.
	ErrImplementationNotRegistered = errors.New("no discovery factory registered for implementation")
	// ErrNoVTGates should be returned from DiscoverVTGate* methods when they
	// are unable to find any vtgates for the given filter/query/tags.
	ErrNoVTGates = errors.New("no vtgates found")
	// ErrNoVtctlds should be returned from DiscoverVtctld* methods when they
	// are unable to find any vtctlds for the given filter/query/tags.
	ErrNoVtctlds = errors.New("no vtctlds found")
)

// Discovery defines the interface that service discovery plugins must
// implement. See ConsulDiscovery for an example implementation.
type Discovery interface {
	// DiscoverVTGate returns a vtgate found in the discovery service.
	// Tags can optionally be used to filter the set of potential gates further.
	// Which gate in a set of found gates is returned is not specified by the
	// interface, and can be implementation-specific.
	DiscoverVTGate(ctx context.Context, tags []string) (*vtadminpb.VTGate, error)
	// DiscoverVTGateAddr returns the address of a of vtgate found in the
	// discovery service. Tags can optionally be used to filter the set of
	// potential gates further. Which gate in a set of found gates is used to
	// return an address is not specified by the interface, and can be
	// implementation-specific.
	DiscoverVTGateAddr(ctx context.Context, tags []string) (string, error)
	// DiscoverVTGates returns a list of vtgates found in the discovery service.
	// Tags can optionally be used to filter gates. Order of the gates is not
	// specified by the interface, and can be implementation-specific.
	DiscoverVTGates(ctx context.Context, tags []string) ([]*vtadminpb.VTGate, error)
	// DiscoverVtctld returns a vtctld found in the discovery service.
	// Tags can optionally be used to filter the set of potential vtctlds
	// further. Which vtctld in a set of found vtctlds is returned is not
	// specified by the interface, and can be implementation-specific.
	DiscoverVtctld(ctx context.Context, tags []string) (*vtadminpb.Vtctld, error)
	// DiscoverVtctldAddr returns the address of a vtctld found in the discovery
	// service. Tags can optionally be used to filter the set of potential
	// vtctlds further. Which vtctld in a set of potential vtctld is used to
	// return an address is not specified by the interface, and can be
	// implementation-specific.
	DiscoverVtctldAddr(ctx context.Context, tags []string) (string, error)
	// DiscoverVtctlds returns a list of vtctlds found in the discovery service.
	// Tags can optionally be used to filter vtctlds. Order of the vtctlds is
	// not specified by the interface, and can be implementation-specific.
	DiscoverVtctlds(ctx context.Context, tags []string) ([]*vtadminpb.Vtctld, error)
}

// Factory represents a function that can create a Discovery implementation.
// This package will provide several implementations and register them for use.
// The flags FlagSet is provided for convenience, but also to hint to plugin
// developers that they should expect the args to be in a format compatible with
// pflag.
type Factory func(cluster *vtadminpb.Cluster, flags *pflag.FlagSet, args []string) (Discovery, error)

// nolint:gochecknoglobals
var registry = map[string]Factory{}

// Register registers a factory for the given implementation name. Attempting
// to register multiple factories for the same implementation name causes a
// panic.
func Register(name string, factory Factory) {
	_, ok := registry[name]
	if ok {
		panic("[discovery] factory already registered for " + name)
	}

	registry[name] = factory
}

// New returns a Discovery implementation using the registered factory for the
// implementation. Usage of the args slice is dependent on the implementation's
// factory.
func New(impl string, cluster *vtadminpb.Cluster, args []string) (Discovery, error) {
	factory, ok := registry[impl]
	if !ok {
		return nil, fmt.Errorf("%w %s", ErrImplementationNotRegistered, impl)
	}

	return factory(cluster, pflag.NewFlagSet("discovery:"+impl, pflag.ContinueOnError), args)
}

func init() { // nolint:gochecknoinits
	Register("consul", NewConsul)
	Register("staticfile", NewStaticFile)
}
