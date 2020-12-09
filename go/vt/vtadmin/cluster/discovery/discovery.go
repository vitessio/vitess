package discovery

import (
	"context"
	"errors"
	"fmt"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

var (
	// ErrImplementationNotRegistered is returned from discovery.New if it is
	// called with an unknown implementation.
	ErrImplementationNotRegistered = errors.New("no discovery factory registered for implementation")
	// ErrNoVTGates should be returned from DiscoverVTGate* methods when they
	// are unable to find any vtgates for the given filter/query/tags.
	ErrNoVTGates = errors.New("no vtgates found")
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
}

// Factory represents a function that can create a Discovery implementation.
// This package will provide several implementations and register them for use.
type Factory func(cluster string, args []string) (Discovery, error)

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
func New(impl string, cluster string, args []string) (Discovery, error) {
	factory, ok := registry[impl]
	if !ok {
		return nil, fmt.Errorf("%w %s", ErrImplementationNotRegistered, impl)
	}

	return factory(cluster, args)
}
