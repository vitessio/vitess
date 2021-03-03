// Package vtctldclient contains the generic client side of the remote vtctld
// protocol.
package vtctldclient

import (
	"fmt"
	"log"

	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

// VtctldClient augments the vtctlservicepb.VtctlClient interface with io.Closer.
type VtctldClient interface {
	vtctlservicepb.VtctldClient
	Close() error
}

// Factory is a function that creates new VtctldClients.
type Factory func(addr string) (VtctldClient, error)

var registry = map[string]Factory{}

// Register adds a VtctldClient factory for the given name (protocol).
// Attempting to register mulitple factories for the same protocol is a fatal
// error.
func Register(name string, factory Factory) {
	if _, ok := registry[name]; ok {
		log.Fatalf("Register: %s already registered", name)
	}

	registry[name] = factory
}

// New returns a VtctldClient for the given protocol, connected to a
// VtctldServer on the given addr. This function returns an error if no client
// factory was registered for the given protocol.
//
// This is a departure from vtctlclient's New, which relies on a flag in the
// global namespace to determine the protocol to use. Instead, we require
// users to specify their own flag in their own (hopefully not global) namespace
// to determine the protocol to pass into here.
func New(protocol string, addr string) (VtctldClient, error) {
	factory, ok := registry[protocol]
	if !ok {
		return nil, fmt.Errorf("unknown vtctld client protocol: %s", protocol)
	}

	return factory(addr)
}
