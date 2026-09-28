/*
Copyright 2026 The Vitess Authors.

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

// Package vtctldclient contains the generic client side of the remote vtctld
// protocol.
package vtctldclient

import (
	"context"
	"fmt"
	"log"

	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

type VtctldClient interface {
	vtctlservicepb.VtctldClient

	// Close augments the vtctlservicepb.VtctlClient interface with io.Closer.
	Close() error
}

// Factory is a function that creates new VtctldClients.
type Factory func(ctx context.Context, addr string) (VtctldClient, error)

var registry = map[string]Factory{}

// Register adds a VtctldClient factory for the given name (protocol).
// Attempting to register multiple factories for the same protocol is a fatal
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
func New(ctx context.Context, protocol string, addr string) (VtctldClient, error) {
	factory, ok := registry[protocol]
	if !ok {
		return nil, fmt.Errorf("unknown vtctld client protocol: %s", protocol)
	}

	return factory(ctx, addr)
}
