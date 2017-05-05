/*
Copyright 2017 Google Inc.

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

// Package throttlerclient defines the generic RPC client interface for the
// throttler service. It has to be implemented for the different RPC frameworks
// e.g. gRPC.
package throttlerclient

import (
	"flag"
	"fmt"
	"log"

	"github.com/youtube/vitess/go/vt/proto/throttlerdata"

	"golang.org/x/net/context"
)

// protocol specifices which RPC client implementation should be used.
var protocol = flag.String("throttler_client_protocol", "grpc", "the protocol to use to talk to the integrated throttler service")

// Client defines the generic RPC interface for the throttler service.
type Client interface {
	// MaxRates returns the current max rate for each throttler of the process.
	MaxRates(ctx context.Context) (map[string]int64, error)

	// SetMaxRate allows to change the current max rate for all throttlers
	// of the process.
	// It returns the names of the updated throttlers.
	SetMaxRate(ctx context.Context, rate int64) ([]string, error)

	// GetConfiguration returns the configuration of the MaxReplicationlag module
	// for the given throttler or all throttlers if "throttlerName" is empty.
	GetConfiguration(ctx context.Context, throttlerName string) (map[string]*throttlerdata.Configuration, error)

	// UpdateConfiguration (partially) updates the configuration of the
	// MaxReplicationlag module for the given throttler or all throttlers if
	// "throttlerName" is empty.
	// If "copyZeroValues" is true, fields with zero values will be copied
	// as well.
	// The function returns the names of the updated throttlers.
	UpdateConfiguration(ctx context.Context, throttlerName string, configuration *throttlerdata.Configuration, copyZeroValues bool) ([]string, error)

	// ResetConfiguration resets the configuration of the MaxReplicationlag module
	// to the initial configuration for the given throttler or all throttlers if
	// "throttlerName" is empty.
	// The function returns the names of the updated throttlers.
	ResetConfiguration(ctx context.Context, throttlerName string) ([]string, error)

	// Close will terminate the connection and free resources.
	Close()
}

// Factory has to be implemented and must create a new RPC client for a given
// "addr".
type Factory func(addr string) (Client, error)

var factories = make(map[string]Factory)

// RegisterFactory allows a client implementation to register itself.
func RegisterFactory(name string, factory Factory) {
	if _, ok := factories[name]; ok {
		log.Fatalf("RegisterFactory: %s already exists", name)
	}
	factories[name] = factory
}

// New will return a client for the selected RPC implementation.
func New(addr string) (Client, error) {
	factory, ok := factories[*protocol]
	if !ok {
		return nil, fmt.Errorf("unknown throttler client protocol: %v", *protocol)
	}
	return factory(addr)
}
