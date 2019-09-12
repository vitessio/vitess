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

// Package vtworkerclient contains the generic client side of the remote vtworker protocol.
package vtworkerclient

import (
	"flag"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// protocol specifics which RPC client implementation should be used.
var protocol = flag.String("vtworker_client_protocol", "grpc", "the protocol to use to talk to the vtworker server")

// Client defines the interface used to send remote vtworker commands
type Client interface {
	// ExecuteVtworkerCommand will execute the command remotely.
	ExecuteVtworkerCommand(ctx context.Context, args []string) (logutil.EventStream, error)

	// Close will terminate the connection. This object won't be
	// used after this.
	Close()
}

// Factory functions are registered by client implementations.
type Factory func(addr string) (Client, error)

var factories = make(map[string]Factory)

// RegisterFactory allows a client implementation to register itself.
func RegisterFactory(name string, factory Factory) {
	if _, ok := factories[name]; ok {
		log.Fatalf("RegisterFactory: %s already exists", name)
	}
	factories[name] = factory
}

// UnregisterFactoryForTest allows to unregister a client implementation from the static map.
// This function is used by unit tests to cleanly unregister any fake implementations.
// This way, a test package can use the same name for different fakes and no dangling fakes are
// left behind in the static factories map after the test.
func UnregisterFactoryForTest(name string) {
	if _, ok := factories[name]; !ok {
		log.Fatalf("UnregisterFactoryForTest: %s is not registered", name)
	}
	delete(factories, name)
}

// New allows a user of the client library to get its implementation.
func New(addr string) (Client, error) {
	factory, ok := factories[*protocol]
	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "unknown vtworker client protocol: %v", *protocol)
	}
	return factory(addr)
}
