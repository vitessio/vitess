// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtctlclient contains the generic client side of the remote vtctl protocol.
package vtctlclient

import (
	"flag"
	"fmt"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
)

// vtctlClientProtocol specifices which RPC client implementation should be used.
var vtctlClientProtocol = flag.String("vtctl_client_protocol", "grpc", "the protocol to use to talk to the vtctl server")

// VtctlClient defines the interface used to send remote vtctl commands
type VtctlClient interface {
	// ExecuteVtctlCommand will execute the command remotely
	ExecuteVtctlCommand(ctx context.Context, args []string, actionTimeout time.Duration) (logutil.EventStream, error)

	// Close will terminate the connection. This object won't be
	// used after this.
	Close()
}

// Factory functions are registered by client implementations
type Factory func(addr string, connectTimeout time.Duration) (VtctlClient, error)

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
func New(addr string, connectTimeout time.Duration) (VtctlClient, error) {
	factory, ok := factories[*vtctlClientProtocol]
	if !ok {
		return nil, fmt.Errorf("unknown vtctl client protocol: %v", *vtctlClientProtocol)
	}
	return factory(addr, connectTimeout)
}
