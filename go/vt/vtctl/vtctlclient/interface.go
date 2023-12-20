/*
Copyright 2019 The Vitess Authors.

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

// Package vtctlclient contains the generic client side of the remote vtctl protocol.
package vtctlclient

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
)

// vtctlClientProtocol specifics which RPC client implementation should be used.
var vtctlClientProtocol = "grpc"

func RegisterFlags(fs *pflag.FlagSet) {
	fs.StringVar(&vtctlClientProtocol, "vtctl_client_protocol", vtctlClientProtocol, "Protocol to use to talk to the vtctl server.")
}

func init() {
	for _, cmd := range []string{
		"vtctlclient",
		"vttestserver",
	} {
		servenv.OnParseFor(cmd, RegisterFlags)
	}
}

// VtctlClient defines the interface used to send remote vtctl commands
type VtctlClient interface {
	// ExecuteVtctlCommand will execute the command remotely
	ExecuteVtctlCommand(ctx context.Context, args []string, actionTimeout time.Duration) (logutil.EventStream, error)

	// Close will terminate the connection. This object won't be
	// used after this.
	Close()
}

// Factory functions are registered by client implementations
type Factory func(addr string) (VtctlClient, error)

var factories = make(map[string]Factory)

// RegisterFactory allows a client implementation to register itself.
func RegisterFactory(name string, factory Factory) {
	if _, ok := factories[name]; ok {
		log.Fatalf("RegisterFactory: %s already exists", name)
	}
	factories[name] = factory
}

// New allows a user of the client library to get its implementation.
func New(addr string) (VtctlClient, error) {
	factory, ok := factories[vtctlClientProtocol]
	if !ok {
		return nil, fmt.Errorf("unknown vtctl client protocol: %v", vtctlClientProtocol)
	}
	return factory(addr)
}
