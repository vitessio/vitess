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

// Package mysqlctlclient contains the generic client side of the remote
// mysqlctl protocol.
package mysqlctlclient

import (
	"flag"
	"fmt"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"
)

var protocol = flag.String("mysqlctl_client_protocol", "grpc", "the protocol to use to talk to the mysqlctl server")
var connectionTimeout = flag.Duration("mysqlctl_client_connection_timeout", 30*time.Second, "the connection timeout to use to talk to the mysqlctl server")

// MysqlctlClient defines the interface used to send remote mysqlctl commands
type MysqlctlClient interface {
	// Start calls Mysqld.Start remotely.
	Start(ctx context.Context, mysqldArgs ...string) error

	// Shutdown calls Mysqld.Shutdown remotely.
	Shutdown(ctx context.Context, waitForMysqld bool) error

	// RunMysqlUpgrade calls Mysqld.RunMysqlUpgrade remotely.
	RunMysqlUpgrade(ctx context.Context) error

	// ReinitConfig calls Mysqld.ReinitConfig remotely.
	ReinitConfig(ctx context.Context) error

	// Close will terminate the connection. This object won't be used anymore.
	Close()
}

// Factory functions are registered by client implementations.
type Factory func(network, addr string, dialTimeout time.Duration) (MysqlctlClient, error)

var factories = make(map[string]Factory)

// RegisterFactory allows a client implementation to register itself
func RegisterFactory(name string, factory Factory) {
	if _, ok := factories[name]; ok {
		log.Fatalf("RegisterFactory %s already exists", name)
	}
	factories[name] = factory
}

// New creates a client implementation as specified by a flag.
func New(network, addr string) (MysqlctlClient, error) {
	factory, ok := factories[*protocol]
	if !ok {
		return nil, fmt.Errorf("unknown mysqlctl client protocol: %v", *protocol)
	}
	return factory(network, addr, *connectionTimeout)
}
