// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
