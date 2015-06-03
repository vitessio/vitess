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
)

var mysqlctlClientProtocol = flag.String("mysqlctl_client_protocol", "gorpc", "the protocol to use to talk to the mysqlctl server")

// MysqlctlClient defines the interface used to send remote mysqlctl commands
type MysqlctlClient interface {
	// Start calls Mysqld.Start remotely.
	Start(mysqlWaitTime time.Duration) error

	// Shutdown calls Mysqld.Shutdown remotely.
	Shutdown(waitForMysqld bool, mysqlWaitTime time.Duration) error

	// RunMysqlUpgrade calls Mysqld.RunMysqlUpgrade remotely.
	RunMysqlUpgrade() error

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
func New(network, addr string, dialTimeout time.Duration) (MysqlctlClient, error) {
	factory, ok := factories[*mysqlctlClientProtocol]
	if !ok {
		return nil, fmt.Errorf("unknown mysqlctl client protocol: %v", *mysqlctlClientProtocol)
	}
	return factory(network, addr, dialTimeout)
}
