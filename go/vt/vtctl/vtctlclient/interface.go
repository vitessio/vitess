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
	"github.com/henryanand/vitess/go/vt/logutil"
)

var vtctlClientProtocol = flag.String("vtctl_client_protocol", "gorpc", "the protocol to use to talk to the vtctl server")

// ErrFunc is returned by streaming queries to get the error
type ErrFunc func() error

// VtctlClient defines the interface used to send remote vtctl commands
type VtctlClient interface {
	// ExecuteVtctlCommand will execute the command remotely
	ExecuteVtctlCommand(args []string, actionTimeout, lockTimeout time.Duration) (<-chan *logutil.LoggerEvent, ErrFunc)

	// Close will terminate the connection. This object won't be
	// used after this.
	Close()
}

// VtctlClientFactory are registered by client implementations
type VtctlClientFactory func(addr string, dialTimeout time.Duration) (VtctlClient, error)

var vtctlClientFactories = make(map[string]VtctlClientFactory)

// RegisterVtctlClientFactory allows a client implementation to register itself
func RegisterVtctlClientFactory(name string, factory VtctlClientFactory) {
	if _, ok := vtctlClientFactories[name]; ok {
		log.Fatalf("RegisterVtctlClientFactory %s already exists", name)
	}
	vtctlClientFactories[name] = factory
}

// New allows a user of the client library to get its implementation.
func New(addr string, dialTimeout time.Duration) (VtctlClient, error) {
	factory, ok := vtctlClientFactories[*vtctlClientProtocol]
	if !ok {
		return nil, fmt.Errorf("unknown vtctl client protocol: %v", *vtctlClientProtocol)
	}
	return factory(addr, dialTimeout)
}
