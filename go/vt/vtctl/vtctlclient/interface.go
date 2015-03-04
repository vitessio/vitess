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
	"github.com/youtube/vitess/go/vt/logutil"
	"golang.org/x/net/context"
)

var vtctlClientProtocol = flag.String("vtctl_client_protocol", "gorpc", "the protocol to use to talk to the vtctl server")

// ErrFunc is returned by streaming queries to get the error
type ErrFunc func() error

// VtctlClient defines the interface used to send remote vtctl commands
type VtctlClient interface {
	// ExecuteVtctlCommand will execute the command remotely
	ExecuteVtctlCommand(ctx context.Context, args []string, actionTimeout, lockTimeout time.Duration) (<-chan *logutil.LoggerEvent, ErrFunc)

	// Close will terminate the connection. This object won't be
	// used after this.
	Close()
}

// Factory functions are registered by client implementations
type Factory func(addr string, connectTimeout time.Duration) (VtctlClient, error)

var factories = make(map[string]Factory)

// RegisterFactory allows a client implementation to register itself
func RegisterFactory(name string, factory Factory) {
	if _, ok := factories[name]; ok {
		log.Fatalf("RegisterFactory %s already exists", name)
	}
	factories[name] = factory
}

// New allows a user of the client library to get its implementation.
func New(addr string, connectTimeout time.Duration) (VtctlClient, error) {
	factory, ok := factories[*vtctlClientProtocol]
	if !ok {
		return nil, fmt.Errorf("unknown vtctl client protocol: %v", *vtctlClientProtocol)
	}
	return factory(addr, connectTimeout)
}
