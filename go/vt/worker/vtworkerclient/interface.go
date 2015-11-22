// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtworkerclient contains the generic client side of the remote vtworker protocol.
package vtworkerclient

import (
	"flag"
	"fmt"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	logutilpb "github.com/youtube/vitess/go/vt/proto/logutil"
)

// protocol specifices which RPC client implementation should be used.
var protocol = flag.String("vtworker_client_protocol", "grpc", "the protocol to use to talk to the vtworker server")

// ErrFunc is returned by streaming queries to get the error
type ErrFunc func() error

// Client defines the interface used to send remote vtworker commands
type Client interface {
	// ExecuteVtworkerCommand will execute the command remotely.
	// NOTE: ErrFunc should only be checked after the returned channel was closed to avoid races.
	ExecuteVtworkerCommand(ctx context.Context, args []string) (<-chan *logutilpb.Event, ErrFunc, error)

	// Close will terminate the connection. This object won't be
	// used after this.
	Close()
}

// Factory functions are registered by client implementations.
type Factory func(addr string, connectTimeout time.Duration) (Client, error)

var factories = make(map[string]Factory)

// RegisterFactory allows a client implementation to register itself.
func RegisterFactory(name string, factory Factory) {
	if _, ok := factories[name]; ok {
		log.Fatalf("RegisterFactory %s already exists", name)
	}
	factories[name] = factory
}

// New allows a user of the client library to get its implementation.
func New(addr string, connectTimeout time.Duration) (Client, error) {
	factory, ok := factories[*protocol]
	if !ok {
		return nil, fmt.Errorf("unknown vtworker client protocol: %v", *protocol)
	}
	return factory(addr, connectTimeout)
}
