// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtctl/gorpcvtctlserver"
	"github.com/youtube/vitess/go/vt/vtctl/vtctlclient"
	"golang.org/x/net/context"

	// we need to import the gorpcvtctlclient library so the go rpc
	// vtctl client is registered and can be used.
	_ "github.com/youtube/vitess/go/vt/vtctl/gorpcvtctlclient"
)

// VtctlPipe is a vtctl server based on a topo server, and a client that
// is connected to it via bson rpc.
type VtctlPipe struct {
	listener net.Listener
	client   vtctlclient.VtctlClient
	t        *testing.T
}

// NewVtctlPipe creates a new VtctlPipe based on the given topo server.
func NewVtctlPipe(t *testing.T, ts topo.Server) *VtctlPipe {
	// Listen on a random port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}

	// Create a Go Rpc server and listen on the port
	server := rpcplus.NewServer()
	server.Register(gorpcvtctlserver.NewVtctlServer(ts))

	// Create the HTTP server, serve the server from it
	handler := http.NewServeMux()
	bsonrpc.ServeCustomRPC(handler, server, false)
	httpServer := http.Server{
		Handler: handler,
	}
	go httpServer.Serve(listener)

	// Create a VtctlClient Go Rpc client to talk to the fake server
	client, err := vtctlclient.New(listener.Addr().String(), 30*time.Second)
	if err != nil {
		t.Fatalf("Cannot create client: %v", err)
	}

	return &VtctlPipe{
		listener: listener,
		client:   client,
		t:        t,
	}
}

// Close will stop listening and free up all resources.
func (vp *VtctlPipe) Close() {
	vp.client.Close()
	vp.listener.Close()
}

// Run executes the provided command remotely, logs the output in the
// test logs, and returns the command error.
func (vp *VtctlPipe) Run(args []string) error {
	actionTimeout := 30 * time.Second
	lockTimeout := 10 * time.Second
	ctx := context.Background()

	c, errFunc := vp.client.ExecuteVtctlCommand(ctx, args, actionTimeout, lockTimeout)
	for le := range c {
		vp.t.Logf(le.String())
	}
	return errFunc()
}
