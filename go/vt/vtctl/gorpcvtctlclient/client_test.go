// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcvtctlclient

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/vtctl/gorpcvtctlserver"
	"github.com/youtube/vitess/go/vt/vtctl/vtctlclienttest"
)

// the test here creates a fake server implementation, a fake client
// implementation, and runs the test suite against the setup.
func TestVtctlServer(t *testing.T) {
	ts := vtctlclienttest.CreateTopoServer(t)

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
	bsonrpc.ServeCustomRPC(handler, server)
	httpServer := http.Server{
		Handler: handler,
	}
	go httpServer.Serve(listener)

	// Create a VtctlClient Go Rpc client to talk to the fake server
	client, err := goRPCVtctlClientFactory(listener.Addr().String(), 30*time.Second)
	if err != nil {
		t.Fatalf("Cannot create client: %v", err)
	}
	defer client.Close()

	vtctlclienttest.TestSuite(t, ts, client)
}
