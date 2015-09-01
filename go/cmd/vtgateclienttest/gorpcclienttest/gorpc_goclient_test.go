// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcclienttest

import (
	"net"
	"net/http"
	"testing"

	"github.com/youtube/vitess/go/cmd/vtgateclienttest/goclienttest"
	"github.com/youtube/vitess/go/cmd/vtgateclienttest/services"
	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/vtgate/gorpcvtgateservice"
)

// TestGoRPCGoClient tests the go client using goRPC
func TestGoRPCGoClient(t *testing.T) {
	service := services.CreateServices()

	// listen on a random port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	defer listener.Close()

	// Create a Go Rpc server and listen on the port
	server := rpcplus.NewServer()
	server.Register(gorpcvtgateservice.New(service))

	// create the HTTP server, serve the server from it
	handler := http.NewServeMux()
	bsonrpc.ServeCustomRPC(handler, server)
	httpServer := http.Server{
		Handler: handler,
	}
	go httpServer.Serve(listener)

	// and run the test suite
	goclienttest.TestGoClient(t, "gorpc", listener.Addr().String())
}
