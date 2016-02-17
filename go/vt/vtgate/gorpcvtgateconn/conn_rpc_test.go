// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcvtgateconn

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/vt/vtgate/gorpcvtgateservice"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconntest"
	"golang.org/x/net/context"
)

// TestGoRPCVTGateConn makes sure the gorpc (BsonRPC) service works
func TestGoRPCVTGateConn(t *testing.T) {
	// fake service
	service := vtgateconntest.CreateFakeServer(t)

	// listen on a random port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}

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

	// Create a Go RPC client connecting to the server
	ctx := context.Background()
	client, err := dial(ctx, listener.Addr().String(), 30*time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	vtgateconntest.RegisterTestDialProtocol(client)

	// run the test suite
	vtgateconntest.TestSuite(t, client, service)
	vtgateconntest.TestErrorSuite(t, service)

	// and clean up
	client.Close()
}
