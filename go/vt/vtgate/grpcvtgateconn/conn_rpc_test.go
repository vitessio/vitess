// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpcvtgateconn

import (
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/vt/vtgate/grpcvtgateservice"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconntest"
	"golang.org/x/net/context"
)

// TestGRPCVTGateConn makes sure the grpc service works
func TestGRPCVTGateConn(t *testing.T) {
	// fake service
	service := vtgateconntest.CreateFakeServer(t)

	// listen on a random port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}

	// Create a gRPC server and listen on the port
	server := grpc.NewServer()
	grpcvtgateservice.RegisterForTest(server, service)
	go server.Serve(listener)

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
