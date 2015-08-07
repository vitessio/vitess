// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"net"
	"testing"

	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/vt/vtgate/grpcvtgateservice"

	// import the grpc client, it will register itself
	_ "github.com/youtube/vitess/go/vt/vtgate/grpcvtgateconn"
)

// TestGRPCGoClient tests the go client using gRPC
func TestGRPCGoClient(t *testing.T) {
	service := createService()

	// listen on a random port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	defer listener.Close()

	// Create a gRPC server and listen on the port
	server := grpc.NewServer()
	grpcvtgateservice.RegisterForTest(server, service)
	go server.Serve(listener)

	// and run the test suite
	testGoClient(t, "grpc", listener.Addr().String())
}
