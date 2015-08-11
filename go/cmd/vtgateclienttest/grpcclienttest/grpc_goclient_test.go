// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpcclienttest

import (
	"net"
	"testing"

	"google.golang.org/grpc"

	"github.com/youtube/vitess/go/cmd/vtgateclienttest/goclienttest"
	"github.com/youtube/vitess/go/cmd/vtgateclienttest/services"
	"github.com/youtube/vitess/go/vt/vtgate/grpcvtgateservice"
)

// TestGRPCGoClient tests the go client using gRPC
func TestGRPCGoClient(t *testing.T) {
	service := services.CreateServices()

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
	goclienttest.TestGoClient(t, "grpc", listener.Addr().String())
}
