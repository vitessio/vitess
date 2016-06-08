// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpcthrottlerclient

import (
	"fmt"
	"net"
	"testing"

	"github.com/youtube/vitess/go/vt/throttler"
	"github.com/youtube/vitess/go/vt/throttler/grpcthrottlerserver"
	"github.com/youtube/vitess/go/vt/throttler/throttlerclienttest"
	"google.golang.org/grpc"
)

// TestThrottlerServer tests the gRPC implementation using a throttler client
// and server.
func TestThrottlerServer(t *testing.T) {
	s, port := startGRPCServer(t)
	// Use the global manager which is a singleton.
	grpcthrottlerserver.StartServer(s, throttler.GlobalManager)

	// Create a ThrottlerClient gRPC client to talk to the throttler.
	client, err := factory(fmt.Sprintf("localhost:%v", port))
	if err != nil {
		t.Fatalf("Cannot create client: %v", err)
	}
	defer client.Close()

	throttlerclienttest.TestSuite(t, client)
}

// TestThrottlerServerPanics tests the panic handling of the gRPC throttler
// server implementation.
func TestThrottlerServerPanics(t *testing.T) {
	s, port := startGRPCServer(t)
	// For testing the panic handling, use a fake Manager instead.
	grpcthrottlerserver.StartServer(s, &throttlerclienttest.FakeManager{})

	// Create a ThrottlerClient gRPC client to talk to the throttler.
	client, err := factory(fmt.Sprintf("localhost:%v", port))
	if err != nil {
		t.Fatalf("Cannot create client: %v", err)
	}
	defer client.Close()

	throttlerclienttest.TestSuitePanics(t, client)
}

func startGRPCServer(t *testing.T) (*grpc.Server, int) {
	// Listen on a random port.
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}

	// Create a gRPC server and listen on the port.
	s := grpc.NewServer()
	go s.Serve(listener)
	return s, listener.Addr().(*net.TCPAddr).Port
}
