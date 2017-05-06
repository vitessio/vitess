/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
