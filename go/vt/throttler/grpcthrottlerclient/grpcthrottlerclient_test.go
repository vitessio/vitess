/*
Copyright 2019 The Vitess Authors.

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

	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/throttler"
	"vitess.io/vitess/go/vt/throttler/grpcthrottlerserver"
	"vitess.io/vitess/go/vt/throttler/throttlerclienttest"
)

// TestThrottlerServer tests the gRPC implementation using a throttler client
// and server.
func TestThrottlerServer(t *testing.T) {
	// Use the global manager which is a singleton.
	port := startGRPCServer(t, throttler.GlobalManager)

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
	// For testing the panic handling, use a fake Manager instead.
	port := startGRPCServer(t, &throttlerclienttest.FakeManager{})

	// Create a ThrottlerClient gRPC client to talk to the throttler.
	client, err := factory(fmt.Sprintf("localhost:%v", port))
	if err != nil {
		t.Fatalf("Cannot create client: %v", err)
	}
	defer client.Close()

	throttlerclienttest.TestSuitePanics(t, client)
}

func startGRPCServer(t *testing.T, m throttler.Manager) int {
	// Listen on a random port.
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}

	s := grpc.NewServer()
	grpcthrottlerserver.RegisterServer(s, m)
	// Call Serve() after our service has been registered. Otherwise, the test
	// will fail with the error "grpc: Server.RegisterService after Server.Serve".
	go s.Serve(listener)
	return listener.Addr().(*net.TCPAddr).Port
}
