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

// Test gRPC interface using a throttler client and server.
func TestThrottlerServer(t *testing.T) {
	// Listen on a random port.
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port

	// Create a gRPC server and listen on the port.
	s := grpc.NewServer()
	// Use the global manager which is a singleton.
	grpcthrottlerserver.StartServer(s, throttler.GlobalManager)
	go s.Serve(listener)

	// Create a ThrottlerClient gRPC client to talk to the throttler.
	client, err := factory(fmt.Sprintf("localhost:%v", port))
	if err != nil {
		t.Fatalf("Cannot create client: %v", err)
	}
	defer client.Close()

	throttlerclienttest.TestSuite(t, client)
}
