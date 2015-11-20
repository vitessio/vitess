// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpcvtctlclient

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/vtctl/grpcvtctlserver"
	"github.com/youtube/vitess/go/vt/vtctl/vtctlclienttest"
	"google.golang.org/grpc"

	vtctlservicepb "github.com/youtube/vitess/go/vt/proto/vtctlservice"
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
	port := listener.Addr().(*net.TCPAddr).Port

	// Create a gRPC server and listen on the port
	server := grpc.NewServer()
	vtctlservicepb.RegisterVtctlServer(server, grpcvtctlserver.NewVtctlServer(ts))
	go server.Serve(listener)

	// Create a VtctlClient gRPC client to talk to the fake server
	client, err := gRPCVtctlClientFactory(fmt.Sprintf("localhost:%v", port), 30*time.Second)
	if err != nil {
		t.Fatalf("Cannot create client: %v", err)
	}
	defer client.Close()

	vtctlclienttest.TestSuite(t, ts, client)
}
