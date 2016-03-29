// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package grpcvtworkerclient

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/worker/grpcvtworkerserver"
	"github.com/youtube/vitess/go/vt/worker/vtworkerclienttest"
	"google.golang.org/grpc"

	vtworkerservicepb "github.com/youtube/vitess/go/vt/proto/vtworkerservice"
)

// Test gRPC interface using a vtworker and vtworkerclient.
func TestVtworkerServer(t *testing.T) {
	wi := vtworkerclienttest.CreateWorkerInstance(t)

	// Listen on a random port.
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port

	// Create a gRPC server and listen on the port.
	server := grpc.NewServer()
	vtworkerservicepb.RegisterVtworkerServer(server, grpcvtworkerserver.NewVtworkerServer(wi))
	go server.Serve(listener)

	// Create a VtworkerClient gRPC client to talk to the vtworker.
	client, err := gRPCVtworkerClientFactory(fmt.Sprintf("localhost:%v", port), 30*time.Second)
	if err != nil {
		t.Fatalf("Cannot create client: %v", err)
	}
	defer client.Close()

	vtworkerclienttest.TestSuite(t, client)
}
