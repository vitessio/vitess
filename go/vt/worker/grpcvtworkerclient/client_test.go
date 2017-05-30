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
