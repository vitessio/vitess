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
