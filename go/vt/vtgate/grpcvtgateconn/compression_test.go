/*
Copyright 2023 The Vitess Authors.

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

package grpcvtgateconn

import (
	"context"
	"net"
	"testing"

	"github.com/spf13/pflag"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vtgate/grpcvtgateservice"
)

// TestGRPCVTGateConnAuth makes sure gRPC compression works
func TestGRPCCompression(t *testing.T) {
	testGRPCCompression(t, "snappy")
	testGRPCCompression(t, "zstd")
}

func testGRPCCompression(t *testing.T, compressionType string) {
	// fake service
	service := CreateFakeServer(t)

	// listen on a random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}

	// Create a gRPC server and listen on the port
	server := grpc.NewServer()
	grpcvtgateservice.RegisterForTest(server, service)
	go server.Serve(listener)

	// Create a Go RPC client connecting to the server
	ctx := context.Background()
	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	grpcclient.RegisterFlags(fs)

	fs.Parse([]string{
		"--grpc_compression",
		compressionType,
	})
	client, err := dial(ctx, listener.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	RegisterTestDialProtocol(client)

	// run the test suite
	RunTests(t, client, service)
	RunErrorTests(t, service)

	// and clean up
	client.Close()
}

// TestUnsupportedCompression ensures specifying an unsupported compression type errors
func TestUnsupportedCompression(t *testing.T) {
	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	grpcclient.RegisterFlags(fs)

	bogusCompressionType := "foobar"

	err := fs.Parse([]string{
		"--grpc_compression",
		bogusCompressionType,
	})
	if err == nil {
		t.Fatalf("expected error setting bogus compression type: %s", bogusCompressionType)
	}
}
