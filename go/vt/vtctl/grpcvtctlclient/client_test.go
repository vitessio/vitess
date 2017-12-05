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

package grpcvtctlclient

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"testing"

	"github.com/youtube/vitess/go/vt/servenv"
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
	client, err := gRPCVtctlClientFactory(fmt.Sprintf("localhost:%v", port))
	if err != nil {
		t.Fatalf("Cannot create client: %v", err)
	}
	defer client.Close()

	vtctlclienttest.TestSuite(t, ts, client)
}

// the test here creates a fake server implementation, a fake client with auth
// implementation, and runs the test suite against the setup.
func TestVtctlAuthClient(t *testing.T) {
	ts := vtctlclienttest.CreateTopoServer(t)

	// Listen on a random port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port

	// Create a gRPC server and listen on the port
	// add auth interceptors
	var opts []grpc.ServerOption
	opts = append(opts, grpc.StreamInterceptor(servenv.FakeAuthStreamInterceptor))
	opts = append(opts, grpc.UnaryInterceptor(servenv.FakeAuthUnaryInterceptor))
	server := grpc.NewServer(opts...)

	vtctlservicepb.RegisterVtctlServer(server, grpcvtctlserver.NewVtctlServer(ts))
	go server.Serve(listener)

	authJSON := `{
         "Username": "valid",
         "Password": "valid"
        }`

	f, err := ioutil.TempFile("", "static_auth_creds.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	if _, err := io.WriteString(f, authJSON); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	flag.Set("grpc_auth_static_client_creds", f.Name())

	// Create a VtctlClient gRPC client to talk to the fake server
	client, err := gRPCVtctlClientFactory(fmt.Sprintf("localhost:%v", port))
	if err != nil {
		t.Fatalf("Cannot create client: %v", err)
	}
	defer client.Close()

	vtctlclienttest.TestSuite(t, ts, client)
}
