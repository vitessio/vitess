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

package grpcvtworkerclient

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"testing"

	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/worker/grpcvtworkerserver"
	"vitess.io/vitess/go/vt/worker/vtworkerclienttest"

	vtworkerservicepb "vitess.io/vitess/go/vt/proto/vtworkerservice"
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
	client, err := gRPCVtworkerClientFactory(fmt.Sprintf("localhost:%v", port))
	if err != nil {
		t.Fatalf("Cannot create client: %v", err)
	}
	defer client.Close()

	vtworkerclienttest.TestSuite(t, client)
}

// Test gRPC interface using a vtworker and vtworkerclient with auth.
func TestVtworkerServerAuth(t *testing.T) {
	wi := vtworkerclienttest.CreateWorkerInstance(t)

	// Listen on a random port.
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port

	// Create a gRPC server and listen on the port
	var opts []grpc.ServerOption
	opts = append(opts, grpc.StreamInterceptor(servenv.FakeAuthStreamInterceptor))
	opts = append(opts, grpc.UnaryInterceptor(servenv.FakeAuthUnaryInterceptor))
	server := grpc.NewServer(opts...)

	vtworkerservicepb.RegisterVtworkerServer(server, grpcvtworkerserver.NewVtworkerServer(wi))
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

	// Create a VtworkerClient gRPC client to talk to the vtworker.
	flag.Set("grpc_auth_static_client_creds", f.Name())
	client, err := gRPCVtworkerClientFactory(fmt.Sprintf("localhost:%v", port))
	if err != nil {
		t.Fatalf("Cannot create client: %v", err)
	}
	defer client.Close()

	vtworkerclienttest.TestSuite(t, client)
}
