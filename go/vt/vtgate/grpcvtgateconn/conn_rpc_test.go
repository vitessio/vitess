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

package grpcvtgateconn

import (
	"flag"
	"io"
	"io/ioutil"
	"net"
	"os"
	"testing"

	"google.golang.org/grpc"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtgate/grpcvtgateservice"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateconntest"
)

// TestGRPCVTGateConn makes sure the grpc service works
func TestGRPCVTGateConn(t *testing.T) {
	// fake service
	service := vtgateconntest.CreateFakeServer(t)

	// listen on a random port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}

	// Create a gRPC server and listen on the port
	server := grpc.NewServer()
	grpcvtgateservice.RegisterForTest(server, service)
	go server.Serve(listener)

	// Create a Go RPC client connecting to the server
	ctx := context.Background()
	client, err := dial(ctx, listener.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	vtgateconntest.RegisterTestDialProtocol(client)

	// run the test suite
	vtgateconntest.TestSuite(t, client, service)
	vtgateconntest.TestErrorSuite(t, service)

	// and clean up
	client.Close()
}

// TestGRPCVTGateConnAuth makes sure the grpc with auth plugin works
func TestGRPCVTGateConnAuth(t *testing.T) {
	var opts []grpc.ServerOption
	// fake service
	service := vtgateconntest.CreateFakeServer(t)

	// listen on a random port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}

	// add auth interceptors
	opts = append(opts, grpc.StreamInterceptor(servenv.FakeAuthStreamInterceptor))
	opts = append(opts, grpc.UnaryInterceptor(servenv.FakeAuthUnaryInterceptor))

	// Create a gRPC server and listen on the port
	server := grpc.NewServer(opts...)
	grpcvtgateservice.RegisterForTest(server, service)
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

	// Create a Go RPC client connecting to the server
	ctx := context.Background()
	flag.Set("grpc_auth_static_client_creds", f.Name())
	client, err := dial(ctx, listener.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	vtgateconntest.RegisterTestDialProtocol(client)

	// run the test suite
	vtgateconntest.TestSuite(t, client, service)
	vtgateconntest.TestErrorSuite(t, service)

	// and clean up
	client.Close()

	invalidAuthJSON := `{
	 "Username": "invalid",
	 "Password": "valid"
	}`

	f, err = ioutil.TempFile("", "static_auth_creds.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	if _, err := io.WriteString(f, invalidAuthJSON); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Create a Go RPC client connecting to the server
	ctx = context.Background()
	flag.Set("grpc_auth_static_client_creds", f.Name())
	client, err = dial(ctx, listener.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	vtgateconntest.RegisterTestDialProtocol(client)
	conn, _ := vtgateconn.DialProtocol(context.Background(), "test", "")
	// run the test suite
	_, err = conn.Begin(context.Background())
	want := "rpc error: code = Unauthenticated desc = username and password must be provided"
	if err == nil || err.Error() != want {
		t.Errorf("expected auth failure:\n%v, want\n%s", err, want)
	}
	// and clean up again
	client.Close()
}
