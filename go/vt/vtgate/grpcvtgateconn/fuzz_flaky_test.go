// +build gofuzz

/*
Copyright 2021 The Vitess Authors.
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
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"testing"

	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtgate/grpcvtgateservice"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

func init() {
	testing.Init()
}

func IsDivisibleBy(n int, divisibleby int) bool {
	return (n % divisibleby) == 0
}

func Fuzz(data []byte) int {
	t := &testing.T{}
	if len(data) < 20 {
		return -1
	}
	if IsDivisibleBy(len(data), 10) == false {
		return -1
	}

	var opts []grpc.ServerOption
	// fake service
	service := CreateFakeServer(t)

	// listen on a random port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		fmt.Println("Cannot listen: %v", err)
		return -1
	}
	defer listener.Close()

	// add auth interceptors
	opts = append(opts, grpc.StreamInterceptor(servenv.FakeAuthStreamInterceptor))
	opts = append(opts, grpc.UnaryInterceptor(servenv.FakeAuthUnaryInterceptor))

	// Create a gRPC server and listen on the port
	server := grpc.NewServer(opts...)
	grpcvtgateservice.RegisterForTest(server, service)
	go server.Serve(listener)
	defer server.GracefulStop()

	authJSON := `{
         "Username": "valid",
         "Password": "valid"
        }`

	f, err := ioutil.TempFile("", "static_auth_creds.json")
	if err != nil {
		return -1
	}
	defer os.Remove(f.Name())
	if _, err := io.WriteString(f, authJSON); err != nil {
		return -1
	}
	if err := f.Close(); err != nil {
		return -1
	}

	// Create a Go RPC client connecting to the server
	ctx := context.Background()
	flag.Set("grpc_auth_static_client_creds", f.Name())
	client, err := dial(ctx, listener.Addr().String())
	if err != nil {
		fmt.Println("dial failed: %v", err)
		return -1
	}
	defer client.Close()

	RegisterTestDialProtocol(client)
	conn, err := vtgateconn.DialProtocol(context.Background(), "test", "")
	if err != nil {
		fmt.Println("Got err: %v from vtgateconn.DialProtocol", err)
		return -1
	}
	session := conn.Session("connection_ks@rdonly", testExecuteOptions)

	// Do the actual fuzzing:
	// 10 executions per fuzz run
	ctx = newContext()
	chunkSize := len(data) / 10
	for i := 0; i < len(data); i = i + chunkSize {
		from := i           //lower
		to := i + chunkSize //upper
		_, _ = session.Execute(ctx, string(data[from:to]), nil)
	}
	return 1
}
