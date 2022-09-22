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
package grpcoptionaltls

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc/credentials/insecure"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"

	"vitess.io/vitess/go/vt/tlstest"
)

// server is used to implement helloworld.GreeterServer.
type server struct {
	pb.UnimplementedGreeterServer
}

// SayHello implements helloworld.GreeterServer
func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func createUnstartedServer(creds credentials.TransportCredentials) *grpc.Server {
	s := grpc.NewServer(grpc.Creds(creds))
	pb.RegisterGreeterServer(s, &server{})
	return s
}

type testCredentials struct {
	client credentials.TransportCredentials
	server credentials.TransportCredentials
}

func createCredentials(t *testing.T) (*testCredentials, error) {
	// Create a temporary directory.
	certDir := t.TempDir()

	certs := tlstest.CreateClientServerCertPairs(certDir)
	cert, err := tls.LoadX509KeyPair(certs.ServerCert, certs.ServerKey)
	if err != nil {
		return nil, err
	}

	clientCredentials, err := credentials.NewClientTLSFromFile(certs.ServerCA, certs.ServerName)
	if err != nil {
		return nil, err
	}
	tc := &testCredentials{
		client: clientCredentials,
		server: credentials.NewServerTLSFromCert(&cert),
	}
	return tc, nil
}

func TestOptionalTLS(t *testing.T) {
	testCtx, testCancel := context.WithCancel(context.Background())
	defer testCancel()

	tc, err := createCredentials(t)
	if err != nil {
		t.Fatalf("failed to create credentials %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen %v", err)
	}
	defer lis.Close()
	addr := lis.Addr().String()

	srv := createUnstartedServer(New(tc.server))
	go func() {
		srv.Serve(lis)
	}()
	defer srv.Stop()

	testFunc := func(t *testing.T, dialOpt grpc.DialOption) {
		ctx, cancel := context.WithTimeout(testCtx, 5*time.Second)
		defer cancel()
		conn, err := grpc.DialContext(ctx, addr, dialOpt)
		if err != nil {
			t.Fatalf("failed to connect to the server %v", err)
		}
		defer conn.Close()
		c := pb.NewGreeterClient(conn)
		resp, err := c.SayHello(ctx, &pb.HelloRequest{Name: "Vittes"})
		if err != nil {
			t.Fatalf("could not greet: %v", err)
		}
		if resp.Message != "Hello Vittes" {
			t.Fatalf("unexpected reply %s", resp.Message)
		}
	}

	t.Run("Plain2TLS", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			testFunc(t, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}
	})
	t.Run("TLS2TLS", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			testFunc(t, grpc.WithTransportCredentials(tc.client))
		}
	})
}
