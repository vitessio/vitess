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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
	"google.golang.org/grpc/status"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/vttls"
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
	testCtx := t.Context()

	tc, err := createCredentials(t)
	require.NoError(t, err)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
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
		conn, err := grpc.DialContext(ctx, addr, dialOpt) //nolint:staticcheck
		require.NoError(t, err)
		defer conn.Close()
		c := pb.NewGreeterClient(conn)
		resp, err := c.SayHello(ctx, &pb.HelloRequest{Name: "Vittes"})
		require.NoError(t, err)
		require.Equalf(t, "Hello Vittes", resp.Message, "unexpected reply %s", resp.Message)
	}

	t.Run("Plain2TLS", func(t *testing.T) {
		before := ConnectionCounts.Counts()
		for range 5 {
			testFunc(t, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}
		after := ConnectionCounts.Counts()
		require.Equal(t, int64(5), after[transportPlaintext]-before[transportPlaintext], "plain-text connections counted")
		require.Equal(t, before[transportTLS], after[transportTLS], "no TLS connection counted")
	})
	t.Run("TLS2TLS", func(t *testing.T) {
		before := ConnectionCounts.Counts()
		for range 5 {
			testFunc(t, grpc.WithTransportCredentials(tc.client))
		}
		after := ConnectionCounts.Counts()
		require.Equal(t, int64(5), after[transportTLS]-before[transportTLS], "TLS connections counted")
		require.Equal(t, before[transportPlaintext], after[transportPlaintext], "no plain-text connection counted")
	})
}

// TestOptionalTLSClientCA pins down what optional TLS means for a server that
// verifies client certificates against a CA, as one started with --grpc-ca is:
// plain-text connections are still served, unauthenticated, and the TLS
// connections are held to the certificate check. That is what lets clients move
// to TLS one at a time, with the certificate check validated on the ones that
// have moved, before optional TLS is turned off.
func TestOptionalTLSClientCA(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	config, err := vttls.ServerConfig(certs.ServerCert, certs.ServerKey, certs.ClientCA, "", certs.ServerCA, tls.VersionTLS12)
	require.NoError(t, err)
	require.Equal(t, tls.RequireAndVerifyClientCert, config.ClientAuth)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { lis.Close() })
	srv := createUnstartedServer(New(credentials.NewTLS(config)))
	go func() {
		srv.Serve(lis)
	}()
	t.Cleanup(srv.Stop)

	sayHello := func(t *testing.T, creds credentials.TransportCredentials) error {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(creds))
		require.NoError(t, err)
		defer conn.Close()
		_, err = pb.NewGreeterClient(conn).SayHello(ctx, &pb.HelloRequest{Name: "Vitess"})
		return err
	}

	t.Run("a plain-text connection is served", func(t *testing.T) {
		require.NoError(t, sayHello(t, insecure.NewCredentials()))
	})

	t.Run("a TLS connection without a client certificate is refused", func(t *testing.T) {
		creds, err := credentials.NewClientTLSFromFile(certs.ServerCA, certs.ServerName)
		require.NoError(t, err)
		// In TLS 1.3 the server only rejects the client after the client has
		// finished its side of the handshake, so the client sees either the
		// server's "certificate required" alert or its own write failing on
		// the closed connection.
		err = sayHello(t, creds)
		require.Error(t, err)
		require.Equal(t, codes.Unavailable, status.Code(err))
	})

	t.Run("a TLS connection with a client certificate is served", func(t *testing.T) {
		clientConfig, err := vttls.ClientConfig(vttls.VerifyIdentity, certs.ClientCert, certs.ClientKey, certs.ServerCA, "", certs.ServerName, tls.VersionTLS12)
		require.NoError(t, err)
		require.NoError(t, sayHello(t, credentials.NewTLS(clientConfig)))
	})
}

// TestOptionalTLSOpenConnections checks that the connections a server has open
// are counted by transport for as long as they are open: gRPC connections are
// long-lived, so a plain-text client that connected long ago shows up here,
// where the handshake count would not move for it again.
func TestOptionalTLSOpenConnections(t *testing.T) {
	tc, err := createCredentials(t)
	require.NoError(t, err)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { lis.Close() })
	srv := createUnstartedServer(New(tc.server))
	go func() {
		srv.Serve(lis)
	}()
	t.Cleanup(srv.Stop)

	for transport, creds := range map[string]credentials.TransportCredentials{
		transportPlaintext: insecure.NewCredentials(),
		transportTLS:       tc.client,
	} {
		t.Run(transport, func(t *testing.T) {
			before := OpenConnections.Counts()[transport]
			conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(creds))
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			_, err = pb.NewGreeterClient(conn).SayHello(ctx, &pb.HelloRequest{Name: "Vitess"})
			require.NoError(t, err)
			require.Equal(t, before+1, OpenConnections.Counts()[transport], "the connection is counted while open")

			require.NoError(t, conn.Close())
			// The server notices the close on its own time.
			require.Eventually(t, func() bool {
				return OpenConnections.Counts()[transport] == before
			}, 30*time.Second, 10*time.Millisecond, "the connection is no longer counted once closed")
		})
	}
}
