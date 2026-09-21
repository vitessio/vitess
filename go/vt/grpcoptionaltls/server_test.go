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
	"crypto/x509"
	"errors"
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
	server *tls.Config
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
		server: &tls.Config{Certificates: []tls.Certificate{cert}},
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
		for range 5 {
			testFunc(t, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}
	})
	t.Run("TLS2TLS", func(t *testing.T) {
		for range 5 {
			testFunc(t, grpc.WithTransportCredentials(tc.client))
		}
	})
}

// TestOptionalTLSRequiredClientCert checks that a server that requires client
// certificates, as one configured with --grpc-ca does, refuses the plain-text
// connections that optional TLS otherwise accepts: a plain-text connection can
// never present a certificate, so serving it would leave the requirement
// enforced only for the clients that choose to use TLS.
func TestOptionalTLSRequiredClientCert(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	// The server is configured the way servenv configures it for --grpc-cert,
	// --grpc-key and --grpc-ca.
	config, err := vttls.ServerConfig(certs.ServerCert, certs.ServerKey, certs.ClientCA, "", certs.ServerCA, tls.VersionTLS12)
	require.NoError(t, err)
	require.Equal(t, tls.RequireAndVerifyClientCert, config.ClientAuth)

	// A copy of the credentials has to refuse them as well.
	for name, serverCreds := range map[string]credentials.TransportCredentials{
		"credentials": New(config),
		"cloned":      New(config).Clone(),
	} {
		t.Run(name, func(t *testing.T) {
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer lis.Close()
			srv := createUnstartedServer(serverCreds)
			go func() {
				srv.Serve(lis)
			}()
			defer srv.Stop()

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

			t.Run("a plain-text connection is refused", func(t *testing.T) {
				err := sayHello(t, insecure.NewCredentials())
				require.Error(t, err, "a client that presents no certificate at all must not be served")
				require.Equal(t, codes.Unavailable, status.Code(err))
			})

			t.Run("a TLS connection without a client certificate is refused", func(t *testing.T) {
				creds, err := credentials.NewClientTLSFromFile(certs.ServerCA, certs.ServerName)
				require.NoError(t, err)
				// In TLS 1.3 the server only rejects the client after the
				// client has finished its side of the handshake, so the
				// client sees either the server's "certificate required"
				// alert or its own write failing on the closed connection.
				err = sayHello(t, creds)
				require.Error(t, err)
				require.Equal(t, codes.Unavailable, status.Code(err))
			})

			t.Run("a TLS connection with a client certificate is served", func(t *testing.T) {
				clientConfig, err := vttls.ClientConfig(vttls.VerifyIdentity, certs.ClientCert, certs.ClientKey, certs.ServerCA, "", certs.ServerName, tls.VersionTLS12)
				require.NoError(t, err)
				require.NoError(t, sayHello(t, credentials.NewTLS(clientConfig)))
			})
		})
	}
}

// TestOptionalTLSConfigForClient checks that a TLS configuration that picks the
// configuration to use per TLS client, with a GetConfigForClient callback, has
// plain-text connections refused: the callback can hand a TLS client a
// configuration that requires a certificate whatever the base one says, and it
// never sees a plain-text connection, so what such a server requires cannot be
// told from the base configuration.
func TestOptionalTLSConfigForClient(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	requiringClientCert, err := vttls.ServerConfig(certs.ServerCert, certs.ServerKey, certs.ClientCA, "", certs.ServerCA, tls.VersionTLS12)
	require.NoError(t, err)
	// The base configuration requires no client certificate; the one the
	// callback returns does.
	config, err := vttls.ServerConfig(certs.ServerCert, certs.ServerKey, "", "", certs.ServerCA, tls.VersionTLS12)
	require.NoError(t, err)
	require.Equal(t, tls.NoClientCert, config.ClientAuth)
	config.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		return requiringClientCert, nil
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()
	srv := createUnstartedServer(New(config))
	go func() {
		srv.Serve(lis)
	}()
	defer srv.Stop()

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

	t.Run("a plain-text connection is refused", func(t *testing.T) {
		err := sayHello(t, insecure.NewCredentials())
		require.Error(t, err, "a client that presents no certificate at all must not be served")
		require.Equal(t, codes.Unavailable, status.Code(err))
	})

	t.Run("a TLS connection with a client certificate is served", func(t *testing.T) {
		clientConfig, err := vttls.ClientConfig(vttls.VerifyIdentity, certs.ClientCert, certs.ClientKey, certs.ServerCA, "", certs.ServerName, tls.VersionTLS12)
		require.NoError(t, err)
		require.NoError(t, sayHello(t, credentials.NewTLS(clientConfig)))
	})
}

// TestOptionalTLSVerificationCallback checks that plain-text connections are
// refused when the client authentication policy leaves the certificate to the
// client and a verification callback is what requires it: Go runs the callback
// for a TLS client that presented no certificate, which it can then refuse, and
// it never runs for a plain-text connection.
func TestOptionalTLSVerificationCallback(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	errNoClientCert := errors.New("a client certificate is required")

	for name, setCallback := range map[string]func(*tls.Config){
		"VerifyConnection": func(config *tls.Config) {
			config.VerifyConnection = func(cs tls.ConnectionState) error {
				if len(cs.PeerCertificates) == 0 {
					return errNoClientCert
				}
				return nil
			}
		},
		"VerifyPeerCertificate": func(config *tls.Config) {
			config.VerifyPeerCertificate = func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
				if len(rawCerts) == 0 {
					return errNoClientCert
				}
				return nil
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			config, err := vttls.ServerConfig(certs.ServerCert, certs.ServerKey, certs.ClientCA, "", certs.ServerCA, tls.VersionTLS12)
			require.NoError(t, err)
			// The policy leaves the certificate to the client; the callback
			// is what requires it.
			config.ClientAuth = tls.VerifyClientCertIfGiven
			setCallback(config)

			lis, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer lis.Close()
			srv := createUnstartedServer(New(config))
			go func() {
				srv.Serve(lis)
			}()
			defer srv.Stop()

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

			t.Run("a plain-text connection is refused", func(t *testing.T) {
				err := sayHello(t, insecure.NewCredentials())
				require.Error(t, err, "a client that presents no certificate at all must not be served")
				require.Equal(t, codes.Unavailable, status.Code(err))
			})

			t.Run("a TLS connection without a client certificate is refused", func(t *testing.T) {
				creds, err := credentials.NewClientTLSFromFile(certs.ServerCA, certs.ServerName)
				require.NoError(t, err)
				err = sayHello(t, creds)
				require.Error(t, err)
				require.Equal(t, codes.Unavailable, status.Code(err))
			})

			t.Run("a TLS connection with a client certificate is served", func(t *testing.T) {
				clientConfig, err := vttls.ClientConfig(vttls.VerifyIdentity, certs.ClientCert, certs.ClientKey, certs.ServerCA, "", certs.ServerName, tls.VersionTLS12)
				require.NoError(t, err)
				require.NoError(t, sayHello(t, credentials.NewTLS(clientConfig)))
			})
		})
	}
}

// TestRequiresClientCert checks which client authentication policies make the
// optional TLS credentials refuse plain-text connections: the ones that
// require a certificate, and any policy this package does not know.
func TestRequiresClientCert(t *testing.T) {
	for clientAuth, want := range map[tls.ClientAuthType]bool{
		tls.NoClientCert:               false,
		tls.RequestClientCert:          false,
		tls.VerifyClientCertIfGiven:    false,
		tls.RequireAnyClientCert:       true,
		tls.RequireAndVerifyClientCert: true,
		tls.ClientAuthType(99):         true,
	} {
		t.Run(clientAuth.String(), func(t *testing.T) {
			require.Equal(t, want, RequiresClientCert(&tls.Config{ClientAuth: clientAuth}))
		})
	}
	require.False(t, RequiresClientCert(nil))

	t.Run("a GetConfigForClient callback", func(t *testing.T) {
		require.True(t, RequiresClientCert(&tls.Config{
			ClientAuth:         tls.NoClientCert,
			GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) { return nil, nil },
		}))
	})

	// A verification callback can refuse a client that presented no
	// certificate, but only where one is requested: with NoClientCert no
	// client can present one, so no callback can be what requires it.
	verifyConnection := func(tls.ConnectionState) error { return nil }
	verifyPeerCertificate := func([][]byte, [][]*x509.Certificate) error { return nil }
	for clientAuth, want := range map[tls.ClientAuthType]bool{
		tls.NoClientCert:            false,
		tls.RequestClientCert:       true,
		tls.VerifyClientCertIfGiven: true,
	} {
		t.Run("a VerifyConnection callback with "+clientAuth.String(), func(t *testing.T) {
			require.Equal(t, want, RequiresClientCert(&tls.Config{ClientAuth: clientAuth, VerifyConnection: verifyConnection}))
		})
		t.Run("a VerifyPeerCertificate callback with "+clientAuth.String(), func(t *testing.T) {
			require.Equal(t, want, RequiresClientCert(&tls.Config{ClientAuth: clientAuth, VerifyPeerCertificate: verifyPeerCertificate}))
		})
	}
}
