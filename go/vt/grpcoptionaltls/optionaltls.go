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
	"net"
	"sync"

	"google.golang.org/grpc/credentials"

	"vitess.io/vitess/go/stats"
)

const (
	transportTLS       = "tls"
	transportPlaintext = "plaintext"
)

// Optional TLS serves plain-text connections unauthenticated so that clients
// can move to TLS one at a time. These stats, by transport, are the evidence
// an operator has for whether every client has moved, before dropping the
// flag: OpenConnections at zero for plaintext means no plain-text client is
// connected right now, which matters because gRPC connections are long-lived
// and a client that connected long ago does not handshake again;
// ConnectionCounts no longer increasing for plaintext means none has
// connected lately. Neither shows a client that is offline or connects only
// now and then, so they support the decision rather than prove it.
var (
	// ConnectionCounts counts the connections handshaken, by transport.
	ConnectionCounts = stats.NewCountersWithSingleLabel(
		"GrpcOptionalTlsConnections",
		"Connections handshaken by a gRPC server with optional TLS, by transport (tls or plaintext). Plain-text connections are served unauthenticated.",
		"Transport",
		transportPlaintext, transportTLS,
	)
	// OpenConnections counts the connections currently open, by transport.
	OpenConnections = stats.NewGaugesWithSingleLabel(
		"GrpcOptionalTlsOpenConnections",
		"Connections currently open on a gRPC server with optional TLS, by transport (tls or plaintext). Optional TLS can be turned off once no plain-text connection is open and none is being made.",
		"Transport",
		transportPlaintext, transportTLS,
	)
)

type (
	optionalTLSCreds struct {
		credentials.TransportCredentials
	}

	// countedConn is a handshaken connection that is counted as open until
	// it is closed, once, however many times Close is called.
	countedConn struct {
		net.Conn
		transport string
		closeOnce sync.Once
	}
)

func newCountedConn(conn net.Conn, transport string) net.Conn {
	ConnectionCounts.Add(transport, 1)
	OpenConnections.Add(transport, 1)
	return &countedConn{Conn: conn, transport: transport}
}

func (c *countedConn) Close() error {
	c.closeOnce.Do(func() {
		OpenConnections.Add(c.transport, -1)
	})
	return c.Conn.Close()
}

func (c *optionalTLSCreds) Clone() credentials.TransportCredentials {
	return New(c.TransportCredentials.Clone())
}

func (c *optionalTLSCreds) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	isTLS, bytes, err := DetectTLS(conn)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	wc := NewWrappedConn(conn, bytes)
	if isTLS {
		tlsConn, authInfo, err := c.TransportCredentials.ServerHandshake(wc)
		if err != nil {
			return nil, nil, err
		}
		return newCountedConn(tlsConn, transportTLS), authInfo, nil
	}

	authInfo := info{
		SecurityLevel: credentials.NoSecurity,
	}

	return newCountedConn(wc, transportPlaintext), authInfo, nil
}

func New(tc credentials.TransportCredentials) credentials.TransportCredentials {
	return &optionalTLSCreds{TransportCredentials: tc}
}

type info struct {
	credentials.CommonAuthInfo
}

func (info) AuthType() string {
	return "insecure"
}
