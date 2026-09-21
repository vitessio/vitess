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
	"crypto/tls"
	"net"

	"google.golang.org/grpc/credentials"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	optionalTLSCreds struct {
		credentials.TransportCredentials
		// requireClientCert is set when the TLS configuration requires the
		// client to present a certificate. A plain-text connection can
		// never present one, so it is refused rather than served without
		// the authentication the server was configured to require.
		requireClientCert bool
	}

	info struct {
		credentials.CommonAuthInfo
	}
)

func (c *optionalTLSCreds) Clone() credentials.TransportCredentials {
	return &optionalTLSCreds{
		TransportCredentials: c.TransportCredentials.Clone(),
		requireClientCert:    c.requireClientCert,
	}
}

func (c *optionalTLSCreds) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	isTLS, bytes, err := DetectTLS(conn)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	wc := NewWrappedConn(conn, bytes)
	if isTLS {
		return c.TransportCredentials.ServerHandshake(wc)
	}

	if c.requireClientCert {
		conn.Close()
		return nil, nil, vterrors.New(vtrpcpb.Code_UNAUTHENTICATED, "plain-text connection refused: the server requires a client certificate, which only a TLS connection can present")
	}

	authInfo := info{
		SecurityLevel: credentials.NoSecurity,
	}

	return wc, authInfo, nil
}

// New returns server credentials for config that accept plain-text
// connections as well as TLS ones, unless config requires the client to
// present a certificate, see RequiresClientCert: a plain-text connection
// cannot present one, so those are then refused and only TLS is served.
func New(config *tls.Config) credentials.TransportCredentials {
	return &optionalTLSCreds{
		TransportCredentials: credentials.NewTLS(config),
		requireClientCert:    RequiresClientCert(config),
	}
}

// RequiresClientCert reports whether config requires the client to present a
// certificate, going by its ClientAuth. Only the policies known not to
// require one say no, so that one this package does not know is taken to
// require it. So is a config with a GetConfigForClient callback: the callback
// can hand a TLS client a configuration that requires a certificate whatever
// this one says, and it never sees a plain-text connection, so what such a
// server requires cannot be told from config.
func RequiresClientCert(config *tls.Config) bool {
	if config == nil {
		return false
	}
	if config.GetConfigForClient != nil {
		return true
	}
	switch config.ClientAuth {
	case tls.NoClientCert, tls.RequestClientCert, tls.VerifyClientCertIfGiven:
		return false
	default:
		return true
	}
}

func (info) AuthType() string {
	return "insecure"
}
