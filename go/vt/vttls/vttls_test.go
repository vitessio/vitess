/*
Copyright 2026 The Vitess Authors.

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

package vttls

import (
	"crypto/rand"
	"crypto/tls"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/tlstest"
)

type handshakeResult struct {
	clientErr   error
	serverErr   error
	clientState tls.ConnectionState
}

// handshake completes one TLS connection between a server using
// serverConfig and a client using clientConfig over a loopback
// listener and reports what each side saw.
func handshake(t *testing.T, serverConfig, clientConfig *tls.Config) handshakeResult {
	t.Helper()

	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverConfig)
	require.NoError(t, err)
	defer ln.Close()

	serverErr := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		if err := conn.(*tls.Conn).Handshake(); err != nil {
			serverErr <- err
			return
		}
		// A TLS 1.3 server hands out the session ticket after the
		// handshake, and the client only processes it while reading.
		_, err = conn.Write([]byte("ok"))
		serverErr <- err
	}()

	var res handshakeResult
	conn, err := tls.Dial("tcp", ln.Addr().String(), clientConfig)
	if err == nil {
		_, err = io.ReadAll(conn)
		res.clientState = conn.ConnectionState()
		conn.Close()
	}
	res.clientErr = err
	res.serverErr = <-serverErr
	return res
}

func TestClientConfigCRL(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())

	// The servers present the intermediate CA that signed both their
	// certificates and the CRL, as a real server chain does.
	revokedServer, err := ServerConfig(certs.RevokedServerCert, certs.RevokedServerKey, "", "", certs.ServerCA, tls.VersionTLS12)
	require.NoError(t, err)
	validServer, err := ServerConfig(certs.ServerCert, certs.ServerKey, "", "", certs.ServerCA, tls.VersionTLS12)
	require.NoError(t, err)

	for _, mode := range []SslMode{Preferred, Required, VerifyCA, VerifyIdentity} {
		t.Run(string(mode), func(t *testing.T) {
			t.Run("a revoked server certificate is rejected", func(t *testing.T) {
				clientConfig, err := ClientConfig(mode, "", "", certs.ServerCA, certs.ServerCRL, certs.RevokedServerName, tls.VersionTLS12)
				require.NoError(t, err)

				res := handshake(t, revokedServer, clientConfig)
				require.ErrorContains(t, res.clientErr, "Certificate revoked: CommonName="+certs.RevokedServerName)
			})
			t.Run("a server certificate that is not revoked is accepted", func(t *testing.T) {
				clientConfig, err := ClientConfig(mode, "", "", certs.ServerCA, certs.ServerCRL, certs.ServerName, tls.VersionTLS12)
				require.NoError(t, err)

				res := handshake(t, validServer, clientConfig)
				require.NoError(t, res.clientErr)
				require.NoError(t, res.serverErr)
			})
		})
	}

	t.Run("without a configured CA, the issuer presented by the server binds the CRL", func(t *testing.T) {
		clientConfig, err := ClientConfig(Required, "", "", "", certs.ServerCRL, certs.RevokedServerName, tls.VersionTLS12)
		require.NoError(t, err)

		res := handshake(t, revokedServer, clientConfig)
		require.ErrorContains(t, res.clientErr, "Certificate revoked: CommonName="+certs.RevokedServerName)
	})
}

func TestServerConfigCRL(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())

	newServerConfig := func(t *testing.T, crl string) *tls.Config {
		t.Helper()
		serverConfig, err := ServerConfig(certs.ServerCert, certs.ServerKey, certs.ClientCA, crl, certs.ServerCA, tls.VersionTLS12)
		require.NoError(t, err)
		return serverConfig
	}
	newClientConfig := func(t *testing.T, cert, key string) *tls.Config {
		t.Helper()
		clientConfig, err := ClientConfig(VerifyIdentity, cert, key, certs.ServerCA, "", certs.ServerName, tls.VersionTLS12)
		require.NoError(t, err)
		return clientConfig
	}

	t.Run("a revoked client certificate is rejected", func(t *testing.T) {
		res := handshake(t, newServerConfig(t, certs.ClientCRL), newClientConfig(t, certs.RevokedClientCert, certs.RevokedClientKey))
		require.ErrorContains(t, res.serverErr, "Certificate revoked: CommonName="+certs.RevokedClientName)
	})

	t.Run("a client certificate that is not revoked is accepted", func(t *testing.T) {
		res := handshake(t, newServerConfig(t, certs.ClientCRL), newClientConfig(t, certs.ClientCert, certs.ClientKey))
		require.NoError(t, res.clientErr)
		require.NoError(t, res.serverErr)
	})

	t.Run("a revoked client certificate is rejected on a resumed session", func(t *testing.T) {
		// The session is established before the certificate is revoked
		// and then resumed against a server that has since loaded the
		// CRL. The two configurations share their session ticket keys,
		// which is what happens when a server reloads its TLS
		// configuration in place.
		var ticketKey [32]byte
		_, err := rand.Read(ticketKey[:])
		require.NoError(t, err)
		beforeRevocation := newServerConfig(t, "")
		beforeRevocation.SetSessionTicketKeys([][32]byte{ticketKey})
		afterRevocation := newServerConfig(t, certs.ClientCRL)
		afterRevocation.SetSessionTicketKeys([][32]byte{ticketKey})

		clientConfig := newClientConfig(t, certs.RevokedClientCert, certs.RevokedClientKey)
		clientConfig.ClientSessionCache = tls.NewLRUClientSessionCache(1)

		res := handshake(t, beforeRevocation, clientConfig)
		require.NoError(t, res.clientErr)
		require.NoError(t, res.serverErr)
		require.False(t, res.clientState.DidResume)

		res = handshake(t, beforeRevocation, clientConfig)
		require.NoError(t, res.clientErr)
		require.NoError(t, res.serverErr)
		require.True(t, res.clientState.DidResume, "the client must resume the session for this test to be meaningful")

		res = handshake(t, afterRevocation, clientConfig)
		require.ErrorContains(t, res.serverErr, "Certificate revoked: CommonName="+certs.RevokedClientName)
	})
}
