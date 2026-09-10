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
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
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
	// Generous, so that a stalled side fails the test rather than
	// hanging the package until the go test timeout.
	const timeout = 30 * time.Second

	ln, err := tls.Listen("tcp", "127.0.0.1:0", serverConfig)
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })

	serverErr := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
			serverErr <- err
			return
		}
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
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", ln.Addr().String(), clientConfig)
	if err == nil {
		if err = conn.SetDeadline(time.Now().Add(timeout)); err == nil {
			_, err = io.ReadAll(conn)
		}
		res.clientState = conn.ConnectionState()
		conn.Close()
	}
	res.clientErr = err
	require.Eventually(t, func() bool {
		select {
		case res.serverErr = <-serverErr:
			return true
		default:
			return false
		}
	}, timeout, 10*time.Millisecond, "the server side did not finish the handshake in time")
	return res
}

// tlsVersions are the protocol versions the handshakes that depend on
// the version's mechanics, such as session resumption, are run with.
var tlsVersions = []uint16{tls.VersionTLS12, tls.VersionTLS13}

// recordResumption wraps config's VerifyConnection so that the test
// can tell whether the handshake that the callback rejected was a
// resumed one, which the connection state of a failed handshake does
// not reveal.
func recordResumption(config *tls.Config) *bool {
	var resumed bool
	verify := config.VerifyConnection
	config.VerifyConnection = func(cs tls.ConnectionState) error {
		resumed = cs.DidResume
		return verify(cs)
	}
	return &resumed
}

// TestClientConfigCRL checks the revocation of server certificates by
// a client configured with a CRL, through real TLS handshakes: in
// every SSL mode, on resumed sessions, and against the chain that the
// client builds for the server itself in the modes where Go verifies
// none.
func TestClientConfigCRL(t *testing.T) {
	root := t.TempDir()
	certs := tlstest.CreateClientServerCertPairs(root)

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

	// In the modes where Go verifies nothing, the chain the CRL is
	// held against is built to the configured CA, or to the system
	// roots without one, which the test CA is not among.
	for _, mode := range []SslMode{Preferred, Required} {
		t.Run(fmt.Sprintf("in %s mode, a server whose chain cannot be built to a trusted CA is rejected when a CRL is configured", mode), func(t *testing.T) {
			clientConfig, err := ClientConfig(mode, "", "", "", certs.ServerCRL, certs.ServerName, tls.VersionTLS12)
			require.NoError(t, err)

			res := handshake(t, validServer, clientConfig)
			require.ErrorContains(t, res.clientErr, "cannot check the revocation of the peer's certificates against the configured CRL, since no chain to a trusted CA could be built")
		})
		t.Run(fmt.Sprintf("in %s mode, a server whose chain cannot be built to a trusted CA is accepted when no CRL is configured", mode), func(t *testing.T) {
			clientConfig, err := ClientConfig(mode, "", "", "", "", certs.ServerName, tls.VersionTLS12)
			require.NoError(t, err)

			res := handshake(t, validServer, clientConfig)
			require.NoError(t, res.clientErr)
			require.NoError(t, res.serverErr)
		})
	}

	// These servers present their certificate alone, without the
	// intermediate CA that issued it.
	leafOnlyRevokedServer, err := ServerConfig(certs.RevokedServerCert, certs.RevokedServerKey, "", "", "", tls.VersionTLS12)
	require.NoError(t, err)
	leafOnlyValidServer, err := ServerConfig(certs.ServerCert, certs.ServerKey, "", "", "", tls.VersionTLS12)
	require.NoError(t, err)
	rootCA := path.Join(root, "ca-cert.pem")

	t.Run("a server that presents only its certificate is checked against the chain built to the configured CA", func(t *testing.T) {
		clientConfig, err := ClientConfig(Required, "", "", certs.ServerCA, certs.ServerCRL, certs.RevokedServerName, tls.VersionTLS12)
		require.NoError(t, err)
		res := handshake(t, leafOnlyRevokedServer, clientConfig)
		require.ErrorContains(t, res.clientErr, "Certificate revoked: CommonName="+certs.RevokedServerName)

		clientConfig, err = ClientConfig(Required, "", "", certs.ServerCA, certs.ServerCRL, certs.ServerName, tls.VersionTLS12)
		require.NoError(t, err)
		res = handshake(t, leafOnlyValidServer, clientConfig)
		require.NoError(t, res.clientErr)
		require.NoError(t, res.serverErr)
	})

	t.Run("a server that presents only its certificate is rejected when the configured CA is not its issuer", func(t *testing.T) {
		// The client trusts the root, and the server leaves out the
		// intermediate that would connect its certificate to it.
		clientConfig, err := ClientConfig(Required, "", "", rootCA, certs.ServerCRL, certs.ServerName, tls.VersionTLS12)
		require.NoError(t, err)

		res := handshake(t, leafOnlyValidServer, clientConfig)
		require.ErrorContains(t, res.clientErr, "no chain to a trusted CA could be built")
	})

	t.Run("a server certificate is accepted when no CRL is configured for its issuer", func(t *testing.T) {
		// The only CRL configured comes from the clients' CA, which
		// has nothing to say about the server's certificate.
		clientConfig, err := ClientConfig(Required, "", "", certs.ServerCA, certs.ClientCRL, certs.ServerName, tls.VersionTLS12)
		require.NoError(t, err)

		res := handshake(t, leafOnlyValidServer, clientConfig)
		require.NoError(t, res.clientErr)
		require.NoError(t, res.serverErr)
	})

	for _, mode := range []SslMode{Preferred, Required, VerifyCA, VerifyIdentity} {
		for _, version := range tlsVersions {
			t.Run(fmt.Sprintf("a revoked server certificate is rejected on a resumed %s session in %s mode", tls.VersionName(version), mode), func(t *testing.T) {
				// The session is established before the client has
				// the CRL and resumed once it does. The two client
				// configurations share the session cache, which is
				// keyed by server name.
				server := revokedServer.Clone()
				server.MaxVersion = version
				sessionCache := tls.NewLRUClientSessionCache(1)
				beforeRevocation, err := ClientConfig(mode, "", "", certs.ServerCA, "", certs.RevokedServerName, tls.VersionTLS12)
				require.NoError(t, err)
				beforeRevocation.MaxVersion = version
				beforeRevocation.ClientSessionCache = sessionCache
				afterRevocation, err := ClientConfig(mode, "", "", certs.ServerCA, certs.ServerCRL, certs.RevokedServerName, tls.VersionTLS12)
				require.NoError(t, err)
				afterRevocation.MaxVersion = version
				afterRevocation.ClientSessionCache = sessionCache
				resumed := recordResumption(afterRevocation)

				res := handshake(t, server, beforeRevocation)
				require.NoError(t, res.clientErr)
				require.NoError(t, res.serverErr)
				require.False(t, res.clientState.DidResume)
				require.Equal(t, version, res.clientState.Version)

				res = handshake(t, server, beforeRevocation)
				require.NoError(t, res.clientErr)
				require.NoError(t, res.serverErr)
				require.True(t, res.clientState.DidResume, "the client must resume the session for this test to be meaningful")

				res = handshake(t, server, afterRevocation)
				require.ErrorContains(t, res.clientErr, "Certificate revoked: CommonName="+certs.RevokedServerName)
				require.True(t, *resumed, "the rejected handshake must be a resumed one")
			})
		}
	}

	leaf := loadOneCert(t, certs.ServerCert)
	intermediate := loadOneCert(t, certs.ServerCA)
	keyPair, err := tls.LoadX509KeyPair(certs.ServerCert, certs.ServerKey)
	require.NoError(t, err)
	serverPresenting := func(chain ...*x509.Certificate) *tls.Config {
		certificate := tls.Certificate{PrivateKey: keyPair.PrivateKey}
		for _, cert := range chain {
			certificate.Certificate = append(certificate.Certificate, cert.Raw)
		}
		return &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	}

	t.Run("verify_ca hands the chain it builds to the check, so certificates beyond it are not inspected", func(t *testing.T) {
		// The server presents, after its own chain, a certificate
		// that the client's CRLs revoke along with that certificate's
		// issuer. Neither takes part in verifying the server.
		clientConfig, err := ClientConfig(VerifyCA, "", "", certs.ServerCA, certs.CombinedCRL, certs.ServerName, tls.VersionTLS12)
		require.NoError(t, err)

		res := handshake(t, serverPresenting(leaf, intermediate, loadOneCert(t, certs.RevokedClientCert), loadOneCert(t, certs.ClientCA)), clientConfig)
		require.NoError(t, res.clientErr)
		require.NoError(t, res.serverErr)
	})

	// Revoke the intermediate CA that issued the server certificate,
	// under the root CA.
	intermediateName := strings.TrimSuffix(filepath.Base(certs.ServerCA), "-cert.pem")
	tlstest.RevokeCertAndRegenerateCRL(root, tlstest.CA, intermediateName)
	rootCRL := path.Join(root, "ca-crl.pem")

	for _, mode := range []SslMode{Required, VerifyCA, VerifyIdentity} {
		t.Run(fmt.Sprintf("in %s mode, a revoked intermediate in the chain to the configured root is rejected", mode), func(t *testing.T) {
			clientConfig, err := ClientConfig(mode, "", "", rootCA, rootCRL, certs.ServerName, tls.VersionTLS12)
			require.NoError(t, err)

			res := handshake(t, validServer, clientConfig)
			require.ErrorContains(t, res.clientErr, "Certificate revoked: CommonName="+intermediate.Subject.CommonName)
		})
	}

	t.Run("an intermediate configured along with the root that revokes it is rejected as a trust anchor", func(t *testing.T) {
		// The CA file holds the intermediate along with the root, so
		// a chain may end at the intermediate, a trust anchor that
		// no chain checks. The root is configured too, and its CRL
		// lists the intermediate, so no chain may end there: the
		// server is rejected whether or not it presents the
		// intermediate.
		intermediatePEM, err := os.ReadFile(certs.ServerCA)
		require.NoError(t, err)
		rootPEM, err := os.ReadFile(rootCA)
		require.NoError(t, err)
		bundle := path.Join(t.TempDir(), "bundle.pem")
		require.NoError(t, os.WriteFile(bundle, append(intermediatePEM, rootPEM...), 0o600))
		clientConfig, err := ClientConfig(VerifyCA, "", "", bundle, rootCRL, certs.ServerName, tls.VersionTLS12)
		require.NoError(t, err)

		for name, server := range map[string]*tls.Config{"presenting the intermediate": validServer, "presenting its certificate alone": leafOnlyValidServer} {
			res := handshake(t, server, clientConfig)
			require.ErrorContains(t, res.clientErr, "Certificate revoked: CommonName="+intermediate.Subject.CommonName, name)
		}
	})

	t.Run("an intermediate configured without the root that revokes it is a trust anchor that is not checked", func(t *testing.T) {
		clientConfig, err := ClientConfig(VerifyCA, "", "", certs.ServerCA, rootCRL, certs.ServerName, tls.VersionTLS12)
		require.NoError(t, err)

		res := handshake(t, validServer, clientConfig)
		require.NoError(t, res.clientErr)
		require.NoError(t, res.serverErr)
	})
}

// TestPeerChainRoots checks what the modes that build the peer's
// chain themselves verify it against: the configured CA when there
// is one, and the system roots otherwise, resolved once when the
// configuration is built rather than on every handshake.
func TestPeerChainRoots(t *testing.T) {
	configured := x509.NewCertPool()
	roots, err := peerChainRoots(configured)
	require.NoError(t, err)
	require.Same(t, configured, roots)

	system, err := peerChainRoots(nil)
	require.NoError(t, err)
	require.NotNil(t, system)
}

// loadOneCert returns the single certificate in the PEM file.
func loadOneCert(t *testing.T, file string) *x509.Certificate {
	t.Helper()
	loaded, err := loadx509Certificates(file)
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	return loaded[0]
}

// TestServerConfigCRL checks the revocation of client certificates by
// a server configured with a CRL, through real TLS handshakes: on
// full and resumed handshakes, and with certificates presented beyond
// the verified chain.
func TestServerConfigCRL(t *testing.T) {
	root := t.TempDir()
	certs := tlstest.CreateClientServerCertPairs(root)

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

	t.Run("a CRL without a CA is refused, since no client certificate would be requested", func(t *testing.T) {
		_, err := ServerConfig(certs.ServerCert, certs.ServerKey, "", certs.ClientCRL, certs.ServerCA, tls.VersionTLS12)
		require.ErrorContains(t, err, "a CRL is configured without a CA")
	})

	t.Run("a revoked client certificate is rejected", func(t *testing.T) {
		res := handshake(t, newServerConfig(t, certs.ClientCRL), newClientConfig(t, certs.RevokedClientCert, certs.RevokedClientKey))
		require.ErrorContains(t, res.serverErr, "Certificate revoked: CommonName="+certs.RevokedClientName)
	})

	t.Run("a client certificate that is not revoked is accepted", func(t *testing.T) {
		res := handshake(t, newServerConfig(t, certs.ClientCRL), newClientConfig(t, certs.ClientCert, certs.ClientKey))
		require.NoError(t, res.clientErr)
		require.NoError(t, res.serverErr)
	})

	for _, version := range tlsVersions {
		t.Run(fmt.Sprintf("a revoked client certificate is rejected on a resumed %s session", tls.VersionName(version)), func(t *testing.T) {
			// The session is established before the certificate is
			// revoked and then resumed against a server that has
			// since loaded the CRL. The two configurations share
			// their session ticket keys, which is what happens when
			// a server reloads its TLS configuration in place.
			var ticketKey [32]byte
			_, err := rand.Read(ticketKey[:])
			require.NoError(t, err)
			beforeRevocation := newServerConfig(t, "")
			beforeRevocation.MaxVersion = version
			beforeRevocation.SetSessionTicketKeys([][32]byte{ticketKey})
			afterRevocation := newServerConfig(t, certs.ClientCRL)
			afterRevocation.MaxVersion = version
			afterRevocation.SetSessionTicketKeys([][32]byte{ticketKey})

			clientConfig := newClientConfig(t, certs.RevokedClientCert, certs.RevokedClientKey)
			clientConfig.ClientSessionCache = tls.NewLRUClientSessionCache(1)
			resumed := recordResumption(afterRevocation)

			res := handshake(t, beforeRevocation, clientConfig)
			require.NoError(t, res.clientErr)
			require.NoError(t, res.serverErr)
			require.False(t, res.clientState.DidResume)
			require.Equal(t, version, res.clientState.Version)

			res = handshake(t, beforeRevocation, clientConfig)
			require.NoError(t, res.clientErr)
			require.NoError(t, res.serverErr)
			require.True(t, res.clientState.DidResume, "the client must resume the session for this test to be meaningful")

			res = handshake(t, afterRevocation, clientConfig)
			require.ErrorContains(t, res.serverErr, "Certificate revoked: CommonName="+certs.RevokedClientName)
			require.True(t, *resumed, "the rejected handshake must be a resumed one")
		})
	}

	// Revoke the clients' CA under the root.
	clientCA := loadOneCert(t, certs.ClientCA)
	tlstest.RevokeCertAndRegenerateCRL(root, tlstest.CA, strings.TrimSuffix(filepath.Base(certs.ClientCA), "-cert.pem"))
	rootCA := path.Join(root, "ca-cert.pem")
	rootCRL := path.Join(root, "ca-crl.pem")
	clientPresentingItsCA := func(t *testing.T) *tls.Config {
		t.Helper()
		clientConfig := newClientConfig(t, certs.ClientCert, certs.ClientKey)
		clientConfig.Certificates = []tls.Certificate{{
			Certificate: [][]byte{loadOneCert(t, certs.ClientCert).Raw, clientCA.Raw},
			PrivateKey:  clientConfig.Certificates[0].PrivateKey,
		}}
		return clientConfig
	}

	t.Run("a revoked intermediate in the chain to the configured root is rejected", func(t *testing.T) {
		serverConfig, err := ServerConfig(certs.ServerCert, certs.ServerKey, rootCA, rootCRL, certs.ServerCA, tls.VersionTLS12)
		require.NoError(t, err)

		res := handshake(t, serverConfig, clientPresentingItsCA(t))
		require.ErrorContains(t, res.serverErr, "Certificate revoked: CommonName="+clientCA.Subject.CommonName)
	})

	t.Run("a configured intermediate that the configured root's CRL revokes ends no chain", func(t *testing.T) {
		// The server trusts the root and the clients' CA, so a
		// client's chain may end at either; the root's CRL revokes
		// the clients' CA, so a chain ending there is rejected too,
		// whether or not the client presents the CA.
		rootPEM, err := os.ReadFile(rootCA)
		require.NoError(t, err)
		clientCAPEM, err := os.ReadFile(certs.ClientCA)
		require.NoError(t, err)
		bundle := path.Join(t.TempDir(), "bundle.pem")
		require.NoError(t, os.WriteFile(bundle, append(clientCAPEM, rootPEM...), 0o600))
		serverConfig, err := ServerConfig(certs.ServerCert, certs.ServerKey, bundle, rootCRL, certs.ServerCA, tls.VersionTLS12)
		require.NoError(t, err)

		for name, client := range map[string]*tls.Config{"presenting the CA": clientPresentingItsCA(t), "presenting its certificate alone": newClientConfig(t, certs.ClientCert, certs.ClientKey)} {
			res := handshake(t, serverConfig, client)
			require.ErrorContains(t, res.serverErr, "Certificate revoked: CommonName="+clientCA.Subject.CommonName, name)
		}
	})

	t.Run("the trust anchor is not checked against the CRL of its own issuer", func(t *testing.T) {
		// The server trusts the clients' CA alone, so it is the
		// anchor of every client's chain, trusted as configured: the
		// root's CRL, which revokes it, is not held against it, even
		// when the client presents the CA.
		rootCRLPEM, err := os.ReadFile(rootCRL)
		require.NoError(t, err)
		clientCRLPEM, err := os.ReadFile(certs.ClientCRL)
		require.NoError(t, err)
		bothCRLs := path.Join(t.TempDir(), "both-crl.pem")
		require.NoError(t, os.WriteFile(bothCRLs, append(clientCRLPEM, rootCRLPEM...), 0o600))

		res := handshake(t, newServerConfig(t, bothCRLs), clientPresentingItsCA(t))
		require.NoError(t, res.serverErr)
		require.NoError(t, res.clientErr)
	})

	t.Run("certificates presented beyond the verified chain are ignored", func(t *testing.T) {
		// The client presents, after its own chain, a certificate
		// that the server's CRLs revoke along with that certificate's
		// issuer. Neither takes part in verifying the client, so
		// neither is inspected.
		serverConfig, err := ServerConfig(certs.ServerCert, certs.ServerKey, certs.ClientCA, certs.CombinedCRL, certs.ServerCA, tls.VersionTLS12)
		require.NoError(t, err)
		clientConfig := newClientConfig(t, certs.ClientCert, certs.ClientKey)
		clientConfig.Certificates = []tls.Certificate{{
			Certificate: [][]byte{
				loadOneCert(t, certs.ClientCert).Raw,
				loadOneCert(t, certs.RevokedServerCert).Raw,
				loadOneCert(t, certs.ServerCA).Raw,
			},
			PrivateKey: clientConfig.Certificates[0].PrivateKey,
		}}

		res := handshake(t, serverConfig, clientConfig)
		require.NoError(t, res.serverErr)
		require.NoError(t, res.clientErr)
	})
}

// TestCRLCheckerVerifiedChains checks that the certificates of a
// verified chain, but the trust anchor it ends at, are checked
// against the CRLs of their issuer, the next certificate of the
// chain, and that the certificates a peer presents are not checked
// on their own.
func TestCRLCheckerVerifiedChains(t *testing.T) {
	root := t.TempDir()
	certs := tlstest.CreateClientServerCertPairs(root)

	// Revoke the intermediate CA that issued the server certificate,
	// under the root CA that issued the intermediate.
	intermediateName := strings.TrimSuffix(filepath.Base(certs.ServerCA), "-cert.pem")
	tlstest.RevokeCertAndRegenerateCRL(root, tlstest.CA, intermediateName)
	rootCA := path.Join(root, "ca-cert.pem")
	rootCRL := path.Join(root, "ca-crl.pem")

	leaf := loadOneCert(t, certs.ServerCert)
	intermediate := loadOneCert(t, certs.ServerCA)
	rootCert := loadOneCert(t, rootCA)

	checker, err := newCRLChecker(rootCRL, rootCA)
	require.NoError(t, err)

	t.Run("a revoked certificate below the anchor is rejected", func(t *testing.T) {
		err := checker.check([][]*x509.Certificate{{leaf, intermediate, rootCert}})
		require.ErrorContains(t, err, "Certificate revoked: CommonName="+intermediate.Subject.CommonName)
	})

	t.Run("the anchor is not checked", func(t *testing.T) {
		err := checker.check([][]*x509.Certificate{{leaf, intermediate}})
		require.NoError(t, err)
	})

	t.Run("the certificates a peer presents are not checked on their own", func(t *testing.T) {
		err := checker.verifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{intermediate, rootCert}})
		require.NoError(t, err)
	})

	t.Run("a configured anchor that its configured issuer's CRL revokes ends no chain", func(t *testing.T) {
		// The anchor's issuer is beyond the chain, but when both are
		// configured the checker holds the issuer and its CRL, with
		// no part for the peer in it.
		intermediatePEM, err := os.ReadFile(certs.ServerCA)
		require.NoError(t, err)
		rootPEM, err := os.ReadFile(rootCA)
		require.NoError(t, err)
		bundle := path.Join(t.TempDir(), "bundle.pem")
		require.NoError(t, os.WriteFile(bundle, append(intermediatePEM, rootPEM...), 0o600))
		checker, err := newCRLChecker(rootCRL, bundle)
		require.NoError(t, err)

		err = checker.check([][]*x509.Certificate{{leaf, intermediate}})
		require.ErrorContains(t, err, "Certificate revoked: CommonName="+intermediate.Subject.CommonName)
		err = checker.check([][]*x509.Certificate{{leaf, intermediate}, {leaf, intermediate, rootCert}})
		require.ErrorContains(t, err, "Certificate revoked: CommonName="+intermediate.Subject.CommonName)
	})
}

// TestCRLCheckerBindsChainIssuersOnce checks that the CRLs of an
// issuer that is not configured but found in a verified chain, such
// as an intermediate the peer presents while the CA file holds the
// root, are bound on first sight and kept, so that later handshakes
// through that issuer do not verify the CRL signatures again.
func TestCRLCheckerBindsChainIssuersOnce(t *testing.T) {
	root := t.TempDir()
	certs := tlstest.CreateClientServerCertPairs(root)
	rootCert := loadOneCert(t, path.Join(root, "ca-cert.pem"))
	intermediate := loadOneCert(t, certs.ServerCA)
	revokedLeaf := loadOneCert(t, certs.RevokedServerCert)
	checker, err := newCRLChecker(certs.ServerCRL, path.Join(root, "ca-cert.pem"))
	require.NoError(t, err)
	_, cached := checker.bound.Load(string(intermediate.Raw))
	require.False(t, cached)

	for range 2 {
		err = checker.check([][]*x509.Certificate{{revokedLeaf, intermediate, rootCert}})
		require.ErrorContains(t, err, "Certificate revoked: CommonName="+certs.RevokedServerName)
	}
	_, cached = checker.bound.Load(string(intermediate.Raw))
	require.True(t, cached, "the intermediate's CRLs were not kept")
	_, cached = checker.bound.Load(string(rootCert.Raw))
	require.False(t, cached, "the configured root is bound when the checker is built, not here")
}

// TestCRLCheckerRejectedChainsLeaveNoBindings checks that a chain
// rejected for a revoked CA certificate leaves no binding behind for
// the certificates below it: whoever holds the key of a revoked
// intermediate can mint any number of CA certificates under it, each
// of which Go verifies, and each would otherwise be bound and kept
// for the life of the configuration before the chain is rejected.
func TestCRLCheckerRejectedChainsLeaveNoBindings(t *testing.T) {
	root := t.TempDir()
	certs := tlstest.CreateClientServerCertPairs(root)
	tlstest.RevokeCertAndRegenerateCRL(root, tlstest.CA, strings.TrimSuffix(filepath.Base(certs.ServerCA), "-cert.pem"))
	rootCert := loadOneCert(t, path.Join(root, "ca-cert.pem"))
	intermediate := loadOneCert(t, certs.ServerCA)
	keyPair, err := tls.LoadX509KeyPair(certs.ServerCA, strings.TrimSuffix(certs.ServerCA, "-cert.pem")+"-key.pem")
	require.NoError(t, err)
	intermediateKey := keyPair.PrivateKey.(*ecdsa.PrivateKey)
	checker, err := newCRLChecker(path.Join(root, "ca-crl.pem"), path.Join(root, "ca-cert.pem"))
	require.NoError(t, err)

	for i := range 20 {
		subCAKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		subCADER, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
			SerialNumber:          big.NewInt(int64(100 + i)),
			Subject:               pkix.Name{CommonName: fmt.Sprintf("Sub CA %d", i)},
			NotBefore:             time.Now().Add(-time.Hour),
			NotAfter:              time.Now().Add(time.Hour),
			IsCA:                  true,
			BasicConstraintsValid: true,
			KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		}, intermediate, &subCAKey.PublicKey, intermediateKey)
		require.NoError(t, err)
		subCA, err := x509.ParseCertificate(subCADER)
		require.NoError(t, err)
		leaf := signedLeaf(t, subCA, subCAKey, int64(200+i), fmt.Sprintf("leaf%d.example.com", i))

		err = checker.check([][]*x509.Certificate{{leaf, subCA, intermediate, rootCert}})
		require.ErrorContains(t, err, "Certificate revoked: CommonName="+intermediate.Subject.CommonName)
	}
	bindings := 0
	checker.bound.Range(func(any, any) bool { bindings++; return true })
	require.Zero(t, bindings, "the rejected chains left bindings behind")
}

// selfSignedCA returns a self-signed CA certificate and its key, with
// the given serial number and common name, or with rawSubject as its
// subject when it is set.
func selfSignedCA(t *testing.T, serial int64, commonName string, rawSubject []byte) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: commonName},
		RawSubject:            rawSubject,
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert, key
}

// TestCRLCheckerIssuersSharingASubject checks that a CRL from a CA is
// not held against the certificates of another CA that carries the
// same subject with a different key, as a re-keyed CA and its
// predecessor do: the CRL's authority key identifier names the key
// that signed it.
func TestCRLCheckerIssuersSharingASubject(t *testing.T) {
	root := t.TempDir()
	tlstest.CreateCA(root)
	tlstest.CreateIntermediateCA(root, tlstest.CA, "01", "old-ca", "Shared CA")
	tlstest.CreateIntermediateCA(root, tlstest.CA, "02", "new-ca", "Shared CA")
	tlstest.CreateSignedCert(root, "old-ca", "03", "old-leaf", "old.example.com")
	tlstest.CreateCRL(root, "new-ca")
	rootCert := loadOneCert(t, path.Join(root, "ca-cert.pem"))
	oldCA := loadOneCert(t, path.Join(root, "old-ca-cert.pem"))
	newCA := loadOneCert(t, path.Join(root, "new-ca-cert.pem"))
	oldLeaf := loadOneCert(t, path.Join(root, "old-leaf-cert.pem"))
	require.Equal(t, oldCA.RawSubject, newCA.RawSubject)
	require.NotEqual(t, oldCA.PublicKey, newCA.PublicKey)

	// Both CAs are configured, but only the new one has a CRL.
	oldPEM, err := os.ReadFile(path.Join(root, "old-ca-cert.pem"))
	require.NoError(t, err)
	newPEM, err := os.ReadFile(path.Join(root, "new-ca-cert.pem"))
	require.NoError(t, err)
	bundle := path.Join(root, "shared-ca-bundle.pem")
	require.NoError(t, os.WriteFile(bundle, append(oldPEM, newPEM...), 0o600))
	checker, err := newCRLChecker(path.Join(root, "new-ca-crl.pem"), bundle)
	require.NoError(t, err)

	err = checker.check([][]*x509.Certificate{{oldLeaf, oldCA, rootCert}})
	require.NoError(t, err)

	t.Run("the CRLs of both CAs apply, each to the certificates of its own", func(t *testing.T) {
		// The old CA revokes its leaf; the new CA's CRL, under the
		// same name and with a higher number, revokes a leaf of its
		// own and must not supersede the old CA's CRL.
		tlstest.RevokeCertAndRegenerateCRL(root, "old-ca", "old-leaf")
		tlstest.CreateSignedCert(root, "new-ca", "04", "new-leaf", "new.example.com")
		tlstest.RevokeCertAndRegenerateCRL(root, "new-ca", "new-leaf")
		tlstest.RevokeCertAndRegenerateCRL(root, "new-ca", "new-leaf")
		oldCRL, err := os.ReadFile(path.Join(root, "old-ca-crl.pem"))
		require.NoError(t, err)
		newCRL, err := os.ReadFile(path.Join(root, "new-ca-crl.pem"))
		require.NoError(t, err)
		require.Positive(t, loadOneCRL(t, path.Join(root, "new-ca-crl.pem")).Number.Cmp(loadOneCRL(t, path.Join(root, "old-ca-crl.pem")).Number), "the new CA's CRL must be the newer one by number for this test to be meaningful")
		bothCRLs := path.Join(t.TempDir(), "both-crl.pem")
		require.NoError(t, os.WriteFile(bothCRLs, append(newCRL, oldCRL...), 0o600))
		checker, err := newCRLChecker(bothCRLs, bundle)
		require.NoError(t, err)

		err = checker.check([][]*x509.Certificate{{oldLeaf, oldCA, rootCert}})
		require.ErrorContains(t, err, "Certificate revoked: CommonName=old.example.com")
		newLeaf := loadOneCert(t, path.Join(root, "new-leaf-cert.pem"))
		err = checker.check([][]*x509.Certificate{{newLeaf, newCA, rootCert}})
		require.ErrorContains(t, err, "Certificate revoked: CommonName=new.example.com")
	})
}

// TestNewCRLCheckerDeltaCRL checks that a delta CRL is refused: the
// checker applies each CRL on its own, and a delta CRL's entries only
// make sense together with the base CRL it amends, an entry that
// takes a certificate off hold included.
func TestNewCRLCheckerDeltaCRL(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	ca := loadOneCert(t, certs.ServerCA)
	keyPair, err := tls.LoadX509KeyPair(certs.ServerCA, strings.TrimSuffix(certs.ServerCA, "-cert.pem")+"-key.pem")
	require.NoError(t, err)
	baseNumber, err := asn1.Marshal(1)
	require.NoError(t, err)
	der, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number:          big.NewInt(2),
		ThisUpdate:      time.Now().Add(-time.Hour),
		NextUpdate:      time.Now().Add(time.Hour),
		ExtraExtensions: []pkix.Extension{{Id: asn1.ObjectIdentifier{2, 5, 29, 27}, Critical: true, Value: baseNumber}},
	}, ca, keyPair.PrivateKey.(crypto.Signer))
	require.NoError(t, err)

	_, err = newCRLChecker(crlFile(t, der), certs.ServerCA)
	require.ErrorContains(t, err, "delta CRLs are not supported")
}

// crlFile writes a DER encoded CRL to a PEM file and returns its path.
func crlFile(t *testing.T, der []byte) string {
	t.Helper()
	file := path.Join(t.TempDir(), "crl.pem")
	require.NoError(t, os.WriteFile(file, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: der}), 0o600))
	return file
}

// TestCRLCheckerCRLThatCannotBeValidated checks that a CRL from the
// issuer that cannot be validated fails closed rather than passing for
// the CRL of another CA under the same name: one signed with an
// algorithm that Go cannot verify is refused when it is loaded, and
// one whose signature does not verify is refused when its issuer is
// configured, and rejects the connection when its issuer is only in
// the chain.
func TestCRLCheckerCRLThatCannotBeValidated(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	ca := loadOneCert(t, certs.ServerCA)
	keyPair, err := tls.LoadX509KeyPair(certs.ServerCA, strings.TrimSuffix(certs.ServerCA, "-cert.pem")+"-key.pem")
	require.NoError(t, err)
	leaf := loadOneCert(t, certs.ServerCert)
	template := &x509.RevocationList{Number: big.NewInt(1), ThisUpdate: time.Now().Add(-time.Hour), NextUpdate: time.Now().Add(time.Hour)}
	validDER, err := x509.CreateRevocationList(rand.Reader, template, ca, keyPair.PrivateKey.(crypto.Signer))
	require.NoError(t, err)

	t.Run("a CRL signed with an unsupported algorithm is refused", func(t *testing.T) {
		// The signature algorithm, ecdsa-with-SHA256, is rewritten,
		// inside and outside the signed part, to an identifier that
		// Go does not know.
		ecdsaWithSHA256 := mustMarshal(t, pkix.AlgorithmIdentifier{Algorithm: asn1.ObjectIdentifier{1, 2, 840, 10045, 4, 3, 2}})
		require.Equal(t, 2, bytes.Count(validDER, ecdsaWithSHA256))
		unknownAlgorithm := slices.Clone(ecdsaWithSHA256)
		unknownAlgorithm[len(unknownAlgorithm)-1] = 0x09
		unsupportedDER := bytes.ReplaceAll(validDER, ecdsaWithSHA256, unknownAlgorithm)
		unsupported, err := x509.ParseRevocationList(unsupportedDER)
		require.NoError(t, err)
		require.Equal(t, x509.UnknownSignatureAlgorithm, unsupported.SignatureAlgorithm)

		_, err = newCRLChecker(crlFile(t, unsupportedDER), "")
		require.ErrorContains(t, err, "cannot be validated")
	})

	corruptDER := slices.Clone(validDER)
	corruptDER[len(corruptDER)-1] ^= 0xff
	corrupt, err := x509.ParseRevocationList(corruptDER)
	require.NoError(t, err)
	require.Equal(t, ca.SubjectKeyId, corrupt.AuthorityKeyId)

	t.Run("a CRL whose signature does not verify is refused when its issuer is configured", func(t *testing.T) {
		_, err := newCRLChecker(crlFile(t, corruptDER), certs.ServerCA)
		require.ErrorContains(t, err, "its signature does not verify")
	})

	t.Run("a CRL whose signature does not verify rejects the connection when its issuer is only in the chain", func(t *testing.T) {
		checker, err := newCRLChecker(crlFile(t, corruptDER), "")
		require.NoError(t, err)

		err = checker.check([][]*x509.Certificate{{leaf, ca}})
		require.ErrorContains(t, err, "cannot check the revocation of certificate CommonName="+certs.ServerName+": the CRL from issuer "+ca.Subject.CommonName+" cannot be validated: its signature does not verify")
	})
}

// TestNewCRLCheckerIndirectCRL checks that an indirect CRL is refused:
// its entries may belong to other issuers than the CRL's, which the
// checker, keying every CRL by its issuer, cannot tell.
func TestNewCRLCheckerIndirectCRL(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	ca := loadOneCert(t, certs.ServerCA)
	keyPair, err := tls.LoadX509KeyPair(certs.ServerCA, strings.TrimSuffix(certs.ServerCA, "-cert.pem")+"-key.pem")
	require.NoError(t, err)
	distributionPoint, err := asn1.Marshal(struct {
		IndirectCRL bool `asn1:"tag:4,optional"`
	}{IndirectCRL: true})
	require.NoError(t, err)
	der, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number:          big.NewInt(1),
		ThisUpdate:      time.Now().Add(-time.Hour),
		NextUpdate:      time.Now().Add(time.Hour),
		ExtraExtensions: []pkix.Extension{{Id: asn1.ObjectIdentifier{2, 5, 29, 28}, Critical: true, Value: distributionPoint}},
	}, ca, keyPair.PrivateKey.(crypto.Signer))
	require.NoError(t, err)

	_, err = newCRLChecker(crlFile(t, der), certs.ServerCA)
	require.ErrorContains(t, err, "indirect CRLs are not supported")
}

// TestCRLCheckerNewestCompleteCRL checks that of several complete CRLs
// from one issuer, the newest one alone applies: a certificate that an
// older one lists and the newest one dropped, as one taken off hold,
// is not revoked, while one the newest lists is.
func TestCRLCheckerNewestCompleteCRL(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	ca := loadOneCert(t, certs.ServerCA)
	keyPair, err := tls.LoadX509KeyPair(certs.ServerCA, strings.TrimSuffix(certs.ServerCA, "-cert.pem")+"-key.pem")
	require.NoError(t, err)
	leaf := loadOneCert(t, certs.ServerCert)
	chain := [][]*x509.Certificate{{leaf, ca}}
	completeCRL := func(number int64, issued time.Time, serials ...*big.Int) []byte {
		template := &x509.RevocationList{Number: big.NewInt(number), ThisUpdate: issued, NextUpdate: issued.Add(24 * time.Hour)}
		for _, serial := range serials {
			template.RevokedCertificateEntries = append(template.RevokedCertificateEntries, x509.RevocationListEntry{SerialNumber: serial, RevocationTime: issued})
		}
		der, err := x509.CreateRevocationList(rand.Reader, template, ca, keyPair.PrivateKey.(crypto.Signer))
		require.NoError(t, err)
		return der
	}
	older := time.Now().Add(-2 * time.Hour)
	newer := time.Now().Add(-time.Hour)
	holdThenReleased := []struct {
		name string
		file string
	}{
		{"older first", crlsFile(t, completeCRL(1, older, leaf.SerialNumber), completeCRL(2, newer))},
		{"newer first", crlsFile(t, completeCRL(2, newer), completeCRL(1, older, leaf.SerialNumber))},
	}
	for _, tc := range holdThenReleased {
		t.Run("a certificate the newest CRL dropped is not revoked, "+tc.name, func(t *testing.T) {
			checker, err := newCRLChecker(tc.file, certs.ServerCA)
			require.NoError(t, err)
			require.NoError(t, checker.check(chain))
		})
	}

	t.Run("a certificate the newest CRL lists is revoked", func(t *testing.T) {
		checker, err := newCRLChecker(crlsFile(t, completeCRL(1, older), completeCRL(2, newer, leaf.SerialNumber)), certs.ServerCA)
		require.NoError(t, err)
		err = checker.check(chain)
		require.ErrorContains(t, err, "Certificate revoked: CommonName="+certs.ServerName)
	})
}

// crlsFile writes the DER encoded CRLs to one PEM file and returns
// its path.
func crlsFile(t *testing.T, crls ...[]byte) string {
	t.Helper()
	var content []byte
	for _, der := range crls {
		content = append(content, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: der})...)
	}
	file := path.Join(t.TempDir(), "crls.pem")
	require.NoError(t, os.WriteFile(file, content, 0o600))
	return file
}

// TestCRLCheckerPartitionedCRLs checks that the CRLs of one issuer
// that its issuing distribution point partitions, one per
// distribution point, each apply, and that a newer one supersedes
// the older one of its own partition alone.
func TestCRLCheckerPartitionedCRLs(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	ca := loadOneCert(t, certs.ServerCA)
	keyPair, err := tls.LoadX509KeyPair(certs.ServerCA, strings.TrimSuffix(certs.ServerCA, "-cert.pem")+"-key.pem")
	require.NoError(t, err)
	leaf := loadOneCert(t, certs.ServerCert)
	chain := [][]*x509.Certificate{{leaf, ca}}
	partition := func(number int64, distributionPoint int, serials ...*big.Int) []byte {
		uri := mustMarshal(t, asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 6, Bytes: fmt.Appendf(nil, "http://crl.example.com/%d", distributionPoint)})
		fullName := mustMarshal(t, asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 0, IsCompound: true, Bytes: uri})
		scope := mustMarshal(t, struct{ DistributionPoint asn1.RawValue }{asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 0, IsCompound: true, Bytes: fullName}})
		template := &x509.RevocationList{
			Number:          big.NewInt(number),
			ThisUpdate:      time.Now().Add(-time.Duration(number) * time.Hour),
			NextUpdate:      time.Now().Add(time.Hour),
			ExtraExtensions: []pkix.Extension{{Id: asn1.ObjectIdentifier{2, 5, 29, 28}, Critical: true, Value: scope}},
		}
		for _, serial := range serials {
			template.RevokedCertificateEntries = append(template.RevokedCertificateEntries, x509.RevocationListEntry{SerialNumber: serial, RevocationTime: template.ThisUpdate})
		}
		der, err := x509.CreateRevocationList(rand.Reader, template, ca, keyPair.PrivateKey.(crypto.Signer))
		require.NoError(t, err)
		return der
	}

	t.Run("every partition applies", func(t *testing.T) {
		checker, err := newCRLChecker(crlsFile(t, partition(1, 0), partition(1, 1, leaf.SerialNumber)), certs.ServerCA)
		require.NoError(t, err)
		err = checker.check(chain)
		require.ErrorContains(t, err, "Certificate revoked: CommonName="+certs.ServerName)
	})

	t.Run("a newer partition supersedes the older one of its own distribution point alone", func(t *testing.T) {
		checker, err := newCRLChecker(crlsFile(t, partition(1, 0, leaf.SerialNumber), partition(2, 0), partition(1, 1, big.NewInt(42))), certs.ServerCA)
		require.NoError(t, err)
		require.NoError(t, checker.check(chain))
	})
}

// TestCRLCheckerCAThatMayNotSignCRLs checks that a CRL that the key
// of a CA certificate signed while the certificate is not allowed to
// sign CRLs, for want of the cRLSign key usage, fails closed: the
// configuration is refused when the CA is configured, and the
// connection is rejected when the CA is only in the chain.
func TestCRLCheckerCAThatMayNotSignCRLs(t *testing.T) {
	// The CA may sign certificates but not CRLs; a copy of it that
	// may is what signs the CRL, with the same key.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "No CRL Signing CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	ca, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)
	caFile := path.Join(t.TempDir(), "ca-cert.pem")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw}), 0o600))
	leaf := signedLeaf(t, ca, caKey, 2, "leaf.example.com")

	crlSigner := *ca
	crlSigner.KeyUsage |= x509.KeyUsageCRLSign
	crlDER, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number:     big.NewInt(1),
		ThisUpdate: time.Now().Add(-time.Hour),
		NextUpdate: time.Now().Add(time.Hour),
	}, &crlSigner, caKey)
	require.NoError(t, err)
	crl := crlFile(t, crlDER)
	notAllowed := "the CRL from issuer No CRL Signing CA cannot be validated: the certificate of that issuer is not allowed to sign CRLs"

	t.Run("with the CA configured, the configuration is refused", func(t *testing.T) {
		_, err := newCRLChecker(crl, caFile)
		require.ErrorContains(t, err, notAllowed)
	})

	t.Run("with the CA only in the chain, the connection is rejected", func(t *testing.T) {
		checker, err := newCRLChecker(crl, "")
		require.NoError(t, err)

		err = checker.check([][]*x509.Certificate{{leaf, ca}})
		require.ErrorContains(t, err, "cannot check the revocation of certificate CommonName=leaf.example.com: "+notAllowed)
	})
}

// signedLeaf returns an end-entity certificate with the given serial
// number and common name, issued by ca with its key.
func signedLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, serial int64, commonName string) *x509.Certificate {
	t.Helper()
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}, ca, &leafKey.PublicKey, caKey)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return leaf
}

// TestNewCRLCheckerCriticalExtensions checks that a CRL is refused
// when it, or one of its entries, carries a critical extension whose
// meaning the checker does not handle, as RFC 5280 requires, while a
// non-critical one it does not know is ignored.
func TestNewCRLCheckerCriticalExtensions(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	ca := loadOneCert(t, certs.ServerCA)
	keyPair, err := tls.LoadX509KeyPair(certs.ServerCA, strings.TrimSuffix(certs.ServerCA, "-cert.pem")+"-key.pem")
	require.NoError(t, err)
	unknown := asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 99999, 1}
	flag := mustMarshal(t, true)
	crlWith := func(extensions []pkix.Extension, entryExtensions []pkix.Extension) string {
		der, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
			Number:                    big.NewInt(1),
			ThisUpdate:                time.Now().Add(-time.Hour),
			NextUpdate:                time.Now().Add(time.Hour),
			ExtraExtensions:           extensions,
			RevokedCertificateEntries: []x509.RevocationListEntry{{SerialNumber: big.NewInt(42), RevocationTime: time.Now().Add(-time.Hour), ExtraExtensions: entryExtensions}},
		}, ca, keyPair.PrivateKey.(crypto.Signer))
		require.NoError(t, err)
		return crlFile(t, der)
	}

	for _, tc := range []struct {
		name string
		file string
		want string
	}{
		{"an unknown critical extension", crlWith([]pkix.Extension{{Id: unknown, Critical: true, Value: flag}}, nil), "critical extension 1.3.6.1.4.1.99999.1"},
		{"an unknown critical entry extension", crlWith(nil, []pkix.Extension{{Id: unknown, Critical: true, Value: flag}}), "critical extension 1.3.6.1.4.1.99999.1"},
		{"an issuing distribution point followed by trailing data", crlWith([]pkix.Extension{{Id: asn1.ObjectIdentifier{2, 5, 29, 28}, Critical: true, Value: append(mustMarshal(t, struct{}{}), 0x00)}}, nil), "cannot be parsed"},
	} {
		t.Run(tc.name+" is refused", func(t *testing.T) {
			_, err := newCRLChecker(tc.file, certs.ServerCA)
			require.ErrorContains(t, err, tc.want)
		})
	}

	t.Run("an unknown non-critical extension is ignored", func(t *testing.T) {
		_, err := newCRLChecker(crlWith([]pkix.Extension{{Id: unknown, Value: flag}}, []pkix.Extension{{Id: unknown, Value: flag}}), certs.ServerCA)
		require.NoError(t, err)
	})
}

// TestNewCRLCheckerPartialCRL checks that a CRL that its issuing
// distribution point limits to part of what its issuer revoked is
// refused, as grpc-go refuses it, since the checker applies every CRL
// as the complete list: one limited to end-entity certificates, to CA
// certificates, to both, which RFC 5280 forbids, to attribute
// certificates, or to some revocation reasons. One that names a
// distribution point without limiting the CRL otherwise is accepted.
func TestNewCRLCheckerPartialCRL(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	ca := loadOneCert(t, certs.ServerCA)
	keyPair, err := tls.LoadX509KeyPair(certs.ServerCA, strings.TrimSuffix(certs.ServerCA, "-cert.pem")+"-key.pem")
	require.NoError(t, err)
	scopedCRL := func(scope any) string {
		der, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
			Number:          big.NewInt(1),
			ThisUpdate:      time.Now().Add(-time.Hour),
			NextUpdate:      time.Now().Add(time.Hour),
			ExtraExtensions: []pkix.Extension{{Id: asn1.ObjectIdentifier{2, 5, 29, 28}, Critical: true, Value: mustMarshal(t, scope)}},
		}, ca, keyPair.PrivateKey.(crypto.Signer))
		require.NoError(t, err)
		return crlFile(t, der)
	}
	type scope struct {
		OnlyContainsUserCerts      bool           `asn1:"tag:1,optional"`
		OnlyContainsCACerts        bool           `asn1:"tag:2,optional"`
		OnlySomeReasons            asn1.BitString `asn1:"tag:3,optional"`
		OnlyContainsAttributeCerts bool           `asn1:"tag:5,optional"`
	}

	for _, tc := range []struct {
		name  string
		scope scope
		want  string
	}{
		{"end-entity certificates", scope{OnlyContainsUserCerts: true}, "limited to end-entity certificates"},
		{"CA certificates", scope{OnlyContainsCACerts: true}, "limited to CA certificates"},
		{"both end-entity and CA certificates", scope{OnlyContainsUserCerts: true, OnlyContainsCACerts: true}, "limited to"},
		{"attribute certificates", scope{OnlyContainsAttributeCerts: true}, "limited to attribute certificates"},
		{"some revocation reasons", scope{OnlySomeReasons: asn1.BitString{Bytes: []byte{0x40}, BitLength: 2}}, "limited to some revocation reasons"},
	} {
		t.Run("a CRL limited to "+tc.name+" is refused", func(t *testing.T) {
			_, err := newCRLChecker(scopedCRL(tc.scope), certs.ServerCA)
			require.ErrorContains(t, err, tc.want)
		})
	}

	t.Run("a CRL that names its distribution point is accepted", func(t *testing.T) {
		uri := mustMarshal(t, asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 6, Bytes: []byte("http://crl.example.com/ca.crl")})
		fullName := mustMarshal(t, asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 0, IsCompound: true, Bytes: uri})
		_, err := newCRLChecker(scopedCRL(struct{ DistributionPoint asn1.RawValue }{asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 0, IsCompound: true, Bytes: fullName}}), certs.ServerCA)
		require.NoError(t, err)
	})
}

// TestNewCRLCheckerFutureCRL checks that a CRL whose thisUpdate lies
// in the future, beyond what clock skew accounts for, is refused, so
// that a CRL staged ahead of time cannot supersede the current one.
func TestNewCRLCheckerFutureCRL(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	ca := loadOneCert(t, certs.ServerCA)
	keyPair, err := tls.LoadX509KeyPair(certs.ServerCA, strings.TrimSuffix(certs.ServerCA, "-cert.pem")+"-key.pem")
	require.NoError(t, err)
	crlIssuedAt := func(issued time.Time) string {
		der, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{Number: big.NewInt(1), ThisUpdate: issued, NextUpdate: issued.Add(24 * time.Hour)}, ca, keyPair.PrivateKey.(crypto.Signer))
		require.NoError(t, err)
		return crlFile(t, der)
	}

	_, err = newCRLChecker(crlIssuedAt(time.Now().Add(time.Hour)), certs.ServerCA)
	require.ErrorContains(t, err, "is not valid yet")

	_, err = newCRLChecker(crlIssuedAt(time.Now().Add(time.Minute)), certs.ServerCA)
	require.NoError(t, err, "a CRL issued within the clock skew allowance is accepted")
}

// TestCRLCheckerAlternateVerifiedChain checks that a peer whose
// verification found several chains passes when any of them holds no
// revoked certificate, as verification itself accepts the peer on any
// valid chain: an intermediate cross-signed by two roots may be
// revoked by one root and not the other, as during a CA rollover.
func TestCRLCheckerAlternateVerifiedChain(t *testing.T) {
	rootA, rootAKey := selfSignedCA(t, 1, "Root A", nil)
	rootB, rootBKey := selfSignedCA(t, 2, "Root B", nil)
	crossKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	crossSigned := func(serial int64, root *x509.Certificate, rootKey *ecdsa.PrivateKey) *x509.Certificate {
		der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
			SerialNumber:          big.NewInt(serial),
			Subject:               pkix.Name{CommonName: "Cross-signed CA"},
			NotBefore:             time.Now().Add(-time.Hour),
			NotAfter:              time.Now().Add(time.Hour),
			IsCA:                  true,
			BasicConstraintsValid: true,
			KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		}, root, &crossKey.PublicKey, rootKey)
		require.NoError(t, err)
		cert, err := x509.ParseCertificate(der)
		require.NoError(t, err)
		return cert
	}
	crossA := crossSigned(10, rootA, rootAKey)
	crossB := crossSigned(20, rootB, rootBKey)
	leaf := signedLeaf(t, crossA, crossKey, 3, "leaf.example.com")

	// Root A revokes its cross-signed certificate; root B has not.
	crlDER, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number:                    big.NewInt(1),
		ThisUpdate:                time.Now().Add(-time.Hour),
		NextUpdate:                time.Now().Add(time.Hour),
		RevokedCertificateEntries: []x509.RevocationListEntry{{SerialNumber: crossA.SerialNumber, RevocationTime: time.Now().Add(-time.Hour)}},
	}, rootA, rootAKey)
	require.NoError(t, err)
	bundle := path.Join(t.TempDir(), "roots.pem")
	require.NoError(t, os.WriteFile(bundle, append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootA.Raw}), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootB.Raw})...), 0o600))
	checker, err := newCRLChecker(crlFile(t, crlDER), bundle)
	require.NoError(t, err)

	t.Run("the chain through the other root carries the peer", func(t *testing.T) {
		err := checker.check([][]*x509.Certificate{{leaf, crossA, rootA}, {leaf, crossB, rootB}})
		require.NoError(t, err)
	})

	t.Run("the peer fails when the only chain holds the revoked certificate", func(t *testing.T) {
		err := checker.check([][]*x509.Certificate{{leaf, crossA, rootA}})
		require.ErrorContains(t, err, "Certificate revoked: CommonName=Cross-signed CA")
	})
}

// TestCRLCheckerIssuerOfEachChain checks that each chain is checked
// with its own certificates as the issuers: with a CA certificate
// that may not sign CRLs in one chain and one carrying the same key
// and name that may in another, the first chain fails on the CRL
// signed by that key, whatever the second one holds, and the second
// fails on its own revocation.
func TestCRLCheckerIssuerOfEachChain(t *testing.T) {
	rootA, rootAKey := selfSignedCA(t, 1, "Root A", nil)
	rootB, rootBKey := selfSignedCA(t, 2, "Root B", nil)
	crossKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	crossSigned := func(serial int64, keyUsage x509.KeyUsage, root *x509.Certificate, rootKey *ecdsa.PrivateKey) *x509.Certificate {
		der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
			SerialNumber:          big.NewInt(serial),
			Subject:               pkix.Name{CommonName: "Cross-signed CA"},
			NotBefore:             time.Now().Add(-time.Hour),
			NotAfter:              time.Now().Add(time.Hour),
			IsCA:                  true,
			BasicConstraintsValid: true,
			KeyUsage:              keyUsage,
		}, root, &crossKey.PublicKey, rootKey)
		require.NoError(t, err)
		cert, err := x509.ParseCertificate(der)
		require.NoError(t, err)
		return cert
	}
	crossA := crossSigned(10, x509.KeyUsageCertSign, rootA, rootAKey)
	crossB := crossSigned(20, x509.KeyUsageCertSign|x509.KeyUsageCRLSign, rootB, rootBKey)
	require.Equal(t, crossA.SubjectKeyId, crossB.SubjectKeyId)
	leaf := signedLeaf(t, crossB, crossKey, 3, "leaf.example.com")

	// The cross-signed key signs a CRL, and root B revokes its
	// cross-signed certificate.
	crossCRL, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{Number: big.NewInt(1), ThisUpdate: time.Now().Add(-time.Hour), NextUpdate: time.Now().Add(time.Hour)}, crossB, crossKey)
	require.NoError(t, err)
	rootBCRL, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number:                    big.NewInt(1),
		ThisUpdate:                time.Now().Add(-time.Hour),
		NextUpdate:                time.Now().Add(time.Hour),
		RevokedCertificateEntries: []x509.RevocationListEntry{{SerialNumber: crossB.SerialNumber, RevocationTime: time.Now().Add(-time.Hour)}},
	}, rootB, rootBKey)
	require.NoError(t, err)
	roots := path.Join(t.TempDir(), "roots.pem")
	require.NoError(t, os.WriteFile(roots, append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootA.Raw}), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootB.Raw})...), 0o600))
	checker, err := newCRLChecker(crlsFile(t, crossCRL, rootBCRL), roots)
	require.NoError(t, err)

	notAllowed := "cannot check the revocation of certificate CommonName=leaf.example.com: the CRL from issuer Cross-signed CA cannot be validated: the certificate of that issuer is not allowed to sign CRLs"
	err = checker.check([][]*x509.Certificate{{leaf, crossA, rootA}})
	require.ErrorContains(t, err, notAllowed)
	err = checker.check([][]*x509.Certificate{{leaf, crossB, rootB}})
	require.ErrorContains(t, err, "Certificate revoked: CommonName=Cross-signed CA")
	err = checker.check([][]*x509.Certificate{{leaf, crossA, rootA}, {leaf, crossB, rootB}})
	require.ErrorContains(t, err, notAllowed)
}

// TestCRLCheckerCRLWithoutAuthorityKeyIdentifier checks that a CRL
// without an authority key identifier, as OpenSSL writes them unless
// told otherwise, is accepted and applied, and that such a CRL under
// the issuer's name that the issuer does not validate is refused,
// since nothing then tells it from a CRL of the issuer's that has
// gone bad, while one whose identifier names another key is passed
// over as another CA's.
func TestCRLCheckerCRLWithoutAuthorityKeyIdentifier(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	ca := loadOneCert(t, certs.ServerCA)
	keyPair, err := tls.LoadX509KeyPair(certs.ServerCA, strings.TrimSuffix(certs.ServerCA, "-cert.pem")+"-key.pem")
	require.NoError(t, err)
	revokedLeaf := loadOneCert(t, certs.RevokedServerCert)
	chain := [][]*x509.Certificate{{revokedLeaf, ca}}
	// Go will not create a CRL without one, so this one is put
	// together by hand: a bare list with no extension at all.
	der := crlWithoutExtensions(t, ca, keyPair.PrivateKey.(*ecdsa.PrivateKey), revokedLeaf.SerialNumber)
	crl, err := x509.ParseRevocationList(der)
	require.NoError(t, err)
	require.Empty(t, crl.AuthorityKeyId)

	checker, err := newCRLChecker(crlFile(t, der), certs.ServerCA)
	require.NoError(t, err)
	err = checker.check(chain)
	require.ErrorContains(t, err, "Certificate revoked: CommonName="+certs.RevokedServerName)

	// Another CA under the same name, with a key of its own.
	other, otherKey := selfSignedCA(t, 1, "", ca.RawSubject)

	t.Run("such a CRL that the issuer does not validate is refused", func(t *testing.T) {
		_, err := newCRLChecker(crlFile(t, crlWithoutExtensions(t, other, otherKey)), certs.ServerCA)
		require.ErrorContains(t, err, "the CRL from issuer "+ca.Subject.CommonName+" cannot be validated: its signature does not verify")
	})

	t.Run("a CRL whose authority key identifier names another key is passed over", func(t *testing.T) {
		otherDER, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
			Number:                    big.NewInt(1),
			ThisUpdate:                time.Now().Add(-time.Hour),
			NextUpdate:                time.Now().Add(time.Hour),
			RevokedCertificateEntries: []x509.RevocationListEntry{{SerialNumber: revokedLeaf.SerialNumber, RevocationTime: time.Now().Add(-time.Hour)}},
		}, other, otherKey)
		require.NoError(t, err)
		checker, err := newCRLChecker(crlFile(t, otherDER), certs.ServerCA)
		require.NoError(t, err)
		require.NoError(t, checker.check(chain))
	})
}

// TestNewCRLCheckerEmptyCRLFile checks that a CRL file that holds no
// CRL is refused rather than silently enforcing nothing.
func TestNewCRLCheckerEmptyCRLFile(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	// A certificate is valid PEM but not a CRL.
	_, err := newCRLChecker(certs.ServerCert, "")
	require.ErrorContains(t, err, "no CRL found in file")
}

// TestCertIsRevokedWarnsPerExpiredCRL checks that each expired CRL gets
// its own throttled warning, so that one stale CRL that is consulted
// often does not hide another one that also needs updating.
func TestCertIsRevokedWarnsPerExpiredCRL(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	cert := loadOneCert(t, certs.ServerCert)
	expired := time.Now().Add(-time.Hour)
	var crls []*x509.RevocationList
	for _, ca := range []string{certs.ServerCA, certs.ClientCA} {
		crls = append(crls, loadOneCRL(t, crlDueAt(t, ca, 1, expired)))
	}

	checker, err := newCRLCheckerFrom(crls, nil)
	require.NoError(t, err)
	for _, crl := range crls {
		require.False(t, checker.isRevoked(cert, crl))
	}
	for _, crl := range crls {
		warned, found := expiredCRLWarnings.Load(expiredCRLKey(crl))
		require.True(t, found, "the CRL from %s was not warned about", crl.Issuer.CommonName)
		require.False(t, warned.(time.Time).IsZero())
	}

	t.Run("CRLs of one issuer due at the same time are told apart by their number", func(t *testing.T) {
		var keys []string
		for number := int64(1); number <= 2; number++ {
			crl := loadOneCRL(t, crlDueAt(t, certs.ServerCA, number, expired))
			checker, err := newCRLCheckerFrom([]*x509.RevocationList{crl}, nil)
			require.NoError(t, err)
			require.False(t, checker.isRevoked(cert, crl))
			_, found := expiredCRLWarnings.Load(expiredCRLKey(crl))
			require.True(t, found, "CRL number %d was not warned about", number)
			keys = append(keys, expiredCRLKey(crl))
		}
		require.NotEqual(t, keys[0], keys[1])
	})

	t.Run("handshakes consulting an expired CRL at once warn about it once", func(t *testing.T) {
		warnings := atomic.Int32{}
		warn := log.Warn
		log.Warn = func(string, ...slog.Attr) { warnings.Add(1) }
		t.Cleanup(func() { log.Warn = warn })
		crl := loadOneCRL(t, crlDueAt(t, certs.ServerCA, 7, expired))
		checker, err := newCRLCheckerFrom([]*x509.RevocationList{crl}, nil)
		require.NoError(t, err)

		var handshakes sync.WaitGroup
		for range 50 {
			handshakes.Go(func() { checker.isRevoked(cert, crl) })
		}
		handshakes.Wait()
		require.EqualValues(t, 1, warnings.Load())
	})

	t.Run("CRLs that differ in content alone are told apart", func(t *testing.T) {
		// The CRL number is optional, so two CRLs of one issuer due
		// at the same time can carry the same number, or none.
		ca := loadOneCert(t, certs.ServerCA)
		keyPair, err := tls.LoadX509KeyPair(certs.ServerCA, strings.TrimSuffix(certs.ServerCA, "-cert.pem")+"-key.pem")
		require.NoError(t, err)
		var keys []string
		for serial := int64(1); serial <= 2; serial++ {
			der, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
				Number:                    big.NewInt(1),
				ThisUpdate:                expired.Add(-2 * time.Hour),
				NextUpdate:                expired,
				RevokedCertificateEntries: []x509.RevocationListEntry{{SerialNumber: big.NewInt(serial), RevocationTime: expired.Add(-2 * time.Hour)}},
			}, ca, keyPair.PrivateKey.(crypto.Signer))
			require.NoError(t, err)
			crl, err := x509.ParseRevocationList(der)
			require.NoError(t, err)
			keys = append(keys, expiredCRLKey(crl))
		}
		require.NotEqual(t, keys[0], keys[1])
	})
}

// crlDueAt writes an empty CRL with the given number, due at
// nextUpdate, signed by the CA whose certificate file is given, whose
// key file sits next to it, and returns its path.
func crlDueAt(t *testing.T, caCert string, number int64, nextUpdate time.Time) string {
	t.Helper()
	ca := loadOneCert(t, caCert)
	keyPair, err := tls.LoadX509KeyPair(caCert, strings.TrimSuffix(caCert, "-cert.pem")+"-key.pem")
	require.NoError(t, err)
	der, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number:     big.NewInt(number),
		ThisUpdate: nextUpdate.Add(-2 * time.Hour),
		NextUpdate: nextUpdate,
	}, ca, keyPair.PrivateKey.(crypto.Signer))
	require.NoError(t, err)
	return crlFile(t, der)
}

// loadOneCRL loads the CRL file and returns its only CRL.
func loadOneCRL(t *testing.T, file string) *x509.RevocationList {
	t.Helper()
	crls, err := loadCRLSet(file)
	require.NoError(t, err)
	require.Len(t, crls, 1)
	return crls[0]
}

// mustMarshal DER encodes v.
func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	der, err := asn1.Marshal(v)
	require.NoError(t, err)
	return der
}

// crlWithoutExtensions DER encodes and signs, with the given CA and
// its key, a CRL revoking the given serial numbers that carries no
// extension at all, which Go's own constructor cannot produce: it
// always writes the authority key identifier.
func crlWithoutExtensions(t *testing.T, ca *x509.Certificate, key *ecdsa.PrivateKey, serials ...*big.Int) []byte {
	t.Helper()
	type revokedCertificate struct {
		SerialNumber   *big.Int
		RevocationTime time.Time
	}
	var revoked []revokedCertificate
	for _, serial := range serials {
		revoked = append(revoked, revokedCertificate{SerialNumber: serial, RevocationTime: time.Now().Add(-time.Hour).UTC().Truncate(time.Second)})
	}
	ecdsaWithSHA256 := pkix.AlgorithmIdentifier{Algorithm: asn1.ObjectIdentifier{1, 2, 840, 10045, 4, 3, 2}}
	tbs, err := asn1.Marshal(struct {
		Version             int
		Signature           pkix.AlgorithmIdentifier
		Issuer              asn1.RawValue
		ThisUpdate          time.Time
		NextUpdate          time.Time
		RevokedCertificates []revokedCertificate `asn1:"optional"`
	}{
		Version:             1,
		Signature:           ecdsaWithSHA256,
		Issuer:              asn1.RawValue{FullBytes: ca.RawSubject},
		ThisUpdate:          time.Now().Add(-time.Hour).UTC().Truncate(time.Second),
		NextUpdate:          time.Now().Add(time.Hour).UTC().Truncate(time.Second),
		RevokedCertificates: revoked,
	})
	require.NoError(t, err)
	digest := sha256.Sum256(tbs)
	signature, err := ecdsa.SignASN1(rand.Reader, key, digest[:])
	require.NoError(t, err)
	der, err := asn1.Marshal(struct {
		TBSCertList        asn1.RawValue
		SignatureAlgorithm pkix.AlgorithmIdentifier
		SignatureValue     asn1.BitString
	}{
		TBSCertList:        asn1.RawValue{FullBytes: tbs},
		SignatureAlgorithm: ecdsaWithSHA256,
		SignatureValue:     asn1.BitString{Bytes: signature, BitLength: len(signature) * 8},
	})
	require.NoError(t, err)
	return der
}
