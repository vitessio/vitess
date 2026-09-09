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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

	// These servers present their certificate alone, without the
	// intermediate CA that issued it.
	leafOnlyRevokedServer, err := ServerConfig(certs.RevokedServerCert, certs.RevokedServerKey, "", "", "", tls.VersionTLS12)
	require.NoError(t, err)
	leafOnlyValidServer, err := ServerConfig(certs.ServerCert, certs.ServerKey, "", "", "", tls.VersionTLS12)
	require.NoError(t, err)

	t.Run("a server that presents only its certificate is rejected when its issuer is not available", func(t *testing.T) {
		clientConfig, err := ClientConfig(Required, "", "", "", certs.ServerCRL, certs.ServerName, tls.VersionTLS12)
		require.NoError(t, err)

		res := handshake(t, leafOnlyValidServer, clientConfig)
		require.ErrorContains(t, res.clientErr, "cannot check the revocation of certificate CommonName="+certs.ServerName)
	})

	t.Run("a server that presents only its certificate is checked against the configured CA", func(t *testing.T) {
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

	t.Run("a revoked server certificate is rejected on a resumed session", func(t *testing.T) {
		// The session is established before the client has the CRL
		// and resumed once it does. The two client configurations
		// share the session cache, which is keyed by server name.
		sessionCache := tls.NewLRUClientSessionCache(1)
		beforeRevocation, err := ClientConfig(VerifyIdentity, "", "", certs.ServerCA, "", certs.RevokedServerName, tls.VersionTLS12)
		require.NoError(t, err)
		beforeRevocation.ClientSessionCache = sessionCache
		afterRevocation, err := ClientConfig(VerifyIdentity, "", "", certs.ServerCA, certs.ServerCRL, certs.RevokedServerName, tls.VersionTLS12)
		require.NoError(t, err)
		afterRevocation.ClientSessionCache = sessionCache
		resumed := recordResumption(afterRevocation)

		res := handshake(t, revokedServer, beforeRevocation)
		require.NoError(t, res.clientErr)
		require.NoError(t, res.serverErr)
		require.False(t, res.clientState.DidResume)

		res = handshake(t, revokedServer, beforeRevocation)
		require.NoError(t, res.clientErr)
		require.NoError(t, res.serverErr)
		require.True(t, res.clientState.DidResume, "the client must resume the session for this test to be meaningful")

		res = handshake(t, revokedServer, afterRevocation)
		require.ErrorContains(t, res.clientErr, "Certificate revoked: CommonName="+certs.RevokedServerName)
		require.True(t, *resumed, "the rejected handshake must be a resumed one")
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
		resumed := recordResumption(afterRevocation)

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
		require.True(t, *resumed, "the rejected handshake must be a resumed one")
	})
}

// TestCRLCheckerVerifiedChains checks that a certificate which only a
// verified chain contains, as happens when a platform verifier
// completes the chain with certificates the peer did not present,
// is checked against the CRLs like the presented ones.
func TestCRLCheckerVerifiedChains(t *testing.T) {
	root := t.TempDir()
	certs := tlstest.CreateClientServerCertPairs(root)

	// Revoke the intermediate CA that issued the server certificate,
	// under the root CA that issued the intermediate.
	intermediateName := strings.TrimSuffix(filepath.Base(certs.ServerCA), "-cert.pem")
	tlstest.RevokeCertAndRegenerateCRL(root, tlstest.CA, intermediateName)
	rootCA := path.Join(root, "ca-cert.pem")
	rootCRL := path.Join(root, "ca-crl.pem")

	loadCert := func(t *testing.T, file string) *x509.Certificate {
		t.Helper()
		loaded, err := loadx509Certificates(file)
		require.NoError(t, err)
		require.Len(t, loaded, 1)
		return loaded[0]
	}
	leaf := loadCert(t, certs.ServerCert)
	intermediate := loadCert(t, certs.ServerCA)
	rootCert := loadCert(t, rootCA)

	checker, err := newCRLChecker(rootCRL, rootCA)
	require.NoError(t, err)

	err = checker.verifyConnection(tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{leaf},
		VerifiedChains:   [][]*x509.Certificate{{leaf, intermediate, rootCert}},
	})
	require.ErrorContains(t, err, "Certificate revoked: CommonName="+intermediate.Subject.CommonName)
}

// sameSubjectCACerts returns n distinct self-signed CA certificates
// that all carry the given subject, each with its own key.
func sameSubjectCACerts(t *testing.T, rawSubject []byte, n int) []*x509.Certificate {
	t.Helper()
	certs := make([]*x509.Certificate, 0, n)
	for i := range n {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		template := &x509.Certificate{
			SerialNumber:          big.NewInt(int64(i + 1)),
			Subject:               pkix.Name{CommonName: "same subject"},
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
		certs = append(certs, cert)
	}
	return certs
}

// TestCRLCheckerBoundedIssuerSearch checks that a peer cannot make the
// checker spend unbounded signature verifications by padding its chain
// with certificates that carry the issuer's name, and that the padding
// does not hide a revoked certificate when the issuer is configured.
func TestCRLCheckerBoundedIssuerSearch(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	loaded, err := loadx509Certificates(certs.RevokedServerCert)
	require.NoError(t, err)
	leaf := loaded[0]
	padded := append([]*x509.Certificate{leaf}, sameSubjectCACerts(t, leaf.RawIssuer, 200)...)

	t.Run("the search stops and fails closed when the issuer is not available", func(t *testing.T) {
		checker, err := newCRLChecker(certs.ServerCRL, "")
		require.NoError(t, err)

		start := time.Now()
		err = checker.verifyConnection(tls.ConnectionState{PeerCertificates: padded})
		t.Logf("checked a %d certificate chain in %s", len(padded), time.Since(start))
		require.ErrorContains(t, err, "cannot check the revocation of certificate CommonName="+certs.RevokedServerName)
	})

	t.Run("the configured issuer is consulted before the presented certificates", func(t *testing.T) {
		checker, err := newCRLChecker(certs.ServerCRL, certs.ServerCA)
		require.NoError(t, err)

		err = checker.verifyConnection(tls.ConnectionState{PeerCertificates: padded})
		require.ErrorContains(t, err, "Certificate revoked: CommonName="+certs.RevokedServerName)
	})
}
