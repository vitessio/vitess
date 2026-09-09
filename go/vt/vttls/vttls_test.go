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
	"unicode/utf16"

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
// every SSL mode, on resumed sessions, with and without a configured
// CA, and against chains crafted to dodge or exhaust the check.
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

	t.Run("a server that presents only its certificate is accepted when no CRL is configured for its issuer", func(t *testing.T) {
		// The only CRL configured comes from the clients' CA, which
		// has nothing to say about the server's certificate.
		clientConfig, err := ClientConfig(Required, "", "", "", certs.ClientCRL, certs.ServerName, tls.VersionTLS12)
		require.NoError(t, err)

		res := handshake(t, leafOnlyValidServer, clientConfig)
		require.NoError(t, res.clientErr)
		require.NoError(t, res.serverErr)
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

	for _, mode := range []SslMode{Required, VerifyCA, VerifyIdentity} {
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
	// under the root CA, and have servers present that chain with
	// and without padding. The client has no CA configured, so the
	// issuers are only found among the certificates presented.
	intermediateName := strings.TrimSuffix(filepath.Base(certs.ServerCA), "-cert.pem")
	tlstest.RevokeCertAndRegenerateCRL(root, tlstest.CA, intermediateName)
	rootCRL := path.Join(root, "ca-crl.pem")
	rootCert := loadOneCert(t, path.Join(root, "ca-cert.pem"))

	t.Run("verify_ca consults the root's CRL for a configured intermediate that the chain ends at", func(t *testing.T) {
		// The CA file holds the intermediate and the root, so
		// verification of a server that presents only its
		// certificate stops at the intermediate. It is the root's
		// CRL that revokes the intermediate.
		intermediatePEM, err := os.ReadFile(certs.ServerCA)
		require.NoError(t, err)
		rootPEM, err := os.ReadFile(path.Join(root, "ca-cert.pem"))
		require.NoError(t, err)
		bundle := path.Join(t.TempDir(), "bundle.pem")
		require.NoError(t, os.WriteFile(bundle, append(intermediatePEM, rootPEM...), 0o600))
		clientConfig, err := ClientConfig(VerifyCA, "", "", bundle, rootCRL, certs.ServerName, tls.VersionTLS12)
		require.NoError(t, err)

		res := handshake(t, leafOnlyValidServer, clientConfig)
		require.ErrorContains(t, res.clientErr, "Certificate revoked: CommonName="+intermediate.Subject.CommonName)
	})

	t.Run("verify_ca consults the root's CRL for a trusted intermediate when the server presents the root", func(t *testing.T) {
		// The client trusts the intermediate alone, so verification
		// stops at it, but the server presents the root too, and it
		// is the root's CRL that revokes the intermediate.
		clientConfig, err := ClientConfig(VerifyCA, "", "", certs.ServerCA, rootCRL, certs.ServerName, tls.VersionTLS12)
		require.NoError(t, err)

		res := handshake(t, serverPresenting(leaf, intermediate, rootCert), clientConfig)
		require.ErrorContains(t, res.clientErr, "Certificate revoked: CommonName="+intermediate.Subject.CommonName)
	})

	t.Run("a revoked intermediate that the server presents is rejected", func(t *testing.T) {
		clientConfig, err := ClientConfig(Required, "", "", "", rootCRL, certs.ServerName, tls.VersionTLS12)
		require.NoError(t, err)

		res := handshake(t, serverPresenting(leaf, intermediate, rootCert), clientConfig)
		require.ErrorContains(t, res.clientErr, "Certificate revoked: CommonName="+intermediate.Subject.CommonName)
	})

	t.Run("a revoked intermediate presented without its root is rejected for want of the root", func(t *testing.T) {
		clientConfig, err := ClientConfig(Required, "", "", "", rootCRL, certs.ServerName, tls.VersionTLS12)
		require.NoError(t, err)

		res := handshake(t, serverPresenting(leaf, intermediate), clientConfig)
		require.ErrorContains(t, res.clientErr, "cannot check the revocation of certificate CommonName="+intermediate.Subject.CommonName+": no certificate is available for its issuer")
	})

	t.Run("a chain padded to exhaust the signature checks is rejected", func(t *testing.T) {
		// The CRLs come from both the intermediate and the root, so
		// the leaf's issuer is worth looking for. The decoys carry
		// the intermediate's name, so finding it spends the whole
		// budget on them and the real intermediate, and binding its
		// CRL would need one more check.
		intermediateCRL, err := os.ReadFile(certs.ServerCRL)
		require.NoError(t, err)
		rootCRLBytes, err := os.ReadFile(rootCRL)
		require.NoError(t, err)
		bothCRLs := path.Join(t.TempDir(), "both-crl.pem")
		require.NoError(t, os.WriteFile(bothCRLs, append(intermediateCRL, rootCRLBytes...), 0o600))
		chain := append([]*x509.Certificate{leaf}, selfSignedCACerts(t, leaf.RawIssuer, maxSignatureChecks-1)...)
		chain = append(chain, intermediate, rootCert)
		clientConfig, err := ClientConfig(Required, "", "", "", bothCRLs, certs.ServerName, tls.VersionTLS12)
		require.NoError(t, err)

		res := handshake(t, serverPresenting(chain...), clientConfig)
		require.ErrorContains(t, res.clientErr, fmt.Sprintf("exceeded the %d signature checks allowed per connection", maxSignatureChecks))
	})

	// A forged issuer carries the real intermediate's subject and
	// public key, so it verifies the certificates the intermediate
	// issued, but it lacks the CRL signing key usage, so it
	// validates none of the intermediate's CRLs.
	revokedLeaf := loadOneCert(t, certs.RevokedServerCert)
	revokedKeyPair, err := tls.LoadX509KeyPair(certs.RevokedServerCert, certs.RevokedServerKey)
	require.NoError(t, err)
	forged := forgedIssuer(t, intermediate)
	revokedServerPresenting := func(chain ...*x509.Certificate) *tls.Config {
		serverConfig := serverPresenting(chain...)
		serverConfig.Certificates[0].PrivateKey = revokedKeyPair.PrivateKey
		return serverConfig
	}

	t.Run("a decoy issuer that carries the issuer's name but not its key is not the issuer", func(t *testing.T) {
		clientConfig, err := ClientConfig(Required, "", "", "", certs.ServerCRL, certs.RevokedServerName, tls.VersionTLS12)
		require.NoError(t, err)

		res := handshake(t, revokedServerPresenting(revokedLeaf, selfSignedCACerts(t, revokedLeaf.RawIssuer, 1)[0]), clientConfig)
		require.ErrorContains(t, res.clientErr, "cannot check the revocation of certificate CommonName="+certs.RevokedServerName+": no certificate is available for its issuer")
	})

	t.Run("a forged issuer that validates no CRL fails the check when it is the only one", func(t *testing.T) {
		clientConfig, err := ClientConfig(Required, "", "", "", certs.ServerCRL, certs.RevokedServerName, tls.VersionTLS12)
		require.NoError(t, err)

		res := handshake(t, revokedServerPresenting(revokedLeaf, forged), clientConfig)
		require.ErrorContains(t, res.clientErr, "cannot check the revocation of certificate CommonName="+certs.RevokedServerName+": a CRL signed by the key of its issuer "+intermediate.Subject.CommonName+" is configured, but none of the certificates found for that issuer may sign CRLs")
	})

	t.Run("a forged issuer presented ahead of the configured one does not hide the CRL", func(t *testing.T) {
		clientConfig, err := ClientConfig(Required, "", "", certs.ServerCA, certs.ServerCRL, certs.RevokedServerName, tls.VersionTLS12)
		require.NoError(t, err)

		res := handshake(t, revokedServerPresenting(revokedLeaf, forged), clientConfig)
		require.ErrorContains(t, res.clientErr, "Certificate revoked: CommonName="+certs.RevokedServerName)
	})

	t.Run("a forged issuer presented ahead of the real one does not hide the CRL", func(t *testing.T) {
		clientConfig, err := ClientConfig(Required, "", "", "", certs.ServerCRL, certs.RevokedServerName, tls.VersionTLS12)
		require.NoError(t, err)

		res := handshake(t, revokedServerPresenting(revokedLeaf, forged, intermediate), clientConfig)
		require.ErrorContains(t, res.clientErr, "Certificate revoked: CommonName="+certs.RevokedServerName)
	})
}

// forgedIssuer returns a CA certificate that carries issuer's subject
// and public key but not the CRL signing key usage, signed by a key
// of the forger's own.
func forgedIssuer(t *testing.T, issuer *x509.Certificate) *x509.Certificate {
	t.Helper()
	return forgedIssuerWithKeyUsage(t, issuer, x509.KeyUsageCertSign)
}

// forgedIssuerWithKeyUsage returns a CA certificate that carries
// issuer's subject and public key with the given key usage, signed by
// a key of the forger's own.
func forgedIssuerWithKeyUsage(t *testing.T, issuer *x509.Certificate, keyUsage x509.KeyUsage) *x509.Certificate {
	t.Helper()
	forgerKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	forger := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "forger"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	forgerDER, err := x509.CreateCertificate(rand.Reader, forger, forger, &forgerKey.PublicKey, forgerKey)
	require.NoError(t, err)
	forgerCert, err := x509.ParseCertificate(forgerDER)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: "forged"},
		RawSubject:            issuer.RawSubject,
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              keyUsage,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, forgerCert, issuer.PublicKey, forgerKey)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert
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

	t.Run("a CRL from the issuer of the trust anchor does not require that issuer to be configured", func(t *testing.T) {
		// The server trusts the clients' CA alone, while its CRL
		// file also holds the root's CRL. The root is not
		// configured, but verification vouches for the anchor.
		tlstest.CreateCRL(root, tlstest.CA)
		rootCRL, err := os.ReadFile(path.Join(root, "ca-crl.pem"))
		require.NoError(t, err)
		clientCRL, err := os.ReadFile(certs.ClientCRL)
		require.NoError(t, err)
		bothCRLs := path.Join(t.TempDir(), "both-crl.pem")
		require.NoError(t, os.WriteFile(bothCRLs, append(clientCRL, rootCRL...), 0o600))

		res := handshake(t, newServerConfig(t, bothCRLs), newClientConfig(t, certs.ClientCert, certs.ClientKey))
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

	leaf := loadOneCert(t, certs.ServerCert)
	intermediate := loadOneCert(t, certs.ServerCA)
	rootCert := loadOneCert(t, rootCA)

	checker, err := newCRLChecker(rootCRL, rootCA)
	require.NoError(t, err)

	err = checker.verifyConnection(tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{leaf},
		VerifiedChains:   [][]*x509.Certificate{{leaf, intermediate, rootCert}},
	})
	require.ErrorContains(t, err, "Certificate revoked: CommonName="+intermediate.Subject.CommonName)

	t.Run("a configured CA that a verified chain ends at is checked against its issuer's CRL", func(t *testing.T) {
		// The intermediate is configured as well as the root, so
		// verification of a leaf presented alone builds the one
		// chain that ends at the intermediate. It is the root's
		// CRL that revokes it, and the root is configured.
		intermediatePEM, err := os.ReadFile(certs.ServerCA)
		require.NoError(t, err)
		rootPEM, err := os.ReadFile(rootCA)
		require.NoError(t, err)
		bundle := path.Join(t.TempDir(), "bundle.pem")
		require.NoError(t, os.WriteFile(bundle, append(intermediatePEM, rootPEM...), 0o600))
		checker, err := newCRLChecker(rootCRL, bundle)
		require.NoError(t, err)

		err = checker.verifyConnection(tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{leaf},
			VerifiedChains:   [][]*x509.Certificate{{leaf, intermediate}},
		})
		require.ErrorContains(t, err, "Certificate revoked: CommonName="+intermediate.Subject.CommonName)
	})

	t.Run("certificates presented beyond the verified chain cost nothing", func(t *testing.T) {
		// The check of a verified chain spends the same signature
		// checks whether or not the peer presented other certificates
		// after its own, since those are not inspected.
		validLeaf := loadOneCert(t, certs.ServerCert)
		chain := [][]*x509.Certificate{{validLeaf, intermediate, rootCert}}
		checker, err := newCRLChecker(certs.ServerCRL, rootCA)
		require.NoError(t, err)

		alone := checker.newCheck([]*x509.Certificate{validLeaf}, chain)
		require.NoError(t, alone.run())
		padded := checker.newCheck(append([]*x509.Certificate{validLeaf}, selfSignedCACerts(t, nil, 50)...), chain)
		require.NoError(t, padded.run())
		require.Equal(t, alone.signatureChecks, padded.signatureChecks)
		require.Positive(t, alone.signatureChecks)
	})
}

// selfSignedCACerts returns n distinct self-signed CA certificates,
// each with its own key. They all carry rawSubject when it is set,
// and each its own subject otherwise.
func selfSignedCACerts(t *testing.T, rawSubject []byte, n int) []*x509.Certificate {
	t.Helper()
	certs := make([]*x509.Certificate, 0, n)
	for i := range n {
		cert, _ := selfSignedCA(t, int64(i+1), fmt.Sprintf("decoy %d", i+1), rawSubject)
		certs = append(certs, cert)
	}
	return certs
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

// multiValuedRDN encodes a distinguished name made of one RDN that
// holds the given common names, in the given order.
func multiValuedRDN(t *testing.T, commonNames ...string) []byte {
	t.Helper()
	var rdn pkix.RelativeDistinguishedNameSET
	for _, commonName := range commonNames {
		rdn = append(rdn, pkix.AttributeTypeAndValue{
			Type:  asn1.ObjectIdentifier{2, 5, 4, 3},
			Value: asn1.RawValue{Class: asn1.ClassUniversal, Tag: asn1.TagPrintableString, Bytes: []byte(commonName)},
		})
	}
	raw, err := asn1.Marshal(pkix.RDNSequence{rdn})
	require.NoError(t, err)
	return raw
}

// TestCRLCheckerMultiValuedRDN checks that a CRL whose issuer name
// holds the attributes of a multi-valued RDN in another order, and
// another case, than the CA certificate's subject is still bound to
// that CA: the attributes of an RDN form a set.
func TestCRLCheckerMultiValuedRDN(t *testing.T) {
	dir := t.TempDir()
	ca, caKey := selfSignedCA(t, 1, "", multiValuedRDN(t, "Bob", "amy"))
	caFile := path.Join(dir, "ca-cert.pem")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw}), 0o600))

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	leafDER, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "leaf.example.com"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}, ca, &leafKey.PublicKey, caKey)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(leafDER)
	require.NoError(t, err)

	reordered := *ca
	reordered.RawSubject = multiValuedRDN(t, "AMY", "bob")
	require.NotEqual(t, ca.RawSubject, reordered.RawSubject)
	crlDER, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number:                    big.NewInt(1),
		ThisUpdate:                time.Now().Add(-time.Hour),
		NextUpdate:                time.Now().Add(time.Hour),
		RevokedCertificateEntries: []x509.RevocationListEntry{{SerialNumber: leaf.SerialNumber, RevocationTime: time.Now().Add(-time.Hour)}},
	}, &reordered, caKey)
	require.NoError(t, err)
	crlFile := path.Join(dir, "reordered-crl.pem")
	require.NoError(t, os.WriteFile(crlFile, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: crlDER}), 0o600))

	checker, err := newCRLChecker(crlFile, caFile)
	require.NoError(t, err)
	err = checker.verifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}})
	require.ErrorContains(t, err, "Certificate revoked: CommonName=leaf.example.com")
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
	padded := append([]*x509.Certificate{leaf}, selfSignedCACerts(t, leaf.RawIssuer, 200)...)

	t.Run("the search stops and fails closed once the signature checks are spent", func(t *testing.T) {
		checker, err := newCRLChecker(certs.ServerCRL, "")
		require.NoError(t, err)

		start := time.Now()
		err = checker.verifyConnection(tls.ConnectionState{PeerCertificates: padded})
		t.Logf("checked a %d certificate chain in %s", len(padded), time.Since(start))
		require.ErrorContains(t, err, fmt.Sprintf("cannot check the revocation of certificate CommonName=%s: checking it exceeded the %d signature checks allowed per connection", certs.RevokedServerName, maxSignatureChecks))
	})

	t.Run("the configured issuer is consulted before the presented certificates", func(t *testing.T) {
		checker, err := newCRLChecker(certs.ServerCRL, certs.ServerCA)
		require.NoError(t, err)

		err = checker.verifyConnection(tls.ConnectionState{PeerCertificates: padded})
		require.ErrorContains(t, err, "Certificate revoked: CommonName="+certs.RevokedServerName)
	})

	t.Run("self-signed padding that no CRL applies to costs nothing", func(t *testing.T) {
		// The padding carries names that no CRL comes from, so it
		// is not worth a signature check, and the check spends the
		// same on the chain with and without it.
		checker, err := newCRLChecker(certs.ServerCRL, certs.ServerCA)
		require.NoError(t, err)
		valid := loadOneCert(t, certs.ServerCert)
		alone := checker.newCheck([]*x509.Certificate{valid}, nil)
		require.NoError(t, alone.run())
		selfSignedPadded := checker.newCheck(append([]*x509.Certificate{valid}, selfSignedCACerts(t, nil, 200)...), nil)
		require.NoError(t, selfSignedPadded.run())
		require.Equal(t, alone.signatureChecks, selfSignedPadded.signatureChecks)
		require.Positive(t, alone.signatureChecks)
	})

	// manyCRLs holds as many CRLs from the leaf's issuer as the
	// budget allows, each for a distribution point of its own, as
	// partitioned CRLs are.
	manyCRLs := partitionedCRLs(t, certs.ServerCA, strings.TrimSuffix(certs.ServerCA, "-cert.pem")+"-key.pem", maxSignatureChecks)

	t.Run("the CRLs of a configured issuer cost no signature checks per connection", func(t *testing.T) {
		// They are validated once, when the checker is built, so
		// the connection only spends the check that finds the
		// leaf's issuer.
		checker, err := newCRLChecker(manyCRLs, certs.ServerCA)
		require.NoError(t, err)

		check := checker.newCheck([]*x509.Certificate{loadOneCert(t, certs.ServerCert)}, nil)
		require.NoError(t, check.run())
		require.Equal(t, 1, check.signatureChecks)
	})

	t.Run("the CRLs of an issuer that is only presented count against the signature checks", func(t *testing.T) {
		checker, err := newCRLChecker(manyCRLs, "")
		require.NoError(t, err)

		err = checker.verifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{loadOneCert(t, certs.ServerCert), loadOneCert(t, certs.ServerCA)}})
		require.ErrorContains(t, err, fmt.Sprintf("exceeded the %d signature checks allowed per connection", maxSignatureChecks))
	})
}

// TestCRLCheckerIssuersSharingASubject checks that a CRL from a CA is
// not held against the certificates of another CA that carries the
// same subject with a different key, as a re-keyed CA and its
// predecessor do.
func TestCRLCheckerIssuersSharingASubject(t *testing.T) {
	root := t.TempDir()
	tlstest.CreateCA(root)
	tlstest.CreateIntermediateCA(root, tlstest.CA, "01", "old-ca", "Shared CA")
	tlstest.CreateIntermediateCA(root, tlstest.CA, "02", "new-ca", "Shared CA")
	tlstest.CreateSignedCert(root, "old-ca", "03", "old-leaf", "old.example.com")
	tlstest.CreateCRL(root, "new-ca")
	oldCA := loadOneCert(t, path.Join(root, "old-ca-cert.pem"))
	newCA := loadOneCert(t, path.Join(root, "new-ca-cert.pem"))
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

	err = checker.verifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{loadOneCert(t, path.Join(root, "old-leaf-cert.pem"))}})
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

		err = checker.verifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{loadOneCert(t, path.Join(root, "old-leaf-cert.pem"))}})
		require.ErrorContains(t, err, "Certificate revoked: CommonName=old.example.com")
	})
}

// crlWithIssuerName writes a CRL signed by the CA whose certificate
// and key files are given, revoking the given serial number, but with
// the issuer's common name written as commonName with the given ASN.1
// string tag rather than as the CA certificate's subject has it, as a
// CRL produced by another tool than the certificate can be.
func crlWithIssuerName(t *testing.T, caCert, caKey string, serial *big.Int, commonName string, tag int, nextUpdate time.Time) string {
	t.Helper()
	ca := loadOneCert(t, caCert)
	keyPair, err := tls.LoadX509KeyPair(caCert, caKey)
	require.NoError(t, err)
	renamed := *ca
	renamed.RawSubject = encodedCommonName(t, commonName, tag)
	require.NotEqual(t, ca.RawSubject, renamed.RawSubject)

	der, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number:     big.NewInt(1),
		ThisUpdate: nextUpdate.Add(-2 * time.Hour),
		NextUpdate: nextUpdate,
		RevokedCertificateEntries: []x509.RevocationListEntry{{
			SerialNumber:   serial,
			RevocationTime: nextUpdate.Add(-2 * time.Hour),
		}},
	}, &renamed, keyPair.PrivateKey.(crypto.Signer))
	require.NoError(t, err)
	file := path.Join(t.TempDir(), "renamed-crl.pem")
	require.NoError(t, os.WriteFile(file, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: der}), 0o600))
	return file
}

// TestCRLCheckerIssuerNameEncoding checks that a CRL whose issuer name
// is encoded differently from the subject of the CA certificate is
// still bound to that CA by its signature, as it was before the
// issuer names were compared.
func TestCRLCheckerIssuerNameEncoding(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	revokedLeaf := loadOneCert(t, certs.RevokedServerCert)
	intermediate := loadOneCert(t, certs.ServerCA)
	crl := crlWithIssuerName(t, certs.ServerCA, strings.TrimSuffix(certs.ServerCA, "-cert.pem")+"-key.pem", revokedLeaf.SerialNumber, intermediate.Subject.CommonName, asn1.TagUTF8String, time.Now().Add(time.Hour))

	t.Run("with the issuer configured", func(t *testing.T) {
		checker, err := newCRLChecker(crl, certs.ServerCA)
		require.NoError(t, err)

		err = checker.verifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{revokedLeaf}})
		require.ErrorContains(t, err, "Certificate revoked: CommonName="+certs.RevokedServerName)
	})

	t.Run("with the issuer presented", func(t *testing.T) {
		checker, err := newCRLChecker(crl, "")
		require.NoError(t, err)

		err = checker.verifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{revokedLeaf, intermediate}})
		require.ErrorContains(t, err, "Certificate revoked: CommonName="+certs.RevokedServerName)
	})

	t.Run("with the issuer missing, the check fails closed rather than silently", func(t *testing.T) {
		checker, err := newCRLChecker(crl, "")
		require.NoError(t, err)

		err = checker.verifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{revokedLeaf}})
		require.ErrorContains(t, err, "no certificate is available for its issuer")
	})
}

// TestNewCRLCheckerDeltaCRL checks that a delta CRL is refused: the
// checker evaluates each CRL on its own, and a delta CRL's entries
// only make sense together with the base CRL it amends, an entry that
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
	file := path.Join(t.TempDir(), "delta-crl.pem")
	require.NoError(t, os.WriteFile(file, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: der}), 0o600))

	_, err = newCRLChecker(file, certs.ServerCA)
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
// algorithm that Go cannot verify, and one that names the issuer's
// certificate as its authority while its signature does not verify.
func TestCRLCheckerCRLThatCannotBeValidated(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	ca := loadOneCert(t, certs.ServerCA)
	keyPair, err := tls.LoadX509KeyPair(certs.ServerCA, strings.TrimSuffix(certs.ServerCA, "-cert.pem")+"-key.pem")
	require.NoError(t, err)
	leaf := loadOneCert(t, certs.ServerCert)
	template := &x509.RevocationList{Number: big.NewInt(1), ThisUpdate: time.Now().Add(-time.Hour), NextUpdate: time.Now().Add(time.Hour)}

	validDER, err := x509.CreateRevocationList(rand.Reader, template, ca, keyPair.PrivateKey.(crypto.Signer))
	require.NoError(t, err)
	valid, err := x509.ParseRevocationList(validDER)
	require.NoError(t, err)

	// The signature algorithm is rewritten, inside and outside the
	// signed part, to an identifier that Go does not know.
	unknownAlgorithm := slices.Clone(valid.RawSignatureAlgorithm)
	unknownAlgorithm[len(unknownAlgorithm)-1] = 0x09
	unsupportedDER := bytes.ReplaceAll(validDER, valid.RawSignatureAlgorithm, unknownAlgorithm)
	require.Equal(t, 2, bytes.Count(validDER, valid.RawSignatureAlgorithm))
	unsupported, err := x509.ParseRevocationList(unsupportedDER)
	require.NoError(t, err)
	require.Equal(t, x509.UnknownSignatureAlgorithm, unsupported.SignatureAlgorithm)

	corruptDER := slices.Clone(validDER)
	corruptDER[len(corruptDER)-1] ^= 0xff
	corrupt, err := x509.ParseRevocationList(corruptDER)
	require.NoError(t, err)
	require.Equal(t, ca.SubjectKeyId, corrupt.AuthorityKeyId)

	for _, tc := range []struct {
		name string
		der  []byte
		want string
	}{
		{"unsupported signature algorithm", unsupportedDER, "cannot be validated"},
		{"corrupt signature", corruptDER, "its signature does not verify"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := crlFile(t, tc.der)

			t.Run("with the issuer configured, the checker is refused", func(t *testing.T) {
				_, err := newCRLChecker(file, certs.ServerCA)
				require.ErrorContains(t, err, tc.want)
			})

			t.Run("with the issuer presented, the connection is rejected", func(t *testing.T) {
				checker, err := newCRLChecker(file, "")
				require.NoError(t, err)

				err = checker.verifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf, ca}})
				require.ErrorContains(t, err, tc.want)
			})
		})
	}
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
	completeCRL := func(number int64, issued time.Time, serials ...*big.Int) []byte {
		template := &x509.RevocationList{Number: big.NewInt(number), ThisUpdate: issued, NextUpdate: issued.Add(24 * time.Hour)}
		for _, serial := range serials {
			template.RevokedCertificateEntries = append(template.RevokedCertificateEntries, x509.RevocationListEntry{SerialNumber: serial, RevocationTime: issued})
		}
		der, err := x509.CreateRevocationList(rand.Reader, template, ca, keyPair.PrivateKey.(crypto.Signer))
		require.NoError(t, err)
		return der
	}
	crlsFile := func(crls ...[]byte) string {
		var content []byte
		for _, der := range crls {
			content = append(content, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: der})...)
		}
		file := path.Join(t.TempDir(), "crls.pem")
		require.NoError(t, os.WriteFile(file, content, 0o600))
		return file
	}
	older := time.Now().Add(-2 * time.Hour)
	newer := time.Now().Add(-time.Hour)
	holdThenReleased := []struct {
		name string
		file string
	}{
		{"older first", crlsFile(completeCRL(1, older, leaf.SerialNumber), completeCRL(2, newer))},
		{"newer first", crlsFile(completeCRL(2, newer), completeCRL(1, older, leaf.SerialNumber))},
	}
	for _, tc := range holdThenReleased {
		t.Run("a certificate the newest CRL dropped is not revoked, "+tc.name, func(t *testing.T) {
			checker, err := newCRLChecker(tc.file, certs.ServerCA)
			require.NoError(t, err)
			require.NoError(t, checker.verifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}))
		})
	}

	t.Run("a certificate the newest CRL lists is revoked", func(t *testing.T) {
		checker, err := newCRLChecker(crlsFile(completeCRL(1, older), completeCRL(2, newer, leaf.SerialNumber)), certs.ServerCA)
		require.NoError(t, err)
		err = checker.verifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}})
		require.ErrorContains(t, err, "Certificate revoked: CommonName="+certs.ServerName)
	})
}

// TestCRLCheckerWrapperCannotOverrideConfiguredIssuer checks that when
// the configured issuer certificate is not allowed to sign CRLs while
// its key signed the configured CRL, which fails closed, a peer cannot
// lift that by presenting a wrapper certificate that carries the
// issuer's key and subject along with the CRL signing key usage.
func TestCRLCheckerWrapperCannotOverrideConfiguredIssuer(t *testing.T) {
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
	dir := t.TempDir()
	caFile := path.Join(dir, "ca-cert.pem")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw}), 0o600))

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	leafDER, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "leaf.example.com"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}, ca, &leafKey.PublicKey, caKey)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(leafDER)
	require.NoError(t, err)

	crlSigner := *ca
	crlSigner.KeyUsage |= x509.KeyUsageCRLSign
	crlDER, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number:     big.NewInt(1),
		ThisUpdate: time.Now().Add(-time.Hour),
		NextUpdate: time.Now().Add(time.Hour),
	}, &crlSigner, caKey)
	require.NoError(t, err)
	checker, err := newCRLChecker(crlFile(t, crlDER), caFile)
	require.NoError(t, err)

	orphaned := "none of the certificates found for that issuer may sign CRLs"
	t.Run("the configured issuer fails closed on its own", func(t *testing.T) {
		err := checker.verifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}})
		require.ErrorContains(t, err, orphaned)
	})

	t.Run("a presented wrapper allowed to sign CRLs does not lift it", func(t *testing.T) {
		wrapper := forgedIssuerWithKeyUsage(t, ca, x509.KeyUsageCertSign|x509.KeyUsageCRLSign)
		err := checker.verifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf, wrapper}})
		require.ErrorContains(t, err, orphaned)
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

// TestCRLCheckerIssuerNameMatching checks that a CRL whose issuer name
// differs from the subject of the CA certificate only in case and in
// insignificant whitespace, which X.509 treats as the same name, is
// still bound to that CA, while a CRL from a differently named CA is
// still not held against the CA's certificates.
func TestCRLCheckerIssuerNameMatching(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	revokedLeaf := loadOneCert(t, certs.RevokedServerCert)
	intermediate := loadOneCert(t, certs.ServerCA)
	intermediateKey := strings.TrimSuffix(certs.ServerCA, "-cert.pem") + "-key.pem"
	sameName := "  " + strings.ToUpper(intermediate.Subject.CommonName) + "  "
	crl := crlWithIssuerName(t, certs.ServerCA, intermediateKey, revokedLeaf.SerialNumber, sameName, asn1.TagPrintableString, time.Now().Add(time.Hour))

	t.Run("with the issuer configured", func(t *testing.T) {
		checker, err := newCRLChecker(crl, certs.ServerCA)
		require.NoError(t, err)

		err = checker.verifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{revokedLeaf}})
		require.ErrorContains(t, err, "Certificate revoked: CommonName="+certs.RevokedServerName)
	})

	t.Run("with the issuer missing, the check fails closed rather than silently", func(t *testing.T) {
		checker, err := newCRLChecker(crl, "")
		require.NoError(t, err)

		err = checker.verifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{revokedLeaf}})
		require.ErrorContains(t, err, "no certificate is available for its issuer")
	})

	t.Run("a differently named issuer's CRL still does not apply", func(t *testing.T) {
		otherName := crlWithIssuerName(t, certs.ServerCA, intermediateKey, revokedLeaf.SerialNumber, intermediate.Subject.CommonName+" Other", asn1.TagPrintableString, time.Now().Add(time.Hour))
		checker, err := newCRLChecker(otherName, "")
		require.NoError(t, err)

		err = checker.verifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{revokedLeaf}})
		require.NoError(t, err)
	})
}

// TestCertIsRevokedWarnsPerExpiredCRL checks that each expired CRL gets
// its own throttled warning, so that one stale CRL that is consulted
// often does not hide another one that also needs updating.
func TestCertIsRevokedWarnsPerExpiredCRL(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	cert := loadOneCert(t, certs.ServerCert)
	expired := time.Now().Add(-time.Hour)
	var crls []*x509.RevocationList
	for _, ca := range []struct{ cert, name string }{
		{certs.ServerCA, "Expired Servers CA"},
		{certs.ClientCA, "Expired Clients CA"},
	} {
		file := crlWithIssuerName(t, ca.cert, strings.TrimSuffix(ca.cert, "-cert.pem")+"-key.pem", big.NewInt(1), ca.name, asn1.TagPrintableString, expired)
		loaded, err := loadCRLSet(file)
		require.NoError(t, err)
		crls = append(crls, loaded...)
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
		ca := loadOneCert(t, certs.ServerCA)
		keyPair, err := tls.LoadX509KeyPair(certs.ServerCA, strings.TrimSuffix(certs.ServerCA, "-cert.pem")+"-key.pem")
		require.NoError(t, err)
		var keys []string
		for number := int64(1); number <= 2; number++ {
			der, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
				Number:     big.NewInt(number),
				ThisUpdate: expired.Add(-2 * time.Hour),
				NextUpdate: expired,
			}, ca, keyPair.PrivateKey.(crypto.Signer))
			require.NoError(t, err)
			crl, err := x509.ParseRevocationList(der)
			require.NoError(t, err)
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
		crl := loadOneCRL(t, crlWithIssuerName(t, certs.ServerCA, strings.TrimSuffix(certs.ServerCA, "-cert.pem")+"-key.pem", big.NewInt(7), "Expired At Once CA", asn1.TagPrintableString, expired))
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

// TestNameKey pins which distinguished names the CRL matching treats
// as the same: the X.509 matching rules make attribute values compare
// without regard to case or insignificant whitespace and the
// attributes of an RDN compare as a set, while delimiter characters
// inside a value must not make distinct names collide.
func TestNameKey(t *testing.T) {
	commonName := asn1.ObjectIdentifier{2, 5, 4, 3}
	organization := asn1.ObjectIdentifier{2, 5, 4, 10}
	attribute := func(oid asn1.ObjectIdentifier, value string) pkix.AttributeTypeAndValue {
		return pkix.AttributeTypeAndValue{Type: oid, Value: asn1.RawValue{Class: asn1.ClassUniversal, Tag: asn1.TagPrintableString, Bytes: []byte(value)}}
	}
	utf8Attribute := func(oid asn1.ObjectIdentifier, value string) pkix.AttributeTypeAndValue {
		return pkix.AttributeTypeAndValue{Type: oid, Value: asn1.RawValue{Class: asn1.ClassUniversal, Tag: asn1.TagUTF8String, Bytes: []byte(value)}}
	}
	encodedAttribute := func(oid asn1.ObjectIdentifier, tag int, value []byte) pkix.AttributeTypeAndValue {
		return pkix.AttributeTypeAndValue{Type: oid, Value: asn1.RawValue{Class: asn1.ClassUniversal, Tag: tag, Bytes: value}}
	}
	name := func(rdns ...pkix.RelativeDistinguishedNameSET) []byte {
		raw, err := asn1.Marshal(pkix.RDNSequence(rdns))
		require.NoError(t, err)
		return raw
	}
	rdn := func(attributes ...pkix.AttributeTypeAndValue) pkix.RelativeDistinguishedNameSET { return attributes }

	same := []struct {
		name string
		a, b []byte
	}{
		{"case", name(rdn(attribute(commonName, "Example CA"))), name(rdn(attribute(commonName, "EXAMPLE ca")))},
		{"whitespace", name(rdn(attribute(commonName, "Example CA"))), name(rdn(attribute(commonName, "  Example   CA ")))},
		{"attribute order within an RDN", name(rdn(attribute(commonName, "Bob"), attribute(commonName, "amy"))), name(rdn(attribute(commonName, "AMY"), attribute(commonName, "bob")))},
		{"Unicode normalization", name(rdn(utf8Attribute(commonName, "Jos\u00e9"))), name(rdn(utf8Attribute(commonName, "Jose\u0301")))},
		{"case folding beyond ASCII", name(rdn(utf8Attribute(commonName, "Stra\u00dfe"))), name(rdn(utf8Attribute(commonName, "STRASSE")))},
		{"UniversalString and UTF8String", name(rdn(utf8Attribute(commonName, "Jos\u00e9"))), name(rdn(encodedAttribute(commonName, 28, utf32BigEndian("Jos\u00e9"))))},
		{"BMPString and UTF8String", name(rdn(utf8Attribute(commonName, "Jos\u00e9"))), name(rdn(encodedAttribute(commonName, asn1.TagBMPString, utf16BigEndian("Jos\u00e9"))))},
		{"T61String and UTF8String, within ASCII", name(rdn(utf8Attribute(commonName, "Example CA"))), name(rdn(encodedAttribute(commonName, asn1.TagT61String, []byte("Example CA"))))},
		{"T61String read as Latin-1 and UTF8String", name(rdn(utf8Attribute(commonName, "Jos\u00e9"))), name(rdn(encodedAttribute(commonName, asn1.TagT61String, []byte{'J', 'o', 's', 0xe9})))},
	}
	for _, tc := range same {
		t.Run("same "+tc.name, func(t *testing.T) {
			require.Equal(t, nameKey(tc.a), nameKey(tc.b))
		})
	}

	different := []struct {
		name string
		a, b []byte
	}{
		{"values", name(rdn(attribute(commonName, "Example CA"))), name(rdn(attribute(commonName, "Example CA 2")))},
		{"attribute types", name(rdn(attribute(commonName, "Example"))), name(rdn(attribute(organization, "Example")))},
		{"RDN order", name(rdn(attribute(commonName, "a")), rdn(attribute(organization, "b"))), name(rdn(attribute(organization, "b")), rdn(attribute(commonName, "a")))},
		{"one attribute holding delimiters versus two attributes", name(rdn(attribute(commonName, "a+2.5.4.3=b"))), name(rdn(attribute(commonName, "a"), attribute(commonName, "b")))},
		{"one RDN versus two", name(rdn(attribute(commonName, "a"), attribute(organization, "b"))), name(rdn(attribute(commonName, "a")), rdn(attribute(organization, "b")))},
		{"T61String beyond ASCII and its UTF8String reading", name(rdn(utf8Attribute(commonName, "Jos\u00e9"))), name(rdn(encodedAttribute(commonName, asn1.TagT61String, []byte("Jos\u00e9"))))},
	}
	for _, tc := range different {
		t.Run("different "+tc.name, func(t *testing.T) {
			require.NotEqual(t, nameKey(tc.a), nameKey(tc.b))
		})
	}
}

// loadOneCRL loads the CRL file and returns its only CRL.
func loadOneCRL(t *testing.T, file string) *x509.RevocationList {
	t.Helper()
	crls, err := loadCRLSet(file)
	require.NoError(t, err)
	require.Len(t, crls, 1)
	return crls[0]
}

// encodedCommonName encodes a distinguished name made of the given
// common name alone, written with the given ASN.1 string tag.
func encodedCommonName(t *testing.T, commonName string, tag int) []byte {
	t.Helper()
	raw, err := asn1.Marshal(pkix.RDNSequence{{pkix.AttributeTypeAndValue{
		Type:  asn1.ObjectIdentifier{2, 5, 4, 3},
		Value: asn1.RawValue{Class: asn1.ClassUniversal, Tag: tag, Bytes: []byte(commonName)},
	}}})
	require.NoError(t, err)
	return raw
}

// TestCRLCheckerIssuerLookupNameEncoding checks that a certificate
// whose issuer name is encoded differently from the subject of the
// presented certificate that issued it still finds its issuer, so that
// the issuer's CRL is held against it: names are matched under the
// X.509 rules for the lookup too, with the signature as the binding.
func TestCRLCheckerIssuerLookupNameEncoding(t *testing.T) {
	rootCA, rootKey := selfSignedCA(t, 1, "Root CA", nil)

	// The intermediate names its issuer as a UTF8String where the
	// root's subject is a PrintableString.
	intermediateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	renamedRoot := *rootCA
	renamedRoot.RawSubject = encodedCommonName(t, rootCA.Subject.CommonName, asn1.TagUTF8String)
	require.NotEqual(t, rootCA.RawSubject, renamedRoot.RawSubject)
	intermediateDER, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: "Intermediate CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}, &renamedRoot, &intermediateKey.PublicKey, rootKey)
	require.NoError(t, err)
	intermediate, err := x509.ParseCertificate(intermediateDER)
	require.NoError(t, err)
	require.NotEqual(t, rootCA.RawSubject, intermediate.RawIssuer)

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	leafDER, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "leaf.example.com"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}, intermediate, &leafKey.PublicKey, intermediateKey)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(leafDER)
	require.NoError(t, err)

	crlDER, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number:                    big.NewInt(1),
		ThisUpdate:                time.Now().Add(-time.Hour),
		NextUpdate:                time.Now().Add(time.Hour),
		RevokedCertificateEntries: []x509.RevocationListEntry{{SerialNumber: intermediate.SerialNumber, RevocationTime: time.Now().Add(-time.Hour)}},
	}, rootCA, rootKey)
	require.NoError(t, err)
	crlFile := path.Join(t.TempDir(), "root-crl.pem")
	require.NoError(t, os.WriteFile(crlFile, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: crlDER}), 0o600))

	// No CA is configured: the issuers are found among the presented
	// certificates, as in required mode.
	checker, err := newCRLChecker(crlFile, "")
	require.NoError(t, err)
	err = checker.verifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf, intermediate, rootCA}})
	require.ErrorContains(t, err, "Certificate revoked: CommonName=Intermediate CA")
}

// utf32BigEndian encodes text as a UniversalString's bytes.
func utf32BigEndian(text string) []byte {
	var encoded []byte
	for _, r := range text {
		encoded = append(encoded, byte(r>>24), byte(r>>16), byte(r>>8), byte(r))
	}
	return encoded
}

// utf16BigEndian encodes text as a BMPString's bytes.
func utf16BigEndian(text string) []byte {
	var encoded []byte
	for _, unit := range utf16.Encode([]rune(text)) {
		encoded = append(encoded, byte(unit>>8), byte(unit))
	}
	return encoded
}

// partitionedCRLs writes a file holding n complete CRLs from the given
// CA, each scoped to a distribution point of its own by its issuing
// distribution point extension, and returns its path.
func partitionedCRLs(t *testing.T, caCert, caKey string, n int) string {
	t.Helper()
	ca := loadOneCert(t, caCert)
	keyPair, err := tls.LoadX509KeyPair(caCert, caKey)
	require.NoError(t, err)
	var content []byte
	for i := range n {
		uri, err := asn1.Marshal(asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 6, Bytes: fmt.Appendf(nil, "http://crl.example.com/%d", i)})
		require.NoError(t, err)
		fullName, err := asn1.Marshal(asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 0, IsCompound: true, Bytes: uri})
		require.NoError(t, err)
		distributionPoint, err := asn1.Marshal(struct{ DistributionPoint asn1.RawValue }{asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 0, IsCompound: true, Bytes: fullName}})
		require.NoError(t, err)
		der, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
			Number:          big.NewInt(int64(i + 1)),
			ThisUpdate:      time.Now().Add(-time.Hour),
			NextUpdate:      time.Now().Add(time.Hour),
			ExtraExtensions: []pkix.Extension{{Id: asn1.ObjectIdentifier{2, 5, 29, 28}, Critical: true, Value: distributionPoint}},
		}, ca, keyPair.PrivateKey.(crypto.Signer))
		require.NoError(t, err)
		content = append(content, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: der})...)
	}
	file := path.Join(t.TempDir(), "partitioned-crls.pem")
	require.NoError(t, os.WriteFile(file, content, 0o600))
	return file
}
