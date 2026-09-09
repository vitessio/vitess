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
	"math/big"
	"os"
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

	// Revoke the intermediate CA that issued the server certificate,
	// under the root CA, and have servers present that chain with
	// and without padding. The client has no CA configured, so the
	// issuers are only found among the certificates presented.
	intermediateName := strings.TrimSuffix(filepath.Base(certs.ServerCA), "-cert.pem")
	tlstest.RevokeCertAndRegenerateCRL(root, tlstest.CA, intermediateName)
	rootCRL := path.Join(root, "ca-crl.pem")
	leaf := loadOneCert(t, certs.ServerCert)
	intermediate := loadOneCert(t, certs.ServerCA)
	rootCert := loadOneCert(t, path.Join(root, "ca-cert.pem"))
	keyPair, err := tls.LoadX509KeyPair(certs.ServerCert, certs.ServerKey)
	require.NoError(t, err)
	serverPresenting := func(chain ...*x509.Certificate) *tls.Config {
		certificate := tls.Certificate{PrivateKey: keyPair.PrivateKey}
		for _, cert := range chain {
			certificate.Certificate = append(certificate.Certificate, cert.Raw)
		}
		return &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	}

	t.Run("a revoked intermediate that the server presents is rejected", func(t *testing.T) {
		clientConfig, err := ClientConfig(Required, "", "", "", rootCRL, certs.ServerName, tls.VersionTLS12)
		require.NoError(t, err)

		res := handshake(t, serverPresenting(leaf, intermediate, rootCert), clientConfig)
		require.ErrorContains(t, res.clientErr, "Certificate revoked: CommonName="+intermediate.Subject.CommonName)
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
		KeyUsage:              x509.KeyUsageCertSign,
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

	t.Run("a configured CA in the middle of a verified chain is checked against the CRL above it", func(t *testing.T) {
		// The intermediate is configured as well as the root, so
		// verification builds a chain that ends at it and one that
		// goes on to the root. It is the root's CRL that revokes it.
		intermediatePEM, err := os.ReadFile(certs.ServerCA)
		require.NoError(t, err)
		rootPEM, err := os.ReadFile(rootCA)
		require.NoError(t, err)
		bundle := path.Join(t.TempDir(), "bundle.pem")
		require.NoError(t, os.WriteFile(bundle, append(intermediatePEM, rootPEM...), 0o600))
		checker, err := newCRLChecker(rootCRL, bundle)
		require.NoError(t, err)

		err = checker.verifyConnection(tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{leaf, intermediate},
			VerifiedChains:   [][]*x509.Certificate{{leaf, intermediate}, {leaf, intermediate, rootCert}},
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
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		template := &x509.Certificate{
			SerialNumber:          big.NewInt(int64(i + 1)),
			Subject:               pkix.Name{CommonName: fmt.Sprintf("decoy %d", i+1)},
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

	t.Run("the CRL signature checks count against the signature checks", func(t *testing.T) {
		// A CRL file holding as many CRLs from the leaf's issuer as
		// the budget allows cannot all be verified once the issuer
		// itself has been found.
		crl, err := os.ReadFile(certs.ServerCRL)
		require.NoError(t, err)
		manyCRLs := path.Join(t.TempDir(), "many-crl.pem")
		require.NoError(t, os.WriteFile(manyCRLs, bytes.Repeat(crl, maxSignatureChecks), 0o600))
		checker, err := newCRLChecker(manyCRLs, certs.ServerCA)
		require.NoError(t, err)

		err = checker.verifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{loadOneCert(t, certs.ServerCert)}})
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
}

// crlWithReencodedIssuer writes a CRL signed by the CA whose
// certificate and key files are given, revoking the given serial
// number, with the CA's name encoded as a UTF8String rather than the
// PrintableString of the CA certificate, as a CRL produced by another
// tool than the certificate can be.
func crlWithReencodedIssuer(t *testing.T, caCert, caKey string, serial *big.Int) string {
	t.Helper()
	ca := loadOneCert(t, caCert)
	keyPair, err := tls.LoadX509KeyPair(caCert, caKey)
	require.NoError(t, err)
	reencoded := *ca
	reencoded.RawSubject, err = asn1.Marshal(pkix.RDNSequence{{pkix.AttributeTypeAndValue{
		Type:  asn1.ObjectIdentifier{2, 5, 4, 3},
		Value: asn1.RawValue{Class: asn1.ClassUniversal, Tag: asn1.TagUTF8String, Bytes: []byte(ca.Subject.CommonName)},
	}}})
	require.NoError(t, err)
	require.NotEqual(t, ca.RawSubject, reencoded.RawSubject)

	der, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number:     big.NewInt(1),
		ThisUpdate: time.Now().Add(-time.Hour),
		NextUpdate: time.Now().Add(time.Hour),
		RevokedCertificateEntries: []x509.RevocationListEntry{{
			SerialNumber:   serial,
			RevocationTime: time.Now().Add(-time.Hour),
		}},
	}, &reencoded, keyPair.PrivateKey.(crypto.Signer))
	require.NoError(t, err)
	file := path.Join(t.TempDir(), "reencoded-crl.pem")
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
	crl := crlWithReencodedIssuer(t, certs.ServerCA, strings.TrimSuffix(certs.ServerCA, "-cert.pem")+"-key.pem", revokedLeaf.SerialNumber)

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

// TestNewCRLCheckerEmptyCRLFile checks that a CRL file that holds no
// CRL is refused rather than silently enforcing nothing.
func TestNewCRLCheckerEmptyCRLFile(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	// A certificate is valid PEM but not a CRL.
	_, err := newCRLChecker(certs.ServerCert, "")
	require.ErrorContains(t, err, "no CRL found in file")
}
