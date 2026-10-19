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
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/tlstest"
)

// requireNoErrorFor requires err, from ReloadCachedFiles, to be about
// none of the files under dir. The files that other tests of the
// package cached, and may have removed since, can fail to reload.
func requireNoErrorFor(t *testing.T, err error, dir string) {
	t.Helper()
	if err != nil {
		require.NotContains(t, err.Error(), dir)
	}
}

// TestReloadCachedFiles checks, through real handshakes, that the
// configs ClientConfig builds keep the certificate, key and CA they
// loaded until ReloadCachedFiles reads the files again, and then use
// what the files hold, while a file that fails to load keeps what was
// loaded from it before.
func TestReloadCachedFiles(t *testing.T) {
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())

	live := t.TempDir()
	cert, key, ca, crl := path.Join(live, "cert.pem"), path.Join(live, "key.pem"), path.Join(live, "ca.pem"), path.Join(live, "crl.pem")
	copyFile := func(dst, src string) {
		b, err := os.ReadFile(src)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(dst, b, 0o600))
	}
	install := func(certs tlstest.ClientServerKeyPairs) {
		for dst, src := range map[string]string{cert: certs.ClientCert, key: certs.ClientKey, ca: certs.ServerCA, crl: certs.ServerCRL} {
			copyFile(dst, src)
		}
	}
	client := func(certs tlstest.ClientServerKeyPairs) *tls.Config {
		config, err := ClientConfig(VerifyIdentity, cert, key, ca, crl, certs.ServerName, tls.VersionTLS12)
		require.NoError(t, err)
		return config
	}
	// server requires a client certificate that the client CA of certs
	// issued.
	server := func(certs tlstest.ClientServerKeyPairs) *tls.Config {
		config, err := ReadServerConfig(certs.ServerCert, certs.ServerKey, certs.ClientCA, "", "", tls.VersionTLS12)
		require.NoError(t, err)
		return config
	}

	install(oldCerts)
	res := handshake(t, server(oldCerts), client(oldCerts))
	require.NoError(t, res.clientErr)
	require.NoError(t, res.serverErr)

	install(newCerts)
	res = handshake(t, server(newCerts), client(newCerts))
	require.Error(t, res.clientErr, "ClientConfig must keep the CA it loaded until the files are reloaded")

	generation := CachedFilesGeneration()
	changed, err := ReloadCachedFiles()
	requireNoErrorFor(t, err, live)
	require.True(t, changed)
	require.NotEqual(t, generation, CachedFilesGeneration())
	res = handshake(t, server(newCerts), client(newCerts))
	require.NoError(t, res.clientErr, "the client must trust the new CA")
	require.NoError(t, res.serverErr, "the client must present the new certificate")

	generation = CachedFilesGeneration()
	changed, err = ReloadCachedFiles()
	requireNoErrorFor(t, err, live)
	require.False(t, changed, "files that did not change must not count as changed")
	require.Equal(t, generation, CachedFilesGeneration())

	// ClientConfig reads the CRL on every call, but a change to it
	// must still count, for the configs built before it.
	copyFile(crl, newCerts.CombinedCRL)
	changed, err = ReloadCachedFiles()
	requireNoErrorFor(t, err, live)
	require.True(t, changed, "a changed CRL must count as changed")

	// A key that does not match its certificate fails to load, and the
	// pair loaded before stays in use.
	copyFile(key, oldCerts.ClientKey)
	_, err = ReloadCachedFiles()
	require.ErrorContains(t, err, key)
	res = handshake(t, server(newCerts), client(newCerts))
	require.NoError(t, res.clientErr)
	require.NoError(t, res.serverErr)
}

// TestReloadCachedFilesInvalidatesConfigsBuiltFromAnotherCRL checks
// that a config built while the CRL file briefly held B, between two
// times it held A, is invalidated once A is back and reloaded: a gRPC
// client's credentials are built again only when CachedFilesGeneration
// moves past the one they were built at, and would otherwise keep B,
// and the servers that A revokes and B does not, for good.
func TestReloadCachedFilesInvalidatesConfigsBuiltFromAnotherCRL(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	keyPair, err := tls.LoadX509KeyPair(certs.ServerCA, strings.TrimSuffix(certs.ServerCA, "-cert.pem")+"-key.pem")
	require.NoError(t, err)
	crlA, err := os.ReadFile(certs.ServerCRL)
	require.NoError(t, err)
	// B, from the same CA, revokes nothing.
	crlB := pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: crlWithoutExtensions(t, loadOneCert(t, certs.ServerCA), keyPair.PrivateKey.(*ecdsa.PrivateKey))})
	crl := path.Join(t.TempDir(), "crl.pem")
	client := func() {
		t.Helper()
		_, err := ClientConfig(VerifyIdentity, "", "", certs.ServerCA, crl, certs.RevokedServerName, tls.VersionTLS12)
		require.NoError(t, err)
	}
	require.NoError(t, os.WriteFile(crl, crlA, 0o600))
	client()

	// Credentials built now, as reloadingCreds does, record the
	// generation before they build.
	require.NoError(t, os.WriteFile(crl, crlB, 0o600))
	builtAt := CachedFilesGeneration()
	client()
	rebuiltAt := CachedFilesGeneration()

	require.NoError(t, os.WriteFile(crl, crlA, 0o600))
	_, err = ReloadCachedFiles()
	requireNoErrorFor(t, err, path.Dir(crl))
	require.NotEqual(t, builtAt, CachedFilesGeneration(), "credentials built from B must be built again once A is reloaded")
	require.NotEqual(t, rebuiltAt, CachedFilesGeneration(), "credentials built from B again after B was seen must be built again once A is reloaded")
}

// TestClientConfigKeepsLastValidCRL checks, through real handshakes,
// that once ClientConfig loaded a CRL file, a replacement it cannot
// use neither fails the configs it builds nor lets a server that the
// CRLs last loaded from the file revoke through, and that
// ReloadCachedFiles reports that replacement.
func TestClientConfigKeepsLastValidCRL(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	crl := path.Join(t.TempDir(), "crl.pem")
	b, err := os.ReadFile(certs.ServerCRL)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(crl, b, 0o600))

	revokedServer, err := ReadServerConfig(certs.RevokedServerCert, certs.RevokedServerKey, "", "", "", tls.VersionTLS12)
	require.NoError(t, err)
	client := func() (*tls.Config, error) {
		return ClientConfig(VerifyIdentity, "", "", certs.ServerCA, crl, certs.RevokedServerName, tls.VersionTLS12)
	}
	requireRejected := func() {
		t.Helper()
		config, err := client()
		require.NoError(t, err)
		res := handshake(t, revokedServer, config)
		require.ErrorContains(t, res.clientErr, "Certificate revoked: CommonName="+certs.RevokedServerName)
	}

	requireRejected()

	require.NoError(t, os.WriteFile(crl, []byte("not a CRL"), 0o600))
	requireRejected()
	_, err = ReloadCachedFiles()
	require.ErrorContains(t, err, crl)

	require.NoError(t, os.Remove(crl))
	requireRejected()

	// Without a CRL loaded from the file before, there is nothing to
	// hold against the server instead.
	unused := path.Join(t.TempDir(), "crl.pem")
	require.NoError(t, os.WriteFile(unused, []byte("not a CRL"), 0o600))
	_, err = ClientConfig(VerifyIdentity, "", "", certs.ServerCA, unused, certs.RevokedServerName, tls.VersionTLS12)
	require.Error(t, err)
}

// TestClientConfigFallsBackToTheLatestCRLRead checks, through real
// handshakes, that a ClientConfig call that read a CRL file before
// another, and finished validating it after the other stored its
// CRLs, does not leave the older CRLs as the ones to fall back to:
// once the file cannot be read, a server that the newer CRLs revoke
// is still rejected.
func TestClientConfigFallsBackToTheLatestCRLRead(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	keyPair, err := tls.LoadX509KeyPair(certs.ServerCA, strings.TrimSuffix(certs.ServerCA, "-cert.pem")+"-key.pem")
	require.NoError(t, err)
	crl := path.Join(t.TempDir(), "crl.pem")
	// A CRL from the server CA that revokes nothing yet.
	require.NoError(t, os.WriteFile(crl, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: crlWithoutExtensions(t, loadOneCert(t, certs.ServerCA), keyPair.PrivateKey.(*ecdsa.PrivateKey))}), 0o600))

	revokedServer, err := ReadServerConfig(certs.RevokedServerCert, certs.RevokedServerKey, "", "", "", tls.VersionTLS12)
	require.NoError(t, err)
	client := func() (*tls.Config, error) {
		return ClientConfig(VerifyIdentity, "", "", certs.ServerCA, crl, certs.RevokedServerName, tls.VersionTLS12)
	}
	requireRejected := func() {
		t.Helper()
		config, err := client()
		require.NoError(t, err)
		res := handshake(t, revokedServer, config)
		require.ErrorContains(t, res.clientErr, "Certificate revoked: CommonName="+certs.RevokedServerName)
	}

	// The first call reads the CRL that revokes nothing, and is held
	// before it validates it.
	read, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	crlReadTestHook = func() {
		if calls.Add(1) == 1 {
			close(read)
			<-release
		}
	}
	t.Cleanup(func() { crlReadTestHook = nil })
	built := make(chan error, 1)
	go func() {
		_, err := client()
		built <- err
	}()
	select {
	case <-read:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the first call did not read the CRL file in time")
	}

	// The second reads the CRL that revokes the server, and stores it.
	b, err := os.ReadFile(certs.ServerCRL)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(crl, b, 0o600))
	requireRejected()

	close(release)
	require.NoError(t, <-built)

	require.NoError(t, os.Remove(crl))
	requireRejected()
}

// TestClientConfigFallsBackUnderItsOwnCA checks that the CRLs that
// a configuration falls back to are its own: two configurations share
// a CRL file with different CA files, and a replacement holds a CRL
// that the second CA validates and one under the first CA's name that
// another key signed. The second configuration loading it, or a
// reload publishing it for that configuration, must not leave the
// first without the CRLs it loaded before, which still revoke the
// server.
func TestClientConfigFallsBackUnderItsOwnCA(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	crl := path.Join(t.TempDir(), "crl.pem")
	b, err := os.ReadFile(certs.CombinedCRL)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(crl, b, 0o600))

	revokedServer, err := ReadServerConfig(certs.RevokedServerCert, certs.RevokedServerKey, "", "", "", tls.VersionTLS12)
	require.NoError(t, err)
	requireRejected := func(msg string) {
		t.Helper()
		config, err := ClientConfig(VerifyIdentity, "", "", certs.ServerCA, crl, certs.RevokedServerName, tls.VersionTLS12)
		require.NoError(t, err, msg)
		res := handshake(t, revokedServer, config)
		require.ErrorContains(t, res.clientErr, "Certificate revoked: CommonName="+certs.RevokedServerName, msg)
	}
	other := func() {
		t.Helper()
		_, err := ClientConfig(VerifyIdentity, "", "", certs.ClientCA, crl, "", tls.VersionTLS12)
		require.NoError(t, err)
	}
	requireRejected("the first configuration must load the CRL")
	other()

	serverCA := loadOneCert(t, certs.ServerCA)
	imposter, imposterKey := selfSignedCA(t, 1, "", serverCA.RawSubject)
	clientCRL, err := os.ReadFile(certs.ClientCRL)
	require.NoError(t, err)
	replacement := append(pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: crlWithoutExtensions(t, imposter, imposterKey)}), clientCRL...)
	require.NoError(t, os.WriteFile(crl, replacement, 0o600))

	other()
	requireRejected("the second configuration loading the replacement must not take the first's CRLs")

	_, err = ReloadCachedFiles()
	require.ErrorContains(t, err, "cannot use the CRL file "+crl+" under the CA file "+certs.ServerCA)
	require.NotContains(t, err.Error(), "under the CA file "+certs.ClientCA)
	requireRejected("a reload publishing the replacement for the second configuration must not take the first's CRLs")
}

// TestClientConfigReadsCRLOutsideThePublishLock checks that a
// ClientConfig call reading its CRL file, which may stall on a slow or
// unresponsive file system, does not keep ReloadCachedFiles from
// publishing, nor the ClientConfig calls queued behind that publish
// from building their configs.
func TestClientConfigReadsCRLOutsideThePublishLock(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := newClientFiles(t)
	files.installKeyPair(oldCerts)
	files.installTrust(oldCerts)
	files.requireServes(oldCerts, oldCerts)
	// Another configuration's CA changes, so that the reload has
	// something to publish.
	files.copy(files.ca, newCerts.ServerCA)

	read, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	crlReadTestHook = func() {
		if calls.Add(1) == 1 {
			close(read)
			<-release
		}
	}
	t.Cleanup(func() { crlReadTestHook = nil })
	var releaseOnce sync.Once
	releaseRead := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseRead)

	built := make(chan error, 1)
	go func() {
		_, err := ClientConfig(VerifyIdentity, "", "", certs.ServerCA, certs.ServerCRL, certs.ServerName, tls.VersionTLS12)
		built <- err
	}()
	select {
	case <-read:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the call did not read the CRL file in time")
	}

	reloaded := make(chan error, 1)
	go func() {
		_, err := ReloadCachedFiles()
		reloaded <- err
	}()
	var reloadErr error
	published := assert.Eventually(t, func() bool {
		select {
		case reloadErr = <-reloaded:
			return true
		default:
			return false
		}
	}, 30*time.Second, 10*time.Millisecond, "a reload must publish while a ClientConfig call reads its CRL file")
	releaseRead()
	require.NoError(t, <-built)
	if !published {
		<-reloaded
		t.FailNow()
	}
	requireNoErrorFor(t, reloadErr, path.Dir(files.cert))
	files.requireServes(oldCerts, newCerts)
}

// TestClientConfigUsesTheCRLsOfAReloadThatOvertookItsRead checks that a
// ClientConfig call that read its CRL file before a reload published a
// new CA and CRL, and builds its config after, holds the published CRL
// against the peer rather than the one it read: the CRL it read names
// the old CA, which the new CA does not validate, so it would apply to
// no certificate of the new CA, and let one that the published CRL
// revokes through.
func TestClientConfigUsesTheCRLsOfAReloadThatOvertookItsRead(t *testing.T) {
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := newClientFiles(t)
	files.installTrust(oldCerts)
	client := func() (*tls.Config, error) {
		return ClientConfig(VerifyIdentity, "", "", files.ca, files.crl, newCerts.RevokedServerName, tls.VersionTLS12)
	}
	_, err := client()
	require.NoError(t, err)

	// The next call reads the old CRL, and is held before it builds.
	read, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	crlReadTestHook = func() {
		if calls.Add(1) == 1 {
			close(read)
			<-release
		}
	}
	t.Cleanup(func() { crlReadTestHook = nil })
	var releaseOnce sync.Once
	releaseRead := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseRead)
	type built struct {
		config *tls.Config
		err    error
	}
	results := make(chan built, 1)
	go func() {
		config, err := client()
		results <- built{config, err}
	}()
	select {
	case <-read:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the call did not read the CRL file in time")
	}

	// The CA and the CRL are rotated, and the reload publishes them.
	files.installTrust(newCerts)
	_, err = ReloadCachedFiles()
	requireNoErrorFor(t, err, path.Dir(files.ca))

	releaseRead()
	b := <-results
	require.NoError(t, b.err)
	revokedServer, err := ReadServerConfig(newCerts.RevokedServerCert, newCerts.RevokedServerKey, "", "", "", tls.VersionTLS12)
	require.NoError(t, err)
	res := handshake(t, revokedServer, b.config)
	require.ErrorContains(t, res.clientErr, "Certificate revoked: CommonName="+newCerts.RevokedServerName)
}

// TestCRLFallbackIsNotHeldBySlowReads checks that a read of a CRL file
// stalled on the file system does not hold back a ClientConfig call
// that already read the file from storing the CRLs it read: that call
// stores them under publishMu, which ReloadCachedFiles, and the calls
// queued behind it, then wait for.
func TestCRLFallbackIsNotHeldBySlowReads(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	client := func() error {
		_, err := ClientConfig(VerifyIdentity, "", "", certs.ServerCA, certs.ServerCRL, certs.ServerName, tls.VersionTLS12)
		return err
	}

	// The first call has read the file, and is held before it takes
	// publishMu. The second is held in its read of the file.
	firstRead, releaseFirst := make(chan struct{}), make(chan struct{})
	secondReading, releaseSecond := make(chan struct{}), make(chan struct{})
	var reads, fileReads atomic.Int32
	crlReadTestHook = func() {
		if reads.Add(1) == 1 {
			close(firstRead)
			<-releaseFirst
		}
	}
	crlFileReadTestHook = func() {
		if fileReads.Add(1) == 2 {
			close(secondReading)
			<-releaseSecond
		}
	}
	t.Cleanup(func() { crlReadTestHook, crlFileReadTestHook = nil, nil })
	var releaseFirstOnce, releaseSecondOnce sync.Once
	releaseFirstOnly := func() { releaseFirstOnce.Do(func() { close(releaseFirst) }) }
	releaseAll := func() {
		releaseFirstOnly()
		releaseSecondOnce.Do(func() { close(releaseSecond) })
	}
	t.Cleanup(releaseAll)
	waitFor := func(c chan struct{}, what string) {
		t.Helper()
		select {
		case <-c:
		case <-time.After(30 * time.Second):
			require.FailNow(t, what)
		}
	}

	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- client() }()
	waitFor(firstRead, "the first call did not read the CRL file in time")
	go func() { second <- client() }()
	waitFor(secondReading, "the second call did not start reading the CRL file in time")

	releaseFirstOnly()
	var firstErr error
	stored := assert.Eventually(t, func() bool {
		select {
		case firstErr = <-first:
			return true
		default:
			return false
		}
	}, 30*time.Second, 10*time.Millisecond, "a call that read the CRL file must not wait for another call's read to store it")
	releaseAll()
	if !stored {
		firstErr = <-first
	}
	require.NoError(t, firstErr)
	require.NoError(t, <-second)
}

// TestClientConfigChecksCRLsAgainstIssuersOfVerifiedChains checks,
// with a CA file that holds the root alone, that a replacement CRL from
// the intermediate the server presents, which the CA file cannot
// validate, is checked against that intermediate once a verified chain
// brought it: a CRL whose signature does not verify is then reported by
// ReloadCachedFiles, and the client falls back to the CRLs it loaded
// before, which still accept the server and revoke the revoked one,
// rather than failing every handshake through the intermediate.
func TestClientConfigChecksCRLsAgainstIssuersOfVerifiedChains(t *testing.T) {
	root := t.TempDir()
	certs := tlstest.CreateClientServerCertPairs(root)
	rootCA := path.Join(root, "ca-cert.pem")
	crl := path.Join(t.TempDir(), "crl.pem")
	b, err := os.ReadFile(certs.ServerCRL)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(crl, b, 0o600))

	validServer, err := ReadServerConfig(certs.ServerCert, certs.ServerKey, "", "", certs.ServerCA, tls.VersionTLS12)
	require.NoError(t, err)
	revokedServer, err := ReadServerConfig(certs.RevokedServerCert, certs.RevokedServerKey, "", "", certs.ServerCA, tls.VersionTLS12)
	require.NoError(t, err)
	handshakeWith := func(server *tls.Config, name string) handshakeResult {
		t.Helper()
		config, err := ClientConfig(VerifyIdentity, "", "", rootCA, crl, name, tls.VersionTLS12)
		require.NoError(t, err)
		return handshake(t, server, config)
	}
	requireFallback := func(msg string) {
		t.Helper()
		res := handshakeWith(validServer, certs.ServerName)
		require.NoError(t, res.clientErr, msg)
		require.NoError(t, res.serverErr, msg)
		res = handshakeWith(revokedServer, certs.RevokedServerName)
		require.ErrorContains(t, res.clientErr, "Certificate revoked: CommonName="+certs.RevokedServerName, msg)
	}
	// The handshake brings the intermediate in a verified chain.
	requireFallback("the client must load the CRL")

	intermediate := loadOneCert(t, certs.ServerCA)
	imposter, imposterKey := selfSignedCA(t, 1, "", intermediate.RawSubject)
	require.NoError(t, os.WriteFile(crl, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: crlWithoutExtensions(t, imposter, imposterKey)}), 0o600))

	_, err = ReloadCachedFiles()
	require.ErrorContains(t, err, "cannot use the CRL file "+crl)
	requireFallback("a CRL that the intermediate does not validate must leave the client on the CRLs it loaded before")

	require.NoError(t, os.Remove(crl))
	requireFallback("once the file is gone, the client must fall back to the CRLs it loaded before")
}

// TestReloadCachedFilesIgnoresIssuersTheNewCANoLongerTrusts checks
// that an issuer that verified chains brought under the old CA file
// takes no part in validating the CRLs under a new CA file that no
// longer trusts it: the CA is rotated to a new root, whose intermediate
// reuses the old intermediate's name with a new key and issues a valid
// CRL without an authority key identifier, which the old intermediate's
// key would reject, holding back the rotation.
func TestReloadCachedFilesIgnoresIssuersTheNewCANoLongerTrusts(t *testing.T) {
	oldRoot := t.TempDir()
	oldCerts := tlstest.CreateClientServerCertPairs(oldRoot)
	oldIntermediate := loadOneCert(t, oldCerts.ServerCA)
	newRoot := t.TempDir()
	tlstest.CreateCA(newRoot)
	tlstest.CreateIntermediateCA(newRoot, tlstest.CA, "01", "rotated-ca", oldIntermediate.Subject.CommonName)
	tlstest.CreateSignedCert(newRoot, "rotated-ca", "02", "rotated-server", "rotated.example.com")
	newIntermediate := loadOneCert(t, path.Join(newRoot, "rotated-ca-cert.pem"))
	require.Equal(t, oldIntermediate.RawSubject, newIntermediate.RawSubject)
	newKey, err := tls.LoadX509KeyPair(path.Join(newRoot, "rotated-ca-cert.pem"), path.Join(newRoot, "rotated-ca-key.pem"))
	require.NoError(t, err)

	files := newClientFiles(t)
	files.copy(files.ca, path.Join(oldRoot, "ca-cert.pem"))
	files.copy(files.crl, oldCerts.ServerCRL)
	client := func(name string) *tls.Config {
		t.Helper()
		config, err := ClientConfig(VerifyIdentity, "", "", files.ca, files.crl, name, tls.VersionTLS12)
		require.NoError(t, err)
		return config
	}
	oldServer, err := ReadServerConfig(oldCerts.ServerCert, oldCerts.ServerKey, "", "", oldCerts.ServerCA, tls.VersionTLS12)
	require.NoError(t, err)
	// The handshake brings the old intermediate in a verified chain.
	res := handshake(t, oldServer, client(oldCerts.ServerName))
	require.NoError(t, res.clientErr)

	files.copy(files.ca, path.Join(newRoot, "ca-cert.pem"))
	require.NoError(t, os.WriteFile(files.crl, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: crlWithoutExtensions(t, newIntermediate, newKey.PrivateKey.(*ecdsa.PrivateKey))}), 0o600))
	_, err = ReloadCachedFiles()
	requireNoErrorFor(t, err, path.Dir(files.ca))

	newServer, err := ReadServerConfig(path.Join(newRoot, "rotated-server-cert.pem"), path.Join(newRoot, "rotated-server-key.pem"), "", "", path.Join(newRoot, "rotated-ca-cert.pem"), tls.VersionTLS12)
	require.NoError(t, err)
	res = handshake(t, newServer, client("rotated.example.com"))
	require.NoError(t, res.clientErr, "the client must trust the rotated CA")
	require.NoError(t, res.serverErr)
}

// TestClientCRLCheckerKeepsTheLearnedBindings checks that the bindings
// that checking a client's CRLs against the issuers verified chains
// brought before made are kept in the checker, so that its first
// handshake through one of those issuers reads them rather than
// checking the CRLs' signatures again.
func TestClientCRLCheckerKeepsTheLearnedBindings(t *testing.T) {
	root := t.TempDir()
	certs := tlstest.CreateClientServerCertPairs(root)
	rootCert := loadOneCert(t, path.Join(root, "ca-cert.pem"))
	intermediate := loadOneCert(t, certs.ServerCA)
	crls, err := loadCRLSet(certs.ServerCRL)
	require.NoError(t, err)
	var fallback crlFallback
	fallback.learnIssuer(intermediate)

	checker, err := clientCRLCheckerFrom(crls, []*x509.Certificate{rootCert}, &fallback)
	require.NoError(t, err)
	binding, cached := checker.bound.Load(string(intermediate.Raw))
	require.True(t, cached, "the binding made for the learned issuer was not kept")
	require.NoError(t, binding.(crlBinding).err)
	require.Len(t, binding.(crlBinding).crls, 1)
	require.Equal(t, int64(1), checker.boundIssuers.Load())
}

// TestCRLFallbackBoundsTheLearnedIssuers checks that the issuers a
// client's CRL fallback records from verified chains are bounded in
// number: a peer that holds the key of a permitted intermediate can
// present a CA certificate minted under it on every connection.
func TestCRLFallbackBoundsTheLearnedIssuers(t *testing.T) {
	var fallback crlFallback
	for i := range maxBoundIssuers + 5 {
		issuer, _ := selfSignedCA(t, int64(1<<40+i), fmt.Sprintf("Minted CA %d", i), nil)
		fallback.learnIssuer(issuer)
	}
	require.Equal(t, maxBoundIssuers, len(fallback.learnedIssuers()))
}

// clientFiles are the files of a TLS client, installed in turn from
// unrelated sets of certificates, as a rotation replaces them.
type clientFiles struct {
	t                  *testing.T
	cert, key, ca, crl string
}

func newClientFiles(t *testing.T) clientFiles {
	live := t.TempDir()
	return clientFiles{t: t, cert: path.Join(live, "cert.pem"), key: path.Join(live, "key.pem"), ca: path.Join(live, "ca.pem"), crl: path.Join(live, "crl.pem")}
}

func (f clientFiles) copy(dst, src string) {
	b, err := os.ReadFile(src)
	require.NoError(f.t, err)
	require.NoError(f.t, os.WriteFile(dst, b, 0o600))
}

func (f clientFiles) installKeyPair(certs tlstest.ClientServerKeyPairs) {
	f.copy(f.cert, certs.ClientCert)
	f.copy(f.key, certs.ClientKey)
}

func (f clientFiles) installTrust(certs tlstest.ClientServerKeyPairs) {
	f.copy(f.ca, certs.ServerCA)
	f.copy(f.crl, certs.ServerCRL)
}

// requireServes requires the configs ClientConfig builds from f to
// present the client certificate of keyPair and to trust the server
// CA of trust.
func (f clientFiles) requireServes(keyPair, trust tlstest.ClientServerKeyPairs) {
	f.t.Helper()
	config, err := ClientConfig(VerifyIdentity, f.cert, f.key, f.ca, f.crl, "", tls.VersionTLS12)
	require.NoError(f.t, err)
	require.Equal(f.t, loadOneCert(f.t, keyPair.ClientCert).Raw, config.Certificates[0].Certificate[0], "the client must present the certificate of the expected set")
	want, err := readx509CertPool(trust.ServerCA)
	require.NoError(f.t, err)
	require.True(f.t, want.Equal(config.RootCAs), "the client must trust the CA of the expected set")
}

// TestReloadCachedFilesReadsConsistently checks that a rotation that
// lands while ReloadCachedFiles reads the files, the key pair first
// and the CA and CRL a moment later, does not leave the client with
// the new key pair and the old CA: the files are read again until
// they read the same before and after.
func TestReloadCachedFilesReadsConsistently(t *testing.T) {
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := newClientFiles(t)
	files.installKeyPair(oldCerts)
	files.installTrust(oldCerts)
	files.requireServes(oldCerts, oldCerts)

	var attempts int
	reloadTestHook = func() {
		attempts++
		switch attempts {
		case 1:
			files.installKeyPair(newCerts)
		case 2:
			files.installTrust(newCerts)
		}
	}
	t.Cleanup(func() { reloadTestHook = nil })

	changed, err := ReloadCachedFiles()
	requireNoErrorFor(t, err, path.Dir(files.cert))
	require.True(t, changed)
	require.Equal(t, 3, attempts, "the reads that the rotation changed must be read again")
	files.requireServes(newCerts, newCerts)
}

// TestReloadCachedFilesPublishesNothingWhileFilesChange checks that
// when the files keep changing across every read, ReloadCachedFiles
// fails and the client keeps what it loaded before.
func TestReloadCachedFilesPublishesNothingWhileFilesChange(t *testing.T) {
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := newClientFiles(t)
	files.installKeyPair(oldCerts)
	files.installTrust(oldCerts)
	files.requireServes(oldCerts, oldCerts)

	var attempts int
	reloadTestHook = func() {
		attempts++
		if attempts%2 == 1 {
			files.installKeyPair(newCerts)
		} else {
			files.installKeyPair(oldCerts)
		}
	}
	t.Cleanup(func() { reloadTestHook = nil })

	generation := CachedFilesGeneration()
	changed, err := ReloadCachedFiles()
	require.ErrorContains(t, err, "kept changing")
	require.False(t, changed)
	require.Equal(t, generation, CachedFilesGeneration())
	files.requireServes(oldCerts, oldCerts)
}

// TestReloadCachedFilesValidatesCRLsUnderTheirCA checks that a CRL
// that parses but that its CA does not validate, here one under the
// CA's name signed by another key, is reported by ReloadCachedFiles
// rather than counted as reloaded, since ClientConfig keeps the CRLs
// last loaded from the file instead of using it.
func TestReloadCachedFilesValidatesCRLsUnderTheirCA(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := newClientFiles(t)
	files.installTrust(certs)
	_, err := ClientConfig(VerifyIdentity, "", "", files.ca, files.crl, certs.ServerName, tls.VersionTLS12)
	require.NoError(t, err)

	ca := loadOneCert(t, certs.ServerCA)
	other, otherKey := selfSignedCA(t, 1, "", ca.RawSubject)
	require.NoError(t, os.WriteFile(files.crl, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: crlWithoutExtensions(t, other, otherKey)}), 0o600))
	_, err = loadCRLSet(files.crl)
	require.NoError(t, err, "the CRL must parse for this test to be meaningful")

	generation := CachedFilesGeneration()
	_, err = ReloadCachedFiles()
	require.ErrorContains(t, err, "cannot use the CRL file "+files.crl)
	require.Equal(t, generation, CachedFilesGeneration(), "a CRL that its CA does not validate must not count as reloaded")
}

// TestReloadCachedFilesPublishesAtOnce checks that no config is built
// while ReloadCachedFiles updates the caches, so that none combines the
// new key pair with the old CA: ClientConfig and ServerConfig build
// under the read side of publishMu, which the update holds exclusively
// throughout. A config built after the reload takes all of the new
// files.
func TestReloadCachedFilesPublishesAtOnce(t *testing.T) {
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := newClientFiles(t)
	files.installKeyPair(oldCerts)
	files.installTrust(oldCerts)
	files.requireServes(oldCerts, oldCerts)
	files.installKeyPair(newCerts)
	files.installTrust(newCerts)

	var hooked, buildable bool
	publishTestHook = func() {
		hooked = true
		if publishMu.TryRLock() {
			publishMu.RUnlock()
			buildable = true
		}
	}
	t.Cleanup(func() { publishTestHook = nil })

	_, err := ReloadCachedFiles()
	requireNoErrorFor(t, err, path.Dir(files.cert))
	require.True(t, hooked, "the reload must update the caches for this test to be meaningful")
	require.False(t, buildable, "a config could be built while the caches were being updated")
	files.requireServes(newCerts, newCerts)
}

// TestReloadCachedFilesRereadsWhenACRLChangesBeforeItPublishes checks
// that a reload that read a new CA and CRL, and before it publishes them
// sees a ClientConfig call keep other CRL contents, read later and
// validated only under the old CA, reads the files again rather than
// publish the new CA with those CRLs to fall back to: here they are not
// valid under the new CA, so the new CA is held back with them, as it
// would have been had the reload read them.
func TestReloadCachedFilesRereadsWhenACRLChangesBeforeItPublishes(t *testing.T) {
	// Two intermediates under one name with different keys, as a
	// re-keyed CA, each configured as the CA file in turn, with a CRL
	// without an authority key identifier that only its own key
	// validates.
	intermediate := func(name string) (caPEM, crlPEM []byte) {
		root := t.TempDir()
		tlstest.CreateCA(root)
		tlstest.CreateIntermediateCA(root, tlstest.CA, "01", name, "Re-keyed CA")
		caPEM, err := os.ReadFile(path.Join(root, name+"-cert.pem"))
		require.NoError(t, err)
		keyPair, err := tls.LoadX509KeyPair(path.Join(root, name+"-cert.pem"), path.Join(root, name+"-key.pem"))
		require.NoError(t, err)
		ca := loadOneCert(t, path.Join(root, name+"-cert.pem"))
		crlPEM = pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: crlWithoutExtensions(t, ca, keyPair.PrivateKey.(*ecdsa.PrivateKey))})
		return caPEM, crlPEM
	}
	oldCA, oldCRL := intermediate("old-ca")
	newCA, newCRL := intermediate("new-ca")

	files := newClientFiles(t)
	write := func(dst string, b []byte) {
		t.Helper()
		require.NoError(t, os.WriteFile(dst, b, 0o600))
	}
	client := func() *tls.Config {
		t.Helper()
		config, err := ClientConfig(VerifyIdentity, "", "", files.ca, files.crl, "", tls.VersionTLS12)
		require.NoError(t, err)
		return config
	}
	write(files.ca, oldCA)
	write(files.crl, oldCRL)
	client()

	write(files.ca, newCA)
	write(files.crl, newCRL)
	var hooked bool
	prePublishTestHook = func() {
		if hooked {
			return
		}
		hooked = true
		// The old CRL is back, and a call validates it under the old
		// CA, which the reload has not published over yet.
		write(files.crl, oldCRL)
		client()
	}
	t.Cleanup(func() { prePublishTestHook = nil })

	_, err := ReloadCachedFiles()
	require.True(t, hooked)
	require.ErrorContains(t, err, "cannot use the CRL file "+files.crl+" under the CA file "+files.ca)
	want := x509.NewCertPool()
	require.True(t, want.AppendCertsFromPEM(oldCA))
	require.True(t, want.Equal(client().RootCAs), "the new CA must be held back with the CRL it cannot use")
}

// TestReloadCachedFilesKeepsConfigurationsWhole checks that a file
// that fails to load holds back the reload of the files it is used
// with, so that its configuration keeps all of its files from before
// the reload rather than taking some from after it, while another
// configuration, sharing no file with it, reloads.
func TestReloadCachedFilesKeepsConfigurationsWhole(t *testing.T) {
	var olds, news []tlstest.ClientServerKeyPairs
	for range 3 {
		olds = append(olds, tlstest.CreateClientServerCertPairs(t.TempDir()))
		news = append(news, tlstest.CreateClientServerCertPairs(t.TempDir()))
	}
	var clients []clientFiles
	for i := range 3 {
		files := newClientFiles(t)
		files.installKeyPair(olds[i])
		files.installTrust(olds[i])
		files.requireServes(olds[i], olds[i])
		clients = append(clients, files)
	}
	halfRotated, rotated, badCRL := clients[0], clients[1], clients[2]

	// The CA and the certificate are replaced, but the key is not yet.
	halfRotated.installTrust(news[0])
	halfRotated.copy(halfRotated.cert, news[0].ClientCert)

	rotated.installKeyPair(news[1])
	rotated.installTrust(news[1])

	// Everything is replaced, but the CRL is not valid under the new CA.
	badCRL.installKeyPair(news[2])
	badCRL.installTrust(news[2])
	newCA := loadOneCert(t, news[2].ServerCA)
	other, otherKey := selfSignedCA(t, 1, "", newCA.RawSubject)
	require.NoError(t, os.WriteFile(badCRL.crl, pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: crlWithoutExtensions(t, other, otherKey)}), 0o600))

	changed, err := ReloadCachedFiles()
	require.True(t, changed)
	require.ErrorContains(t, err, halfRotated.key)
	require.ErrorContains(t, err, "cannot use the CRL file "+badCRL.crl)
	requireNoErrorFor(t, err, path.Dir(rotated.cert))

	halfRotated.requireServes(olds[0], olds[0])
	rotated.requireServes(news[1], news[1])
	badCRL.requireServes(olds[2], olds[2])
}

// TestReloadCachedFilesRereadsWhenConfigurationsRegister checks that a
// configuration that registers while ReloadCachedFiles reads the
// files, using together entries that the reload took as unrelated, is
// taken into account: here a key pair that fails to load, used so far
// without a CA, and a CA that another configuration uses and that
// changed, which must then be held back with the key pair rather than
// reloaded.
func TestReloadCachedFilesRereadsWhenConfigurationsRegister(t *testing.T) {
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := newClientFiles(t)
	files.installKeyPair(oldCerts)
	files.installTrust(oldCerts)
	other := newClientFiles(t)
	other.installKeyPair(oldCerts)

	build := func(cert, key, ca string) *tls.Config {
		t.Helper()
		config, err := ClientConfig(VerifyIdentity, cert, key, ca, "", "", tls.VersionTLS12)
		require.NoError(t, err)
		return config
	}
	build(files.cert, files.key, files.ca)
	build(other.cert, other.key, "")

	// The CA is replaced, and the other key pair is halfway through
	// its rotation.
	files.installTrust(newCerts)
	other.copy(other.cert, newCerts.ClientCert)

	var attempts int
	reloadTestHook = func() {
		attempts++
		if attempts == 1 {
			build(other.cert, other.key, files.ca)
		}
	}
	t.Cleanup(func() { reloadTestHook = nil })

	_, err := ReloadCachedFiles()
	require.ErrorContains(t, err, other.key)
	require.Greater(t, attempts, 1, "the reload must be read again once a configuration registered")

	config := build(other.cert, other.key, files.ca)
	require.Equal(t, loadOneCert(t, oldCerts.ClientCert).Raw, config.Certificates[0].Certificate[0])
	want, err := readx509CertPool(oldCerts.ServerCA)
	require.NoError(t, err)
	require.True(t, want.Equal(config.RootCAs), "the CA must be held back with the key pair it is now used with")
}
