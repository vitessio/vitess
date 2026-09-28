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
	"crypto/tls"
	"encoding/pem"
	"os"
	"path"
	"testing"
	"time"

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

// TestReloadCachedFilesPublishesAtOnce checks that a config built
// while ReloadCachedFiles updates the caches does not combine the new
// key pair with the old CA: it is built from the files before the
// reload or after it.
func TestReloadCachedFilesPublishesAtOnce(t *testing.T) {
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := newClientFiles(t)
	files.installKeyPair(oldCerts)
	files.installTrust(oldCerts)
	files.requireServes(oldCerts, oldCerts)
	files.installKeyPair(newCerts)
	files.installTrust(newCerts)

	type built struct {
		config *tls.Config
		err    error
	}
	results := make(chan built, 1)
	var duringPublish *built
	publishTestHook = func() {
		go func() {
			config, err := ClientConfig(VerifyIdentity, files.cert, files.key, files.ca, files.crl, "", tls.VersionTLS12)
			results <- built{config, err}
		}()
		// A config built halfway through the update would be ready
		// long before this.
		select {
		case b := <-results:
			duringPublish = &b
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Cleanup(func() { publishTestHook = nil })

	_, err := ReloadCachedFiles()
	requireNoErrorFor(t, err, path.Dir(files.cert))
	require.Nil(t, duringPublish, "no config must be built while the caches are being updated")
	b := <-results
	require.NoError(t, b.err)
	require.Equal(t, loadOneCert(t, newCerts.ClientCert).Raw, b.config.Certificates[0].Certificate[0])
	want, err := readx509CertPool(newCerts.ServerCA)
	require.NoError(t, err)
	require.True(t, want.Equal(b.config.RootCAs))
}
