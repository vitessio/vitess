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
	"os"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/vterrors"
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

func copyFile(t *testing.T, dst, src string) {
	t.Helper()
	b, err := os.ReadFile(src)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dst, b, 0o600))
}

// liveClientFiles are the files of a TLS client, installed in turn
// from unrelated sets of certificates, as a rotation replaces them.
type liveClientFiles struct {
	t                  *testing.T
	cert, key, ca, crl string
}

func newLiveClientFiles(t *testing.T) liveClientFiles {
	live := t.TempDir()
	return liveClientFiles{t: t, cert: path.Join(live, "cert.pem"), key: path.Join(live, "key.pem"), ca: path.Join(live, "ca.pem"), crl: path.Join(live, "crl.pem")}
}

func (f liveClientFiles) install(certs tlstest.ClientServerKeyPairs) {
	copyFile(f.t, f.cert, certs.ClientCert)
	copyFile(f.t, f.key, certs.ClientKey)
	copyFile(f.t, f.ca, certs.ServerCA)
	copyFile(f.t, f.crl, certs.ServerCRL)
}

func (f liveClientFiles) clientConfig(serverName string) (*tls.Config, error) {
	return ClientConfig(VerifyIdentity, f.cert, f.key, f.ca, f.crl, serverName, tls.VersionTLS12)
}

// serverRequiringClientsOf returns the config of the server of certs,
// requiring a client certificate that the client CA of certs issued.
func serverRequiringClientsOf(t *testing.T, certs tlstest.ClientServerKeyPairs) *tls.Config {
	config, err := ServerConfig(certs.ServerCert, certs.ServerKey, certs.ClientCA, "", "", tls.VersionTLS12)
	require.NoError(t, err)
	return config
}

// TestReloadCachedFiles checks, through real handshakes, that the
// configs ClientConfig builds keep the certificate, key and CA they
// loaded until ReloadCachedFiles reads the files again, and then use
// what the files hold.
func TestReloadCachedFiles(t *testing.T) {
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := newLiveClientFiles(t)
	client := func(certs tlstest.ClientServerKeyPairs) *tls.Config {
		config, err := files.clientConfig(certs.ServerName)
		require.NoError(t, err)
		return config
	}

	files.install(oldCerts)
	res := handshake(t, serverRequiringClientsOf(t, oldCerts), client(oldCerts))
	require.NoError(t, res.clientErr)
	require.NoError(t, res.serverErr)

	files.install(newCerts)
	res = handshake(t, serverRequiringClientsOf(t, newCerts), client(newCerts))
	require.Error(t, res.clientErr, "ClientConfig must keep the CA it loaded until the files are reloaded")

	generation := CachedFilesGeneration()
	changed, err := ReloadCachedFiles()
	requireNoErrorFor(t, err, path.Dir(files.cert))
	require.True(t, changed)
	require.NotEqual(t, generation, CachedFilesGeneration())
	res = handshake(t, serverRequiringClientsOf(t, newCerts), client(newCerts))
	require.NoError(t, res.clientErr, "the client must trust the new CA")
	require.NoError(t, res.serverErr, "the client must present the new certificate")

	generation = CachedFilesGeneration()
	changed, err = ReloadCachedFiles()
	requireNoErrorFor(t, err, path.Dir(files.cert))
	require.False(t, changed, "files that did not change must not count as changed")
	require.Equal(t, generation, CachedFilesGeneration())

	// ClientConfig reads the CRL on every call, but a change to it
	// must still count, for the gRPC credentials built before it.
	copyFile(t, files.crl, newCerts.CombinedCRL)
	changed, err = ReloadCachedFiles()
	requireNoErrorFor(t, err, path.Dir(files.cert))
	require.True(t, changed, "a changed CRL must count as changed")
}

// TestReloadCachedFilesKeepsAConfigWhole checks that when one file of
// a client config fails to load, as a certificate replaced before its
// key does, the reload keeps every file of that config as it was,
// including the CA that did load, and reports the error.
func TestReloadCachedFilesKeepsAConfigWhole(t *testing.T) {
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := newLiveClientFiles(t)
	files.install(oldCerts)
	_, err := files.clientConfig(oldCerts.ServerName)
	require.NoError(t, err)

	copyFile(t, files.ca, newCerts.ServerCA)
	copyFile(t, files.cert, newCerts.ClientCert)
	generation := CachedFilesGeneration()
	_, err = ReloadCachedFiles()
	require.ErrorContains(t, err, files.key)

	config, err := files.clientConfig(oldCerts.ServerName)
	require.NoError(t, err)
	res := handshake(t, serverRequiringClientsOf(t, oldCerts), config)
	require.NoError(t, res.clientErr, "the client must keep the CA it loaded with the key pair it kept")
	require.NoError(t, res.serverErr, "the client must keep presenting the certificate whose key it has")

	copyFile(t, files.key, newCerts.ClientKey)
	_, err = ReloadCachedFiles()
	requireNoErrorFor(t, err, path.Dir(files.cert))
	require.NotEqual(t, generation, CachedFilesGeneration())
	config, err = files.clientConfig(newCerts.ServerName)
	require.NoError(t, err)
	res = handshake(t, serverRequiringClientsOf(t, newCerts), config)
	require.NoError(t, res.clientErr)
	require.NoError(t, res.serverErr)
}

// TestClientConfigFailsOnABrokenCRL checks that a client's CRL file
// that cannot be used, as one truncated by the run that writes it,
// fails ClientConfig rather than letting a server that the CRL may
// revoke through, and that ReloadCachedFiles reports it without
// counting it as a change, so that the gRPC credentials built before
// keep the CRLs they were built with.
func TestClientConfigFailsOnABrokenCRL(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := newLiveClientFiles(t)
	files.install(certs)
	_, err := files.clientConfig(certs.ServerName)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(files.crl, nil, 0o600))
	_, err = files.clientConfig(certs.ServerName)
	require.Error(t, err, "a client must not connect while its CRL file cannot be used")

	generation := CachedFilesGeneration()
	changed, err := ReloadCachedFiles()
	require.ErrorContains(t, err, files.crl)
	require.False(t, changed)
	require.Equal(t, generation, CachedFilesGeneration(), "the gRPC credentials must not be rebuilt from a CRL that cannot be used")

	copyFile(t, files.crl, certs.ServerCRL)
	_, err = files.clientConfig(certs.ServerName)
	require.NoError(t, err)
}

// TestClientConfigRecoversFromAFailedFirstLoad checks that a client
// whose files are missing on first use, as a secret mounted late is,
// loads them once they appear, rather than failing until a restart.
func TestClientConfigRecoversFromAFailedFirstLoad(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := newLiveClientFiles(t)
	copyFile(t, files.cert, certs.ClientCert)
	copyFile(t, files.key, certs.ClientKey)
	copyFile(t, files.crl, certs.ServerCRL)

	_, err := files.clientConfig(certs.ServerName)
	require.Equal(t, vtrpc.Code_NOT_FOUND, vterrors.Code(err))
	_, err = ReloadCachedFiles()
	requireNoErrorFor(t, err, path.Dir(files.cert))

	copyFile(t, files.ca, certs.ServerCA)
	_, err = ReloadCachedFiles()
	requireNoErrorFor(t, err, path.Dir(files.cert))
	config, err := files.clientConfig(certs.ServerName)
	require.NoError(t, err)
	server, err := ServerConfig(certs.ServerCert, certs.ServerKey, "", "", "", tls.VersionTLS12)
	require.NoError(t, err)
	res := handshake(t, server, config)
	require.NoError(t, res.clientErr)
	require.NoError(t, res.serverErr)
}

// TestReloadCachedFilesCountsACRLThatOnlyClientConfigRead checks that a
// CRL change that ClientConfig reads before ReloadCachedFiles does
// still counts as a change on the next reload, whichever way the file
// changes, so that the gRPC credentials built before it, or from it,
// are built again.
func TestReloadCachedFilesCountsACRLThatOnlyClientConfigRead(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := newLiveClientFiles(t)
	files.install(certs)
	_, err := files.clientConfig(certs.ServerName)
	require.NoError(t, err)

	for _, crl := range []string{certs.CombinedCRL, certs.ServerCRL} {
		copyFile(t, files.crl, crl)
		_, err = files.clientConfig(certs.ServerName)
		require.NoError(t, err)

		generation := CachedFilesGeneration()
		changed, err := ReloadCachedFiles()
		requireNoErrorFor(t, err, path.Dir(files.cert))
		require.True(t, changed, "a CRL that a config was built from before the reload must count as changed")
		require.NotEqual(t, generation, CachedFilesGeneration())
	}

	// A config built from contents that change back before the reload
	// is built again as well.
	copyFile(t, files.crl, certs.CombinedCRL)
	_, err = files.clientConfig(certs.ServerName)
	require.NoError(t, err)
	copyFile(t, files.crl, certs.ServerCRL)
	changed, err := ReloadCachedFiles()
	requireNoErrorFor(t, err, path.Dir(files.cert))
	require.True(t, changed, "a config built from contents the reload did not see must be built again")
}

// TestReloadCachedFilesValidatesCRLsUnderTheirCA checks that a CRL that
// parses, but that the CA of its configuration does not validate, is
// reported by ReloadCachedFiles rather than counted as a change.
func TestReloadCachedFilesValidatesCRLsUnderTheirCA(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := newLiveClientFiles(t)
	files.install(certs)
	_, err := files.clientConfig(certs.ServerName)
	require.NoError(t, err)

	// A CRL without an authority key identifier, as openssl ca writes
	// by default, under the configured CA's name but signed by another
	// key.
	serverCA := loadOneCert(t, certs.ServerCA)
	other, otherKey := selfSignedCA(t, 1, "", serverCA.RawSubject)
	copyFile(t, files.crl, crlFile(t, crlWithoutExtensions(t, other, otherKey)))
	_, err = files.clientConfig(certs.ServerName)
	require.Error(t, err, "the CA must not validate the CRL for this test to be meaningful")

	generation := CachedFilesGeneration()
	changed, err := ReloadCachedFiles()
	require.ErrorContains(t, err, files.crl)
	require.False(t, changed)
	require.Equal(t, generation, CachedFilesGeneration(), "the gRPC credentials must not be rebuilt from a CRL that its CA does not validate")
}

// changeFilesOnRead has readFile call change each time it has read the
// file at path.
func changeFilesOnRead(t *testing.T, path string, change func()) {
	t.Cleanup(func() { readFile = os.ReadFile })
	readFile = func(name string) ([]byte, error) {
		b, err := os.ReadFile(name)
		if name == path {
			change()
		}
		return b, err
	}
}

// TestReloadCachedFilesReadsAConfigConsistently checks that a rotation
// that replaces the files of a config while ReloadCachedFiles reads
// them, as a Kubernetes secret update can, is loaded whole, rather than
// as the old key pair with the new CA.
func TestReloadCachedFilesReadsAConfigConsistently(t *testing.T) {
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := newLiveClientFiles(t)
	files.install(oldCerts)
	_, err := files.clientConfig(oldCerts.ServerName)
	require.NoError(t, err)

	// The rotation lands between the reads of the key and the CA.
	rotated := false
	changeFilesOnRead(t, files.key, func() {
		if !rotated {
			rotated = true
			files.install(newCerts)
		}
	})
	changed, err := ReloadCachedFiles()
	requireNoErrorFor(t, err, path.Dir(files.cert))
	require.True(t, rotated, "the files must have changed during the reload for this test to be meaningful")
	require.True(t, changed)

	config, err := files.clientConfig(newCerts.ServerName)
	require.NoError(t, err)
	res := handshake(t, serverRequiringClientsOf(t, newCerts), config)
	require.NoError(t, res.clientErr, "the client must trust the new CA")
	require.NoError(t, res.serverErr, "the client must present the new certificate along with the new CA")
}

// reloadDuringRead has readFile, on its read number nth of the file at
// path, call rotate and start a ReloadCachedFiles, which it waits a
// second for: a reload that does not wait for the first load reading
// the files finishes within it. It returns a function that waits for
// the reload to finish.
func reloadDuringRead(t *testing.T, path string, nth int32, rotate func()) (wait func()) {
	var (
		reads     atomic.Int32
		reloadErr error
	)
	done := make(chan struct{})
	t.Cleanup(func() { readFile = os.ReadFile })
	readFile = func(name string) ([]byte, error) {
		b, err := os.ReadFile(name)
		if name == path && reads.Add(1) == nth {
			rotate()
			go func() {
				defer close(done)
				_, reloadErr = ReloadCachedFiles()
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
			}
		}
		return b, err
	}
	return func() {
		select {
		case <-done:
			requireNoErrorFor(t, reloadErr, path)
		case <-time.After(30 * time.Second):
			require.Fail(t, "the reload did not finish")
		}
	}
}

// TestReloadCachedFilesDoesNotPassByAFirstLoad checks that a reload
// that runs while a client's files are read for the first time, after
// a rotation that lands between the reads, loads the rotated files: it
// must not skip the files that are not cached yet and leave the first
// load to cache what they held before the rotation.
func TestReloadCachedFilesDoesNotPassByAFirstLoad(t *testing.T) {
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := newLiveClientFiles(t)
	files.install(oldCerts)

	// The rotation and the reload land after the first load's second,
	// and last, read of the CA, before it caches what it read.
	wait := reloadDuringRead(t, files.ca, 2, func() { files.install(newCerts) })
	_, err := files.clientConfig(oldCerts.ServerName)
	require.NoError(t, err)
	wait()

	config, err := files.clientConfig(newCerts.ServerName)
	require.NoError(t, err)
	res := handshake(t, serverRequiringClientsOf(t, newCerts), config)
	require.NoError(t, res.clientErr, "the client must trust the new CA after the reload")
	require.NoError(t, res.serverErr, "the client must present the new certificate after the reload")
}

// TestReloadCachedFilesDoesNotPassByAFirstCRLRead checks that a reload
// that runs while a CRL file is read for the first time, after it
// changed, counts the change: it must not skip the file that is not
// watched yet and leave the gRPC credentials built from what it held
// before.
func TestReloadCachedFilesDoesNotPassByAFirstCRLRead(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := newLiveClientFiles(t)
	files.install(certs)

	generation := CachedFilesGeneration()
	wait := reloadDuringRead(t, files.crl, 1, func() { copyFile(t, files.crl, certs.CombinedCRL) })
	_, err := files.clientConfig(certs.ServerName)
	require.NoError(t, err)
	wait()
	require.NotEqual(t, generation, CachedFilesGeneration(), "the reload must count the CRL change that a config was built before")
}

// TestReloadCachedFilesKeepsAConfigWhoseFilesKeepChanging checks that a
// config whose files change on every read keeps what it loaded before,
// and that ReloadCachedFiles reports it.
func TestReloadCachedFilesKeepsAConfigWhoseFilesKeepChanging(t *testing.T) {
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	certSets := []tlstest.ClientServerKeyPairs{tlstest.CreateClientServerCertPairs(t.TempDir()), tlstest.CreateClientServerCertPairs(t.TempDir())}
	files := newLiveClientFiles(t)
	files.install(oldCerts)
	_, err := files.clientConfig(oldCerts.ServerName)
	require.NoError(t, err)

	installs := 0
	changeFilesOnRead(t, files.key, func() {
		files.install(certSets[installs%2])
		installs++
	})
	_, err = ReloadCachedFiles()
	require.ErrorContains(t, err, "kept changing")
	require.ErrorContains(t, err, files.ca)

	config, err := files.clientConfig(oldCerts.ServerName)
	require.NoError(t, err)
	res := handshake(t, serverRequiringClientsOf(t, oldCerts), config)
	require.NoError(t, res.clientErr, "the client must keep the CA it loaded")
	require.NoError(t, res.serverErr, "the client must keep the certificate it loaded")
}
