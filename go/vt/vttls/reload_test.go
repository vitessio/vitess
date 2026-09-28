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
	"testing"

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
