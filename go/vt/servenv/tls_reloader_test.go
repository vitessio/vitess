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

package servenv

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/vttls"
)

// liveTLSFiles returns fixed paths under a fresh directory to install
// TLS files at, as a deployment rotates them in place.
func liveTLSFiles(t *testing.T) TLSServerFiles {
	live := t.TempDir()
	return TLSServerFiles{
		Cert:     path.Join(live, "cert.pem"),
		Key:      path.Join(live, "key.pem"),
		CA:       path.Join(live, "ca.pem"),
		CRL:      path.Join(live, "crl.pem"),
		ServerCA: path.Join(live, "server-ca.pem"),
	}
}

// installTLSFiles copies the server side of certs to the paths of
// files.
func installTLSFiles(t *testing.T, files TLSServerFiles, certs tlstest.ClientServerKeyPairs) {
	t.Helper()
	for dst, src := range map[string]string{files.Cert: certs.ServerCert, files.Key: certs.ServerKey, files.CA: certs.ClientCA, files.CRL: certs.ClientCRL, files.ServerCA: certs.ServerCA} {
		copyFile(t, dst, src)
	}
}

func copyFile(t *testing.T, dst, src string) {
	t.Helper()
	b, err := os.ReadFile(src)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dst, b, 0o600))
}

func readCert(t *testing.T, file string) *x509.Certificate {
	t.Helper()
	b, err := os.ReadFile(file)
	require.NoError(t, err)
	block, _ := pem.Decode(b)
	require.NotNil(t, block)
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	return cert
}

// configSink records the configs a reloader hands over.
type configSink struct {
	mu      sync.Mutex
	configs []*tls.Config
}

func (s *configSink) store(config *tls.Config) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configs = append(s.configs, config)
}

func (s *configSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.configs)
}

func (s *configSink) leaf() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.configs[len(s.configs)-1].Certificates[0].Certificate[0]
}

func TestTLSReloaderReloadsOnSignal(t *testing.T) {
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := liveTLSFiles(t)
	installTLSFiles(t, files, oldCerts)

	var sink configSink
	reloader, err := NewTLSReloader(t.Name(), files, tls.VersionTLS12, sink.store)
	require.NoError(t, err)
	require.Equal(t, readCert(t, oldCerts.ServerCert).Raw, sink.leaf())

	signals := make(chan os.Signal, 1)
	reloader.Start(t.Context(), signals, 0)
	t.Cleanup(reloader.Stop)

	installTLSFiles(t, files, newCerts)
	signals <- syscall.SIGHUP
	newLeaf := readCert(t, newCerts.ServerCert)
	assert.Eventually(t, func() bool {
		return sink.count() == 2
	}, 30*time.Second, 10*time.Millisecond)
	require.Equal(t, newLeaf.Raw, sink.leaf())
	require.Equal(t, newLeaf.NotAfter.Unix(), tlsCertNotAfter.Counts()[t.Name()])
	require.NotZero(t, tlsReloadSuccessTimestamp.Counts()[t.Name()])
}

func TestTLSReloaderReloadsPeriodically(t *testing.T) {
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := liveTLSFiles(t)
	installTLSFiles(t, files, oldCerts)

	var sink configSink
	reloader, err := NewTLSReloader(t.Name(), files, tls.VersionTLS12, sink.store)
	require.NoError(t, err)
	reloader.Start(t.Context(), nil, 10*time.Millisecond)
	t.Cleanup(reloader.Stop)

	installTLSFiles(t, files, newCerts)
	newLeaf := readCert(t, newCerts.ServerCert).Raw
	assert.Eventually(t, func() bool {
		return sink.count() > 1 && string(sink.leaf()) == string(newLeaf)
	}, 30*time.Second, 10*time.Millisecond)
}

func TestTLSReloaderSkipsUnchangedFilesUnlessForced(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := liveTLSFiles(t)
	installTLSFiles(t, files, certs)

	var sink configSink
	reloader, err := NewTLSReloader(t.Name(), files, tls.VersionTLS12, sink.store)
	require.NoError(t, err)
	require.Equal(t, 1, sink.count())

	require.NoError(t, reloader.Reload(false))
	require.Equal(t, 1, sink.count(), "an unforced reload of unchanged files must not hand over a new config")

	require.NoError(t, reloader.Reload(true))
	require.Equal(t, 2, sink.count(), "a forced reload must hand over a new config")
}

// TestTLSReloaderDigestsConsistentlyDespiteRaceOnCAChange checks that
// a CA file replaced between Reload's pre-load digest and its config
// load is still detected, rather than recorded under the CA
// fingerprint read before the replacement. Without this, the
// mismatch would silently skip the session-ticket rotation a CA
// change requires, letting a session authenticated under a CA that
// was just removed keep resuming.
func TestTLSReloaderDigestsConsistentlyDespiteRaceOnCAChange(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	otherCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := liveTLSFiles(t)
	installTLSFiles(t, files, certs)

	var sink configSink
	reloader, err := NewTLSReloader(t.Name(), files, tls.VersionTLS12, sink.store)
	require.NoError(t, err)

	// The CA file changes only once the hook fires, simulating a
	// rotation landing exactly between Reload's pre-load digest and
	// its read of the config.
	var fired bool
	reloader.testHook = func() {
		if fired {
			return
		}
		fired = true
		clientCA, err := os.ReadFile(certs.ClientCA)
		require.NoError(t, err)
		otherCA, err := os.ReadFile(otherCerts.ClientCA)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(files.CA, append(clientCA, otherCA...), 0o600))
	}

	require.NoError(t, reloader.Reload(true))
	require.True(t, fired, "the hook must have run for this test to be meaningful")

	wantCA, err := os.ReadFile(files.CA)
	require.NoError(t, err)
	require.Equal(t, sha256.Sum256(wantCA), reloader.caFingerprint,
		"the recorded CA fingerprint must match the CA that was actually loaded, not the one read before the race")
	require.NotNil(t, reloader.ticketKeys, "a CA that changed mid-reload must still rotate the session-ticket keys")
}

func TestTLSReloaderKeepsConfigOnError(t *testing.T) {
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := liveTLSFiles(t)
	installTLSFiles(t, files, oldCerts)

	var sink configSink
	reloader, err := NewTLSReloader(t.Name(), files, tls.VersionTLS12, sink.store)
	require.NoError(t, err)

	// The certificate is replaced before its key, as a rotation that
	// writes the files one at a time does.
	copyFile(t, files.Cert, newCerts.ServerCert)
	err = reloader.Reload(false)
	require.ErrorContains(t, err, "the previous config stays in place")
	require.Equal(t, 1, sink.count())
	require.Equal(t, readCert(t, oldCerts.ServerCert).Raw, sink.leaf())
	require.Equal(t, int64(1), tlsReloadErrors.Counts()[t.Name()])

	// Once the rotation completes, the next reload picks it up.
	installTLSFiles(t, files, newCerts)
	require.NoError(t, reloader.Reload(false))
	require.Equal(t, readCert(t, newCerts.ServerCert).Raw, sink.leaf())
}

func TestNewTLSReloaderFailsOnInvalidFiles(t *testing.T) {
	files := liveTLSFiles(t)
	_, err := NewTLSReloader(t.Name(), files, tls.VersionTLS12, func(*tls.Config) {
		require.FailNow(t, "no config must be handed over")
	})
	require.Error(t, err)
}

// TestTLSReloaderSessionTicketKeys checks, through real handshakes
// against a server that loads its config the way the gRPC server
// does, that sessions established before a reload resume after it
// unless the CA file changed.
func TestTLSReloaderSessionTicketKeys(t *testing.T) {
	for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
		t.Run(tls.VersionName(version), func(t *testing.T) {
			certs := tlstest.CreateClientServerCertPairs(t.TempDir())
			otherCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
			files := liveTLSFiles(t)
			installTLSFiles(t, files, certs)

			var current atomic.Pointer[tls.Config]
			reloader, err := NewTLSReloader(t.Name(), files, tls.VersionTLS12, func(config *tls.Config) {
				config.MaxVersion = version
				current.Store(config)
			})
			require.NoError(t, err)
			serverConfig := &tls.Config{
				GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
					return current.Load(), nil
				},
			}
			newClient := func() *tls.Config {
				config, err := vttls.ClientConfig(vttls.VerifyIdentity, certs.ClientCert, certs.ClientKey, certs.ServerCA, "", certs.ServerName, tls.VersionTLS12)
				require.NoError(t, err)
				config.ClientSessionCache = tls.NewLRUClientSessionCache(1)
				return config
			}

			t.Run("resumes after a reload that leaves the CA as it was", func(t *testing.T) {
				client := newClient()
				require.False(t, tlsHandshake(t, serverConfig, client).DidResume)
				require.True(t, tlsHandshake(t, serverConfig, client).DidResume, "the client must resume for this test to be meaningful")

				copyFile(t, files.CRL, certs.CombinedCRL)
				require.NoError(t, reloader.Reload(false))
				require.True(t, tlsHandshake(t, serverConfig, client).DidResume)
			})

			t.Run("does not resume after a reload that changed the CA", func(t *testing.T) {
				client := newClient()
				require.False(t, tlsHandshake(t, serverConfig, client).DidResume)
				require.True(t, tlsHandshake(t, serverConfig, client).DidResume, "the client must resume for this test to be meaningful")

				clientCA, err := os.ReadFile(certs.ClientCA)
				require.NoError(t, err)
				otherCA, err := os.ReadFile(otherCerts.ClientCA)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(files.CA, append(clientCA, otherCA...), 0o600))
				require.NoError(t, reloader.Reload(false))
				require.False(t, tlsHandshake(t, serverConfig, client).DidResume)
				require.True(t, tlsHandshake(t, serverConfig, client).DidResume, "sessions established after the reload must resume")

				// A later reload that leaves the CA alone keeps the new
				// keys, rather than reverting to the ones sessions under
				// the previous CA were encrypted with.
				copyFile(t, files.CRL, certs.ClientCRL)
				require.NoError(t, reloader.Reload(false))
				require.True(t, tlsHandshake(t, serverConfig, client).DidResume)
			})
		})
	}
}

// TestTLSReloaderRotatesTicketKeysAfterCAChange checks that the key a
// CA change pins (TestTLSReloaderSessionTicketKeys) does not then
// stay in sole use for the life of the process: it rotates and
// expires on the same schedule crypto/tls uses for its own
// automatically-managed ticket keys, so that key does not become a
// single point of long-term exposure once pinned.
func TestTLSReloaderRotatesTicketKeysAfterCAChange(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	otherCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := liveTLSFiles(t)
	installTLSFiles(t, files, certs)

	var sink configSink
	reloader, err := NewTLSReloader(t.Name(), files, tls.VersionTLS12, sink.store)
	require.NoError(t, err)

	now := time.Now()
	reloader.now = func() time.Time { return now }

	clientCA, err := os.ReadFile(certs.ClientCA)
	require.NoError(t, err)
	otherCA, err := os.ReadFile(otherCerts.ClientCA)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(files.CA, append(clientCA, otherCA...), 0o600))
	require.NoError(t, reloader.Reload(true))
	require.Len(t, reloader.ticketKeys, 1, "the CA change must pin exactly one key")
	pinned := reloader.ticketKeys[0]

	// Short of a full rotation period, a reload must leave the pinned
	// key alone.
	now = now.Add(ticketKeyRotation - time.Second)
	require.NoError(t, reloader.Reload(true))
	require.Equal(t, []ticketKeyEntry{pinned}, reloader.ticketKeys,
		"the pinned key must not rotate before a full rotation period has passed")

	// Once a rotation period has passed, the next reload must rotate
	// in a new key rather than keeping the pinned one in sole use
	// indefinitely, while keeping the old one so tickets it already
	// issued keep resuming.
	now = now.Add(2 * time.Second)
	require.NoError(t, reloader.Reload(true))
	require.Len(t, reloader.ticketKeys, 2)
	require.NotEqual(t, pinned.key, reloader.ticketKeys[0].key, "rotation must generate a new key")
	require.Equal(t, pinned, reloader.ticketKeys[1], "the previous key must be kept for tickets it already issued")

	// Once the pinned key is older than its lifetime, it must be
	// dropped rather than kept forever.
	now = pinned.created.Add(ticketKeyLifetime + time.Second)
	require.NoError(t, reloader.Reload(true))
	for _, k := range reloader.ticketKeys {
		require.NotEqual(t, pinned.key, k.key, "an expired key must not still be handed to configs")
	}
}

// tlsHandshake completes one TLS connection between a server using
// serverConfig and a client using clientConfig over a loopback
// listener, requires both sides to succeed, and returns the client's
// connection state.
func tlsHandshake(t *testing.T, serverConfig, clientConfig *tls.Config) tls.ConnectionState {
	t.Helper()
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
			serverErr <- fmt.Errorf("server handshake: %w", err)
			return
		}
		// A TLS 1.3 server hands out the session ticket after the
		// handshake, and the client only processes it while reading.
		_, err = conn.Write([]byte("ok"))
		serverErr <- err
	}()

	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", ln.Addr().String(), clientConfig)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(timeout)))
	_, err = io.ReadAll(conn)
	require.NoError(t, err)
	var serverResult error
	require.Eventually(t, func() bool {
		select {
		case serverResult = <-serverErr:
			return true
		default:
			return false
		}
	}, timeout, 10*time.Millisecond, "the server side did not finish the handshake in time")
	require.NoError(t, serverResult)
	return conn.ConnectionState()
}
