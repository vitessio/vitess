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
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
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

func (s *configSink) current() *tls.Config {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.configs[len(s.configs)-1]
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
	reloader.Start(t.Context(), signals, true, 0)
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

// TestTLSReloaderStartOnProcessSignals checks that a server's TLS files
// are reloaded on the process's SIGHUPs only with
// --tls-reload-on-sighup, so that a SIGHUP sent to reload the table
// ACLs, for one, leaves them as they were loaded.
func TestTLSReloaderStartOnProcessSignals(t *testing.T) {
	// Stands for the process's other handlers of SIGHUP, which keep it
	// from terminating the process.
	sighups := make(chan os.Signal, 1)
	signal.Notify(sighups, syscall.SIGHUP)
	t.Cleanup(func() { signal.Stop(sighups) })
	sighup := func() {
		require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGHUP))
		select {
		case <-sighups:
		case <-time.After(30 * time.Second):
			require.FailNow(t, "the SIGHUP must have been delivered")
		}
	}

	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := liveTLSFiles(t)
	installTLSFiles(t, files, oldCerts)

	var ignoring, reloading configSink
	ignorer, err := NewTLSReloader(t.Name()+"-ignoring", files, tls.VersionTLS12, ignoring.store)
	require.NoError(t, err)
	ignorer.StartOnProcessSignals(false, 0)
	t.Cleanup(ignorer.Stop)
	reloader, err := NewTLSReloader(t.Name()+"-reloading", files, tls.VersionTLS12, reloading.store)
	require.NoError(t, err)
	reloader.StartOnProcessSignals(true, 0)
	t.Cleanup(reloader.Stop)

	installTLSFiles(t, files, newCerts)
	sighup()
	assert.Eventually(t, func() bool {
		return reloading.count() == 2
	}, 30*time.Second, 10*time.Millisecond, "a SIGHUP must reload the files with --tls-reload-on-sighup")
	require.Equal(t, readCert(t, newCerts.ServerCert).Raw, reloading.leaf())
	require.Equal(t, 1, ignoring.count(), "a SIGHUP must not reload the files without --tls-reload-on-sighup")
	require.Equal(t, readCert(t, oldCerts.ServerCert).Raw, ignoring.leaf())
}

// TestTLSReloaderStartOnProcessSignalsOnTheIntervalAlone checks that a
// server's TLS files are reloaded on the interval without
// --tls-reload-on-sighup, as for a Kubernetes secret.
func TestTLSReloaderStartOnProcessSignalsOnTheIntervalAlone(t *testing.T) {
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := liveTLSFiles(t)
	installTLSFiles(t, files, oldCerts)

	var sink configSink
	reloader, err := NewTLSReloader(t.Name(), files, tls.VersionTLS12, sink.store)
	require.NoError(t, err)
	reloader.StartOnProcessSignals(false, 10*time.Millisecond)
	t.Cleanup(reloader.Stop)

	installTLSFiles(t, files, newCerts)
	newLeaf := readCert(t, newCerts.ServerCert).Raw
	assert.Eventually(t, func() bool {
		return sink.count() > 1 && bytes.Equal(newLeaf, sink.leaf())
	}, 30*time.Second, 10*time.Millisecond, "the interval must reload the files without --tls-reload-on-sighup")
}

// TestTLSReloaderReloadsTheCRLAloneOnSignal checks that a signal that
// does not reload every file, as vtgate's MySQL server gets without
// --tls-reload-on-sighup, reloads the CRL alone, validated under the CA
// the config was built from, and keeps the certificate, key and CA.
func TestTLSReloaderReloadsTheCRLAloneOnSignal(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	otherCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := liveTLSFiles(t)
	installTLSFiles(t, files, certs)
	// A CRL of another CA, which revokes none of the clients of certs.
	copyFile(t, files.CRL, otherCerts.ClientCRL)

	var sink configSink
	reloader, err := NewTLSReloader(t.Name(), files, tls.VersionTLS12, sink.store)
	require.NoError(t, err)
	signals := make(chan os.Signal, 1)
	reloader.Start(t.Context(), signals, false, 0)
	t.Cleanup(reloader.Stop)
	client := func(cert, key string) *tls.Config {
		config, err := vttls.ClientConfig(vttls.VerifyIdentity, cert, key, certs.ServerCA, "", certs.ServerName, tls.VersionTLS12)
		require.NoError(t, err)
		return config
	}
	res := tryTLSHandshake(t, sink.current(), client(certs.RevokedClientCert, certs.RevokedClientKey))
	require.NoError(t, res.serverErr, "the client must be accepted before the CRL revokes it for this test to be meaningful")

	// Every file rotates to other certificates, but for the CRL, which
	// now revokes a client of certs.
	installTLSFiles(t, files, otherCerts)
	copyFile(t, files.CRL, certs.ClientCRL)
	signals <- syscall.SIGHUP
	assert.Eventually(t, func() bool {
		return sink.count() == 2
	}, 30*time.Second, 10*time.Millisecond)

	require.Equal(t, readCert(t, certs.ServerCert).Raw, sink.leaf(), "the server must keep its certificate")
	res = tryTLSHandshake(t, sink.current(), client(certs.ClientCert, certs.ClientKey))
	require.NoError(t, res.serverErr, "the server must keep the CA it was built with")
	res = tryTLSHandshake(t, sink.current(), client(certs.RevokedClientCert, certs.RevokedClientKey))
	require.ErrorContains(t, res.serverErr, "Certificate revoked", "the server must check clients against the reloaded CRL")
}

// TestTLSReloaderReloadsAfterACRLOnlyReload checks that once a signal
// reloaded the CRL alone, the next reload on the interval builds the
// config from the files again, even when they are back to what the last
// full load read, rather than skipping them as unchanged and keeping the
// CRL that the file no longer holds.
func TestTLSReloaderReloadsAfterACRLOnlyReload(t *testing.T) {
	certs := tlstest.CreateClientServerCertPairs(t.TempDir())
	otherCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := liveTLSFiles(t)
	installTLSFiles(t, files, certs)
	// A CRL of another CA, which revokes none of the clients of certs.
	copyFile(t, files.CRL, otherCerts.ClientCRL)

	var sink configSink
	reloader, err := NewTLSReloader(t.Name(), files, tls.VersionTLS12, sink.store)
	require.NoError(t, err)
	revokedClient, err := vttls.ClientConfig(vttls.VerifyIdentity, certs.RevokedClientCert, certs.RevokedClientKey, certs.ServerCA, "", certs.ServerName, tls.VersionTLS12)
	require.NoError(t, err)

	copyFile(t, files.CRL, certs.ClientCRL)
	require.NoError(t, reloader.ReloadCRL())
	res := tryTLSHandshake(t, sink.current(), revokedClient)
	require.ErrorContains(t, res.serverErr, "Certificate revoked", "the CRL must have been reloaded for this test to be meaningful")

	// The CRL file goes back to what the last full load read.
	copyFile(t, files.CRL, otherCerts.ClientCRL)
	require.NoError(t, reloader.Reload(false))
	res = tryTLSHandshake(t, sink.current(), revokedClient)
	require.NoError(t, res.serverErr, "the reload must apply the CRL that the file holds again")
}

func TestTLSReloaderReloadsPeriodically(t *testing.T) {
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := liveTLSFiles(t)
	installTLSFiles(t, files, oldCerts)

	var sink configSink
	reloader, err := NewTLSReloader(t.Name(), files, tls.VersionTLS12, sink.store)
	require.NoError(t, err)
	reloader.Start(t.Context(), nil, true, 10*time.Millisecond)
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
// load is recorded with the digest of what was loaded, rather than the
// digest read before the replacement, which would have later reloads
// take the files for unchanged.
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
		copyFile(t, files.CA, otherCerts.ClientCA)
	}

	require.NoError(t, reloader.Reload(true))
	require.True(t, fired, "the hook must have run for this test to be meaningful")

	want, _, err := reloader.digest()
	require.NoError(t, err)
	require.Equal(t, want, reloader.fingerprint, "the recorded digest must be of the files that were actually loaded")
}

func TestTLSReloaderKeepsConfigOnError(t *testing.T) {
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := liveTLSFiles(t)
	installTLSFiles(t, files, oldCerts)

	var sink configSink
	reloader, err := NewTLSReloader(t.Name(), files, tls.VersionTLS12, sink.store)
	require.NoError(t, err)
	errorsBefore := tlsReloadErrors.Counts()[t.Name()]

	// The certificate is replaced before its key, as a rotation that
	// writes the files one at a time does.
	copyFile(t, files.Cert, newCerts.ServerCert)
	err = reloader.Reload(false)
	require.ErrorContains(t, err, "the previous config stays in place")
	require.Equal(t, 1, sink.count())
	require.Equal(t, readCert(t, oldCerts.ServerCert).Raw, sink.leaf())
	require.Equal(t, errorsBefore+1, tlsReloadErrors.Counts()[t.Name()])

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

// TestTLSReloaderSessionResumption checks, through real handshakes
// against a server that loads its config the way the gRPC server
// does, that a session resumed after a reload is held against the
// reloaded config: it resumes while the client is still accepted, and
// not once the CA that issued the client's certificate is removed or
// a reloaded CRL revokes it. Go verifies a resumed session's chain
// against the current CAs, and VerifyConnection runs on resumption.
func TestTLSReloaderSessionResumption(t *testing.T) {
	for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
		t.Run(tls.VersionName(version), func(t *testing.T) {
			certs := tlstest.CreateClientServerCertPairs(t.TempDir())
			otherCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
			newServer := func(t *testing.T) (TLSServerFiles, *TLSReloader, *tls.Config) {
				files := liveTLSFiles(t)
				installTLSFiles(t, files, certs)
				copyFile(t, files.CRL, otherCerts.ClientCRL)
				var current atomic.Pointer[tls.Config]
				reloader, err := NewTLSReloader(t.Name(), files, tls.VersionTLS12, func(config *tls.Config) {
					config.MaxVersion = version
					current.Store(config)
				})
				require.NoError(t, err)
				return files, reloader, &tls.Config{
					GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
						return current.Load(), nil
					},
				}
			}
			newClient := func(t *testing.T, cert, key string) *tls.Config {
				config, err := vttls.ClientConfig(vttls.VerifyIdentity, cert, key, certs.ServerCA, "", certs.ServerName, tls.VersionTLS12)
				require.NoError(t, err)
				config.ClientSessionCache = tls.NewLRUClientSessionCache(1)
				return config
			}
			established := func(t *testing.T, server, client *tls.Config) {
				require.False(t, tlsHandshake(t, server, client).DidResume)
				require.True(t, tlsHandshake(t, server, client).DidResume, "the client must resume for this test to be meaningful")
			}

			t.Run("resumes after a reload that still accepts the client", func(t *testing.T) {
				files, reloader, server := newServer(t)
				client := newClient(t, certs.ClientCert, certs.ClientKey)
				established(t, server, client)

				copyFile(t, files.CRL, certs.ClientCRL)
				require.NoError(t, reloader.Reload(false))
				require.True(t, tlsHandshake(t, server, client).DidResume)
			})

			t.Run("does not resume once the client's CA is removed", func(t *testing.T) {
				files, reloader, server := newServer(t)
				client := newClient(t, certs.ClientCert, certs.ClientKey)
				established(t, server, client)

				copyFile(t, files.CA, otherCerts.ClientCA)
				copyFile(t, files.CRL, otherCerts.ClientCRL)
				require.NoError(t, reloader.Reload(false))
				res := tryTLSHandshake(t, server, client)
				require.Error(t, res.serverErr, "a client whose CA was removed must not be accepted on a session of before")
				require.False(t, res.state.DidResume)
			})

			t.Run("does not resume once a reloaded CRL revokes the client", func(t *testing.T) {
				files, reloader, server := newServer(t)
				client := newClient(t, certs.RevokedClientCert, certs.RevokedClientKey)
				established(t, server, client)

				copyFile(t, files.CRL, certs.ClientCRL)
				require.NoError(t, reloader.Reload(false))
				res := tryTLSHandshake(t, server, client)
				require.ErrorContains(t, res.serverErr, "Certificate revoked")
			})
		})
	}
}

// tlsHandshake completes one TLS connection between a server using
// serverConfig and a client using clientConfig over a loopback
// listener, requires both sides to succeed, and returns the client's
// connection state.
func tlsHandshake(t *testing.T, serverConfig, clientConfig *tls.Config) tls.ConnectionState {
	t.Helper()
	res := tryTLSHandshake(t, serverConfig, clientConfig)
	require.NoError(t, res.clientErr)
	require.NoError(t, res.serverErr)
	return res.state
}

type tlsHandshakeResult struct {
	state                tls.ConnectionState
	clientErr, serverErr error
}

// tryTLSHandshake is tlsHandshake without requiring either side to
// succeed.
func tryTLSHandshake(t *testing.T, serverConfig, clientConfig *tls.Config) tlsHandshakeResult {
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

	var res tlsHandshakeResult
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", ln.Addr().String(), clientConfig)
	if err == nil {
		defer conn.Close()
		require.NoError(t, conn.SetDeadline(time.Now().Add(timeout)))
		// A TLS 1.3 client learns that the server rejected its
		// certificate only once it reads.
		_, res.clientErr = io.ReadAll(conn)
		res.state = conn.ConnectionState()
	} else {
		res.clientErr = err
	}
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
