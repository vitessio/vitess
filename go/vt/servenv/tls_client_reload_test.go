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
	"context"
	"crypto/tls"
	"os"
	"os/signal"
	"path"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/vttls"
)

// TestRunClientTLSReload checks that the certificate a TLS client of
// the process loaded is reloaded on a signal, and on the interval
// alone.
func TestRunClientTLSReload(t *testing.T) {
	for _, tc := range []struct {
		name     string
		signal   bool
		interval time.Duration
	}{
		{name: "signal", signal: true},
		{name: "interval", interval: 10 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(vttls.ResetCachedFilesForTest)
			oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
			newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
			live := t.TempDir()
			cert, key, ca := path.Join(live, "cert.pem"), path.Join(live, "key.pem"), path.Join(live, "ca.pem")
			install := func(certs tlstest.ClientServerKeyPairs) {
				for dst, src := range map[string]string{cert: certs.ClientCert, key: certs.ClientKey, ca: certs.ServerCA} {
					copyFile(t, dst, src)
				}
			}
			clientCert := func() []byte {
				config, err := vttls.ClientConfig(vttls.VerifyIdentity, cert, key, ca, "", "", tls.VersionTLS12)
				require.NoError(t, err)
				return config.Certificates[0].Certificate[0]
			}

			install(oldCerts)
			require.Equal(t, readCert(t, oldCerts.ClientCert).Raw, clientCert())

			ctx, cancel := context.WithCancel(t.Context())
			signals := make(chan os.Signal, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				runClientTLSReload(ctx, signals, tc.interval)
			}()
			t.Cleanup(func() {
				cancel()
				<-done
			})

			install(newCerts)
			if tc.signal {
				signals <- syscall.SIGHUP
			}
			newCert := readCert(t, newCerts.ClientCert).Raw
			assert.Eventually(t, func() bool {
				return bytes.Equal(newCert, clientCert())
			}, 30*time.Second, 10*time.Millisecond, "the client must use the new certificate once its files are reloaded")
		})
	}
}

// clientFiles are the files of a TLS client of the process, which a
// test rotates in place.
type clientFiles struct {
	t             *testing.T
	cert, key, ca string
}

func newClientFiles(t *testing.T) clientFiles {
	vttls.ResetCachedFilesForTest()
	t.Cleanup(vttls.ResetCachedFilesForTest)
	live := t.TempDir()
	return clientFiles{t: t, cert: path.Join(live, "cert.pem"), key: path.Join(live, "key.pem"), ca: path.Join(live, "ca.pem")}
}

func (f clientFiles) install(certs tlstest.ClientServerKeyPairs) {
	for dst, src := range map[string]string{f.cert: certs.ClientCert, f.key: certs.ClientKey, f.ca: certs.ServerCA} {
		copyFile(f.t, dst, src)
	}
}

// clientCert returns the certificate that a config the client builds
// now presents.
func (f clientFiles) clientCert() []byte {
	config, err := vttls.ClientConfig(vttls.VerifyIdentity, f.cert, f.key, f.ca, "", "", tls.VersionTLS12)
	require.NoError(f.t, err)
	return config.Certificates[0].Certificate[0]
}

// isolateOnTermSyncHooks has the test register its OnTermSync hooks
// apart from the other tests', and fires them when it ends.
func isolateOnTermSyncHooks(t *testing.T) {
	// Replacing the hooks while another test's timed-out Fire still holds
	// their lock would leave it unlocking an unlocked mutex: wait for it.
	require.True(t, fireOnTermSyncHooks(30*time.Second))
	onTermSyncHooks = event.Hooks{}
	t.Cleanup(func() {
		require.True(t, fireOnTermSyncHooks(30*time.Second), "the reload must stop when the process terminates")
		onTermSyncHooks = event.Hooks{}
	})
}

// TestStartClientTLSReload checks that with --tls-reload-on-sighup, a
// binary reloads its clients' files on the process's SIGHUPs from
// startup on, and reports each reload in the metrics of the clients.
func TestStartClientTLSReload(t *testing.T) {
	isolateOnTermSyncHooks(t)
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := newClientFiles(t)
	files.install(oldCerts)
	require.Equal(t, readCert(t, oldCerts.ClientCert).Raw, files.clientCert())

	startClientTLSReload(true, 0)

	tlsReloadSuccessTimestamp.Set(clientTLSReloadName, 0)
	files.install(newCerts)
	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGHUP))
	newCert := readCert(t, newCerts.ClientCert).Raw
	assert.Eventually(t, func() bool {
		return bytes.Equal(newCert, files.clientCert()) && tlsReloadSuccessTimestamp.Counts()[clientTLSReloadName] > 0
	}, 30*time.Second, 10*time.Millisecond, "SIGHUP must reload the client's files, and the reload be reported")

	// A key that no longer matches its certificate fails the reload,
	// which is counted, and the client keeps its files.
	errorsBefore := tlsReloadErrors.Counts()[clientTLSReloadName]
	copyFile(t, files.key, oldCerts.ClientKey)
	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGHUP))
	assert.Eventually(t, func() bool {
		return tlsReloadErrors.Counts()[clientTLSReloadName] > errorsBefore
	}, 30*time.Second, 10*time.Millisecond, "a failed reload must be counted")
	require.Equal(t, newCert, files.clientCert())
}

// TestStartClientTLSReloadOnTheIntervalAlone checks that with
// --tls-reload-interval and without --tls-reload-on-sighup, a binary
// reloads its clients' files on the interval, as for a Kubernetes
// secret.
func TestStartClientTLSReloadOnTheIntervalAlone(t *testing.T) {
	isolateOnTermSyncHooks(t)
	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := newClientFiles(t)
	files.install(oldCerts)
	require.Equal(t, readCert(t, oldCerts.ClientCert).Raw, files.clientCert())

	startClientTLSReload(false, 10*time.Millisecond)

	files.install(newCerts)
	newCert := readCert(t, newCerts.ClientCert).Raw
	assert.Eventually(t, func() bool {
		return bytes.Equal(newCert, files.clientCert())
	}, 30*time.Second, 10*time.Millisecond, "the interval must reload the client's files without --tls-reload-on-sighup")
}

// TestStartClientTLSReloadLeavesSIGHUPAloneByDefault checks that
// without --tls-reload-on-sighup, a SIGHUP, such as one sent to reload
// the table ACLs, leaves the clients' files as they were loaded.
func TestStartClientTLSReloadLeavesSIGHUPAloneByDefault(t *testing.T) {
	// Stands for the process's other handlers of SIGHUP, which keep it
	// from terminating the process.
	sighups := make(chan os.Signal, 1)
	signal.Notify(sighups, syscall.SIGHUP)
	t.Cleanup(func() { signal.Stop(sighups) })

	oldCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	newCerts := tlstest.CreateClientServerCertPairs(t.TempDir())
	files := newClientFiles(t)
	files.install(oldCerts)
	oldCert := readCert(t, oldCerts.ClientCert).Raw
	require.Equal(t, oldCert, files.clientCert())

	startClientTLSReload(false, 0)

	files.install(newCerts)
	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGHUP))
	select {
	case <-sighups:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the SIGHUP must have been delivered for this test to be meaningful")
	}
	assert.Never(t, func() bool {
		return !bytes.Equal(oldCert, files.clientCert())
	}, time.Second, 10*time.Millisecond, "a SIGHUP must not reload the clients' files without --tls-reload-on-sighup")
}
