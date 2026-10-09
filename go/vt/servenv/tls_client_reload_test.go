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
	"path"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
