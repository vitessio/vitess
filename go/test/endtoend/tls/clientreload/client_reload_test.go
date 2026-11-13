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

// Package clientreload checks that a running vtgate reloads the TLS
// files of its gRPC client to the tablets (the --tablet-grpc-* flags) on
// SIGHUP: once the tablet restarts, and vtgate reconnects to it, vtgate
// presents the reloaded certificate, verifies the tablet against the
// reloaded CRL, and keeps the files it loaded when a reload fails.
package clientreload

import (
	"flag"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/tlstest"
)

const (
	keyspaceName = "ks"
	cell         = "zone1"
	schemaSQL    = "create table t (id bigint primary key) Engine=InnoDB"
	// tabletServerName is the name of every tablet server certificate,
	// which vtgate verifies the tablet against.
	tabletServerName = "tablet.example.com"
	// queryTimeout bounds how long vtgate may take to reach the tablet,
	// or to give up on it, after the tablet restarts.
	queryTimeout = 60 * time.Second
)

var (
	clusterInstance *cluster.LocalProcessCluster
	// root holds the certificates, named after the arguments of the
	// tlstest calls that create them.
	root string
	// vtgateFiles are the --tablet-grpc-* files of vtgate, and
	// tabletFiles the --grpc-* files of the tablet, which the test
	// rotates in place.
	vtgateFiles struct{ cert, key, ca, crl string }
	tabletFiles struct{ cert, key, ca string }
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, "localhost")
		defer clusterInstance.Teardown()

		if err := clusterInstance.StartTopo(); err != nil {
			fmt.Fprintf(os.Stderr, "cannot start topo: %v\n", err)
			return 1
		}

		if err := createCerts(); err != nil {
			fmt.Fprintf(os.Stderr, "cannot create certs: %v\n", err)
			return 1
		}

		// The tablet requires the clients that connect with TLS, which
		// only vtgate does, to present a certificate that its CA file
		// trusts, and serves vtctld and vtorc in plaintext.
		clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs,
			"--grpc-cert", tabletFiles.cert,
			"--grpc-key", tabletFiles.key,
			"--grpc-ca", tabletFiles.ca,
			"--grpc-enable-optional-tls",
		)
		if err := clusterInstance.StartUnshardedKeyspace(cluster.Keyspace{Name: keyspaceName, SchemaSQL: schemaSQL}, 0, false, clusterInstance.Cell); err != nil {
			fmt.Fprintf(os.Stderr, "cannot start keyspace: %v\n", err)
			return 1
		}

		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs,
			"--tablet-grpc-cert", vtgateFiles.cert,
			"--tablet-grpc-key", vtgateFiles.key,
			"--tablet-grpc-ca", vtgateFiles.ca,
			"--tablet-grpc-crl", vtgateFiles.crl,
			"--tablet-grpc-server-name", tabletServerName,
		)
		if err := clusterInstance.StartVtgate(); err != nil {
			fmt.Fprintf(os.Stderr, "cannot start vtgate: %v\n", err)
			return 1
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

// createCerts creates two tablet server certificates, a CRL that
// revokes neither, and the client certificates of two client CAs, and
// installs the first of each.
func createCerts() error {
	root = path.Join(clusterInstance.TmpDirectory, "certs")
	live := path.Join(root, "live")
	if err := os.MkdirAll(live, 0o700); err != nil {
		return err
	}
	tlstest.CreateCA(root)
	tlstest.CreateIntermediateCA(root, tlstest.CA, "01", "tablet-server-ca", "tablet server CA")
	tlstest.CreateSignedCert(root, "tablet-server-ca", "02", "tablet-server-1", tabletServerName)
	tlstest.CreateSignedCert(root, "tablet-server-ca", "03", "tablet-server-2", tabletServerName)
	tlstest.CreateCRL(root, "tablet-server-ca")
	tlstest.CreateIntermediateCA(root, tlstest.CA, "04", "old-client-ca", "old client CA")
	tlstest.CreateSignedCert(root, "old-client-ca", "05", "old-client", "old client")
	tlstest.CreateIntermediateCA(root, tlstest.CA, "06", "new-client-ca", "new client CA")
	tlstest.CreateSignedCert(root, "new-client-ca", "07", "new-client", "new client")
	tlstest.CreateSignedCert(root, "new-client-ca", "08", "other-client", "other client")

	vtgateFiles.cert, vtgateFiles.key, vtgateFiles.ca, vtgateFiles.crl = path.Join(live, "vtgate-cert.pem"), path.Join(live, "vtgate-key.pem"), path.Join(live, "vtgate-ca.pem"), path.Join(live, "vtgate-crl.pem")
	tabletFiles.cert, tabletFiles.key, tabletFiles.ca = path.Join(live, "tablet-cert.pem"), path.Join(live, "tablet-key.pem"), path.Join(live, "tablet-ca.pem")
	return install(map[string]string{
		vtgateFiles.cert: "old-client-cert.pem",
		vtgateFiles.key:  "old-client-key.pem",
		vtgateFiles.ca:   "tablet-server-ca-cert.pem",
		vtgateFiles.crl:  "tablet-server-ca-crl.pem",
		tabletFiles.cert: "tablet-server-1-cert.pem",
		tabletFiles.key:  "tablet-server-1-key.pem",
		tabletFiles.ca:   "old-client-ca-cert.pem",
	})
}

// install copies each file of root named by files to the live path it
// is keyed by, replacing it by an atomic rename, so that a reload never
// reads a file that is being written.
func install(files map[string]string) error {
	for dst, src := range files {
		b, err := os.ReadFile(path.Join(root, src))
		if err != nil {
			return err
		}
		staged := dst + ".staged"
		if err := os.WriteFile(staged, b, 0o600); err != nil {
			return err
		}
		if err := os.Rename(staged, dst); err != nil {
			return err
		}
	}
	return nil
}

// restartTablet restarts the tablet, which closes vtgate's connection
// to it, so that vtgate connects to it again with its current files.
func restartTablet(t *testing.T) {
	t.Helper()
	tablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet().VttabletProcess
	require.NoError(t, tablet.TearDown())
	require.NoError(t, tablet.Setup())
}

// query runs a query that vtgate sends to the tablet.
func query(t *testing.T) error {
	params := clusterInstance.GetVTParams(keyspaceName)
	conn, err := mysql.Connect(t.Context(), &params)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.ExecuteFetch("select id from t", 1, false)
	return err
}

// requireQueries requires a query through vtgate to succeed, or keep
// failing, within queryTimeout.
func requireQueries(t *testing.T, succeed bool, msg string) {
	t.Helper()
	var err error
	assert.Eventually(t, func() bool {
		err = query(t)
		return (err == nil) == succeed
	}, queryTimeout, time.Second, msg)
	if succeed {
		require.NoError(t, err, msg)
	} else {
		require.Error(t, err, msg)
	}
}

// clientReloadErrors returns vtgate's count of failed reloads of its
// clients' TLS files.
func clientReloadErrors(t *testing.T) float64 {
	t.Helper()
	counts, _ := clusterInstance.VtgateProcess.GetVars()["TLSReloadErrors"].(map[string]any)
	count, _ := counts["clients"].(float64)
	return count
}

// TestClientTLSReload runs in order: each step starts from the files
// the previous one installed.
func TestClientTLSReload(t *testing.T) {
	requireQueries(t, true, "vtgate must reach the tablet with the files it started with")

	t.Run("presents the reloaded certificate", func(t *testing.T) {
		// The tablet trusts only the new client CA from now on, so
		// that vtgate reaches it only with the new certificate.
		require.NoError(t, install(map[string]string{
			vtgateFiles.cert: "new-client-cert.pem",
			vtgateFiles.key:  "new-client-key.pem",
			tabletFiles.ca:   "new-client-ca-cert.pem",
		}))
		require.NoError(t, clusterInstance.VtgateProcess.SendSIGHUP())
		restartTablet(t)
		requireQueries(t, true, "vtgate must present the certificate it reloaded")
	})

	t.Run("keeps its files through a half-finished rotation", func(t *testing.T) {
		errorsBefore := clientReloadErrors(t)
		// The certificate is replaced before its key.
		require.NoError(t, install(map[string]string{vtgateFiles.cert: "other-client-cert.pem"}))
		t.Cleanup(func() {
			require.NoError(t, install(map[string]string{vtgateFiles.cert: "new-client-cert.pem"}))
		})
		require.NoError(t, clusterInstance.VtgateProcess.SendSIGHUP())
		assert.Eventually(t, func() bool {
			return clientReloadErrors(t) > errorsBefore
		}, queryTimeout, 100*time.Millisecond, "the failed reload must be counted")
		restartTablet(t)
		requireQueries(t, true, "vtgate must keep the certificate and key it loaded before")
	})

	t.Run("refuses a tablet that the reloaded CRL revokes", func(t *testing.T) {
		tlstest.RevokeCertAndRegenerateCRL(root, "tablet-server-ca", "tablet-server-1")
		require.NoError(t, install(map[string]string{vtgateFiles.crl: "tablet-server-ca-crl.pem"}))
		require.NoError(t, clusterInstance.VtgateProcess.SendSIGHUP())
		restartTablet(t)
		requireQueries(t, false, "vtgate must refuse the tablet whose certificate the reloaded CRL revokes")

		// The tablet's other certificate, which the CRL does not
		// revoke, is accepted, so it is the revocation that vtgate
		// refused.
		require.NoError(t, install(map[string]string{
			tabletFiles.cert: "tablet-server-2-cert.pem",
			tabletFiles.key:  "tablet-server-2-key.pem",
		}))
		restartTablet(t)
		requireQueries(t, true, "vtgate must accept a tablet certificate that the CRL does not revoke")
	})
}
