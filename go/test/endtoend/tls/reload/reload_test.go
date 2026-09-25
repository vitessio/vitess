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

// Package reload checks that a running vtgate process reloads the TLS
// files of its gRPC and MySQL servers from disk, on SIGHUP and with
// --tls-reload-interval, without dropping established connections.
package reload

import (
	"bytes"
	"crypto/tls"
	"encoding/pem"
	"flag"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/vttls"
)

const (
	keyspaceName = "ks"
	cell         = "zone1"
	// reloadTimeout bounds how long a reload may take to show.
	reloadTimeout = 30 * time.Second
)

var (
	clusterInstance *cluster.LocalProcessCluster
	// oldCerts and newCerts are unrelated sets of certificates, which
	// the test installs in turn at the paths of liveFiles.
	oldCerts, newCerts tlstest.ClientServerKeyPairs
	liveFiles          struct{ cert, key, ca, crl string }
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
		if err := clusterInstance.StartUnshardedKeyspace(cluster.Keyspace{Name: keyspaceName}, 0, false, clusterInstance.Cell); err != nil {
			fmt.Fprintf(os.Stderr, "cannot start keyspace: %v\n", err)
			return 1
		}

		certs := path.Join(clusterInstance.TmpDirectory, "certs")
		for _, dir := range []string{"old", "new", "live"} {
			if err := os.MkdirAll(path.Join(certs, dir), 0o700); err != nil {
				fmt.Fprintf(os.Stderr, "cannot create cert directory: %v\n", err)
				return 1
			}
		}
		oldCerts = tlstest.CreateClientServerCertPairs(path.Join(certs, "old"))
		newCerts = tlstest.CreateClientServerCertPairs(path.Join(certs, "new"))
		live := path.Join(certs, "live")
		liveFiles.cert, liveFiles.key, liveFiles.ca, liveFiles.crl = path.Join(live, "cert.pem"), path.Join(live, "key.pem"), path.Join(live, "ca.pem"), path.Join(live, "crl.pem")
		if err := installCerts(oldCerts); err != nil {
			fmt.Fprintf(os.Stderr, "cannot install certs: %v\n", err)
			return 1
		}

		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs,
			"--grpc-cert", liveFiles.cert,
			"--grpc-key", liveFiles.key,
			"--grpc-ca", liveFiles.ca,
			"--grpc-crl", liveFiles.crl,
			"--mysql-server-ssl-cert", liveFiles.cert,
			"--mysql-server-ssl-key", liveFiles.key,
			"--mysql-server-ssl-ca", liveFiles.ca,
			"--mysql-server-ssl-crl", liveFiles.crl,
		)
		if err := clusterInstance.StartVtgate(); err != nil {
			fmt.Fprintf(os.Stderr, "cannot start vtgate: %v\n", err)
			return 1
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

// installCerts copies the server side of certs to liveFiles, replacing
// each file by an atomic rename, so that a reload racing the rotation
// reads a file that a rotation wrote, never one it is writing.
func installCerts(certs tlstest.ClientServerKeyPairs) error {
	live := path.Dir(liveFiles.cert)
	stage := path.Join(live, "staging")
	if err := os.MkdirAll(stage, 0o700); err != nil {
		return err
	}
	for dst, src := range map[string]string{liveFiles.cert: certs.ServerCert, liveFiles.key: certs.ServerKey, liveFiles.ca: certs.ClientCA, liveFiles.crl: certs.ClientCRL} {
		b, err := os.ReadFile(src)
		if err != nil {
			return err
		}
		staged := path.Join(stage, path.Base(dst))
		if err := os.WriteFile(staged, b, 0o600); err != nil {
			return err
		}
		if err := os.Rename(staged, dst); err != nil {
			return err
		}
	}
	return nil
}

// certificateOf returns the DER encoding of the certificate in file.
func certificateOf(t *testing.T, file string) []byte {
	t.Helper()
	b, err := os.ReadFile(file)
	require.NoError(t, err)
	block, _ := pem.Decode(b)
	require.NotNil(t, block)
	return block.Bytes
}

// grpcServedCert makes a gRPC call to vtgate as a client of certs and
// returns the certificate vtgate presented. It does not fail t, so
// that it can run in assert.Eventually.
func grpcServedCert(t *testing.T, certs tlstest.ClientServerKeyPairs) ([]byte, error) {
	config, err := vttls.ClientConfig(vttls.VerifyIdentity, certs.ClientCert, certs.ClientKey, certs.ServerCA, "", certs.ServerName, tls.VersionTLS12)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(fmt.Sprintf("%s:%d", clusterInstance.Hostname, clusterInstance.VtgateGrpcPort), grpc.WithTransportCredentials(credentials.NewTLS(config)))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	var p peer.Peer
	if _, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{}, grpc.Peer(&p)); err != nil {
		return nil, err
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, fmt.Errorf("not a TLS connection: %T", p.AuthInfo)
	}
	return info.State.PeerCertificates[0].Raw, nil
}

// connectMySQL connects to vtgate's MySQL server as a client of certs,
// verifying vtgate's certificate against the server CA of certs. It
// does not fail t, so that it can run in assert.Eventually.
func connectMySQL(t *testing.T, certs tlstest.ClientServerKeyPairs) (*mysql.Conn, error) {
	params := clusterInstance.GetVTParams(keyspaceName)
	params.SslMode = vttls.VerifyIdentity
	params.SslCa = certs.ServerCA
	params.SslCert = certs.ClientCert
	params.SslKey = certs.ClientKey
	params.ServerName = certs.ServerName
	return mysql.Connect(t.Context(), &params)
}

// servesCerts returns whether vtgate serves the certificate of certs
// on both its gRPC and MySQL servers to a new connection, or an error
// naming the server that does not. It does not fail t, so that it can
// run in assert.Eventually.
func servesCerts(t *testing.T, certs tlstest.ClientServerKeyPairs, cert []byte) error {
	t.Helper()
	served, err := grpcServedCert(t, certs)
	if err != nil {
		return fmt.Errorf("the gRPC server: %v", err)
	}
	if !bytes.Equal(served, cert) {
		return fmt.Errorf("the gRPC server presents a different certificate")
	}
	conn, err := connectMySQL(t, certs)
	if err != nil {
		return fmt.Errorf("the MySQL server: %v", err)
	}
	conn.Close()
	return nil
}

// TestTLSReload runs in order: each step starts from the files the
// previous one installed.
func TestTLSReload(t *testing.T) {
	oldCert, newCert := certificateOf(t, oldCerts.ServerCert), certificateOf(t, newCerts.ServerCert)

	t.Run("SIGHUP", func(t *testing.T) {
		require.NoError(t, servesCerts(t, oldCerts, oldCert), "vtgate must serve the certificates it started with")

		// A MySQL connection established before the reload.
		established, err := connectMySQL(t, oldCerts)
		require.NoError(t, err)
		t.Cleanup(established.Close)

		require.NoError(t, installCerts(newCerts))
		// Without a reload, vtgate keeps serving what it loaded.
		_, err = grpcServedCert(t, newCerts)
		require.Error(t, err, "vtgate must not pick up the new files before it is told to")

		require.NoError(t, clusterInstance.VtgateProcess.SendSIGHUP())
		var reloadErr error
		assert.Eventually(t, func() bool {
			reloadErr = servesCerts(t, newCerts, newCert)
			return reloadErr == nil
		}, reloadTimeout, 100*time.Millisecond, "vtgate must serve the new certificates after SIGHUP: %v", reloadErr)

		// Clients of the old CA are no longer accepted on new
		// connections.
		_, err = grpcServedCert(t, oldCerts)
		require.Error(t, err)
		_, err = connectMySQL(t, oldCerts)
		require.Error(t, err)

		// The connection established before the reload keeps working.
		_, err = established.ExecuteFetch("select 1", 1, false)
		require.NoError(t, err)
	})

	t.Run("--tls-reload-interval", func(t *testing.T) {
		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, "--tls-reload-interval", "1s")
		require.NoError(t, clusterInstance.RestartVtgate())
		require.NoError(t, servesCerts(t, newCerts, newCert), "vtgate must serve the certificates it restarted with")

		require.NoError(t, installCerts(oldCerts))
		var reloadErr error
		assert.Eventually(t, func() bool {
			reloadErr = servesCerts(t, oldCerts, oldCert)
			return reloadErr == nil
		}, reloadTimeout, 100*time.Millisecond, "vtgate must serve the new certificates without a SIGHUP: %v", reloadErr)
	})
}
