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

package grpcclient

import (
	"context"
	"crypto/tls"
	"net"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/vttls"
)

// TestSecureCredentialsReload checks that a gRPC connection that
// reconnects after the client's TLS files were reloaded presents, and
// verifies the server against, what the files hold then, rather than
// what they held when it was dialed.
func TestSecureCredentialsReload(t *testing.T) {
	// Two unrelated CAs, each with a server and a client certificate,
	// under the same names, as a CA rotation issues them.
	oldRoot, newRoot, live := t.TempDir(), t.TempDir(), t.TempDir()
	for _, root := range []string{oldRoot, newRoot} {
		tlstest.CreateCA(root)
		tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", "server.example.com")
		tlstest.CreateSignedCert(root, tlstest.CA, "02", "client", "client.example.com")
	}
	install := func(root string) {
		for _, name := range []string{"client-cert.pem", "client-key.pem", "ca-cert.pem"} {
			b, err := os.ReadFile(path.Join(root, name))
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path.Join(live, name), b, 0o600))
		}
	}
	// serve serves gRPC health checks on addr with the server
	// certificate of root, to clients with a certificate root issued.
	serve := func(root, addr string) *grpc.Server {
		config, err := vttls.ReadServerConfig(path.Join(root, "server-cert.pem"), path.Join(root, "server-key.pem"), path.Join(root, "ca-cert.pem"), "", "", tls.VersionTLS12)
		require.NoError(t, err)
		server := grpc.NewServer(grpc.Creds(credentials.NewTLS(config)))
		healthpb.RegisterHealthServer(server, health.NewServer())
		ln, err := net.Listen("tcp", addr)
		require.NoError(t, err)
		go server.Serve(ln)
		t.Cleanup(server.Stop)
		return server
	}

	install(oldRoot)
	creds, err := secureCredentials(path.Join(live, "client-cert.pem"), path.Join(live, "client-key.pem"), path.Join(live, "ca-cert.pem"), "", "server.example.com")
	require.NoError(t, err)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	oldServer := serve(oldRoot, addr)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	check := func() error {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
		return err
	}
	require.NoError(t, check())

	// The rotation replaces the client's files, which are reloaded,
	// and the server, which drops the connection.
	install(newRoot)
	_, err = vttls.ReloadCachedFiles()
	if err != nil {
		require.NotContains(t, err.Error(), live)
	}
	oldServer.Stop()
	serve(newRoot, addr)

	var checkErr error
	assert.Eventually(t, func() bool {
		checkErr = check()
		return checkErr == nil
	}, 30*time.Second, 50*time.Millisecond, "the connection must reconnect with the reloaded files")
	require.NoError(t, checkErr)
}
