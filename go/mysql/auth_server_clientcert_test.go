/*
Copyright 2019 The Vitess Authors.

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

package mysql

import (
	"context"
	"crypto/tls"
	"net"
	"path"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/vttls"
)

const clientCertUsername = "Client Cert"

func TestValidCert(t *testing.T) {
	th := &testHandler{}

	authServer := newAuthServerClientCert(string(MysqlClearPassword))

	// Create the listener, so we can get its host.
	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false, false, 0, 0)
	require.NoError(t, err, "NewListener failed: %v", err)
	defer l.Close()
	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port

	// Create the certs.
	root := t.TempDir()
	tlstest.CreateCA(root)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", "server.example.com")
	tlstest.CreateSignedCert(root, tlstest.CA, "02", "client", clientCertUsername)
	tlstest.CreateCRL(root, tlstest.CA)

	// Create the server with TLS config.
	serverConfig, err := vttls.ServerConfig(
		path.Join(root, "server-cert.pem"),
		path.Join(root, "server-key.pem"),
		path.Join(root, "ca-cert.pem"),
		path.Join(root, "ca-crl.pem"),
		"",
		tls.VersionTLS12)
	require.NoError(t, err, "TLSServerConfig failed: %v", err)

	l.TLSConfig.Store(serverConfig)
	go func() {
		l.Accept()
	}()

	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: clientCertUsername,
		Pass:  "",
		// SSL flags.
		SslMode:    vttls.VerifyIdentity,
		SslCa:      path.Join(root, "ca-cert.pem"),
		SslCert:    path.Join(root, "client-cert.pem"),
		SslKey:     path.Join(root, "client-key.pem"),
		ServerName: "server.example.com",
	}

	ctx := context.Background()
	conn, err := Connect(ctx, params)
	require.NoError(t, err, "Connect failed: %v", err)

	defer conn.Close()

	// Make sure this went through SSL.
	result, err := conn.ExecuteFetch("ssl echo", 10000, true)
	require.NoError(t, err, "ExecuteFetch failed: %v", err)
	assert.Equal(t, "ON", result.Rows[0][0].ToString(), "Got wrong result from ExecuteFetch(ssl echo): %v", result)

	userData := th.LastConn().UserData.Get()
	assert.Equal(t, clientCertUsername, userData.Username, "userdata username is %v, expected %v", userData.Username, clientCertUsername)

	expectedGroups := []string{"localhost", clientCertUsername}
	assert.True(t, reflect.DeepEqual(userData.Groups, expectedGroups), "userdata groups is %v, expected %v", userData.Groups, expectedGroups)

	// Send a ComQuit to avoid the error message on the server side.
	conn.writeComQuit()
}

func TestNoCert(t *testing.T) {
	th := &testHandler{}

	authServer := newAuthServerClientCert(string(MysqlClearPassword))

	// Create the listener, so we can get its host.
	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false, false, 0, 0)
	require.NoError(t, err, "NewListener failed: %v", err)
	defer l.Close()
	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port

	// Create the certs.
	root := t.TempDir()
	tlstest.CreateCA(root)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", "server.example.com")
	tlstest.CreateCRL(root, tlstest.CA)

	// Create the server with TLS config.
	serverConfig, err := vttls.ServerConfig(
		path.Join(root, "server-cert.pem"),
		path.Join(root, "server-key.pem"),
		path.Join(root, "ca-cert.pem"),
		path.Join(root, "ca-crl.pem"),
		"",
		tls.VersionTLS12)
	require.NoError(t, err, "TLSServerConfig failed: %v", err)

	l.TLSConfig.Store(serverConfig)
	go func() {
		l.Accept()
	}()

	// Setup the right parameters.
	params := &ConnParams{
		Host:       host,
		Port:       port,
		Uname:      "user1",
		Pass:       "",
		SslMode:    vttls.VerifyIdentity,
		SslCa:      path.Join(root, "ca-cert.pem"),
		ServerName: "server.example.com",
	}

	ctx := context.Background()
	conn, err := Connect(ctx, params)
	assert.Error(t, err, "Connect() should have errored due to no client cert")

	if conn != nil {
		conn.Close()
	}
}
