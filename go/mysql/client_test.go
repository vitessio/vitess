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
	"fmt"
	"net"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/vttls"
)

// assertSQLError makes sure we get the right error.
func assertSQLError(t *testing.T, err error, code int, sqlState, subtext, query, pattern string) {
	t.Helper()

	require.Error(t, err, "was expecting SQLError %v / %v / %v but got no error.", code, sqlState, subtext)
	serr, ok := err.(*SQLError)
	require.True(t, ok, "was expecting SQLError %v / %v / %v but got: %v", code, sqlState, subtext, err)
	require.Equal(t, code, serr.Num, "was expecting SQLError %v / %v / %v but got code %v", code, sqlState, subtext, serr.Num)
	require.Equal(t, sqlState, serr.State, "was expecting SQLError %v / %v / %v but got state %v", code, sqlState, subtext, serr.State)
	if pattern != "" {
		require.Regexp(t, regexp.MustCompile(pattern), serr.Message)
	} else {
		require.True(t, subtext == "" || strings.Contains(serr.Message, subtext), "was expecting SQLError %v / %v / %v but got message %v", code, sqlState, subtext, serr.Message)
	}
	require.Equal(t, query, serr.Query, "was expecting SQLError %v / %v / %v with Query '%v' but got query '%v'", code, sqlState, subtext, query, serr.Query)
}

// TestConnectTimeout runs connection failure scenarios against a
// server that's not listening or has trouble.  This test is not meant
// to use a valid server. So we do not test bad handshakes here.
func TestConnectTimeout(t *testing.T) {
	// Create a socket, but it's not accepting. So all Dial
	// attempts will timeout.
	listener, err := net.Listen("tcp", "127.0.0.1:")
	require.NoError(t, err, "cannot listen: %v", err)
	host, port := getHostPort(t, listener.Addr())
	params := &ConnParams{
		Host: host,
		Port: port,
	}
	defer listener.Close()

	// Test that canceling the context really interrupts the Connect.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, err := Connect(ctx, params)
		assert.Equal(t, context.Canceled, err, "Was expecting context.Canceled but got: %v", err)
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	// Tests a connection timeout works.
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = Connect(ctx, params)
	cancel()
	assert.Equal(t, context.DeadlineExceeded, err, "Was expecting context.DeadlineExceeded but got: %v", err)

	// Tests a connection timeout through params
	ctx = context.Background()
	paramsWithTimeout := *params
	paramsWithTimeout.ConnectTimeoutMs = 1
	_, err = Connect(ctx, &paramsWithTimeout)
	cancel()
	assert.Equal(t, context.DeadlineExceeded, err, "Was expecting context.DeadlineExceeded but got: %v", err)

	// Now the server will listen, but close all connections on accept.
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				// Listener was closed.
				return
			}
			conn.Close()
		}
	}()
	ctx = context.Background()
	_, err = Connect(ctx, params)
	assertSQLError(t, err, CRServerLost, SSUnknownSQLState, "initial packet read failed", "", "")

	// Now close the listener. Connect should fail right away,
	// check the error.
	listener.Close()
	wg.Wait()
	_, err = Connect(ctx, params)
	assertSQLError(t, err, CRConnHostError, SSUnknownSQLState, "connection refused", "", "")

	// Tests a connection where Dial to a unix socket fails
	// properly returns the right error. To simulate exactly the
	// right failure, try to dial a Unix socket that's just a temp file.
	fd, err := os.CreateTemp("", "mysql")
	require.NoError(t, err, "cannot create TempFile: %v", err)
	name := fd.Name()
	fd.Close()
	params.UnixSocket = name
	ctx = context.Background()
	_, err = Connect(ctx, params)
	os.Remove(name)
	t.Log(err)
	assertSQLError(t, err, CRConnectionError, SSUnknownSQLState, "connection refused", "", "net\\.Dial\\(([a-z0-9A-Z_\\/]*)\\) to local server failed:")
}

// TestTLSClientDisabled creates a Server with TLS support, then connects
// with a client with TLS disabled.
func TestTLSClientDisabled(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
	}}
	defer authServer.close()

	// Create the listener, so we can get its host.
	// Below, we are enabling --ssl-verify-server-cert, which adds
	// a check that the common name of the certificate matches the
	// server host name we connect to.
	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false)
	require.NoError(t, err)
	defer l.Close()

	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port

	// Create the certs.
	root := t.TempDir()
	tlstest.CreateCA(root)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", host)
	tlstest.CreateSignedCert(root, tlstest.CA, "02", "client", "Client Cert")

	// Create the server with TLS config.
	serverConfig, err := vttls.ServerConfig(
		path.Join(root, "server-cert.pem"),
		path.Join(root, "server-key.pem"),
		"",
		"",
		"",
		tls.VersionTLS12)
	require.NoError(t, err)
	l.TLSConfig.Store(serverConfig)

	var wg sync.WaitGroup
	wg.Add(1)
	go func(l *Listener) {
		wg.Done()
		l.Accept()
	}(l)
	// This is ensure the listener is called
	wg.Wait()
	// Sleep so that the Accept function is called as well.'
	time.Sleep(3 * time.Second)

	// Setup the right parameters.
	params := &ConnParams{
		Host:    host,
		Port:    port,
		Uname:   "user1",
		Pass:    "password1",
		SslMode: vttls.Disabled,
	}

	conn, err := Connect(context.Background(), params)
	require.NoError(t, err)

	// make sure this went through SSL
	results, err := conn.ExecuteFetch("ssl echo", 1000, true)
	require.NoError(t, err)
	assert.Equal(t, "OFF", results.Rows[0][0].ToString())

	if conn != nil {
		conn.Close()
	}
}

// TestTLSClientDisabled creates a Server with TLS support, then connects
// with a client with TLS preferred.
func TestTLSClientPreferredDefault(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
	}}
	defer authServer.close()

	// Create the listener, so we can get its host.
	// Below, we are enabling --ssl-verify-server-cert, which adds
	// a check that the common name of the certificate matches the
	// server host name we connect to.
	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false)
	require.NoError(t, err)
	defer l.Close()

	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port

	// Create the certs.
	root := t.TempDir()
	tlstest.CreateCA(root)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", "server.example.com")
	tlstest.CreateSignedCert(root, tlstest.CA, "02", "client", "Client Cert")

	// Create the server with TLS config.
	serverConfig, err := vttls.ServerConfig(
		path.Join(root, "server-cert.pem"),
		path.Join(root, "server-key.pem"),
		"",
		"",
		"",
		tls.VersionTLS12)
	require.NoError(t, err)
	l.TLSConfig.Store(serverConfig)

	var wg sync.WaitGroup
	wg.Add(1)
	go func(l *Listener) {
		wg.Done()
		l.Accept()
	}(l)
	// This is ensure the listener is called
	wg.Wait()
	// Sleep so that the Accept function is called as well.'
	time.Sleep(3 * time.Second)

	// Setup the right parameters.
	params := &ConnParams{
		Host:       host,
		Port:       port,
		Uname:      "user1",
		Pass:       "password1",
		SslMode:    vttls.Preferred,
		ServerName: "server.example.com",
	}

	conn, err := Connect(context.Background(), params)
	require.NoError(t, err)

	// make sure this went through SSL
	results, err := conn.ExecuteFetch("ssl echo", 1000, true)
	require.NoError(t, err)
	assert.Equal(t, "ON", results.Rows[0][0].ToString())

	if conn != nil {
		conn.Close()
	}
}

// TestTLSClientRequired creates a Server with no TLS support, then connects
// with a client with TLS required.
func TestTLSClientRequired(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
	}}
	defer authServer.close()

	// Create the listener, so we can get its host.
	// Below, we are enabling --ssl-verify-server-cert, which adds
	// a check that the common name of the certificate matches the
	// server host name we connect to.
	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false)
	require.NoError(t, err)
	defer l.Close()

	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port

	var wg sync.WaitGroup
	wg.Add(1)
	go func(l *Listener) {
		wg.Done()
		l.Accept()
	}(l)
	// This is ensure the listener is called
	wg.Wait()
	// Sleep so that the Accept function is called as well.'
	time.Sleep(3 * time.Second)

	// Setup the right parameters.
	params := &ConnParams{
		Host:    host,
		Port:    port,
		Uname:   "user1",
		Pass:    "password1",
		SslMode: vttls.Required,
	}

	_, err = Connect(context.Background(), params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "server doesn't support SSL but client asked for it")
}

// TestTLSClientVerifyCA creates a Server with TLS support, then connects
// with a client with TLS enabled on a wrong hostname but with verify CA on.
func TestTLSClientVerifyCA(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
	}}
	defer authServer.close()

	// Create the listener, so we can get its host.
	// Below, we are enabling --ssl-verify-server-cert, which adds
	// a check that the common name of the certificate matches the
	// server host name we connect to.
	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false)
	require.NoError(t, err)
	defer l.Close()

	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port

	// Create the certs.
	root := t.TempDir()
	tlstest.CreateCA(root)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", "server.example.com")
	tlstest.CreateSignedCert(root, tlstest.CA, "02", "client", "Client Cert")

	// Create the server with TLS config.
	serverConfig, err := vttls.ServerConfig(
		path.Join(root, "server-cert.pem"),
		path.Join(root, "server-key.pem"),
		"",
		"",
		"",
		tls.VersionTLS12)
	require.NoError(t, err)
	l.TLSConfig.Store(serverConfig)

	var wg sync.WaitGroup
	wg.Add(1)
	go func(l *Listener) {
		wg.Done()
		l.Accept()
	}(l)
	// This is ensure the listener is called
	wg.Wait()
	// Sleep so that the Accept function is called as well.'
	time.Sleep(3 * time.Second)

	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
		// SSL flags.
		SslMode:    vttls.VerifyCA,
		ServerName: "server.example.com",
	}

	_, err = Connect(context.Background(), params)
	require.Error(t, err)

	fmt.Printf("Error: %s", err)

	assert.Contains(t, err.Error(), "cannot send HandshakeResponse41: x509:")

	// Now setup proper CA that is valid to verify
	params.SslCa = path.Join(root, "ca-cert.pem")
	conn, err := Connect(context.Background(), params)
	require.NoError(t, err)

	// make sure this went through SSL
	results, err := conn.ExecuteFetch("ssl echo", 1000, true)
	require.NoError(t, err)
	assert.Equal(t, "ON", results.Rows[0][0].ToString())

	if conn != nil {
		conn.Close()
	}
}

// TestTLSClientVerifyIdentity creates a Server with TLS support, then connects
// with a client with TLS enabled on a wrong hostname but with verify CA on.
func TestTLSClientVerifyIdentity(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
	}}
	defer authServer.close()

	// Create the listener, so we can get its host.
	// Below, we are enabling --ssl-verify-server-cert, which adds
	// a check that the common name of the certificate matches the
	// server host name we connect to.
	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false)
	require.NoError(t, err)
	defer l.Close()

	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port

	// Create the certs.
	root := t.TempDir()
	tlstest.CreateCA(root)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", "server.example.com")
	tlstest.CreateSignedCert(root, tlstest.CA, "02", "client", "Client Cert")

	// Create the server with TLS config.
	serverConfig, err := vttls.ServerConfig(
		path.Join(root, "server-cert.pem"),
		path.Join(root, "server-key.pem"),
		"",
		"",
		"",
		tls.VersionTLS12)
	require.NoError(t, err)
	l.TLSConfig.Store(serverConfig)

	var wg sync.WaitGroup
	wg.Add(1)
	go func(l *Listener) {
		wg.Done()
		l.Accept()
	}(l)
	// This is ensure the listener is called
	wg.Wait()
	// Sleep so that the Accept function is called as well.'
	time.Sleep(3 * time.Second)

	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
		// SSL flags.
		SslMode:    vttls.VerifyIdentity,
		ServerName: "server.example.com",
	}

	_, err = Connect(context.Background(), params)
	require.Error(t, err)

	fmt.Printf("Error: %s", err)

	assert.Contains(t, err.Error(), "cannot send HandshakeResponse41: x509:")

	// Now setup proper CA that is valid to verify
	params.SslCa = path.Join(root, "ca-cert.pem")
	conn, err := Connect(context.Background(), params)
	require.NoError(t, err)

	// make sure this went through SSL
	results, err := conn.ExecuteFetch("ssl echo", 1000, true)
	require.NoError(t, err)
	assert.Equal(t, "ON", results.Rows[0][0].ToString())

	if conn != nil {
		conn.Close()
	}

	// Now revoke the server certificate and make sure we can't connect
	tlstest.RevokeCertAndRegenerateCRL(root, tlstest.CA, "server")

	params.SslCrl = path.Join(root, "ca-crl.pem")
	_, err = Connect(context.Background(), params)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Certificate revoked: CommonName=server.example.com")
}
