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

package mysql

import (
	"bytes"
	"context"
	"crypto/tls"
	"net"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/vttls"
)

// placeholderDBPassword is an obvious stand-in for a tablet DB password.
// Tests fail while the client writes these bytes to a peer that asks for
// mysql_clear_password. There is no operator switch for that plugin, so the
// default is to refuse it on every transport, including one that already
// completed TLS or is a unix socket.
//
// handleAuthMoreDataPacket's caching_sha2 full-auth path is a different call
// to writeClearTextPassword. It already writes the password only when
// CapabilityClientSSL is set or UnixSocket is set, so these tests leave it
// alone. An AuthSwitchRequest for "dialog" is rejected before any write.
const placeholderDBPassword = "placeholder-db-password"

// captureConn records bytes read from the peer, which are the bytes the
// client wrote.
type captureConn struct {
	net.Conn
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *captureConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.mu.Lock()
		c.buf.Write(p[:n])
		c.mu.Unlock()
	}
	return n, err
}

func (c *captureConn) Bytes() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.buf.Bytes()...)
}

type acceptedPeer struct {
	mu   sync.Mutex
	conn *captureConn
}

func (p *acceptedPeer) set(c *captureConn) {
	p.mu.Lock()
	p.conn = c
	p.mu.Unlock()
}

func (p *acceptedPeer) bytes() []byte {
	p.mu.Lock()
	c := p.conn
	p.mu.Unlock()
	if c == nil {
		return nil
	}
	return c.Bytes()
}

// TestClearPasswordAuthSwitchNotSent is the client half of a TCP or unix
// handshake. The peer advertises mysql_native_password, waits for the
// client's first response, then sends an AuthSwitchRequest for
// mysql_clear_password. The configured password must not appear in anything
// the client writes, and Connect must fail.
func TestClearPasswordAuthSwitchNotSent(t *testing.T) {
	t.Run("tcp unset ssl mode", func(t *testing.T) {
		dialClearPasswordPeer(t, "tcp", "127.0.0.1:0", func(addr net.Addr) *ConnParams {
			host, port := splitAddr(t, addr)
			return &ConnParams{
				Host:  host,
				Port:  port,
				Uname: "vt_dba",
				Pass:  placeholderDBPassword,
			}
		})
	})

	t.Run("tcp ssl mode disabled", func(t *testing.T) {
		dialClearPasswordPeer(t, "tcp", "127.0.0.1:0", func(addr net.Addr) *ConnParams {
			host, port := splitAddr(t, addr)
			return &ConnParams{
				Host:    host,
				Port:    port,
				Uname:   "vt_app",
				Pass:    placeholderDBPassword,
				SslMode: vttls.Disabled,
			}
		})
	})

	t.Run("unix socket", func(t *testing.T) {
		dir := t.TempDir()
		sock := path.Join(dir, "s.sock")
		dialClearPasswordPeer(t, "unix", sock, func(addr net.Addr) *ConnParams {
			return &ConnParams{
				UnixSocket: addr.String(),
				Uname:      "vt_repl",
				Pass:       placeholderDBPassword,
			}
		})
	})
}

// TestClearPasswordAuthSwitchNotSentOverTLS completes a verified TLS
// handshake and then asks for mysql_clear_password. The password must still
// not be written: nothing turns that plugin on.
func TestClearPasswordAuthSwitchNotSentOverTLS(t *testing.T) {
	root := t.TempDir()
	tlstest.CreateCA(root)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", "server.example.com")
	tlstest.CreateSignedCert(root, tlstest.CA, "02", "client", "Client Cert")

	serverConfig, err := vttls.ServerConfig(
		path.Join(root, "server-cert.pem"),
		path.Join(root, "server-key.pem"),
		path.Join(root, "ca-cert.pem"),
		"",
		"",
		tls.VersionTLS12,
	)
	require.NoError(t, err)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })

	peer := &acceptedPeer{}
	first := make(chan struct{})
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			return
		}
		defer raw.Close()
		_ = raw.SetDeadline(time.Now().Add(5 * time.Second))

		sConn := newConn(raw, 0, 0)
		authServer := NewAuthServerStaticWithAuthMethodDescription("", "", 0, MysqlNativePassword)
		defer authServer.close()
		if _, err := sConn.writeHandshakeV10("8.0.36", authServer, uint8(collations.CollationUtf8mb4ID), true); err != nil {
			return
		}
		sslRequest, err := sConn.readEphemeralPacketDirect()
		if err != nil {
			return
		}
		flags, _, ok := readUint32(sslRequest, 0)
		sConn.recycleReadPacket()
		if !ok || flags&CapabilityClientSSL == 0 {
			return
		}

		plain := &captureConn{Conn: tls.Server(raw, serverConfig)}
		peer.set(plain)
		sConn.conn = plain
		sConn.bufferedReader.Reset(plain)
		if _, err := sConn.readPacket(); err != nil {
			return
		}
		close(first)
		if err := sConn.writeAuthSwitchRequest(string(MysqlClearPassword), nil); err != nil {
			return
		}
		_, _ = sConn.readPacket()
	}()

	host, port := splitAddr(t, ln.Addr())
	params := &ConnParams{
		Host:       host,
		Port:       port,
		Uname:      "vt_dba",
		Pass:       placeholderDBPassword,
		SslMode:    vttls.VerifyIdentity,
		SslCa:      path.Join(root, "ca-cert.pem"),
		SslCert:    path.Join(root, "client-cert.pem"),
		SslKey:     path.Join(root, "client-key.pem"),
		ServerName: "server.example.com",
	}
	assertClearPasswordRefused(t, params, peer, first)
}

func dialClearPasswordPeer(t *testing.T, network, address string, paramsFor func(net.Addr) *ConnParams) {
	t.Helper()

	ln, err := net.Listen(network, address)
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })

	peer := &acceptedPeer{}
	first := make(chan struct{})
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			return
		}
		defer raw.Close()
		_ = raw.SetDeadline(time.Now().Add(5 * time.Second))

		capConn := &captureConn{Conn: raw}
		peer.set(capConn)
		sConn := newConn(capConn, 0, 0)
		authServer := NewAuthServerStaticWithAuthMethodDescription("", "", 0, MysqlNativePassword)
		defer authServer.close()
		if _, err := sConn.writeHandshakeV10("8.0.36", authServer, uint8(collations.CollationUtf8mb4ID), false); err != nil {
			return
		}
		if _, err := sConn.readPacket(); err != nil {
			return
		}
		close(first)
		if err := sConn.writeAuthSwitchRequest(string(MysqlClearPassword), nil); err != nil {
			return
		}
		_, _ = sConn.readPacket()
	}()

	assertClearPasswordRefused(t, paramsFor(ln.Addr()), peer, first)
}

func assertClearPasswordRefused(t *testing.T, params *ConnParams, peer *acceptedPeer, first <-chan struct{}) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := Connect(ctx, params)
		errCh <- err
	}()

	select {
	case <-first:
	case err := <-errCh:
		require.Failf(t, "client finished before the auth switch", "%v", err)
	case <-ctx.Done():
		require.Fail(t, "timed out waiting for the client handshake response")
	}

	var err error
	select {
	case err = <-errCh:
	case <-ctx.Done():
		require.Fail(t, "timed out waiting for Connect to return")
	}

	got := peer.bytes()
	require.NotEmpty(t, got, "client never wrote a handshake response")
	require.False(t, bytes.Contains(got, []byte(placeholderDBPassword)), "configured password was written to the peer")
	require.Error(t, err)
}

func splitAddr(t *testing.T, addr net.Addr) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr.String())
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)
	return host, port
}
