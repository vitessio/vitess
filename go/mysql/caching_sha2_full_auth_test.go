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
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/pem"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/vttls"
)

// placeholderPassword is the stand-in secret the peer must not learn.
// It is not a real credential.
const placeholderPassword = "placeholder-password"

// namedAuthServer lets a test handshake advertise a chosen plugin.
// writeHandshakeV10 reads only DefaultAuthMethodDescription.
type namedAuthServer struct {
	method AuthMethodDescription
}

func (n namedAuthServer) AuthMethods() []AuthMethod { return nil }

func (n namedAuthServer) DefaultAuthMethodDescription() AuthMethodDescription { return n.method }

// peerCanReadPassword reports whether payload is the password in the clear
// or OAEP-encrypted to priv the way EncryptPasswordWithPublicKey does.
func peerCanReadPassword(priv *rsa.PrivateKey, salt, payload, password []byte) bool {
	clear := make([]byte, len(password)+1)
	copy(clear, password)
	if bytes.Equal(payload, clear) {
		return true
	}
	plain, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, priv, payload, nil)
	if err != nil || len(salt) == 0 {
		return false
	}
	for i := range plain {
		plain[i] ^= salt[i%len(salt)]
	}
	return bytes.Equal(plain, clear)
}

type fullAuthExchange struct {
	sawClientHandshake bool
	requestedKey       bool
	leaked             bool
	setupErr           error
}

// exerciseCachingSha2FullAuth speaks the server half of a TCP handshake.
// When authSwitch is set, the server switches the client to caching_sha2
// before asking for full authentication. Otherwise it asks for full
// authentication immediately, which the client handles even if the
// advertised plugin is not caching_sha2.
func exerciseCachingSha2FullAuth(t *testing.T, params ConnParams, initial AuthMethodDescription, authSwitch bool) (fullAuthExchange, error) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	require.NoError(t, err)
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	tcp := ln.Addr().(*net.TCPAddr)
	params.Host = tcp.IP.String()
	params.Port = tcp.Port
	params.Uname = "placeholder-user"
	params.Pass = placeholderPassword
	params.UnixSocket = ""

	exch := make(chan fullAuthExchange, 1)
	go func() {
		var out fullAuthExchange
		defer func() { exch <- out }()

		raw, err := ln.Accept()
		if err != nil {
			out.setupErr = err
			return
		}
		defer raw.Close()
		_ = raw.SetDeadline(time.Now().Add(5 * time.Second))
		sConn := newConn(raw, 0, 0)

		pluginData, err := sConn.writeHandshakeV10("8.0.40", namedAuthServer{initial}, uint8(collations.CollationUtf8mb4ID), false)
		if err != nil {
			out.setupErr = err
			return
		}
		if len(pluginData) < 2 || pluginData[len(pluginData)-1] != 0 {
			out.setupErr = errSaltShape
			return
		}
		salt := pluginData[:len(pluginData)-1]

		if _, err := sConn.ReadPacket(); err != nil {
			return
		}
		out.sawClientHandshake = true

		if authSwitch {
			switchSalt := make([]byte, 20)
			if _, err := rand.Read(switchSalt); err != nil {
				out.setupErr = err
				return
			}
			if err := writeAuthSwitch(sConn, string(CachingSha2Password), switchSalt); err != nil {
				out.setupErr = err
				return
			}
			if _, err := sConn.ReadPacket(); err != nil {
				return
			}
			salt = switchSalt
		}

		if err := writeCachingSha2FullAuth(sConn); err != nil {
			out.setupErr = err
			return
		}

		for {
			pkt, err := sConn.ReadPacket()
			if err != nil {
				return
			}
			if peerCanReadPassword(priv, salt, pkt, []byte(placeholderPassword)) {
				out.leaked = true
				return
			}
			// requestPublicKey writes a single 0x02 byte to ask the peer for a key.
			if len(pkt) == 1 && pkt[0] == 0x02 {
				out.requestedKey = true
				if err := writeAuthMoreData(sConn, pubPEM); err != nil {
					out.setupErr = err
					return
				}
			}
		}
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	conn, connectErr := Connect(ctx, &params)
	if conn != nil {
		conn.Close()
	}
	select {
	case out := <-exch:
		return out, connectErr
	case <-ctx.Done():
		t.Fatal("peer did not finish the exchange")
		return fullAuthExchange{}, connectErr
	}
}

var errSaltShape = errString("handshake plugin data is not a 20-byte salt plus a trailing NUL")

type errString string

func (e errString) Error() string { return string(e) }

func writeAuthSwitch(c *Conn, plugin string, salt []byte) error {
	length := 1 + len(plugin) + 1 + len(salt)
	data, pos := c.startEphemeralPacketWithHeader(length)
	data[pos] = AuthSwitchRequestPacket
	pos++
	pos = writeNullString(data, pos, plugin)
	pos += copy(data[pos:], salt)
	if pos != len(data) {
		return errString("auth switch packet has the wrong length")
	}
	return c.writeEphemeralPacket()
}

func writeCachingSha2FullAuth(c *Conn) error {
	data, pos := c.startEphemeralPacketWithHeader(2)
	data[pos] = AuthMoreDataPacket
	data[pos+1] = CachingSha2FullAuth
	return c.writeEphemeralPacket()
}

func writeAuthMoreData(c *Conn, body []byte) error {
	data, pos := c.startEphemeralPacketWithHeader(1 + len(body))
	data[pos] = AuthMoreDataPacket
	copy(data[pos+1:], body)
	return c.writeEphemeralPacket()
}

// TestCachingSha2FullAuthUnprotectedTCPDoesNotRevealPassword drives the real
// client handshake over TCP with no TLS. The peer supplies its own RSA key
// after caching_sha2 full authentication. The client must not ask for that
// key and must not send the password in a form the peer can read.
func TestCachingSha2FullAuthUnprotectedTCPDoesNotRevealPassword(t *testing.T) {
	cases := []struct {
		name       string
		sslMode    vttls.SslMode
		initial    AuthMethodDescription
		authSwitch bool
	}{{
		name:    "unset ssl mode",
		initial: CachingSha2Password,
	}, {
		name:    "ssl mode disabled",
		sslMode: vttls.Disabled,
		initial: CachingSha2Password,
	}, {
		name:       "auth switch to caching_sha2",
		initial:    MysqlNativePassword,
		authSwitch: true,
	}, {
		// handleAuthMoreDataPacket does not look at the plugin name, so a
		// full-auth byte after a native handshake takes the same branch.
		name:    "full auth without caching_sha2 plugin name",
		initial: MysqlNativePassword,
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, connectErr := exerciseCachingSha2FullAuth(t, ConnParams{SslMode: tc.sslMode}, tc.initial, tc.authSwitch)
			require.NoError(t, out.setupErr)
			require.True(t, out.sawClientHandshake, "client never answered the server handshake")
			require.False(t, out.leaked, "unauthenticated peer recovered the password")
			require.False(t, out.requestedKey, "client asked the unauthenticated peer for a public key")
			require.Error(t, connectErr)
		})
	}
}
