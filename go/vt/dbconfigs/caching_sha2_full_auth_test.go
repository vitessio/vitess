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

package dbconfigs

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
)

// placeholderPassword is the stand-in secret the peer must not learn.
// It is not a real credential.
const placeholderPassword = "placeholder-password"

// peerCanReadPassword reports whether payload is the password in the clear
// or OAEP-encrypted to priv the way mysql.EncryptPasswordWithPublicKey does.
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

// serverCapabilities matches Conn.writeHandshakeV10 with TLS disabled.
func serverCapabilities() uint32 {
	return mysql.CapabilityClientLongPassword |
		mysql.CapabilityClientFoundRows |
		mysql.CapabilityClientLongFlag |
		mysql.CapabilityClientConnectWithDB |
		mysql.CapabilityClientProtocol41 |
		mysql.CapabilityClientTransactions |
		mysql.CapabilityClientSecureConnection |
		mysql.CapabilityClientMultiStatements |
		mysql.CapabilityClientMultiResults |
		mysql.CapabilityClientPluginAuth |
		mysql.CapabilityClientPluginAuthLenencClientData |
		mysql.CapabilityClientDeprecateEOF |
		mysql.CapabilityClientConnAttr
}

// initialHandshake is the packet payload writeHandshakeV10 sends, with the
// plugin name fixed to caching_sha2_password and no CLIENT_SSL bit.
func initialHandshake(salt []byte) []byte {
	const version = "8.0.40"
	plugin := string(mysql.CachingSha2Password)
	caps := serverCapabilities()
	var b []byte
	b = append(b, 10) // protocol version
	b = append(b, version...)
	b = append(b, 0)
	var id [4]byte
	binary.LittleEndian.PutUint32(id[:], 1)
	b = append(b, id[:]...)
	b = append(b, salt[:8]...)
	b = append(b, 0) // filler
	var u16 [2]byte
	binary.LittleEndian.PutUint16(u16[:], uint16(caps))
	b = append(b, u16[:]...)
	b = append(b, byte(collations.CollationUtf8mb4ID))
	binary.LittleEndian.PutUint16(u16[:], 0) // status
	b = append(b, u16[:]...)
	binary.LittleEndian.PutUint16(u16[:], uint16(caps>>16))
	b = append(b, u16[:]...)
	b = append(b, 21) // auth plugin data length
	b = append(b, make([]byte, 10)...)
	b = append(b, salt[8:]...) // 12 bytes plus the trailing NUL
	b = append(b, plugin...)
	b = append(b, 0)
	return b
}

func writePayload(c *mysql.Conn, payload []byte) error {
	pkt := make([]byte, mysql.PacketHeaderSize+len(payload))
	copy(pkt[mysql.PacketHeaderSize:], payload)
	return c.WritePacket(pkt)
}

type fullAuthExchange struct {
	sawClientHandshake bool
	requestedKey       bool
	leaked             bool
	setupErr           error
}

func runFullAuthPeer(ln net.Listener, priv *rsa.PrivateKey, pubPEM, salt []byte) fullAuthExchange {
	var out fullAuthExchange
	raw, err := ln.Accept()
	if err != nil {
		out.setupErr = err
		return out
	}
	defer raw.Close()
	_ = raw.SetDeadline(time.Now().Add(5 * time.Second))
	sConn := mysql.NewConnForTest(raw)

	if err := writePayload(sConn, initialHandshake(salt)); err != nil {
		out.setupErr = err
		return out
	}
	if _, err := sConn.ReadPacket(); err != nil {
		return out
	}
	out.sawClientHandshake = true

	// AuthMoreData: full authentication required.
	if err := writePayload(sConn, []byte{mysql.AuthMoreDataPacket, mysql.CachingSha2FullAuth}); err != nil {
		out.setupErr = err
		return out
	}
	for {
		pkt, err := sConn.ReadPacket()
		if err != nil {
			return out
		}
		if peerCanReadPassword(priv, salt[:20], pkt, []byte(placeholderPassword)) {
			out.leaked = true
			return out
		}
		// The client asks for a public key with a single 0x02 payload.
		if len(pkt) == 1 && pkt[0] == 0x02 {
			out.requestedKey = true
			body := make([]byte, 1+len(pubPEM))
			body[0] = mysql.AuthMoreDataPacket
			copy(body[1:], pubPEM)
			if err := writePayload(sConn, body); err != nil {
				out.setupErr = err
				return out
			}
		}
	}
}

func connectorFor(dbcfgs *DBConfigs, userKey string) Connector {
	switch userKey {
	case App:
		return dbcfgs.AppWithDB()
	case AppDebug:
		return dbcfgs.AppDebugWithDB()
	case AllPrivs:
		return dbcfgs.AllPrivsConnector()
	case Dba:
		return dbcfgs.DbaConnector()
	case Filtered:
		return dbcfgs.FilteredWithDB()
	case Repl:
		return dbcfgs.ReplConnector()
	case ExternalRepl:
		return dbcfgs.ExternalRepl()
	case Clone:
		return dbcfgs.CloneConnector()
	default:
		panic("unknown user " + userKey)
	}
}

// TestTabletTCPFullAuthDoesNotRevealPassword builds tablet connection
// parameters with db-ssl-mode left unset and connects each user over TCP.
// The peer answers caching_sha2 full authentication with its own RSA key.
func TestTabletTCPFullAuthDoesNotRevealPassword(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	require.NoError(t, err)
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	tcp := ln.Addr().(*net.TCPAddr)

	for _, userKey := range All {
		t.Run(userKey, func(t *testing.T) {
			dbcfgs := DBConfigs{
				Host:    tcp.IP.String(),
				Port:    tcp.Port,
				Charset: "utf8mb4",
				DBName:  "db",
			}
			for _, key := range All {
				uc, _ := dbcfgs.getParams(key)
				uc.User = "vt_" + key
				uc.Password = placeholderPassword
				uc.UseSSL = true
			}
			// ExternalRepl keeps its own host. Set it so this user is TCP too.
			_, erepl := dbcfgs.getParams(ExternalRepl)
			erepl.Host = tcp.IP.String()
			erepl.Port = tcp.Port

			dbcfgs.InitWithSocket("/tmp/mysql.sock", collations.MySQL8())

			salt := make([]byte, 21)
			_, err := rand.Read(salt[:20])
			require.NoError(t, err)
			salt[20] = 0

			exch := make(chan fullAuthExchange, 1)
			go func() { exch <- runFullAuthPeer(ln, priv, pubPEM, salt) }()

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			connector := connectorFor(&dbcfgs, userKey)
			conn, connectErr := connector.Connect(ctx)
			if conn != nil {
				conn.Close()
			}
			var out fullAuthExchange
			select {
			case out = <-exch:
			case <-ctx.Done():
				t.Fatal("peer did not finish the exchange")
			}
			require.NoError(t, out.setupErr)
			require.True(t, out.sawClientHandshake, "client never answered the server handshake")
			require.False(t, out.leaked, "unauthenticated peer recovered the password")
			require.False(t, out.requestedKey, "client asked the unauthenticated peer for a public key")
			require.Error(t, connectErr)
		})
	}
}
