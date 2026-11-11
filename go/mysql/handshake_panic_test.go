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
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/vttls"
)

// These tests are the spec for panics in the MySQL client handshake.
// Connect runs that handshake on a goroutine with no recover. Until the
// handlers reject a short packet and a missing or non-RSA PEM block,
// the cases below crash the test process instead of returning an error.

const clientHandshakeCapabilities = CapabilityClientLongPassword |
	CapabilityClientFoundRows |
	CapabilityClientLongFlag |
	CapabilityClientConnectWithDB |
	CapabilityClientProtocol41 |
	CapabilityClientTransactions |
	CapabilityClientSecureConnection |
	CapabilityClientMultiStatements |
	CapabilityClientMultiResults |
	CapabilityClientPluginAuth |
	CapabilityClientPluginAuthLenencClientData |
	CapabilityClientDeprecateEOF |
	CapabilityClientConnAttr

func TestConnectReturnsErrorOnZeroLengthAuthResponse(t *testing.T) {
	err := connectToMalformedServer(t, &ConnParams{
		Uname:   "user",
		Pass:    "password",
		SslMode: vttls.Disabled,
	}, func(c *Conn, attackSent chan<- struct{}) error {
		if err := writeInitialHandshake(c, MysqlNativePassword, clientHandshakeCapabilities); err != nil {
			return err
		}
		if _, err := c.readPacket(); err != nil {
			return err
		}
		if err := writeZeroLengthPacket(c); err != nil {
			return err
		}
		close(attackSent)
		return nil
	})
	requireHandshakeError(t, err)
}

func TestConnectReturnsErrorOnShortAuthMoreData(t *testing.T) {
	err := connectToMalformedServer(t, &ConnParams{
		Uname:   "user",
		Pass:    "password",
		SslMode: vttls.Disabled,
	}, func(c *Conn, attackSent chan<- struct{}) error {
		if err := writeInitialHandshake(c, MysqlNativePassword, clientHandshakeCapabilities); err != nil {
			return err
		}
		if _, err := c.readPacket(); err != nil {
			return err
		}
		// One byte is enough to take the AuthMoreData branch, which then reads the next byte.
		if err := writePayload(c, []byte{AuthMoreDataPacket}); err != nil {
			return err
		}
		close(attackSent)
		return nil
	})
	requireHandshakeError(t, err)
}

func TestConnectReturnsErrorOnZeroLengthPublicKeyPacket(t *testing.T) {
	err := connectToMalformedServer(t, &ConnParams{
		Uname:   "user",
		Pass:    "password",
		SslMode: vttls.Disabled,
	}, func(c *Conn, attackSent chan<- struct{}) error {
		if err := writeCachingSha2FullAuth(c); err != nil {
			return err
		}
		if err := writeZeroLengthPacket(c); err != nil {
			return err
		}
		close(attackSent)
		return nil
	})
	requireHandshakeError(t, err)
}

func TestConnectReturnsErrorOnNonPEMPublicKey(t *testing.T) {
	err := connectToMalformedServer(t, &ConnParams{
		Uname:   "user",
		Pass:    "password",
		SslMode: vttls.Disabled,
	}, func(c *Conn, attackSent chan<- struct{}) error {
		if err := writeCachingSha2FullAuth(c); err != nil {
			return err
		}
		payload := append([]byte{AuthMoreDataPacket}, []byte("not a pem key")...)
		if err := writePayload(c, payload); err != nil {
			return err
		}
		close(attackSent)
		return nil
	})
	requireHandshakeError(t, err)
}

func TestConnectReturnsErrorOnNonRSAPublicKey(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)
	encoded := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	require.NotEmpty(t, encoded)

	err = connectToMalformedServer(t, &ConnParams{
		Uname:   "user",
		Pass:    "password",
		SslMode: vttls.Disabled,
	}, func(c *Conn, attackSent chan<- struct{}) error {
		if err := writeCachingSha2FullAuth(c); err != nil {
			return err
		}
		payload := append([]byte{AuthMoreDataPacket}, encoded...)
		if err := writePayload(c, payload); err != nil {
			return err
		}
		close(attackSent)
		return nil
	})
	requireHandshakeError(t, err)
}

func TestConnectReturnsErrorOnZeroLengthInitDBResponse(t *testing.T) {
	err := connectToMalformedServer(t, &ConnParams{
		Uname:   "user",
		Pass:    "password",
		DbName:  "db",
		SslMode: vttls.Disabled,
	}, func(c *Conn, attackSent chan<- struct{}) error {
		caps := uint32(clientHandshakeCapabilities) &^ CapabilityClientConnectWithDB
		if err := writeInitialHandshake(c, MysqlNativePassword, caps); err != nil {
			return err
		}
		if _, err := c.readPacket(); err != nil {
			return err
		}
		if err := c.writeOKPacket(&PacketOK{}); err != nil {
			return err
		}
		pkt, err := c.readPacket()
		if err != nil {
			return err
		}
		if len(pkt) == 0 || pkt[0] != ComInitDB {
			return fmt.Errorf("client did not send COM_INIT_DB: %v", pkt)
		}
		if err := writeZeroLengthPacket(c); err != nil {
			return err
		}
		close(attackSent)
		return nil
	})
	requireHandshakeError(t, err)
}

func requireHandshakeError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.NotErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorIs(t, err, context.Canceled)
}

func connectToMalformedServer(t *testing.T, params *ConnParams, script func(c *Conn, attackSent chan<- struct{}) error) error {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })

	attackSent := make(chan struct{})
	go func() {
		raw, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		defer raw.Close()
		_ = raw.SetDeadline(time.Now().Add(20 * time.Second))
		if scriptErr := script(newConn(raw, 0, 0), attackSent); scriptErr != nil {
			_ = ln.Close()
		}
	}()

	host, portStr, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)
	params.Host = host
	params.Port = port

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	_, err = Connect(ctx, params)

	select {
	case <-attackSent:
	default:
		t.Fatalf("Connect returned before the malformed server packet was written: %v", err)
	}
	return err
}

func writeCachingSha2FullAuth(c *Conn) error {
	if err := writeInitialHandshake(c, CachingSha2Password, clientHandshakeCapabilities); err != nil {
		return err
	}
	if _, err := c.readPacket(); err != nil {
		return err
	}
	if err := writePayload(c, []byte{AuthMoreDataPacket, CachingSha2FullAuth}); err != nil {
		return err
	}
	req, err := c.readPacket()
	if err != nil {
		return err
	}
	if len(req) != 1 || req[0] != 0x02 {
		return fmt.Errorf("client did not request a public key: %v", req)
	}
	return nil
}

func writeInitialHandshake(c *Conn, authMethod AuthMethodDescription, capabilities uint32) error {
	pluginData, err := newSalt()
	if err != nil {
		return err
	}
	pluginData = append(pluginData, 0)

	serverVersion := "8.0.40"
	length := 1 +
		lenNullString(serverVersion) +
		4 +
		8 +
		1 +
		2 +
		1 +
		2 +
		2 +
		1 +
		10 +
		13 +
		lenNullString(string(authMethod))

	data, pos := c.startEphemeralPacketWithHeader(length)
	pos = writeByte(data, pos, protocolVersion)
	pos = writeNullString(data, pos, serverVersion)
	pos = writeUint32(data, pos, 1)
	pos += copy(data[pos:], pluginData[:8])
	pos = writeByte(data, pos, 0)
	pos = writeUint16(data, pos, uint16(capabilities))
	pos = writeByte(data, pos, uint8(collations.CollationUtf8mb4ID))
	pos = writeUint16(data, pos, 0)
	pos = writeUint16(data, pos, uint16(capabilities>>16))
	pos = writeByte(data, pos, 21)
	pos = writeZeroes(data, pos, 10)
	pos += copy(data[pos:], pluginData[8:])
	pos = writeNullString(data, pos, string(authMethod))
	if pos != len(data) {
		return fmt.Errorf("handshake packed %d bytes, allocated %d", pos, len(data))
	}
	return c.writeEphemeralPacket()
}

func writePayload(c *Conn, payload []byte) error {
	data, pos := c.startEphemeralPacketWithHeader(len(payload))
	copy(data[pos:], payload)
	return c.writeEphemeralPacket()
}

func writeZeroLengthPacket(c *Conn) error {
	return c.writePacket(make([]byte, PacketHeaderSize))
}
