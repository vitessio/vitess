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
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
)

// logicalPacketCap is the most payload bytes one logical MySQL packet may
// accumulate. 64MiB is MySQL's default max_allowed_packet. It is larger than
// the multi-packet exchanges already covered by TestPackets and
// TestBytesReadCountsMultiPacketHeadersAndPayloads (MaxPacketSize+1000).
//
// A reader may consume the protocol packet that crosses this cap, because
// that packet's length is only known after its header is read, and it may
// look ahead by connBufferSize. It must not read another MaxPacketSize
// packet, and it must return an error instead of assembling the payload.
const logicalPacketCap = 64 << 20

// maxLogicalPacketWireBytes is the most bytes a capped reader may pull from
// the connection for one logical packet, including headers and one extra
// protocol packet that crosses logicalPacketCap.
func maxLogicalPacketWireBytes() int64 {
	packets := logicalPacketCap/MaxPacketSize + 1
	return int64(packets*(MaxPacketSize+PacketHeaderSize) + connBufferSize)
}

// countingConn counts bytes the server actually reads. A fix that buffers the
// whole stream and only then returns an error still fails these tests.
type countingConn struct {
	net.Conn
	n atomic.Int64
}

func (c *countingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 {
		c.n.Add(int64(n))
	}
	return n, err
}

// TestUnauthenticatedAuthSwitchStopsOversizedPacket drives the listener
// handshake. An empty auth response makes the server write an auth-switch
// request and read the reply with readEphemeralPacket. A stream of
// MaxPacketSize packets with no short terminator must be refused and the
// connection closed before the server has read more than one logical packet.
func TestUnauthenticatedAuthSwitchStopsOversizedPacket(t *testing.T) {
	// Static auth accepts the user before it checks a password, which is the
	// default vtgate auth server. An empty auth response still takes the
	// auth-switch branch.
	authServer := NewAuthServerStatic("", "{}", 0)
	t.Cleanup(authServer.close)

	l, err := NewListener("tcp", "127.0.0.1:", authServer, &testHandler{}, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	var counted atomic.Pointer[countingConn]
	l.PreHandleFunc = func(_ context.Context, conn net.Conn, _ uint32) (net.Conn, error) {
		wrapped := &countingConn{Conn: conn}
		counted.Store(wrapped)
		return wrapped, nil
	}
	go l.Accept()
	t.Cleanup(func() { l.Close() })

	client, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	require.NoError(t, client.SetDeadline(time.Now().Add(20*time.Second)))
	handshake, seq, err := readRawPacket(client)
	require.NoError(t, err)
	require.NotEmpty(t, handshake)
	require.Equal(t, byte(0), seq)

	_, err = client.Write(emptyAuthHandshake())
	require.NoError(t, err)

	response, seq, err := readRawPacket(client)
	require.NoError(t, err)
	require.Equal(t, byte(2), seq)
	require.NotEmpty(t, response)
	require.Equalf(t, byte(AuthSwitchRequestPacket), response[0], "auth switch not taken, first bytes %x", response[:min(len(response), 32)])
	require.Contains(t, string(response), string(MysqlNativePassword))

	server := counted.Load()
	require.NotNil(t, server)
	baseline := server.n.Load()
	// Handshake reads are done. Clear the deadline so the close watcher
	// below is not tripped by it.
	require.NoError(t, client.SetDeadline(time.Time{}))

	var closed atomic.Bool
	go func() {
		buf := make([]byte, 256)
		for {
			_, err := client.Read(buf)
			if err != nil {
				closed.Store(true)
				return
			}
		}
	}()

	wroteAll := floodMaxPackets(t, client, server, baseline, 3)
	if wroteAll {
		require.Eventually(t, func() bool {
			return server.n.Load()-baseline > maxLogicalPacketWireBytes() || closed.Load()
		}, 3*time.Second, 5*time.Millisecond)
	}
	require.LessOrEqualf(t, server.n.Load()-baseline, maxLogicalPacketWireBytes(),
		"auth-switch read consumed %d bytes", server.n.Load()-baseline)
	require.Eventually(t, closed.Load, 2*time.Second, 5*time.Millisecond)
}

// TestReadEphemeralPacketStopsOversizedLogicalPacket covers the shared
// reassembly loop used by the auth-switch response, the post-SSL handshake
// reread, and handleNextCommand.
func TestReadEphemeralPacketStopsOversizedLogicalPacket(t *testing.T) {
	assertReaderStops(t, func(c *Conn) ([]byte, error) {
		return c.readEphemeralPacket()
	})
}

// TestReadPacketStopsOversizedLogicalPacket covers the same missing cap in
// readPacket, which ReadPacket and the caching_sha2 full-auth password read
// both call.
//
// COM_STMT_SEND_LONG_DATA is not this bug: each command is already one
// logical packet, and the handler appends those packets afterward. These
// tests do not cover that accumulation.
func TestReadPacketStopsOversizedLogicalPacket(t *testing.T) {
	assertReaderStops(t, func(c *Conn) ([]byte, error) {
		return c.readPacket()
	})
}

func assertReaderStops(t *testing.T, read func(*Conn) ([]byte, error)) {
	t.Helper()

	// Pipe writes block until the server reads them, so a completed flood
	// means the reader accepted every byte. No socket buffer can hide it.
	serverNet, client := net.Pipe()
	t.Cleanup(func() {
		serverNet.Close()
		client.Close()
	})

	counted := &countingConn{Conn: serverNet}
	sConn := newConn(counted, DefaultFlushDelay, 0)
	t.Cleanup(func() { sConn.Close() })

	readDone := make(chan error, 1)
	go func() {
		_, err := read(sConn)
		readDone <- err
		// Unblock a writer stuck on the next packet after the reader returns.
		client.Close()
	}()

	wroteAll := floodMaxPackets(t, client, counted, 0, 0)
	if wroteAll {
		t.Fatalf("reader accepted %d bytes with no logical packet cap", counted.n.Load())
	}

	select {
	case err := <-readDone:
		require.Error(t, err)
		var ne net.Error
		require.Falsef(t, errors.As(err, &ne) && ne.Timeout(), "reader blocked until the deadline after %d bytes: %v", counted.n.Load(), err)
		require.LessOrEqual(t, counted.n.Load(), maxLogicalPacketWireBytes())
	case <-time.After(3 * time.Second):
		t.Fatalf("reader kept waiting after %d bytes; logical packets are not capped", counted.n.Load())
	}
}

// floodMaxPackets writes MaxPacketSize packets until a write fails or the
// payload exceeds the cap by more than one protocol packet. It returns false
// when the peer stops accepting writes. It fails the test as soon as the peer
// has read more than maxLogicalPacketWireBytes.
func floodMaxPackets(t *testing.T, conn net.Conn, counted *countingConn, baseline int64, seq byte) bool {
	t.Helper()
	payload := make([]byte, MaxPacketSize)
	// One packet past the crossing packet. A capped reader must not consume it.
	limit := logicalPacketCap + MaxPacketSize + 1
	written := 0
	for written < limit {
		if over := counted.n.Load() - baseline; over > maxLogicalPacketWireBytes() {
			t.Fatalf("peer read %d bytes after the baseline, past the %d byte logical packet cap", over, logicalPacketCap)
		}
		// Once the payload already crosses the cap, don't wait on a peer
		// that has stopped reading but has not closed yet. net.Pipe returns
		// an error from SetWriteDeadline; TCP honors it.
		if written > logicalPacketCap {
			_ = conn.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
		}
		if err := writeRawPacket(conn, seq, payload); err != nil {
			return false
		}
		written += len(payload)
		seq++
	}
	if over := counted.n.Load() - baseline; over > maxLogicalPacketWireBytes() {
		t.Fatalf("peer read %d bytes after the baseline, past the %d byte logical packet cap", over, logicalPacketCap)
	}
	return true
}

func emptyAuthHandshake() []byte {
	user := "user"
	plugin := string(MysqlNativePassword)
	flags := uint32(CapabilityClientLongPassword |
		CapabilityClientLongFlag |
		CapabilityClientProtocol41 |
		CapabilityClientTransactions |
		CapabilityClientSecureConnection |
		CapabilityClientMultiResults |
		CapabilityClientPluginAuth |
		CapabilityClientPluginAuthLenencClientData)
	payloadLen := 4 + 4 + 1 + 23 + len(user) + 1 + 1 + len(plugin) + 1
	pkt := make([]byte, PacketHeaderSize+payloadLen)
	pkt[0] = byte(payloadLen)
	pkt[1] = byte(payloadLen >> 8)
	pkt[2] = byte(payloadLen >> 16)
	pkt[3] = 1
	pos := PacketHeaderSize
	pkt[pos] = byte(flags)
	pkt[pos+1] = byte(flags >> 8)
	pkt[pos+2] = byte(flags >> 16)
	pkt[pos+3] = byte(flags >> 24)
	pos += 8 // max-packet size stays zero
	pkt[pos] = byte(collations.CollationUtf8mb4ID)
	pos++
	pos += 23
	copy(pkt[pos:], user)
	pos += len(user)
	// null username terminator and a lenenc auth length of zero
	pos += 2
	copy(pkt[pos:], plugin)
	pos += len(plugin)
	if pos+1 != len(pkt) {
		panic("emptyAuthHandshake length mismatch")
	}
	return pkt
}

func readRawPacket(conn net.Conn) ([]byte, byte, error) {
	var hdr [PacketHeaderSize]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return nil, 0, err
	}
	n := int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
	payload := make([]byte, n)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, hdr[3], err
	}
	return payload, hdr[3], nil
}

func writeRawPacket(conn net.Conn, seq byte, payload []byte) error {
	var hdr [PacketHeaderSize]byte
	n := len(payload)
	hdr[0] = byte(n)
	hdr[1] = byte(n >> 8)
	hdr[2] = byte(n >> 16)
	hdr[3] = seq
	if err := writeAll(conn, hdr[:]); err != nil {
		return err
	}
	return writeAll(conn, payload)
}

func writeAll(conn net.Conn, b []byte) error {
	for len(b) > 0 {
		n, err := conn.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}
