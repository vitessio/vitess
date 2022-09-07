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
	"bytes"
	"context"
	crypto_rand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func createSocketPair(t *testing.T) (net.Listener, *Conn, *Conn) {
	// Create a listener.
	listener, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	addr := listener.Addr().String()
	listener.(*net.TCPListener).SetDeadline(time.Now().Add(10 * time.Second))

	// Dial a client, Accept a server.
	wg := sync.WaitGroup{}

	var clientConn net.Conn
	var clientErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		clientConn, clientErr = net.DialTimeout("tcp", addr, 10*time.Second)
	}()

	var serverConn net.Conn
	var serverErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverConn, serverErr = listener.Accept()
	}()

	wg.Wait()

	if clientErr != nil {
		t.Fatalf("Dial failed: %v", clientErr)
	}
	if serverErr != nil {
		t.Fatalf("Accept failed: %v", serverErr)
	}

	// Create a Conn on both sides.
	cConn := newConn(clientConn)
	sConn := newConn(serverConn)
	sConn.PrepareData = map[uint32]*PrepareData{}

	return listener, sConn, cConn
}

func useWritePacket(t *testing.T, cConn *Conn, data []byte) {
	defer func() {
		if x := recover(); x != nil {
			t.Fatalf("%v", x)
		}
	}()

	dataLen := len(data)
	dataWithHeader := make([]byte, packetHeaderSize+dataLen)
	copy(dataWithHeader[packetHeaderSize:], data)

	if err := cConn.writePacket(dataWithHeader); err != nil {
		t.Fatalf("writePacket failed: %v", err)
	}
}

func useWriteEphemeralPacketBuffered(t *testing.T, cConn *Conn, data []byte) {
	defer func() {
		if x := recover(); x != nil {
			t.Fatalf("%v", x)
		}
	}()
	cConn.startWriterBuffering()
	defer cConn.endWriterBuffering()

	buf, pos := cConn.startEphemeralPacketWithHeader(len(data))
	copy(buf[pos:], data)
	if err := cConn.writeEphemeralPacket(); err != nil {
		t.Fatalf("writeEphemeralPacket(false) failed: %v", err)
	}
}

func useWriteEphemeralPacketDirect(t *testing.T, cConn *Conn, data []byte) {
	defer func() {
		if x := recover(); x != nil {
			t.Fatalf("%v", x)
		}
	}()

	buf, pos := cConn.startEphemeralPacketWithHeader(len(data))
	copy(buf[pos:], data)
	if err := cConn.writeEphemeralPacket(); err != nil {
		t.Fatalf("writeEphemeralPacket(true) failed: %v", err)
	}
}

func verifyPacketCommsSpecific(t *testing.T, cConn *Conn, data []byte,
	write func(t *testing.T, cConn *Conn, data []byte),
	read func() ([]byte, error)) {
	// Have to do it in the background if it cannot be buffered.
	// Note we have to wait for it to finish at the end of the
	// test, as the write may write all the data to the socket,
	// and the flush may not be done after we're done with the read.
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		write(t, cConn, data)
		wg.Done()
	}()

	received, err := read()
	if err != nil || !bytes.Equal(data, received) {
		t.Fatalf("ReadPacket failed: %v %v", received, err)
	}
	wg.Wait()
}

// Write a packet on one side, read it on the other, check it's
// correct.  We use all possible read and write methods.
func verifyPacketComms(t *testing.T, cConn, sConn *Conn, data []byte) {
	// All three writes, with ReadPacket.
	verifyPacketCommsSpecific(t, cConn, data, useWritePacket, sConn.ReadPacket)
	verifyPacketCommsSpecific(t, cConn, data, useWriteEphemeralPacketBuffered, sConn.ReadPacket)
	verifyPacketCommsSpecific(t, cConn, data, useWriteEphemeralPacketDirect, sConn.ReadPacket)

	// All three writes, with readEphemeralPacket.
	verifyPacketCommsSpecific(t, cConn, data, useWritePacket, sConn.readEphemeralPacket)
	sConn.recycleReadPacket()
	verifyPacketCommsSpecific(t, cConn, data, useWriteEphemeralPacketBuffered, sConn.readEphemeralPacket)
	sConn.recycleReadPacket()
	verifyPacketCommsSpecific(t, cConn, data, useWriteEphemeralPacketDirect, sConn.readEphemeralPacket)
	sConn.recycleReadPacket()

	// All three writes, with readEphemeralPacketDirect, if size allows it.
	if len(data) < MaxPacketSize {
		verifyPacketCommsSpecific(t, cConn, data, useWritePacket, sConn.readEphemeralPacketDirect)
		sConn.recycleReadPacket()
		verifyPacketCommsSpecific(t, cConn, data, useWriteEphemeralPacketBuffered, sConn.readEphemeralPacketDirect)
		sConn.recycleReadPacket()
		verifyPacketCommsSpecific(t, cConn, data, useWriteEphemeralPacketDirect, sConn.readEphemeralPacketDirect)
		sConn.recycleReadPacket()
	}
}

func TestRawConnection(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()
	assert.IsType(t, &net.TCPConn{}, sConn.GetRawConn())
	assert.IsType(t, &net.TCPConn{}, cConn.GetRawConn())
}

func TestPackets(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	// Verify all packets go through correctly.
	// Small one.
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	verifyPacketComms(t, cConn, sConn, data)

	// 0 length packet
	data = []byte{}
	verifyPacketComms(t, cConn, sConn, data)

	// Under the limit, still one packet.
	data = make([]byte, MaxPacketSize-1)
	data[0] = 0xab
	data[MaxPacketSize-2] = 0xef
	verifyPacketComms(t, cConn, sConn, data)

	// Exactly the limit, two packets.
	data = make([]byte, MaxPacketSize)
	data[0] = 0xab
	data[MaxPacketSize-1] = 0xef
	verifyPacketComms(t, cConn, sConn, data)

	// Over the limit, two packets.
	data = make([]byte, MaxPacketSize+1000)
	data[0] = 0xab
	data[MaxPacketSize+999] = 0xef
	verifyPacketComms(t, cConn, sConn, data)
}

func TestBasicPackets(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	// Write OK packet, read it, compare.
	err := sConn.writeOKPacket(&PacketOK{
		affectedRows: 12,
		lastInsertID: 34,
		statusFlags:  56,
		warnings:     78,
	})
	require.NoError(err)

	data, err := cConn.ReadPacket()
	require.NoError(err)
	require.NotEmpty(data)
	assert.EqualValues(data[0], OKPacket, "OKPacket")

	packetOk, err := cConn.parseOKPacket(data)
	require.NoError(err)
	assert.EqualValues(12, packetOk.affectedRows)
	assert.EqualValues(34, packetOk.lastInsertID)
	assert.EqualValues(56, packetOk.statusFlags)
	assert.EqualValues(78, packetOk.warnings)

	// Write OK packet with affected GTIDs, read it, compare.
	sConn.Capabilities |= CapabilityClientSessionTrack
	cConn.Capabilities |= CapabilityClientSessionTrack
	ok := PacketOK{
		affectedRows:     23,
		lastInsertID:     45,
		statusFlags:      67 | ServerSessionStateChanged,
		warnings:         89,
		info:             "",
		sessionStateData: "foo-bar",
	}
	err = sConn.writeOKPacket(&ok)
	require.NoError(err)

	data, err = cConn.ReadPacket()
	require.NoError(err)
	require.NotEmpty(data)
	assert.EqualValues(data[0], OKPacket, "OKPacket")

	packetOk, err = cConn.parseOKPacket(data)
	require.NoError(err)
	assert.EqualValues(23, packetOk.affectedRows)
	assert.EqualValues(45, packetOk.lastInsertID)
	assert.EqualValues(ServerSessionStateChanged, packetOk.statusFlags&ServerSessionStateChanged)
	assert.EqualValues(89, packetOk.warnings)
	assert.EqualValues("foo-bar", packetOk.sessionStateData)

	// Write OK packet with EOF header, read it, compare.
	ok = PacketOK{
		affectedRows: 12,
		lastInsertID: 34,
		statusFlags:  56,
		warnings:     78,
	}
	err = sConn.writeOKPacketWithEOFHeader(&ok)
	require.NoError(err)

	data, err = cConn.ReadPacket()
	require.NoError(err)
	require.NotEmpty(data)
	assert.True(cConn.isEOFPacket(data), "expected EOF")

	packetOk, err = cConn.parseOKPacket(data)
	require.NoError(err)
	assert.EqualValues(12, packetOk.affectedRows)
	assert.EqualValues(34, packetOk.lastInsertID)
	assert.EqualValues(56, packetOk.statusFlags)
	assert.EqualValues(78, packetOk.warnings)

	// Write error packet, read it, compare.
	err = sConn.writeErrorPacket(ERAccessDeniedError, SSAccessDeniedError, "access denied: %v", "reason")
	require.NoError(err)
	data, err = cConn.ReadPacket()
	require.NoError(err)
	require.NotEmpty(data)
	assert.EqualValues(data[0], ErrPacket, "ErrPacket")

	err = ParseErrorPacket(data)
	utils.MustMatch(t, err, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "access denied: reason"), "")

	// Write error packet from error, read it, compare.
	err = sConn.writeErrorPacketFromError(NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "access denied"))
	require.NoError(err)

	data, err = cConn.ReadPacket()
	require.NoError(err)
	require.NotEmpty(data)
	assert.EqualValues(data[0], ErrPacket, "ErrPacket")

	err = ParseErrorPacket(data)
	utils.MustMatch(t, err, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "access denied"), "")

	// Write EOF packet, read it, compare first byte. Payload is always ignored.
	err = sConn.writeEOFPacket(0x8912, 0xabba)
	require.NoError(err)

	data, err = cConn.ReadPacket()
	require.NoError(err)
	require.NotEmpty(data)
	assert.True(cConn.isEOFPacket(data), "expected EOF")
}

func TestOkPackets(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	testCases := []struct {
		dataIn      string
		dataOut     string
		cc          uint32
		expectedErr string
	}{{
		dataIn: `
00000000  00 00 00 02 00 00 00                              |.......|`,
		cc: CapabilityClientProtocol41,
	}, {
		dataIn: `
00000000  00 00 00 02 00                                    |.....|`,
		cc:          CapabilityClientTransactions,
		expectedErr: "invalid OK packet warnings: &{[0 0 0 2 0] 0}",
	}, {
		dataIn: `
00000000  00 00 00 02 40 00 00 00  2a 03 28 00 26 66 32 37  |....@...*.(.&f27|
00000010  66 36 39 37 31 2d 30 33  65 37 2d 31 31 65 62 2d  |f6971-03e7-11eb-|
00000020  38 35 63 35 2d 39 38 61  66 36 35 61 36 64 63 34  |85c5-98af65a6dc4|
00000030  61 3a 32                                          |a:2|`,
		cc: CapabilityClientProtocol41 | CapabilityClientTransactions | CapabilityClientSessionTrack,
	}, {
		dataIn:  `00000000  00 00 00 02 40 00 00 00  07 01 05 04 74 65 73 74  |....@.......test|`,
		dataOut: `00000000  00 00 00 02 40 00 00 00  04 03 02 00 00  |....@........|`,
		cc:      CapabilityClientProtocol41 | CapabilityClientTransactions | CapabilityClientSessionTrack,
	}, {
		dataIn: `
00000000  00 00 00 00 40 00 00 00  14 00 0f 0a 61 75 74 6f  |....@.......auto|
00000010  63 6f 6d 6d 69 74 03 4f  46 46 02 01 31           |commit.OFF..1|`,
		dataOut: `
00000000  00 00 00 00 40 00 00 00  04 03 02 00 00           |....@........|`,
		cc: CapabilityClientProtocol41 | CapabilityClientTransactions | CapabilityClientSessionTrack,
	}, {
		dataIn: `
00000000  00 00 00 00 40 00 00 00  0a 01 05 04 74 65 73 74  |....@.......test|
00000010  02 01 31                                          |..1|`,
		dataOut: `
00000000  00 00 00 00 40 00 00 00  04 03 02 00 00           |....@........|`,
		cc: CapabilityClientProtocol41 | CapabilityClientTransactions | CapabilityClientSessionTrack,
	}, {
		dataIn: `0000   00 00 00 03 40 00 00 00 fc 56 04 03   |a.......@....V..|
0010   fc 47 04 00 fc 43 04 30 63 36 63 36 62 34 61 2d   |.G...C.0c6c6b4a-|
0020   32 64 65 35 2d 31 31 65 64 2d 62 63 37 61 2d 61   |2de5-11ed-bc7a-a|
0030   38 61 31 35 39 38 33 64 35 62 64 3a 31 2d 34 2c   |8a15983d5bd:1-4,|
0040   0a 31 33 65 62 66 38 32 38 2d 32 64 65 35 2d 31   |.13ebf828-2de5-1|
0050   31 65 64 2d 62 34 65 35 2d 61 38 61 31 35 39 38   |1ed-b4e5-a8a1598|
0060   33 64 35 62 64 3a 31 2d 39 2c 0a 31 38 61 30 66   |3d5bd:1-9,.18a0f|
0070   30 34 38 2d 32 64 65 34 2d 31 31 65 64 2d 38 63   |048-2de4-11ed-8c|
0080   31 63 2d 61 38 61 31 35 39 38 33 64 35 62 64 3a   |1c-a8a15983d5bd:|
0090   31 2d 33 2c 0a 31 66 36 34 31 62 36 33 2d 32 64   |1-3,.1f641b63-2d|
00a0   65 35 2d 31 31 65 64 2d 61 35 31 62 2d 61 38 61   |e5-11ed-a51b-a8a|
00b0   31 35 39 38 33 64 35 62 64 3a 31 2d 39 2c 0a 32   |15983d5bd:1-9,.2|
00c0   63 36 35 36 35 37 31 2d 32 64 65 35 2d 31 31 65   |c656571-2de5-11e|
00d0   64 2d 61 34 37 34 2d 61 38 61 31 35 39 38 33 64   |d-a474-a8a15983d|
00e0   35 62 64 3a 31 2d 35 2c 0a 33 32 32 61 34 32 35   |5bd:1-5,.322a425|
00f0   34 2d 32 64 65 35 2d 31 31 65 64 2d 61 65 64 31   |4-2de5-11ed-aed1|
0100   2d 61 38 61 31 35 39 38 33 64 35 62 64 3a 31 2d   |-a8a15983d5bd:1-|
0110   34 2c 0a 33 37 63 35 64 30 34 31 2d 32 64 65 35   |4,.37c5d041-2de5|
0120   2d 31 31 65 64 2d 38 64 33 66 2d 61 38 61 31 35   |-11ed-8d3f-a8a15|
0130   39 38 33 64 35 62 64 3a 31 2d 31 32 2c 0a 34 31   |983d5bd:1-12,.41|
0140   34 33 32 37 32 33 2d 32 64 65 35 2d 31 31 65 64   |432723-2de5-11ed|
0150   2d 61 61 36 66 2d 61 38 61 31 35 39 38 33 64 35   |-aa6f-a8a15983d5|
0160   62 64 3a 31 2d 37 2c 0a 34 39 38 38 38 35 36 66   |bd:1-7,.4988856f|
0170   2d 32 64 65 34 2d 31 31 65 64 2d 39 37 31 36 2d   |-2de4-11ed-9716-|
0180   61 38 61 31 35 39 38 33 64 35 62 64 3a 31 2d 35   |a8a15983d5bd:1-5|
0190   2c 0a 35 35 38 36 61 64 34 65 2d 32 64 65 34 2d   |,.5586ad4e-2de4-|
01a0   31 31 65 64 2d 38 63 37 33 2d 61 38 61 31 35 39   |11ed-8c73-a8a159|
01b0   38 33 64 35 62 64 3a 31 2d 36 2c 0a 36 34 65 39   |83d5bd:1-6,.64e9|
01c0   66 32 32 66 2d 32 64 65 34 2d 31 31 65 64 2d 39   |f22f-2de4-11ed-9|
01d0   62 65 31 2d 61 38 61 31 35 39 38 33 64 35 62 64   |be1-a8a15983d5bd|
01e0   3a 31 2d 33 2c 0a 36 62 31 36 34 37 30 65 2d 32   |:1-3,.6b16470e-2|
01f0   64 65 34 2d 31 31 65 64 2d 61 31 33 64 2d 61 38   |de4-11ed-a13d-a8|
0200   61 31 35 39 38 33 64 35 62 64 3a 31 2d 34 2c 0a   |a15983d5bd:1-4,.|
0210   37 35 65 37 65 32 38 65 2d 32 37 61 38 2d 31 31   |75e7e28e-27a8-11|
0220   65 64 2d 39 61 30 36 2d 61 38 61 31 35 39 38 33   |ed-9a06-a8a15983|
0230   64 35 62 64 3a 31 2d 39 2c 0a 38 31 34 30 32 37   |d5bd:1-9,.814027|
0240   66 31 2d 32 64 65 34 2d 31 31 65 64 2d 39 65 33   |f1-2de4-11ed-9e3|
0250   63 2d 61 38 61 31 35 39 38 33 64 35 62 64 3a 31   |c-a8a15983d5bd:1|
0260   2d 34 2c 0a 38 37 63 32 38 64 64 63 2d 32 64 65   |-4,.87c28ddc-2de|
0270   34 2d 31 31 65 64 2d 38 32 37 32 2d 61 38 61 31   |4-11ed-8272-a8a1|
0280   35 39 38 33 64 35 62 64 3a 31 2d 31 39 2c 0a 39   |5983d5bd:1-19,.9|
0290   30 35 38 33 35 62 37 2d 32 64 65 35 2d 31 31 65   |05835b7-2de5-11e|
02a0   64 2d 61 32 39 39 2d 61 38 61 31 35 39 38 33 64   |d-a299-a8a15983d|
02b0   35 62 64 3a 31 2d 38 2c 0a 39 37 64 66 36 30 63   |5bd:1-8,.97df60c|
02c0   39 2d 32 64 65 34 2d 31 31 65 64 2d 62 39 30 65   |9-2de4-11ed-b90e|
02d0   2d 61 38 61 31 35 39 38 33 64 35 62 64 3a 31 2d   |-a8a15983d5bd:1-|
02e0   35 2c 0a 39 37 65 39 30 63 30 38 2d 32 64 65 35   |5,.97e90c08-2de5|
02f0   2d 31 31 65 64 2d 39 37 30 39 2d 61 38 61 31 35   |-11ed-9709-a8a15|
0300   39 38 33 64 35 62 64 3a 31 2d 33 38 2c 0a 39 39   |983d5bd:1-38,.99|
0310   64 66 61 32 62 64 2d 32 64 65 33 2d 31 31 65 64   |dfa2bd-2de3-11ed|
0320   2d 62 37 39 65 2d 61 38 61 31 35 39 38 33 64 35   |-b79e-a8a15983d5|
0330   62 64 3a 31 2c 0a 61 31 62 63 34 33 34 32 2d 32   |bd:1,.a1bc4342-2|
0340   64 65 34 2d 31 31 65 64 2d 61 30 62 31 2d 61 38   |de4-11ed-a0b1-a8|
0350   61 31 35 39 38 33 64 35 62 64 3a 31 2d 31 36 2c   |a15983d5bd:1-16,|
0360   0a 61 62 65 35 65 32 61 34 2d 32 64 65 34 2d 31   |.abe5e2a4-2de4-1|
0370   31 65 64 2d 62 62 33 63 2d 61 38 61 31 35 39 38   |1ed-bb3c-a8a1598|
0380   33 64 35 62 64 3a 31 2d 33 2c 0a 62 37 64 39 61   |3d5bd:1-3,.b7d9a|
0390   62 39 37 2d 32 64 65 34 2d 31 31 65 64 2d 39 33   |b97-2de4-11ed-93|
03a0   39 64 2d 61 38 61 31 35 39 38 33 64 35 62 64 3a   |9d-a8a15983d5bd:|
03b0   31 2c 0a 62 64 33 64 30 34 30 30 2d 32 64 65 34   |1,.bd3d0400-2de4|
03c0   2d 31 31 65 64 2d 38 62 36 61 2d 61 38 61 31 35   |-11ed-8b6a-a8a15|
03d0   39 38 33 64 35 62 64 3a 31 2d 36 2c 0a 63 36 61   |983d5bd:1-6,.c6a|
03e0   38 37 33 61 63 2d 32 64 65 35 2d 31 31 65 64 2d   |873ac-2de5-11ed-|
03f0   38 35 30 33 2d 61 38 61 31 35 39 38 33 64 35 62   |8503-a8a15983d5b|
0400   64 3a 31 2d 32 31 2c 0a 64 34 37 65 30 36 32 65   |d:1-21,.d47e062e|
0410   2d 32 64 65 35 2d 31 31 65 64 2d 38 63 39 62 2d   |-2de5-11ed-8c9b-|
0420   61 38 61 31 35 39 38 33 64 35 62 64 3a 31 2d 39   |a8a15983d5bd:1-9|
0430   2c 0a 64 65 30 64 63 37 38 30 2d 32 64 65 35 2d   |,.de0dc780-2de5-|
0440   31 31 65 64 2d 62 31 62 31 2d 61 38 61 31 35 39   |11ed-b1b1-a8a159|
0450   38 33 64 35 62 64 3a 31 2d 37 05 09 08 54 5f 52   |83d5bd:1-7...T_R|
0460   5f 5f 5f 5f 5f                                    |_____|
`,
		dataOut: `
00000000  00 00 00 03 40 00 00 00  fc 4b 04 03 fc 47 04 00  |....@....K...G..|
00000010  fc 43 04 30 63 36 63 36  62 34 61 2d 32 64 65 35  |.C.0c6c6b4a-2de5|
00000020  2d 31 31 65 64 2d 62 63  37 61 2d 61 38 61 31 35  |-11ed-bc7a-a8a15|
00000030  39 38 33 64 35 62 64 3a  31 2d 34 2c 0a 31 33 65  |983d5bd:1-4,.13e|
00000040  62 66 38 32 38 2d 32 64  65 35 2d 31 31 65 64 2d  |bf828-2de5-11ed-|
00000050  62 34 65 35 2d 61 38 61  31 35 39 38 33 64 35 62  |b4e5-a8a15983d5b|
00000060  64 3a 31 2d 39 2c 0a 31  38 61 30 66 30 34 38 2d  |d:1-9,.18a0f048-|
00000070  32 64 65 34 2d 31 31 65  64 2d 38 63 31 63 2d 61  |2de4-11ed-8c1c-a|
00000080  38 61 31 35 39 38 33 64  35 62 64 3a 31 2d 33 2c  |8a15983d5bd:1-3,|
00000090  0a 31 66 36 34 31 62 36  33 2d 32 64 65 35 2d 31  |.1f641b63-2de5-1|
000000a0  31 65 64 2d 61 35 31 62  2d 61 38 61 31 35 39 38  |1ed-a51b-a8a1598|
000000b0  33 64 35 62 64 3a 31 2d  39 2c 0a 32 63 36 35 36  |3d5bd:1-9,.2c656|
000000c0  35 37 31 2d 32 64 65 35  2d 31 31 65 64 2d 61 34  |571-2de5-11ed-a4|
000000d0  37 34 2d 61 38 61 31 35  39 38 33 64 35 62 64 3a  |74-a8a15983d5bd:|
000000e0  31 2d 35 2c 0a 33 32 32  61 34 32 35 34 2d 32 64  |1-5,.322a4254-2d|
000000f0  65 35 2d 31 31 65 64 2d  61 65 64 31 2d 61 38 61  |e5-11ed-aed1-a8a|
00000100  31 35 39 38 33 64 35 62  64 3a 31 2d 34 2c 0a 33  |15983d5bd:1-4,.3|
00000110  37 63 35 64 30 34 31 2d  32 64 65 35 2d 31 31 65  |7c5d041-2de5-11e|
00000120  64 2d 38 64 33 66 2d 61  38 61 31 35 39 38 33 64  |d-8d3f-a8a15983d|
00000130  35 62 64 3a 31 2d 31 32  2c 0a 34 31 34 33 32 37  |5bd:1-12,.414327|
00000140  32 33 2d 32 64 65 35 2d  31 31 65 64 2d 61 61 36  |23-2de5-11ed-aa6|
00000150  66 2d 61 38 61 31 35 39  38 33 64 35 62 64 3a 31  |f-a8a15983d5bd:1|
00000160  2d 37 2c 0a 34 39 38 38  38 35 36 66 2d 32 64 65  |-7,.4988856f-2de|
00000170  34 2d 31 31 65 64 2d 39  37 31 36 2d 61 38 61 31  |4-11ed-9716-a8a1|
00000180  35 39 38 33 64 35 62 64  3a 31 2d 35 2c 0a 35 35  |5983d5bd:1-5,.55|
00000190  38 36 61 64 34 65 2d 32  64 65 34 2d 31 31 65 64  |86ad4e-2de4-11ed|
000001a0  2d 38 63 37 33 2d 61 38  61 31 35 39 38 33 64 35  |-8c73-a8a15983d5|
000001b0  62 64 3a 31 2d 36 2c 0a  36 34 65 39 66 32 32 66  |bd:1-6,.64e9f22f|
000001c0  2d 32 64 65 34 2d 31 31  65 64 2d 39 62 65 31 2d  |-2de4-11ed-9be1-|
000001d0  61 38 61 31 35 39 38 33  64 35 62 64 3a 31 2d 33  |a8a15983d5bd:1-3|
000001e0  2c 0a 36 62 31 36 34 37  30 65 2d 32 64 65 34 2d  |,.6b16470e-2de4-|
000001f0  31 31 65 64 2d 61 31 33  64 2d 61 38 61 31 35 39  |11ed-a13d-a8a159|
00000200  38 33 64 35 62 64 3a 31  2d 34 2c 0a 37 35 65 37  |83d5bd:1-4,.75e7|
00000210  65 32 38 65 2d 32 37 61  38 2d 31 31 65 64 2d 39  |e28e-27a8-11ed-9|
00000220  61 30 36 2d 61 38 61 31  35 39 38 33 64 35 62 64  |a06-a8a15983d5bd|
00000230  3a 31 2d 39 2c 0a 38 31  34 30 32 37 66 31 2d 32  |:1-9,.814027f1-2|
00000240  64 65 34 2d 31 31 65 64  2d 39 65 33 63 2d 61 38  |de4-11ed-9e3c-a8|
00000250  61 31 35 39 38 33 64 35  62 64 3a 31 2d 34 2c 0a  |a15983d5bd:1-4,.|
00000260  38 37 63 32 38 64 64 63  2d 32 64 65 34 2d 31 31  |87c28ddc-2de4-11|
00000270  65 64 2d 38 32 37 32 2d  61 38 61 31 35 39 38 33  |ed-8272-a8a15983|
00000280  64 35 62 64 3a 31 2d 31  39 2c 0a 39 30 35 38 33  |d5bd:1-19,.90583|
00000290  35 62 37 2d 32 64 65 35  2d 31 31 65 64 2d 61 32  |5b7-2de5-11ed-a2|
000002a0  39 39 2d 61 38 61 31 35  39 38 33 64 35 62 64 3a  |99-a8a15983d5bd:|
000002b0  31 2d 38 2c 0a 39 37 64  66 36 30 63 39 2d 32 64  |1-8,.97df60c9-2d|
000002c0  65 34 2d 31 31 65 64 2d  62 39 30 65 2d 61 38 61  |e4-11ed-b90e-a8a|
000002d0  31 35 39 38 33 64 35 62  64 3a 31 2d 35 2c 0a 39  |15983d5bd:1-5,.9|
000002e0  37 65 39 30 63 30 38 2d  32 64 65 35 2d 31 31 65  |7e90c08-2de5-11e|
000002f0  64 2d 39 37 30 39 2d 61  38 61 31 35 39 38 33 64  |d-9709-a8a15983d|
00000300  35 62 64 3a 31 2d 33 38  2c 0a 39 39 64 66 61 32  |5bd:1-38,.99dfa2|
00000310  62 64 2d 32 64 65 33 2d  31 31 65 64 2d 62 37 39  |bd-2de3-11ed-b79|
00000320  65 2d 61 38 61 31 35 39  38 33 64 35 62 64 3a 31  |e-a8a15983d5bd:1|
00000330  2c 0a 61 31 62 63 34 33  34 32 2d 32 64 65 34 2d  |,.a1bc4342-2de4-|
00000340  31 31 65 64 2d 61 30 62  31 2d 61 38 61 31 35 39  |11ed-a0b1-a8a159|
00000350  38 33 64 35 62 64 3a 31  2d 31 36 2c 0a 61 62 65  |83d5bd:1-16,.abe|
00000360  35 65 32 61 34 2d 32 64  65 34 2d 31 31 65 64 2d  |5e2a4-2de4-11ed-|
00000370  62 62 33 63 2d 61 38 61  31 35 39 38 33 64 35 62  |bb3c-a8a15983d5b|
00000380  64 3a 31 2d 33 2c 0a 62  37 64 39 61 62 39 37 2d  |d:1-3,.b7d9ab97-|
00000390  32 64 65 34 2d 31 31 65  64 2d 39 33 39 64 2d 61  |2de4-11ed-939d-a|
000003a0  38 61 31 35 39 38 33 64  35 62 64 3a 31 2c 0a 62  |8a15983d5bd:1,.b|
000003b0  64 33 64 30 34 30 30 2d  32 64 65 34 2d 31 31 65  |d3d0400-2de4-11e|
000003c0  64 2d 38 62 36 61 2d 61  38 61 31 35 39 38 33 64  |d-8b6a-a8a15983d|
000003d0  35 62 64 3a 31 2d 36 2c  0a 63 36 61 38 37 33 61  |5bd:1-6,.c6a873a|
000003e0  63 2d 32 64 65 35 2d 31  31 65 64 2d 38 35 30 33  |c-2de5-11ed-8503|
000003f0  2d 61 38 61 31 35 39 38  33 64 35 62 64 3a 31 2d  |-a8a15983d5bd:1-|
00000400  32 31 2c 0a 64 34 37 65  30 36 32 65 2d 32 64 65  |21,.d47e062e-2de|
00000410  35 2d 31 31 65 64 2d 38  63 39 62 2d 61 38 61 31  |5-11ed-8c9b-a8a1|
00000420  35 39 38 33 64 35 62 64  3a 31 2d 39 2c 0a 64 65  |5983d5bd:1-9,.de|
00000430  30 64 63 37 38 30 2d 32  64 65 35 2d 31 31 65 64  |0dc780-2de5-11ed|
00000440  2d 62 31 62 31 2d 61 38  61 31 35 39 38 33 64 35  |-b1b1-a8a15983d5|
00000450  62 64 3a 31 2d 37                                 |bd:1-7|`,
		cc: CapabilityClientProtocol41 | CapabilityClientTransactions | CapabilityClientSessionTrack,
	}}

	for i, testCase := range testCases {
		t.Run("data packet:"+strconv.Itoa(i), func(t *testing.T) {
			data := ReadHexDump(testCase.dataIn)
			dataOut := data
			if testCase.dataOut != "" {
				dataOut = ReadHexDump(testCase.dataOut)
			}

			cConn.Capabilities = testCase.cc
			sConn.Capabilities = testCase.cc
			// parse the packet
			packetOk, err := cConn.parseOKPacket(data)
			if testCase.expectedErr != "" {
				require.Error(t, err)
				require.Equal(t, testCase.expectedErr, err.Error())
				return
			}
			require.NoError(t, err, "failed to parse OK packet")

			// write the ok packet from server
			err = sConn.writeOKPacket(packetOk)
			require.NoError(t, err, "failed to write OK packet")

			// receive the ok packet on client
			readData, err := cConn.ReadPacket()
			require.NoError(t, err, "failed to read packet that was written")
			assert.Equal(t, dataOut, readData, "data read and written does not match")
		})
	}
}

func ReadHexDump(value string) []byte {
	lines := strings.Split(value, "\n")
	var data []byte
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		indexOfPipe := strings.Index(line, "|")
		indexOfFirstSpace := strings.Index(line, " ")
		s := line[indexOfFirstSpace:indexOfPipe]
		hexValues := strings.Split(s, " ")
		for _, val := range hexValues {
			if val != "" {
				i, _ := hex.DecodeString(val)
				data = append(data, i...)
			}
		}
	}

	return data
}

// Mostly a sanity check.
func TestEOFOrLengthEncodedIntFuzz(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	for i := 0; i < 100; i++ {
		bytes := make([]byte, rand.Intn(16)+1)
		_, err := crypto_rand.Read(bytes)
		if err != nil {
			t.Fatalf("error doing rand.Read")
		}
		bytes[0] = 0xfe

		_, _, isInt := readLenEncInt(bytes, 0)
		isEOF := cConn.isEOFPacket(bytes)
		if (isInt && isEOF) || (!isInt && !isEOF) {
			t.Fatalf("0xfe bytestring is EOF xor Int. Bytes %v", bytes)
		}
	}
}

func TestMultiStatementStopsOnError(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	sConn.Capabilities |= CapabilityClientMultiStatements
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	err := cConn.WriteComQuery("error;select 2")
	require.NoError(t, err)

	// this handler will return results according to the query. In case the query contains "error" it will return an error
	// panic if the query contains "panic" and it will return selectRowsResult in case of any other query
	handler := &testRun{t: t, err: fmt.Errorf("execution failed")}
	res := sConn.handleNextCommand(handler)
	// Execution error will occur in this case because the query sent is error and testRun will throw an error.
	// We should send an error packet but not close the connection.
	require.True(t, res, "we should not break the connection because of execution errors")

	data, err := cConn.ReadPacket()
	require.NoError(t, err)
	require.NotEmpty(t, data)
	require.EqualValues(t, data[0], ErrPacket) // we should see the error here
}

func TestMultiStatement(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	sConn.Capabilities |= CapabilityClientMultiStatements
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	err := cConn.WriteComQuery("select 1;select 2")
	require.NoError(t, err)

	// this handler will return results according to the query. In case the query contains "error" it will return an error
	// panic if the query contains "panic" and it will return selectRowsResult in case of any other query
	handler := &testRun{t: t, err: NewSQLError(CRMalformedPacket, SSUnknownSQLState, "cannot get column number")}
	res := sConn.handleNextCommand(handler)
	//The queries run will be select 1; and select 2; These queries do not return any errors, so the connection should still be open
	require.True(t, res, "we should not break the connection in case of no errors")
	// Read the result of the query and assert that it is indeed what we want. This will contain the result of the first query.
	data, more, _, err := cConn.ReadQueryResult(100, true)
	require.NoError(t, err)
	// Since we executed 2 queries, there should be more results to be read
	require.True(t, more)
	require.True(t, data.Equal(selectRowsResult))

	// Read the results for the second query and verify the correctness
	data, more, _, err = cConn.ReadQueryResult(100, true)
	require.NoError(t, err)
	// This was the final query run, so we expect that more should be false as there are no more queries.
	require.False(t, more)
	require.True(t, data.Equal(selectRowsResult))

	// This time we run two queries fist of which will return an error
	err = cConn.WriteComQuery("error;select 2")
	require.NoError(t, err)

	res = sConn.handleNextCommand(handler)
	// Even if the query returns an error we should not close the connection as it is an execution error
	require.True(t, res, "we should not break the connection because of execution errors")

	// Read the result and assert that we indeed see the error that testRun throws.
	data, more, _, err = cConn.ReadQueryResult(100, true)
	require.EqualError(t, err, "cannot get column number (errno 2027) (sqlstate HY000)")
	// In case of errors in a multi-statement, the following statements are not executed, therefore we want that more should be false
	require.False(t, more)
	require.Nil(t, data)
}

func TestMultiStatementOnSplitError(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	// Set the splitStatementFunction to return an error.
	splitStatementFunction = func(blob string) (pieces []string, err error) {
		return nil, fmt.Errorf("Error in split statements")
	}
	defer func() {
		// Set the splitStatementFunction to the correct function back
		splitStatementFunction = sqlparser.SplitStatementToPieces
	}()
	sConn.Capabilities |= CapabilityClientMultiStatements
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	err := cConn.WriteComQuery("select 1;select 2")
	require.NoError(t, err)

	// this handler will return results according to the query. In case the query contains "error" it will return an error
	// panic if the query contains "panic" and it will return selectRowsResult in case of any other query
	handler := &testRun{t: t, err: fmt.Errorf("execution failed")}

	// We will encounter an error in split statement when this multi statement is processed.
	res := sConn.handleNextCommand(handler)
	// Since this is an execution error, we should not be closing the connection.
	require.True(t, res, "we should not break the connection because of execution errors")
	// Assert that the returned packet is an error packet.
	data, err := cConn.ReadPacket()
	require.NoError(t, err)
	require.NotEmpty(t, data)
	require.EqualValues(t, data[0], ErrPacket) // we should see the error here
}

func TestInitDbAgainstWrongDbDoesNotDropConnection(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	sConn.Capabilities |= CapabilityClientMultiStatements
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	err := cConn.writeComInitDB("error")
	require.NoError(t, err)

	// this handler will return results according to the query. In case the query contains "error" it will return an error
	// panic if the query contains "panic" and it will return selectRowsResult in case of any other query
	handler := &testRun{t: t, err: fmt.Errorf("execution failed")}
	res := sConn.handleNextCommand(handler)
	require.True(t, res, "we should not break the connection because of execution errors")

	data, err := cConn.ReadPacket()
	require.NoError(t, err)
	require.NotEmpty(t, data)
	require.EqualValues(t, data[0], ErrPacket) // we should see the error here
}

func TestConnectionErrorWhileWritingComQuery(t *testing.T) {
	// Set the conn for the server connection to the simulated connection which always returns an error on writing
	sConn := newConn(testConn{
		writeToPass: []bool{false, true},
		pos:         -1,
		queryPacket: []byte{0x21, 0x00, 0x00, 0x00, ComQuery, 0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20, 0x40, 0x40, 0x76, 0x65, 0x72, 0x73,
			0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e, 0x74, 0x20, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x20, 0x31},
	})

	// this handler will return an error on the first run, and fail the test if it's run more times
	errorString := make([]byte, 17000)
	handler := &testRun{t: t, err: fmt.Errorf(string(errorString))}
	res := sConn.handleNextCommand(handler)
	require.False(t, res, "we should beak the connection in case of error writing error packet")
}

func TestConnectionErrorWhileWritingComStmtSendLongData(t *testing.T) {
	// Set the conn for the server connection to the simulated connection which always returns an error on writing
	sConn := newConn(testConn{
		writeToPass: []bool{false, true},
		pos:         -1,
		queryPacket: []byte{0x21, 0x00, 0x00, 0x00, ComStmtSendLongData, 0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20, 0x40, 0x40, 0x76, 0x65, 0x72, 0x73,
			0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e, 0x74, 0x20, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x20, 0x31},
	})

	// this handler will return an error on the first run, and fail the test if it's run more times
	handler := &testRun{t: t, err: fmt.Errorf("not used")}
	res := sConn.handleNextCommand(handler)
	require.False(t, res, "we should beak the connection in case of error writing error packet")
}

func TestConnectionErrorWhileWritingComPrepare(t *testing.T) {
	// Set the conn for the server connection to the simulated connection which always returns an error on writing
	sConn := newConn(testConn{
		writeToPass: []bool{false},
		pos:         -1,
		queryPacket: []byte{0x01, 0x00, 0x00, 0x00, ComPrepare},
	})
	sConn.Capabilities = sConn.Capabilities | CapabilityClientMultiStatements
	// this handler will return an error on the first run, and fail the test if it's run more times
	handler := &testRun{t: t, err: fmt.Errorf("not used")}
	res := sConn.handleNextCommand(handler)
	require.False(t, res, "we should beak the connection in case of error writing error packet")
}

func TestConnectionErrorWhileWritingComStmtExecute(t *testing.T) {
	// Set the conn for the server connection to the simulated connection which always returns an error on writing
	sConn := newConn(testConn{
		writeToPass: []bool{false},
		pos:         -1,
		queryPacket: []byte{0x21, 0x00, 0x00, 0x00, ComStmtExecute, 0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20, 0x40, 0x40, 0x76, 0x65, 0x72, 0x73,
			0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e, 0x74, 0x20, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x20, 0x31},
	})
	// this handler will return an error on the first run, and fail the test if it's run more times
	handler := &testRun{t: t, err: fmt.Errorf("not used")}
	res := sConn.handleNextCommand(handler)
	require.False(t, res, "we should beak the connection in case of error writing error packet")
}

var _ Handler = (*testRun)(nil)

type testConn struct {
	writeToPass []bool
	pos         int
	queryPacket []byte
}

func (t testConn) Read(b []byte) (n int, err error) {
	copy(b, t.queryPacket)
	return len(b), nil
}

func (t testConn) Write(b []byte) (n int, err error) {
	t.pos = t.pos + 1
	if t.writeToPass[t.pos] {
		return 0, nil
	}
	return 0, fmt.Errorf("error in writing to connection")
}

func (t testConn) Close() error {
	panic("implement me")
}

func (t testConn) LocalAddr() net.Addr {
	panic("implement me")
}

func (t testConn) RemoteAddr() net.Addr {
	return mockAddress{s: "a"}
}

func (t testConn) SetDeadline(t1 time.Time) error {
	panic("implement me")
}

func (t testConn) SetReadDeadline(t1 time.Time) error {
	panic("implement me")
}

func (t testConn) SetWriteDeadline(t1 time.Time) error {
	panic("implement me")
}

var _ net.Conn = (*testConn)(nil)

type mockAddress struct {
	s string
}

func (m mockAddress) Network() string {
	return m.s
}

func (m mockAddress) String() string {
	return m.s
}

var _ net.Addr = (*mockAddress)(nil)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TestPrepareAndExecute(t *testing.T) {
	// this test starts a lot of clients that all send prepared statement parameter values
	// and check that the handler received the correct input
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for i := 0; i < 100; i++ {
		startGoRoutine(ctx, t, fmt.Sprintf("%d:%s", i, randSeq(i)))
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			if t.Failed() {
				return
			}
		}
	}
}

func startGoRoutine(ctx context.Context, t *testing.T, s string) {
	go func(longData string) {
		listener, sConn, cConn := createSocketPair(t)
		defer func() {
			listener.Close()
			sConn.Close()
			cConn.Close()
		}()

		sql := "SELECT * FROM test WHERE id = ?"
		mockData := preparePacket(t, sql)

		err := cConn.writePacket(mockData)
		require.NoError(t, err)

		handler := &testRun{
			t:              t,
			expParamCounts: 1,
			expQuery:       sql,
			expStmtID:      1,
		}

		ok := sConn.handleNextCommand(handler)
		require.True(t, ok, "error handling command for id: %s", s)

		resp, err := cConn.ReadPacket()
		require.NoError(t, err)
		require.EqualValues(t, 0, resp[0])

		for count := 0; ; count++ {
			select {
			case <-ctx.Done():
				return
			default:
			}
			cConn.sequence = 0
			longDataPacket := createSendLongDataPacket(sConn.StatementID, 0, []byte(longData))
			err = cConn.writePacket(longDataPacket)
			assert.NoError(t, err)

			assert.True(t, sConn.handleNextCommand(handler))
			data := sConn.PrepareData[sConn.StatementID]
			assert.NotNil(t, data)
			variable := data.BindVars["v1"]
			assert.NotNil(t, variable, fmt.Sprintf("%#v", data.BindVars))
			assert.Equalf(t, []byte(longData), variable.Value[len(longData)*count:], "failed at: %d", count)
		}
	}(s)
}

func createSendLongDataPacket(stmtID uint32, paramID uint16, data []byte) []byte {
	stmtIDBinary := make([]byte, 4)
	binary.LittleEndian.PutUint32(stmtIDBinary, stmtID)

	paramIDBinary := make([]byte, 2)
	binary.LittleEndian.PutUint16(paramIDBinary, paramID)

	packet := []byte{0, 0, 0, 0, ComStmtSendLongData}
	packet = append(packet, stmtIDBinary...)  // append stmt ID
	packet = append(packet, paramIDBinary...) // append param ID
	packet = append(packet, data...)          // append data
	return packet
}

type testRun struct {
	UnimplementedHandler
	t              *testing.T
	err            error
	expParamCounts int
	expQuery       string
	expStmtID      int
}

func (t testRun) ComStmtExecute(c *Conn, prepare *PrepareData, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

func (t testRun) ComBinlogDumpGTID(c *Conn, gtidSet GTIDSet) error {
	panic("implement me")
}

func (t testRun) ComQuery(c *Conn, query string, callback func(*sqltypes.Result) error) error {
	if strings.Contains(query, "error") {
		return t.err
	}
	if strings.Contains(query, "panic") {
		panic("test panic attack!")
	}
	if strings.Contains(query, "twice") {
		callback(selectRowsResult)
	}
	callback(selectRowsResult)
	return nil
}

func (t testRun) ComPrepare(c *Conn, query string, bv map[string]*querypb.BindVariable) ([]*querypb.Field, error) {
	assert.Equal(t.t, t.expQuery, query)
	assert.EqualValues(t.t, t.expStmtID, c.StatementID)
	assert.NotNil(t.t, c.PrepareData[c.StatementID])
	assert.EqualValues(t.t, t.expParamCounts, c.PrepareData[c.StatementID].ParamsCount)
	assert.Len(t.t, c.PrepareData, int(c.PrepareData[c.StatementID].ParamsCount))
	return nil, nil
}

func (t testRun) WarningCount(c *Conn) uint16 {
	return 0
}

var _ Handler = (*testRun)(nil)
