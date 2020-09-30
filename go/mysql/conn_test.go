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
	crypto_rand "crypto/rand"
	"encoding/hex"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/test/utils"
)

func createSocketPair(t *testing.T) (net.Listener, *Conn, *Conn) {
	// Create a listener.
	listener, err := net.Listen("tcp", ":0")
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
	gtids := "foo-bar"
	err = sConn.writeOKPacketWithGTIDs(23, 45, 67, 89, gtids)
	require.NoError(err)

	data, err = cConn.ReadPacket()
	require.NoError(err)
	require.NotEmpty(data)
	assert.EqualValues(data[0], OKPacket, "OKPacket")

	cConn.Capabilities = CapabilityFlags
	packetOk, err = cConn.parseOKPacket(data)
	require.NoError(err)
	assert.EqualValues(23, packetOk.affectedRows)
	assert.EqualValues(45, packetOk.lastInsertID)
	assert.EqualValues(67|ServerSessionStateChanged, packetOk.statusFlags)
	assert.EqualValues(89, packetOk.warnings)
	assert.EqualValues(SessionTrackGtids, packetOk.sessionStateChangeType)
	// TODO harshit: fix this assert.EqualValues("SessionTrackGtids", packetOk.sessionStateChangeValue)

	// Write OK packet with EOF header, read it, compare.
	err = sConn.writeOKPacketWithEOFHeader(12, 34, 56, 78)
	require.NoError(err)

	data, err = cConn.ReadPacket()
	require.NoError(err)
	require.NotEmpty(data)
	assert.True(isEOFPacket(data), "expected EOF")

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
	assert.True(isEOFPacket(data), "expected EOF")
}

func TestOkPackets(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	testCases := []struct {
		data []byte
		cc   uint32
	}{{
		data: StringToPacket(`
07 00 00 02 00 00 00 02    00 00 00                   ...........
`), cc: CapabilityFlags,
	}, {
		//		data: StringToPacket(`
		//0d 00 00 02 00 00 00 02    40 00 00 00 04 03 02 01    ........@.......
		//31                                                    1
		//`), cc: CapabilityFlags,
		//	}, {
		data: StringToPacket(`
10 00 00 02 00 00 00 02    40 00 00 00 07 01 05 04    ........@.......
74 65 73 74                                           test
`), cc: CapabilityFlags,
	}, {

		data: StringToPacket(`
1d 00 00 01 00 00 00 00    40 00 00 00 14 00 0f 0a    ........@.......
61 75 74 6f 63 6f 6d 6d    69 74 03 4f 46 46 02 01    autocommit.OFF..
31                                                    1
`), cc: CapabilityFlags,
	}, {

		data: StringToPacket(`
13 00 00 01 00 00 00 00    40 00 00 00 0a 01 05 04    ........@.......
74 65 73 74 02 01 31                                  test..1
`), cc: CapabilityFlags,
	}}

	for i, testCase := range testCases {
		t.Run("data packet:"+strconv.Itoa(i), func(t *testing.T) {
			data := testCase.data
			cConn.Capabilities = testCase.cc
			// parse the packet
			packetOk, err := cConn.parseOKPacket(data[4:])
			require.NoError(err)
			fmt.Printf("packetok: %v\n", packetOk)

			// write the ok packet from server
			err = sConn.writeOKPacket(packetOk)
			require.NoError(err)

			// receive the ok packer on client
			readData, err := cConn.ReadPacket()
			require.NoError(err)
			assert.Equal(data[4:], readData)
		})
	}
}

func StringToPacket(value string) (data []byte) {
	lines := strings.Split(value, "\n")
	data = make([]byte, 0, 16*len(lines))
	var values []string

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		if len(line) < 51 {
			values = strings.Split(line, " ")
		} else {
			values = strings.Split(line[:51], " ")
		}
		for _, val := range values {
			i, _ := hex.DecodeString(val)
			data = append(data, i...)
		}
	}

	return data
}

// Mostly a sanity check.
func TestEOFOrLengthEncodedIntFuzz(t *testing.T) {
	for i := 0; i < 100; i++ {
		bytes := make([]byte, rand.Intn(16)+1)
		_, err := crypto_rand.Read(bytes)
		if err != nil {
			t.Fatalf("error doing rand.Read")
		}
		bytes[0] = 0xfe

		_, _, isInt := readLenEncInt(bytes, 0)
		isEOF := isEOFPacket(bytes)
		if (isInt && isEOF) || (!isInt && !isEOF) {
			t.Fatalf("0xfe bytestring is EOF xor Int. Bytes %v", bytes)
		}
	}
}
