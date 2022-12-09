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
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	testCases := []struct {
		data        string
		cc          uint32
		expectedErr string
	}{{
		data: `
00000000  00 00 00 02 00 00 00                              |.......|`,
		cc: CapabilityClientProtocol41,
	}, {
		data: `
00000000  00 00 00 02 00                                    |.....|`,
		cc:          CapabilityClientTransactions,
		expectedErr: "invalid OK packet warnings: &{[0 0 0 2 0] 0}",
	}, {
		data: `
00000000  00 00 00 02 40 00 00 00  2a 03 28 00 26 66 32 37  |....@...*.(.&f27|
00000010  66 36 39 37 31 2d 30 33  65 37 2d 31 31 65 62 2d  |f6971-03e7-11eb-|
00000020  38 35 63 35 2d 39 38 61  66 36 35 61 36 64 63 34  |85c5-98af65a6dc4|
00000030  61 3a 32                                          |a:2|`,
		cc: CapabilityClientProtocol41 | CapabilityClientTransactions | CapabilityClientSessionTrack,
	}, {
		data:        `00000000  00 00 00 02 40 00 00 00  07 01 05 04 74 65 73 74  |....@.......test|`,
		cc:          CapabilityClientProtocol41 | CapabilityClientTransactions | CapabilityClientSessionTrack,
		expectedErr: "invalid OK packet session state change type: 1",
	}, {
		data: `
00000000  00 00 00 00 40 00 00 00  14 00 0f 0a 61 75 74 6f  |....@.......auto|
00000010  63 6f 6d 6d 69 74 03 4f  46 46 02 01 31           |commit.OFF..1|`,
		cc:          CapabilityClientProtocol41 | CapabilityClientTransactions | CapabilityClientSessionTrack,
		expectedErr: "invalid OK packet session state change type: 0",
	}, {
		data: `
00000000  00 00 00 00 40 00 00 00  0a 01 05 04 74 65 73 74  |....@.......test|
00000010  02 01 31                                          |..1|`,
		cc:          CapabilityClientProtocol41 | CapabilityClientTransactions | CapabilityClientSessionTrack,
		expectedErr: "invalid OK packet session state change type: 1",
	}}

	for i, testCase := range testCases {
		t.Run("data packet:"+strconv.Itoa(i), func(t *testing.T) {
			data := ReadHexDump(testCase.data)

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

			// receive the ok packer on client
			readData, err := cConn.ReadPacket()
			require.NoError(t, err, "failed to read packet that was written")
			assert.Equal(t, data, readData, "data read and written does not match")
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
		s := line[8:indexOfPipe]
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
	if strings.Contains(query, "close before rows read") {
		c.writeFields(selectRowsResult)
		// We want to close the connection after the fields are written
		// and read on the client. So we sleep for 100 milliseconds
		time.Sleep(100 * time.Millisecond)
		c.Close()
		return nil
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
