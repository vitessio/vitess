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
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
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
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	// Write OK packet, read it, compare.
	if err := sConn.writeOKPacket(12, 34, 56, 78); err != nil {
		t.Fatalf("writeOKPacket failed: %v", err)
	}
	data, err := cConn.ReadPacket()
	if err != nil || len(data) == 0 || data[0] != OKPacket {
		t.Fatalf("cConn.ReadPacket - OKPacket failed: %v %v", data, err)
	}
	affectedRows, lastInsertID, statusFlags, warnings, err := parseOKPacket(data)
	if err != nil || affectedRows != 12 || lastInsertID != 34 || statusFlags != 56 || warnings != 78 {
		t.Errorf("parseOKPacket returned unexpected data: %v %v %v %v %v", affectedRows, lastInsertID, statusFlags, warnings, err)
	}

	// Write OK packet with EOF header, read it, compare.
	if err := sConn.writeOKPacketWithEOFHeader(12, 34, 56, 78); err != nil {
		t.Fatalf("writeOKPacketWithEOFHeader failed: %v", err)
	}
	data, err = cConn.ReadPacket()
	if err != nil || len(data) == 0 || !isEOFPacket(data) {
		t.Fatalf("cConn.ReadPacket - OKPacket with EOF header failed: %v %v", data, err)
	}
	affectedRows, lastInsertID, statusFlags, warnings, err = parseOKPacket(data)
	if err != nil || affectedRows != 12 || lastInsertID != 34 || statusFlags != 56 || warnings != 78 {
		t.Errorf("parseOKPacket returned unexpected data: %v %v %v %v %v", affectedRows, lastInsertID, statusFlags, warnings, err)
	}

	// Write error packet, read it, compare.
	if err := sConn.writeErrorPacket(ERAccessDeniedError, SSAccessDeniedError, "access denied: %v", "reason"); err != nil {
		t.Fatalf("writeErrorPacket failed: %v", err)
	}
	data, err = cConn.ReadPacket()
	if err != nil || len(data) == 0 || data[0] != ErrPacket {
		t.Fatalf("cConn.ReadPacket - ErrorPacket failed: %v %v", data, err)
	}
	err = ParseErrorPacket(data)
	if !reflect.DeepEqual(err, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "access denied: reason")) {
		t.Errorf("ParseErrorPacket returned unexpected data: %v", err)
	}

	// Write error packet from error, read it, compare.
	if err := sConn.writeErrorPacketFromError(NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "access denied")); err != nil {
		t.Fatalf("writeErrorPacketFromError failed: %v", err)
	}
	data, err = cConn.ReadPacket()
	if err != nil || len(data) == 0 || data[0] != ErrPacket {
		t.Fatalf("cConn.ReadPacket - ErrorPacket failed: %v %v", data, err)
	}
	err = ParseErrorPacket(data)
	if !reflect.DeepEqual(err, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "access denied")) {
		t.Errorf("ParseErrorPacket returned unexpected data: %v", err)
	}

	// Write EOF packet, read it, compare first byte. Payload is always ignored.
	if err := sConn.writeEOFPacket(0x8912, 0xabba); err != nil {
		t.Fatalf("writeEOFPacket failed: %v", err)
	}
	data, err = cConn.ReadPacket()
	if err != nil || len(data) == 0 || !isEOFPacket(data) {
		t.Fatalf("cConn.ReadPacket - EOFPacket failed: %v %v", data, err)
	}
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

	err := cConn.WriteComQuery("select 1;select 2")
	require.NoError(t, err)

	// this handler will return an error on the first run, and fail the test if it's run more times
	handler := &singleRun{t: t, err: fmt.Errorf("execution failed")}
	res := sConn.handleNextCommand(handler)
	require.True(t, res, res, "we should not break the connection because of execution errors")

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

	err := cConn.writeComInitDB("database")
	require.NoError(t, err)

	handler := &singleRun{t: t, err: fmt.Errorf("execution failed")}
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
	handler := &singleRun{t: t, err: fmt.Errorf(string(errorString))}
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
	handler := &singleRun{t: t, err: fmt.Errorf("not used")}
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
	handler := &singleRun{t: t, err: fmt.Errorf("not used")}
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
	handler := &singleRun{t: t, err: fmt.Errorf("not used")}
	res := sConn.handleNextCommand(handler)
	require.False(t, res, "we should beak the connection in case of error writing error packet")
}

type singleRun struct {
	hasRun bool
	t      *testing.T
	err    error
}

func (h *singleRun) NewConnection(*Conn) {
	panic("implement me")
}

func (h *singleRun) ConnectionClosed(*Conn) {
	panic("implement me")
}

func (h *singleRun) ComQuery(*Conn, string, func(*sqltypes.Result) error) error {
	if h.hasRun {
		debug.PrintStack()
		h.t.Fatal("don't do this!")
	}
	h.hasRun = true
	return h.err
}

func (h *singleRun) ComPrepare(*Conn, string, map[string]*querypb.BindVariable) ([]*querypb.Field, error) {
	panic("implement me")
}

func (h *singleRun) ComStmtExecute(*Conn, *PrepareData, func(*sqltypes.Result) error) error {
	panic("implement me")
}

func (h *singleRun) WarningCount(*Conn) uint16 {
	return 0
}

func (h *singleRun) ComResetConnection(*Conn) {
	panic("implement me")
}

var _ Handler = (*singleRun)(nil)

type testConn struct {
	writeToPass []bool
	pos         int
	queryPacket []byte
}

func (t testConn) Read(b []byte) (n int, err error) {
	for j, i := range t.queryPacket {
		b[j] = i
	}
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
	for i := 0; i < 1000; i++ {
		startGoRoutine(ctx, t, randSeq(i))
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
		require.True(t, ok, "oh noes")

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
	t              *testing.T
	expParamCounts int
	expQuery       string
	expStmtID      int
}

func (t testRun) ComStmtExecute(c *Conn, prepare *PrepareData, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

func (t testRun) NewConnection(c *Conn) {
	panic("implement me")
}

func (t testRun) ConnectionClosed(c *Conn) {
	panic("implement me")
}

func (t testRun) ComQuery(c *Conn, query string, callback func(*sqltypes.Result) error) error {
	panic("implement me")
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

func (t testRun) ComResetConnection(c *Conn) {
	panic("implement me")
}

var _ Handler = (*testRun)(nil)
