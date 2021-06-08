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
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

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

	return listener, sConn, cConn
}

func useWritePacket(t *testing.T, cConn *Conn, data []byte) {
	defer func() {
		if x := recover(); x != nil {
			t.Fatalf("%v", x)
		}
	}()
	if err := cConn.writePacket(data); err != nil {
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

	buf := cConn.startEphemeralPacket(len(data))
	copy(buf, data)
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

	buf := cConn.startEphemeralPacket(len(data))
	copy(buf, data)
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

func TestPrepareAndExecute(t *testing.T) {
	packetDataArray := []struct {
		bvType []byte
		value  []byte
	}{
		{bvType: []byte{0x0f, 0x80}, value: []byte{0x03, 0x66, 0x6f, 0x6f}},
		{bvType: []byte{0x0f, 0x80}, value: []byte{0x03, 0x66, 0x6f, 0x6f}},
		{bvType: []byte{0x0f, 0x80}, value: []byte{0x03, 0x66, 0x6f, 0x6f}},
		{bvType: []byte{0x0f, 0x80}, value: []byte{0x03, 0x66, 0x6f, 0x6f}},
		{bvType: []byte{0x0f, 0x80}, value: []byte{0x03, 0x66, 0x6f, 0x6f}},
	}

	wg := sync.WaitGroup{}
	wg.Add(len(packetDataArray))

	for i, packetData := range packetDataArray {
		packetData := packetData
		go func(i int) {
			execID := uint32(i + 1)
			execIDBinary := make([]byte, 4)
			binary.LittleEndian.PutUint32(execIDBinary, execID)

			packet := []byte{0x21, 0x00, 0x00, 0x00, ComStmtExecute}
			packet = append(packet, execIDBinary...)        // append exec stmt ID
			packet = append(packet, 0x74)                   // append flag, CURSOR_TYPE_NO_CURSOR
			packet = append(packet, 0x01, 0x00, 0x00, 0x00) // append iteration count, always 1
			packet = append(packet, 0x76)                   // append bitMap
			packet = append(packet, 0x01)                   // append params bound flag
			packet = append(packet, packetData.bvType...)   // append bind variable type
			packet = append(packet, packetData.value...)    // append bind variable values

			conn := testConn{
				writeToPass: []bool{false},
				pos:         -1,
				queryPacket: packet,
			}
			sConn := newConn(conn)
			sConn.PrepareData = map[uint32]*PrepareData{execID: {
				StatementID: execID,
				ParamsCount: 1,
				ParamsType:  []int32{int32(querypb.Type_VARBINARY)},
				PrepareStmt: "select * from table where name = ?",
				ColumnNames: []string{"name"},
			}}

			handler := &testRun{
				t:   t,
				err: fmt.Errorf("not used"),
				expectedBindVarsValues: map[string][]byte{
					"v1": packetData.value[1:],
				},
				done: func() {
					wg.Done()
				},
			}
			defer handler.done()
			err := sConn.handleNextCommand(handler)
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()
}

// real stmt execute, then do multiple with multiple BV values
// check in the handler if all values are clean and correct

type testRun struct {
	t                      *testing.T
	err                    error
	expectedBindVarsValues map[string][]byte
	done                   func()
}

func (t testRun) NewConnection(c *Conn) {
	panic("implement me")
}

func (t testRun) ConnectionClosed(c *Conn) {
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

func (t testRun) ComPrepare(c *Conn, query string) ([]*querypb.Field, error) {
	panic("implement me")
}

func (t testRun) ComStmtExecute(c *Conn, prepare *PrepareData, bindVars map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) error {
	for key, val := range t.expectedBindVarsValues {
		require.Equal(t.t, val, bindVars[key].Value, "execute statement bind variables values are different than what's expected")
	}
	return nil
}

func (t testRun) WarningCount(c *Conn) uint16 {
	return 0
}

func (t testRun) ComResetConnection(c *Conn) {
	panic("implement me")
}

var _ Handler = (*testRun)(nil)

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
