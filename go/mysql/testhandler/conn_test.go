/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package testhandler

import (
	"bytes"
	crypto_rand "crypto/rand"
	"math/rand"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
)

func CreateSocketPair(t *testing.T) (net.Listener, *mysql.Conn, *mysql.Conn) {
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
	cConn := mysql.NewConn(clientConn)
	sConn := mysql.NewConn(serverConn)

	return listener, sConn, cConn
}

func useWritePacket(t *testing.T, cConn *mysql.Conn, data []byte) {
	defer func() {
		if x := recover(); x != nil {
			t.Fatalf("%v", x)
		}
	}()
	if err := cConn.WritePacket(data); err != nil {
		t.Fatalf("writePacket failed: %v", err)
	}
}

func useWriteEphemeralPacketBuffered(t *testing.T, cConn *mysql.Conn, data []byte) {
	defer func() {
		if x := recover(); x != nil {
			t.Fatalf("%v", x)
		}
	}()
	cConn.StartWriterBuffering()
	defer cConn.Flush()

	buf := cConn.StartEphemeralPacket(len(data))
	copy(buf, data)
	if err := cConn.WriteEphemeralPacket(); err != nil {
		t.Fatalf("writeEphemeralPacket(false) failed: %v", err)
	}
}

func useWriteEphemeralPacketDirect(t *testing.T, cConn *mysql.Conn, data []byte) {
	defer func() {
		if x := recover(); x != nil {
			t.Fatalf("%v", x)
		}
	}()

	buf := cConn.StartEphemeralPacket(len(data))
	copy(buf, data)
	if err := cConn.WriteEphemeralPacket(); err != nil {
		t.Fatalf("writeEphemeralPacket(true) failed: %v", err)
	}
}

func verifyPacketCommsSpecific(t *testing.T, cConn *mysql.Conn, data []byte,
	write func(t *testing.T, cConn *mysql.Conn, data []byte),
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
func verifyPacketComms(t *testing.T, cConn, sConn *mysql.Conn, data []byte) {
	// All three writes, with ReadPacket.
	verifyPacketCommsSpecific(t, cConn, data, useWritePacket, sConn.ReadPacket)
	verifyPacketCommsSpecific(t, cConn, data, useWriteEphemeralPacketBuffered, sConn.ReadPacket)
	verifyPacketCommsSpecific(t, cConn, data, useWriteEphemeralPacketDirect, sConn.ReadPacket)

	// All three writes, with readEphemeralPacket.
	verifyPacketCommsSpecific(t, cConn, data, useWritePacket, sConn.ReadEphemeralPacket)
	sConn.RecycleReadPacket()
	verifyPacketCommsSpecific(t, cConn, data, useWriteEphemeralPacketBuffered, sConn.ReadEphemeralPacket)
	sConn.RecycleReadPacket()
	verifyPacketCommsSpecific(t, cConn, data, useWriteEphemeralPacketDirect, sConn.ReadEphemeralPacket)
	sConn.RecycleReadPacket()

	// All three writes, with readEphemeralPacketDirect, if size allows it.
	if len(data) < mysql.MaxPacketSize {
		verifyPacketCommsSpecific(t, cConn, data, useWritePacket, sConn.ReadEphemeralPacketDirect)
		sConn.RecycleReadPacket()
		verifyPacketCommsSpecific(t, cConn, data, useWriteEphemeralPacketBuffered, sConn.ReadEphemeralPacketDirect)
		sConn.RecycleReadPacket()
		verifyPacketCommsSpecific(t, cConn, data, useWriteEphemeralPacketDirect, sConn.ReadEphemeralPacketDirect)
		sConn.RecycleReadPacket()
	}
}

func TestPackets(t *testing.T) {
	listener, sConn, cConn := CreateSocketPair(t)
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
	data = make([]byte, mysql.MaxPacketSize-1)
	data[0] = 0xab
	data[mysql.MaxPacketSize-2] = 0xef
	verifyPacketComms(t, cConn, sConn, data)

	// Exactly the limit, two packets.
	data = make([]byte, mysql.MaxPacketSize)
	data[0] = 0xab
	data[mysql.MaxPacketSize-1] = 0xef
	verifyPacketComms(t, cConn, sConn, data)

	// Over the limit, two packets.
	data = make([]byte, mysql.MaxPacketSize+1000)
	data[0] = 0xab
	data[mysql.MaxPacketSize+999] = 0xef
	verifyPacketComms(t, cConn, sConn, data)
}

func TestBasicPackets(t *testing.T) {
	listener, sConn, cConn := CreateSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	// Write OK packet, read it, compare.
	if err := sConn.WriteOKPacket(12, 34, 56, 78); err != nil {
		t.Fatalf("writeOKPacket failed: %v", err)
	}
	data, err := cConn.ReadPacket()
	if err != nil || len(data) == 0 || data[0] != mysql.OKPacket {
		t.Fatalf("cConn.ReadPacket - OKPacket failed: %v %v", data, err)
	}
	affectedRows, lastInsertID, statusFlags, warnings, err := mysql.ParseOKPacket(data)
	if err != nil || affectedRows != 12 || lastInsertID != 34 || statusFlags != 56 || warnings != 78 {
		t.Errorf("parseOKPacket returned unexpected data: %v %v %v %v %v", affectedRows, lastInsertID, statusFlags, warnings, err)
	}

	// Write OK packet with EOF header, read it, compare.
	if err := sConn.WriteOKPacketWithEOFHeader(12, 34, 56, 78); err != nil {
		t.Fatalf("writeOKPacketWithEOFHeader failed: %v", err)
	}
	data, err = cConn.ReadPacket()
	if err != nil || len(data) == 0 || !mysql.IsEOFPacket(data) {
		t.Fatalf("cConn.ReadPacket - OKPacket with EOF header failed: %v %v", data, err)
	}
	affectedRows, lastInsertID, statusFlags, warnings, err = mysql.ParseOKPacket(data)
	if err != nil || affectedRows != 12 || lastInsertID != 34 || statusFlags != 56 || warnings != 78 {
		t.Errorf("parseOKPacket returned unexpected data: %v %v %v %v %v", affectedRows, lastInsertID, statusFlags, warnings, err)
	}

	// Write error packet, read it, compare.
	if err := sConn.WriteErrorPacket(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "access denied: %v", "reason"); err != nil {
		t.Fatalf("writeErrorPacket failed: %v", err)
	}
	data, err = cConn.ReadPacket()
	if err != nil || len(data) == 0 || data[0] != mysql.ErrPacket {
		t.Fatalf("cConn.ReadPacket - ErrorPacket failed: %v %v", data, err)
	}
	err = mysql.ParseErrorPacket(data)
	if !reflect.DeepEqual(err, mysql.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "access denied: reason")) {
		t.Errorf("ParseErrorPacket returned unexpected data: %v", err)
	}

	// Write error packet from error, read it, compare.
	if err := sConn.WriteErrorPacketFromError(mysql.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "access denied")); err != nil {
		t.Fatalf("writeErrorPacketFromError failed: %v", err)
	}
	data, err = cConn.ReadPacket()
	if err != nil || len(data) == 0 || data[0] != mysql.ErrPacket {
		t.Fatalf("cConn.ReadPacket - ErrorPacket failed: %v %v", data, err)
	}
	err = mysql.ParseErrorPacket(data)
	if !reflect.DeepEqual(err, mysql.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "access denied")) {
		t.Errorf("ParseErrorPacket returned unexpected data: %v", err)
	}

	// Write EOF packet, read it, compare first byte. Payload is always ignored.
	if err := sConn.WriteEOFPacket(0x8912, 0xabba); err != nil {
		t.Fatalf("writeEOFPacket failed: %v", err)
	}
	data, err = cConn.ReadPacket()
	if err != nil || len(data) == 0 || !mysql.IsEOFPacket(data) {
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

		_, _, isInt := mysql.ReadLenEncInt(bytes, 0)
		isEOF := mysql.IsEOFPacket(bytes)
		if (isInt && isEOF) || (!isInt && !isEOF) {
			t.Fatalf("0xfe bytestring is EOF xor Int. Bytes %v", bytes)
		}
	}
}
