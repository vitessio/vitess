package mysqlconn

import (
	"bytes"
	"net"
	"reflect"
	"sync"
	"testing"

	"github.com/youtube/vitess/go/sqldb"
)

func createSocketPair(t *testing.T) (net.Listener, *Conn, *Conn) {
	// Create a listener.
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	addr := listener.Addr().String()

	// Dial a client, Accept a server.
	wg := sync.WaitGroup{}

	var clientConn net.Conn
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		clientConn, err = net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("Dial failed: %v", err)
		}
	}()

	var serverConn net.Conn
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		serverConn, err = listener.Accept()
		if err != nil {
			t.Fatalf("Accept failed: %v", err)
		}
	}()

	wg.Wait()

	// Create a Conn on both sides.
	cConn := newConn(clientConn)
	sConn := newConn(serverConn)

	return listener, sConn, cConn
}

// Write a packet on one side, read it on the other, check it's correct.
func verifyPacketComms(t *testing.T, cConn, sConn *Conn, data []byte) {
	// Have to do it in the background if it cannot be buffered.
	go func() {
		defer func() {
			if x := recover(); x != nil {
				t.Fatalf("%v", x)
			}
		}()
		if err := cConn.writePacket(data); err != nil {
			t.Fatalf("writePacket failed: %v", err)
		}
		cConn.flush()
	}()

	received, err := sConn.readPacket()
	if err != nil || !bytes.Equal(data, received) {
		t.Fatalf("readPacket failed: %v %v", received, err)
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
	data, err := cConn.readPacket()
	if err != nil || len(data) == 0 || data[0] != OKPacket {
		t.Fatalf("cConn.readPacket - OKPacket failed: %v %v", data, err)
	}
	affectedRows, lastInsertID, statusFlags, warnings, err := parseOKPacket(data)
	if err != nil || affectedRows != 12 || lastInsertID != 34 || statusFlags != 56 || warnings != 78 {
		t.Errorf("parseOKPacket returned unexpected data: %v %v %v %v %v", affectedRows, lastInsertID, statusFlags, warnings, err)
	}

	// Write OK packet with EOF header, read it, compare.
	if err := sConn.writeOKPacketWithEOFHeader(12, 34, 56, 78); err != nil {
		t.Fatalf("writeOKPacketWithEOFHeader failed: %v", err)
	}
	data, err = cConn.readPacket()
	if err != nil || len(data) == 0 || data[0] != EOFPacket {
		t.Fatalf("cConn.readPacket - OKPacket with EOF header failed: %v %v", data, err)
	}
	affectedRows, lastInsertID, statusFlags, warnings, err = parseOKPacket(data)
	if err != nil || affectedRows != 12 || lastInsertID != 34 || statusFlags != 56 || warnings != 78 {
		t.Errorf("parseOKPacket returned unexpected data: %v %v %v %v %v", affectedRows, lastInsertID, statusFlags, warnings, err)
	}

	// Write error packet, read it, compare.
	if err := sConn.writeErrorPacket(ERAccessDeniedError, SSAccessDeniedError, "access denied: %v", "reason"); err != nil {
		t.Fatalf("writeErrorPacket failed: %v", err)
	}
	data, err = cConn.readPacket()
	if err != nil || len(data) == 0 || data[0] != ErrPacket {
		t.Fatalf("cConn.readPacket - ErrorPacket failed: %v %v", data, err)
	}
	err = parseErrorPacket(data)
	if !reflect.DeepEqual(err, sqldb.NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "access denied: reason")) {
		t.Errorf("parseErrorPacket returned unexpected data: %v", err)
	}

	// Write error packet from error, read it, compare.
	if err := sConn.writeErrorPacketFromError(sqldb.NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "access denied")); err != nil {
		t.Fatalf("writeErrorPacketFromError failed: %v", err)
	}
	data, err = cConn.readPacket()
	if err != nil || len(data) == 0 || data[0] != ErrPacket {
		t.Fatalf("cConn.readPacket - ErrorPacket failed: %v %v", data, err)
	}
	err = parseErrorPacket(data)
	if !reflect.DeepEqual(err, sqldb.NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "access denied")) {
		t.Errorf("parseErrorPacket returned unexpected data: %v", err)
	}

	// Write EOF packet, read it, compare first byte. Payload is always ignored.
	if err := sConn.writeEOFPacket(0x8912, 0xabba); err != nil {
		t.Fatalf("writeEOFPacket failed: %v", err)
	}
	sConn.flush()
	data, err = cConn.readPacket()
	if err != nil || len(data) == 0 || data[0] != EOFPacket {
		t.Fatalf("cConn.readPacket - EOFPacket failed: %v %v", data, err)
	}
}
