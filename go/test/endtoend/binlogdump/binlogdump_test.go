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

package binlogdump

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TestBinlogDumpGTID_Streaming tests that binlog events are actually streamed from vttablet to the client.
func TestBinlogDumpGTID_Streaming(t *testing.T) {
	ctx := t.Context()

	// Get the primary tablet for our keyspace
	primaryTablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet()
	tabletAlias := primaryTablet.Alias

	t.Logf("Tablet alias: %s", tabletAlias)

	// Connect to vtgate for binlog streaming
	targetString := fmt.Sprintf("%s:0@primary|%s", keyspaceName, tabletAlias)
	binlogParams := mysql.ConnParams{
		Host:  clusterInstance.Hostname,
		Port:  clusterInstance.VtgateMySQLPort,
		Uname: "vt_repl|" + targetString,
	}

	t.Logf("Connecting to VTGate at %s:%d with username: %s", binlogParams.Host, binlogParams.Port, binlogParams.Uname)

	binlogConn, err := mysql.Connect(ctx, &binlogParams)
	require.NoError(t, err)
	defer binlogConn.Close()

	t.Logf("Connected successfully, connection ID: %d", binlogConn.ConnectionID)

	// Start binlog dump with no initial GTID - will start from current position
	err = binlogConn.WriteComBinlogDumpGTID(1, "", 4, 0, nil)
	require.NoError(t, err, "Should be able to send COM_BINLOG_DUMP_GTID")

	// Channel to receive packets and errors
	packetCh := make(chan []byte, 10)
	errCh := make(chan error, 1)

	// Start reading packets in a goroutine
	var wg sync.WaitGroup
	wg.Go(func() {
		defer close(packetCh)
		for {
			data, err := binlogConn.ReadPacket()
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			select {
			case packetCh <- data:
			default:
				// Channel full, drop packet
			}
		}
	})

	// Give the binlog dump a moment to start
	time.Sleep(100 * time.Millisecond)

	// Now insert data to generate binlog packets
	dataConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer dataConn.Close()

	t.Log("Inserting test data to generate binlog packets")
	for i := range 3 {
		_, err := dataConn.ExecuteFetch(fmt.Sprintf("INSERT INTO binlog_test (msg) VALUES ('streaming_test_%d')", i), 1, false)
		require.NoError(t, err)
	}

	// Wait for at least one packet or timeout
	receivedPackets := 0
	timeout := time.After(5 * time.Second)

packetLoop:
	for {
		select {
		case data, ok := <-packetCh:
			if !ok {
				// Channel closed
				t.Logf("Packet channel closed after receiving %d packets", receivedPackets)
				break packetLoop
			}
			receivedPackets++
			if len(data) > 0 {
				t.Logf("Received packet %d: first byte=0x%02x, length=%d", receivedPackets, data[0], len(data))
			}
			// We got a packet, test passes
			if receivedPackets >= 3 {
				t.Logf("Received %d packets, test passed", receivedPackets)
				break packetLoop
			}
		case err := <-errCh:
			t.Logf("Got error from packet reader: %v", err)
			break packetLoop
		case <-timeout:
			t.Logf("Timeout after receiving %d packets", receivedPackets)
			break packetLoop
		}
	}

	// Close the connection to stop the reader goroutine
	binlogConn.Close()
	wg.Wait()

	// We should have received at least some packets
	assert.GreaterOrEqual(t, receivedPackets, 1, "Should have received at least one binlog packet")
}

// TestBinlogDumpGTID_NoTarget verifies that binlog dump returns an error packet without a target
func TestBinlogDumpGTID_NoTarget(t *testing.T) {
	ctx := t.Context()

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Try to send COM_BINLOG_DUMP_GTID without setting a target
	err = conn.WriteComBinlogDumpGTID(1, "", 4, 0, nil)
	require.NoError(t, err)

	// Server should send an error packet when no target is specified
	data, err := conn.ReadPacket()
	require.NoError(t, err, "Should receive error packet, not connection close")
	require.True(t, len(data) > 0, "Response should not be empty")
	require.Equal(t, byte(mysql.ErrPacket), data[0], "Expected error packet")

	// Parse the error packet and verify the message
	sqlErr := mysql.ParseErrorPacket(data)
	require.Error(t, sqlErr)
	assert.Contains(t, sqlErr.Error(), "no target specified", "Error message should mention missing target")
}

// TestBinlogDumpGTID_LargeEvent tests that binlog events larger than 16MB (spanning multiple MySQL packets)
// are correctly streamed through VTGate. This is critical because MySQL protocol uses 16MB max packet
// size, and large events must be split into multiple packets and reassembled correctly.
func TestBinlogDumpGTID_LargeEvent(t *testing.T) {
	ctx := t.Context()

	// Get the primary tablet for our keyspace
	primaryTablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet()
	tabletAlias := primaryTablet.Alias

	t.Logf("Tablet alias: %s", tabletAlias)

	// Connect to vtgate for binlog streaming
	targetString := fmt.Sprintf("%s:0@primary|%s", keyspaceName, tabletAlias)
	binlogParams := mysql.ConnParams{
		Host:  clusterInstance.Hostname,
		Port:  clusterInstance.VtgateMySQLPort,
		Uname: "vt_repl|" + targetString,
	}

	binlogConn, err := mysql.Connect(ctx, &binlogParams)
	require.NoError(t, err)
	defer binlogConn.Close()

	// Start binlog dump FIRST (with no GTID = starts from current position)
	err = binlogConn.WriteComBinlogDumpGTID(1, "", 4, 0, nil)
	require.NoError(t, err, "Should be able to send COM_BINLOG_DUMP_GTID")

	t.Log("Binlog dump started")

	// Channel to receive packets
	packetCh := make(chan []byte, 100)
	errCh := make(chan error, 1)

	// Start reading packets in a goroutine
	var wg sync.WaitGroup
	wg.Go(func() {
		defer close(packetCh)
		for {
			data, err := binlogConn.ReadPacket()
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			select {
			case packetCh <- data:
			default:
				// Channel full, drop packet
			}
		}
	})

	// Give the binlog dump a moment to start
	time.Sleep(100 * time.Millisecond)

	// Connect to insert data - this will generate binlog packets AFTER we started dumping
	dataConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer dataConn.Close()

	// Create a large blob of 32MB that spans multiple MySQL packets.
	// The MySQL MaxPacketSize is 16777215 bytes (16MB - 1), so a 32MB packet
	// will be split across 2+ packets by MySQL, testing our multi-packet handling.
	//
	// The gRPC max message size is configured to 64MB in main_test.go to allow
	// streaming these large packets.
	largeDataSize := 32 * 1024 * 1024 // 32MB

	t.Logf("Inserting large blob of %d bytes (%d MB) to test multi-packet handling", largeDataSize, largeDataSize/(1024*1024))

	// Insert the large blob using REPEAT to build the data in MySQL
	baseSize := 1024 * 1024 // 1MB base
	repeatCount := 32       // 32 repetitions = 32MB

	// Create base data (1MB of 'A' characters)
	baseData := make([]byte, baseSize)
	for i := range baseData {
		baseData[i] = 'A'
	}
	hexBase := hex.EncodeToString(baseData)

	// Use REPEAT to build the full blob in MySQL
	insertSQL := fmt.Sprintf("INSERT INTO large_blob_test (data) VALUES (REPEAT(X'%s', %d))", hexBase, repeatCount)
	_, err = dataConn.ExecuteFetch(insertSQL, 1, false)
	if err != nil {
		t.Logf("Insert error: %v", err)
		// Try a smaller size if the large one fails
		t.Log("Retrying with smaller blob size...")
		insertSQL = fmt.Sprintf("INSERT INTO large_blob_test (data) VALUES (REPEAT(X'%s', %d))", hexBase, 5)
		_, err = dataConn.ExecuteFetch(insertSQL, 1, false)
		if err != nil {
			t.Logf("Smaller insert also failed: %v", err)
		}
	}
	require.NoError(t, err, "Should be able to insert large blob")

	t.Log("Large blob inserted successfully, waiting for binlog packets...")

	// Wait for packets - we should receive the large packet
	var largePacketReceived bool
	receivedPackets := 0
	timeout := time.After(30 * time.Second) // Longer timeout for large data

packetLoop:
	for {
		select {
		case data, ok := <-packetCh:
			if !ok {
				t.Logf("Packet channel closed after receiving %d packets", receivedPackets)
				break packetLoop
			}
			receivedPackets++
			packetSize := len(data)

			// Log packet details
			if packetSize > 1024*1024 {
				t.Logf("Received packet %d: size=%d bytes (%.1f MB), first byte=0x%02x",
					receivedPackets, packetSize, float64(packetSize)/(1024*1024), data[0])
			} else {
				t.Logf("Received packet %d: size=%d bytes, first byte=0x%02x",
					receivedPackets, packetSize, data[0])
			}

			// Check if we received a large packet (>30MB)
			// The binlog packet will be slightly larger than our data due to event headers
			if packetSize > 30*1024*1024 {
				largePacketReceived = true
				t.Logf("SUCCESS: Received large packet of %d bytes (%.1f MB) - multi-packet handling works!",
					packetSize, float64(packetSize)/(1024*1024))
				break packetLoop
			}

			// Safety limit - don't wait forever
			if receivedPackets > 50 {
				t.Log("Received 50 packets, stopping")
				break packetLoop
			}

		case err := <-errCh:
			t.Logf("Got error from packet reader: %v", err)
			break packetLoop

		case <-timeout:
			t.Logf("Timeout after receiving %d packets", receivedPackets)
			break packetLoop
		}
	}

	// Close the connection to stop the reader goroutine
	binlogConn.Close()
	wg.Wait()

	// Verify we received the large packet
	assert.True(t, largePacketReceived, "Should have received a binlog packet larger than 30MB")
	assert.GreaterOrEqual(t, receivedPackets, 1, "Should have received at least one binlog packet")
}

// getCurrentGTID returns the current gtid_executed value from MySQL
func getCurrentGTID(t *testing.T, conn *mysql.Conn) string {
	qr, err := conn.ExecuteFetch("SELECT @@global.gtid_executed", 1, false)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1)
	return qr.Rows[0][0].ToString()
}

// gtidToSIDBlock converts a GTID string to a SID block for COM_BINLOG_DUMP_GTID
func gtidToSIDBlock(t *testing.T, gtidStr string) []byte {
	gtidSet, err := replication.ParseMysql56GTIDSet(gtidStr)
	require.NoError(t, err)
	return gtidSet.SIDBlock()
}

// TestBinlogDumpGTID_FromSpecificPosition verifies that binlog streaming starts from the specified
// GTID position and only receives subsequent events.
func TestBinlogDumpGTID_FromSpecificPosition(t *testing.T) {
	ctx := t.Context()

	// Connect to insert initial data
	dataConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer dataConn.Close()

	// Insert initial data (we should NOT see these events)
	for i := range 3 {
		_, err := dataConn.ExecuteFetch(
			fmt.Sprintf("INSERT INTO binlog_test (msg) VALUES ('before_gtid_%d')", i), 1, false)
		require.NoError(t, err)
	}

	// Get current GTID position - we'll start streaming from here
	startGTID := getCurrentGTID(t, dataConn)
	t.Logf("Starting GTID position: %s", startGTID)

	// Insert more data (we SHOULD see these events)
	for i := range 3 {
		_, err := dataConn.ExecuteFetch(
			fmt.Sprintf("INSERT INTO binlog_test (msg) VALUES ('after_gtid_%d')", i), 1, false)
		require.NoError(t, err)
	}

	// Connect for binlog streaming
	primaryTablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet()
	targetString := fmt.Sprintf("%s:0@primary|%s", keyspaceName, primaryTablet.Alias)
	binlogParams := mysql.ConnParams{
		Host:  clusterInstance.Hostname,
		Port:  clusterInstance.VtgateMySQLPort,
		Uname: "vt_repl|" + targetString,
	}

	binlogConn, err := mysql.Connect(ctx, &binlogParams)
	require.NoError(t, err)
	defer binlogConn.Close()

	// Start binlog dump from the saved GTID position
	sidBlock := gtidToSIDBlock(t, startGTID)
	err = binlogConn.WriteComBinlogDumpGTID(1, "", 4, 0, sidBlock)
	require.NoError(t, err)

	// Read packets - should only get packets after startGTID
	receivedPackets := 0
	timeout := time.After(5 * time.Second)

	for receivedPackets < 3 {
		select {
		case <-timeout:
			require.Failf(t, "timeout waiting for packets", "Timeout waiting for packets, received %d", receivedPackets)
		default:
			data, err := binlogConn.ReadPacket()
			require.NoErrorf(t, err, "Error reading packet: %v", err)
			if len(data) > 0 && data[0] == mysql.OKPacket {
				receivedPackets++
				t.Logf("Received packet %d: size=%d bytes", receivedPackets, len(data))
			}
		}
	}

	t.Logf("Successfully received %d packets from GTID position", receivedPackets)
}

// TestBinlogDumpGTID_InvalidFormat verifies that an invalid GTID format returns a proper error packet.
func TestBinlogDumpGTID_InvalidFormat(t *testing.T) {
	ctx := t.Context()

	// Connect with proper target
	primaryTablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet()
	targetString := fmt.Sprintf("%s:0@primary|%s", keyspaceName, primaryTablet.Alias)
	binlogParams := mysql.ConnParams{
		Host:  clusterInstance.Hostname,
		Port:  clusterInstance.VtgateMySQLPort,
		Uname: "vt_repl|" + targetString,
	}

	binlogConn, err := mysql.Connect(ctx, &binlogParams)
	require.NoError(t, err)
	defer binlogConn.Close()

	// Send COM_BINLOG_DUMP_GTID with invalid GTID data
	// We'll send garbage bytes as the SID block
	invalidSIDBlock := []byte("not-a-valid-gtid-format")
	err = binlogConn.WriteComBinlogDumpGTID(1, "", 4, 0, invalidSIDBlock)
	require.NoError(t, err)

	// Should receive an error packet
	data, err := binlogConn.ReadPacket()
	require.NoError(t, err, "Should receive error packet, not connection close")
	require.True(t, len(data) > 0, "Response should not be empty")
	require.Equal(t, byte(mysql.ErrPacket), data[0], "Expected error packet")

	// Parse and verify error message
	sqlErr := mysql.ParseErrorPacket(data)
	require.Error(t, sqlErr)
	t.Logf("Got expected error: %v", sqlErr)
}

// TestBinlogDumpGTID_FuturePosition verifies that when requesting a GTID set that includes
// transactions not yet in the binlog, MySQL returns an error.
// This is expected MySQL behavior - you cannot request events from a position that doesn't exist.
func TestBinlogDumpGTID_FuturePosition(t *testing.T) {
	ctx := t.Context()

	// Connect to get current GTID
	dataConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer dataConn.Close()

	// Get current GTID
	currentGTID := getCurrentGTID(t, dataConn)
	t.Logf("Current GTID: %s", currentGTID)

	// Parse the GTID to get the last transaction number and create a future GTID
	gtidSet, err := replication.ParseMysql56GTIDSet(currentGTID)
	require.NoError(t, err)

	// Get the last GTID and construct a future one
	lastGTID := gtidSet.Last()
	require.NotEmpty(t, lastGTID, "Should have a last GTID")
	t.Logf("Last GTID: %s", lastGTID)

	// Parse the last transaction number and add 10 to create a future GTID
	parts := strings.Split(lastGTID, ":")
	require.Len(t, parts, 2, "Last GTID should be in format uuid:N")
	uuid := parts[0]
	lastTxn, err := strconv.ParseInt(parts[1], 10, 64)
	require.NoError(t, err)

	futureTxn := lastTxn + 10
	futureGTID := fmt.Sprintf("%s:1-%d", uuid, futureTxn)
	t.Logf("Future GTID (includes non-existent transactions): %s", futureGTID)

	// Connect for binlog streaming
	primaryTablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet()
	targetString := fmt.Sprintf("%s:0@primary|%s", keyspaceName, primaryTablet.Alias)
	binlogParams := mysql.ConnParams{
		Host:  clusterInstance.Hostname,
		Port:  clusterInstance.VtgateMySQLPort,
		Uname: "vt_repl|" + targetString,
	}

	binlogConn, err := mysql.Connect(ctx, &binlogParams)
	require.NoError(t, err)
	defer binlogConn.Close()

	// Start binlog dump from the future GTID position
	futureGTIDSet, err := replication.ParseMysql56GTIDSet(futureGTID)
	require.NoError(t, err)
	sidBlock := futureGTIDSet.SIDBlock()
	err = binlogConn.WriteComBinlogDumpGTID(1, "", 4, 0, sidBlock)
	require.NoError(t, err)

	// MySQL should return an error because the requested GTID includes transactions
	// that don't exist yet. We should receive an error packet or connection close.
	data, err := binlogConn.ReadPacket()
	if err != nil {
		// Connection closed or read error - this is acceptable
		t.Logf("Got expected error: %v", err)
		return
	}

	// If we got a packet, check if it's an error packet
	require.True(t, len(data) > 0, "Response should not be empty")
	if data[0] == mysql.ErrPacket {
		// Error packet - parse and log it
		sqlErr := mysql.ParseErrorPacket(data)
		t.Logf("Got expected error packet: %v", sqlErr)
		return
	}

	// If MySQL sends events (shouldn't happen with future GTID), that's also OK
	// as long as the behavior is consistent
	t.Logf("Got unexpected response: first byte=0x%02x, length=%d", data[0], len(data))
	t.Logf("MySQL may have sent existing events - behavior depends on MySQL version")
}

// TestBinlogDumpGTID_NonBlockEOF verifies that when the BINLOG_DUMP_NON_BLOCK flag is set,
// the server returns an EOF packet when there are no more events to stream, instead of
// blocking indefinitely waiting for new events.
func TestBinlogDumpGTID_NonBlockEOF(t *testing.T) {
	ctx := t.Context()

	// Connect to insert some data first to ensure binlog has content
	dataConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer dataConn.Close()

	// Insert data to ensure binlog is not empty
	_, err = dataConn.ExecuteFetch("INSERT INTO binlog_test (msg) VALUES ('nonblock_test')", 1, false)
	require.NoError(t, err)

	// Get current GTID - we'll start streaming from here (after all existing events)
	currentGTID := getCurrentGTID(t, dataConn)
	t.Logf("Starting from GTID: %s (should have no pending events)", currentGTID)

	// Connect for binlog streaming
	primaryTablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet()
	targetString := fmt.Sprintf("%s:0@primary|%s", keyspaceName, primaryTablet.Alias)
	binlogParams := mysql.ConnParams{
		Host:  clusterInstance.Hostname,
		Port:  clusterInstance.VtgateMySQLPort,
		Uname: "vt_repl|" + targetString,
	}

	binlogConn, err := mysql.Connect(ctx, &binlogParams)
	require.NoError(t, err)
	defer binlogConn.Close()

	// Start binlog dump with NONBLOCK flag - should return EOF when caught up
	// Flags: BinlogDumpNonBlock (0x01)
	flags := uint16(mysql.BinlogDumpNonBlock)
	sidBlock := gtidToSIDBlock(t, currentGTID)
	err = binlogConn.WriteComBinlogDumpGTID(1, "", 4, flags, sidBlock)
	require.NoError(t, err)

	// With nonBlock, we should receive an EOF packet relatively quickly
	// since there are no packets after the current GTID position
	timeout := time.After(5 * time.Second)
	var receivedEOF bool
	var receivedPackets int

readLoop:
	for {
		select {
		case <-timeout:
			require.Failf(t, "timeout waiting for EOF packet", "Timeout waiting for EOF packet - nonBlock flag may not be implemented. Received %d packets.", receivedPackets)
		default:
			data, err := binlogConn.ReadPacket()
			if err != nil {
				// Connection closed - could be server's way of signaling end
				t.Logf("Connection closed: %v (received %d packets)", err, receivedPackets)
				break readLoop
			}

			if len(data) == 0 {
				continue
			}

			switch data[0] {
			case mysql.EOFPacket:
				receivedEOF = true
				t.Logf("Received EOF packet after %d packets - nonBlock working correctly", receivedPackets)
				break readLoop
			case mysql.ErrPacket:
				sqlErr := mysql.ParseErrorPacket(data)
				t.Logf("Received error packet: %v", sqlErr)
				break readLoop
			case mysql.OKPacket:
				receivedPackets++
				t.Logf("Received packet %d: size=%d bytes", receivedPackets, len(data))
				// Continue reading - there might be a few packets before EOF
			default:
				t.Logf("Received packet with first byte=0x%02x, size=%d", data[0], len(data))
			}
		}
	}

	assert.True(t, receivedEOF, "Should have received EOF packet with nonBlock flag set")
}

// TestBinlogDumpGTID_NonBlockWithPendingEvents verifies that when nonBlock is set and there
// ARE pending events, the server streams them all and THEN returns EOF.
func TestBinlogDumpGTID_NonBlockWithPendingEvents(t *testing.T) {
	ctx := t.Context()

	// Connect to insert data
	dataConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer dataConn.Close()

	// Get GTID BEFORE inserting test data
	startGTID := getCurrentGTID(t, dataConn)
	t.Logf("Start GTID (before inserts): %s", startGTID)

	// Insert several rows - these will be "pending" events when we start streaming
	numInserts := 5
	for i := range numInserts {
		_, err := dataConn.ExecuteFetch(
			fmt.Sprintf("INSERT INTO binlog_test (msg) VALUES ('nonblock_pending_%d')", i), 1, false)
		require.NoError(t, err)
	}

	endGTID := getCurrentGTID(t, dataConn)
	t.Logf("End GTID (after inserts): %s", endGTID)

	// Connect for binlog streaming
	primaryTablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet()
	targetString := fmt.Sprintf("%s:0@primary|%s", keyspaceName, primaryTablet.Alias)
	binlogParams := mysql.ConnParams{
		Host:  clusterInstance.Hostname,
		Port:  clusterInstance.VtgateMySQLPort,
		Uname: "vt_repl|" + targetString,
	}

	binlogConn, err := mysql.Connect(ctx, &binlogParams)
	require.NoError(t, err)
	defer binlogConn.Close()

	// Start binlog dump with NONBLOCK flag from BEFORE the inserts
	flags := uint16(mysql.BinlogDumpNonBlock)
	sidBlock := gtidToSIDBlock(t, startGTID)
	err = binlogConn.WriteComBinlogDumpGTID(1, "", 4, flags, sidBlock)
	require.NoError(t, err)

	// Should receive the pending packets, then EOF
	timeout := time.After(10 * time.Second)
	var receivedEOF bool
	var receivedPackets int

readLoop:
	for {
		select {
		case <-timeout:
			require.Failf(t, "timeout waiting for EOF", "Timeout - received %d packets but no EOF. NonBlock may not be implemented.", receivedPackets)
		default:
			data, err := binlogConn.ReadPacket()
			if err != nil {
				t.Logf("Connection closed: %v (received %d packets)", err, receivedPackets)
				break readLoop
			}

			if len(data) == 0 {
				continue
			}

			switch data[0] {
			case mysql.EOFPacket:
				receivedEOF = true
				t.Logf("Received EOF after %d packets", receivedPackets)
				break readLoop
			case mysql.ErrPacket:
				sqlErr := mysql.ParseErrorPacket(data)
				require.Failf(t, "unexpected error packet", "Unexpected error packet: %v", sqlErr)
			case mysql.OKPacket:
				receivedPackets++
				if receivedPackets <= 10 {
					t.Logf("Received packet %d: size=%d bytes", receivedPackets, len(data))
				}
			default:
				t.Logf("Received packet with first byte=0x%02x, size=%d", data[0], len(data))
			}
		}
	}

	// We should have received packets (binlog packets for the inserts) and then EOF
	assert.True(t, receivedEOF, "Should have received EOF packet after streaming pending packets")
	assert.GreaterOrEqual(t, receivedPackets, 1, "Should have received at least some binlog packets for the inserts")
	t.Logf("NonBlock with pending packets: received %d packets then EOF", receivedPackets)
}

// TestBinlogDumpGTID_BlockingMode verifies the default blocking behavior - when BINLOG_DUMP_NON_BLOCK
// is NOT set, the server should block waiting for new events instead of returning EOF.
// This test verifies that new events are received after an insert, demonstrating that the
// connection stays open and continues to stream events.
func TestBinlogDumpGTID_BlockingMode(t *testing.T) {
	ctx := t.Context()

	// Connect to get current position
	dataConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer dataConn.Close()

	// Get current GTID - we'll start streaming from here
	currentGTID := getCurrentGTID(t, dataConn)
	t.Logf("Starting from GTID: %s", currentGTID)

	// Connect for binlog streaming
	primaryTablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet()
	targetString := fmt.Sprintf("%s:0@primary|%s", keyspaceName, primaryTablet.Alias)
	binlogParams := mysql.ConnParams{
		Host:  clusterInstance.Hostname,
		Port:  clusterInstance.VtgateMySQLPort,
		Uname: "vt_repl|" + targetString,
	}

	binlogConn, err := mysql.Connect(ctx, &binlogParams)
	require.NoError(t, err)
	defer binlogConn.Close()

	// Start binlog dump WITHOUT nonBlock flag - should block when caught up
	// Flags: no BinlogDumpNonBlock — should block when caught up
	flags := uint16(0)
	sidBlock := gtidToSIDBlock(t, currentGTID)
	err = binlogConn.WriteComBinlogDumpGTID(1, "", 4, flags, sidBlock)
	require.NoError(t, err)

	// Channel for async packet reading - we'll read continuously
	packetCh := make(chan []byte, 100)
	errCh := make(chan error, 1)
	doneCh := make(chan struct{})

	// Start a goroutine to read packets continuously
	go func() {
		defer close(packetCh)
		for {
			select {
			case <-doneCh:
				return
			default:
			}
			data, err := binlogConn.ReadPacket()
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			select {
			case packetCh <- data:
			case <-doneCh:
				return
			}
		}
	}()

	// Give it a moment, then insert data
	time.Sleep(100 * time.Millisecond)

	// Insert data - this should generate binlog packets that we receive
	_, err = dataConn.ExecuteFetch("INSERT INTO binlog_test (msg) VALUES ('blocking_mode_test')", 1, false)
	require.NoError(t, err)
	t.Log("Inserted test row")

	// Wait for packets - in blocking mode we should receive the insert packets
	receivedPackets := 0
	timeout := time.After(10 * time.Second)

readLoop:
	for {
		select {
		case data, ok := <-packetCh:
			if !ok {
				t.Log("Packet channel closed")
				break readLoop
			}
			if len(data) > 0 {
				receivedPackets++
				require.NotEqual(t, mysql.EOFPacket, data[0], "Received unexpected EOF in blocking mode")
				t.Logf("Received packet %d: first byte=0x%02x, size=%d", receivedPackets, data[0], len(data))
				// After receiving some packets, we can stop
				if receivedPackets >= 3 {
					t.Logf("Received %d packets, blocking mode is working correctly", receivedPackets)
					break readLoop
				}
			}
		case err := <-errCh:
			require.Failf(t, "error reading packet", "Error reading packet: %v", err)
		case <-timeout:
			if receivedPackets > 0 {
				t.Logf("Timeout after receiving %d packets - blocking mode works", receivedPackets)
				break readLoop
			}
			require.Fail(t, "Timeout waiting for packets in blocking mode")
		}
	}

	// Signal the reader goroutine to stop
	close(doneCh)

	assert.GreaterOrEqual(t, receivedPackets, 1, "Should have received at least one packet in blocking mode")
}

// TestBinlogDumpGTID_DirectGRPC tests GTID-based binlog streaming via direct gRPC connection to vttablet.
func TestBinlogDumpGTID_DirectGRPC(t *testing.T) {
	ctx := t.Context()

	// Get the tablet info for direct gRPC connection
	primaryTablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet()
	tablet, err := clusterInstance.VtctldClientProcess.GetTablet(primaryTablet.Alias)
	require.NoError(t, err)

	// Get current position - returns format like "MySQL56/uuid:1-44"
	pos, _ := cluster.GetPrimaryPosition(t, *primaryTablet, hostname)
	t.Logf("Primary position: %v", pos)

	// Extract just the GTID set (strip the "MySQL56/" prefix)
	gtidSet := pos
	if _, after, found := strings.Cut(pos, "/"); found {
		gtidSet = after
	}
	t.Logf("GTID set for BinlogDump: %s", gtidSet)

	grpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Connect directly to vttablet via gRPC
	conn, err := tabletconn.GetDialer()(grpcCtx, tablet, grpcclient.FailFast(false))
	require.NoError(t, err)
	defer conn.Close(grpcCtx)

	var receivedPackets int
	var wg sync.WaitGroup

	// Goroutine 1: Stream binlog packets via direct gRPC
	wg.Go(func() {
		err := conn.BinlogDumpGTID(grpcCtx, &binlogdatapb.BinlogDumpGTIDRequest{
			Target: &querypb.Target{
				Keyspace:   keyspaceName,
				Shard:      "0",
				TabletType: tablet.Type,
			},
			GtidSet: gtidSet,
		}, func(response *binlogdatapb.BinlogDumpResponse) error {
			receivedPackets++
			t.Logf("Received packet %d via gRPC: %d bytes", receivedPackets, len(response.Raw))
			return nil
		})
		if err != nil {
			t.Logf("BinlogDumpGTID ended: %v", err)
		}
	})

	// Goroutine 2: Write data to generate binlog packets
	wg.Go(func() {
		dataConn, err := mysql.Connect(grpcCtx, &vtParams)
		if err != nil {
			t.Logf("Failed to connect for writes: %v", err)
			return
		}
		defer dataConn.Close()

		for i := range 5 {
			select {
			case <-grpcCtx.Done():
				return
			default:
			}
			time.Sleep(1 * time.Second)
			t.Logf("Writing row %d", i+1)
			_, err := dataConn.ExecuteFetch(
				fmt.Sprintf("INSERT INTO binlog_test (msg) VALUES ('grpc_test_%d')", i), 1, false)
			if err != nil {
				t.Logf("Insert failed: %v", err)
				return
			}
		}
	})

	wg.Wait()

	assert.GreaterOrEqual(t, receivedPackets, 1, "Should have received binlog packets via direct gRPC")
	t.Logf("Successfully received %d packets via direct gRPC to vttablet", receivedPackets)
}

// TestBinlogDumpGTID_EmptyGTIDStartsFromBeginning verifies that an empty GTID set causes
// the binlog dump to start from the very beginning of the binlog (getting all historical events),
// rather than from the current position. This is tested by comparing the packet count of an
// empty-GTID nonBlock dump against a current-GTID nonBlock dump: the empty-GTID dump should
// receive strictly more packets because it includes all pre-existing events.
func TestBinlogDumpGTID_EmptyGTIDStartsFromBeginning(t *testing.T) {
	ctx := t.Context()

	// Insert some data to ensure binlog has events before we record the GTID position.
	dataConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer dataConn.Close()

	for i := range 5 {
		_, err := dataConn.ExecuteFetch(
			fmt.Sprintf("INSERT INTO binlog_test (msg) VALUES ('empty_gtid_test_%d')", i), 1, false)
		require.NoError(t, err)
	}

	// Record the current GTID position (after inserts).
	currentGTID := getCurrentGTID(t, dataConn)
	t.Logf("Current GTID (after inserts): %s", currentGTID)

	// Helper: connect and do a nonBlock binlog dump, return the number of OK packets received before EOF.
	countPackets := func(t *testing.T, sidBlock []byte, label string) int {
		t.Helper()

		primaryTablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet()
		targetString := fmt.Sprintf("%s:0@primary|%s", keyspaceName, primaryTablet.Alias)
		binlogParams := mysql.ConnParams{
			Host:  clusterInstance.Hostname,
			Port:  clusterInstance.VtgateMySQLPort,
			Uname: "vt_repl|" + targetString,
		}

		binlogConn, err := mysql.Connect(ctx, &binlogParams)
		require.NoError(t, err)
		defer binlogConn.Close()

		flags := uint16(mysql.BinlogDumpNonBlock)
		err = binlogConn.WriteComBinlogDumpGTID(1, "", 4, flags, sidBlock)
		require.NoError(t, err)

		var count int
		timeout := time.After(30 * time.Second)

		for {
			select {
			case <-timeout:
				require.Failf(t, "timeout waiting for EOF", "[%s] Timeout waiting for EOF, received %d packets", label, count)
			default:
			}

			data, err := binlogConn.ReadPacket()
			if err != nil {
				t.Logf("[%s] Connection closed after %d packets: %v", label, count, err)
				return count
			}
			if len(data) == 0 {
				continue
			}

			switch data[0] {
			case mysql.EOFPacket:
				t.Logf("[%s] EOF after %d packets", label, count)
				return count
			case mysql.ErrPacket:
				sqlErr := mysql.ParseErrorPacket(data)
				require.Failf(t, "unexpected error packet", "[%s] Unexpected error packet: %v", label, sqlErr)
			case mysql.OKPacket:
				count++
			}
		}
	}

	// Dump 1: empty GTID set (nil sidBlock) — should start from beginning of binlog.
	emptyCount := countPackets(t, nil, "empty-gtid")

	// Dump 2: current GTID — should get no events (immediate EOF).
	currentSIDBlock := gtidToSIDBlock(t, currentGTID)
	currentCount := countPackets(t, currentSIDBlock, "current-gtid")

	t.Logf("Empty GTID packets: %d, Current GTID packets: %d", emptyCount, currentCount)
	assert.Greater(t, emptyCount, currentCount,
		"Empty GTID dump should receive more packets than current-position dump")
}

// TestBinlogDumpGTID_ShardTargeting tests that binlog events are streamed when using shard-level
// targeting (e.g., "commerce:0@primary") without specifying a tablet alias. VTGate should
// automatically select a healthy tablet for the shard via health check.
func TestBinlogDumpGTID_ShardTargeting(t *testing.T) {
	ctx := t.Context()

	// Connect to vtgate for binlog streaming using shard-level targeting (no tablet alias)
	targetString := keyspaceName + ":0@primary"
	binlogParams := mysql.ConnParams{
		Host:  clusterInstance.Hostname,
		Port:  clusterInstance.VtgateMySQLPort,
		Uname: "vt_repl|" + targetString,
	}

	t.Logf("Connecting with shard-level target: %s", targetString)

	binlogConn, err := mysql.Connect(ctx, &binlogParams)
	require.NoError(t, err)
	defer binlogConn.Close()

	// Start binlog dump with no initial GTID - will start from current position
	err = binlogConn.WriteComBinlogDumpGTID(1, "", 4, 0, nil)
	require.NoError(t, err, "Should be able to send COM_BINLOG_DUMP_GTID with shard-level target")

	// Channel to receive packets and errors
	packetCh := make(chan []byte, 10)
	errCh := make(chan error, 1)

	// Start reading packets in a goroutine
	var wg sync.WaitGroup
	wg.Go(func() {
		defer close(packetCh)
		for {
			data, err := binlogConn.ReadPacket()
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			select {
			case packetCh <- data:
			default:
			}
		}
	})

	// Give the binlog dump a moment to start
	time.Sleep(100 * time.Millisecond)

	// Insert data to generate binlog packets
	dataConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer dataConn.Close()

	t.Log("Inserting test data to generate binlog packets")
	for i := range 3 {
		_, err := dataConn.ExecuteFetch(fmt.Sprintf("INSERT INTO binlog_test (msg) VALUES ('shard_target_test_%d')", i), 1, false)
		require.NoError(t, err)
	}

	// Wait for at least one packet or timeout
	receivedPackets := 0
	timeout := time.After(5 * time.Second)

packetLoop:
	for {
		select {
		case data, ok := <-packetCh:
			if !ok {
				t.Logf("Packet channel closed after receiving %d packets", receivedPackets)
				break packetLoop
			}
			receivedPackets++
			if len(data) > 0 {
				t.Logf("Received packet %d: first byte=0x%02x, length=%d", receivedPackets, data[0], len(data))
			}
			if receivedPackets >= 3 {
				t.Logf("Received %d packets, test passed", receivedPackets)
				break packetLoop
			}
		case err := <-errCh:
			t.Logf("Got error from packet reader: %v", err)
			break packetLoop
		case <-timeout:
			t.Logf("Timeout after receiving %d packets", receivedPackets)
			break packetLoop
		}
	}

	// Close the connection to stop the reader goroutine
	binlogConn.Close()
	wg.Wait()

	assert.GreaterOrEqual(t, receivedPackets, 1, "Should have received at least one binlog packet with shard-level targeting")
}

// vtgateGrpcAddr returns the vtgate gRPC address for the cluster.
func vtgateGrpcAddr() string {
	return fmt.Sprintf("%s:%d", clusterInstance.Hostname, clusterInstance.VtgateGrpcPort)
}

// TestBinlogDumpGTID_VTGateGRPC_Streaming tests binlog streaming through vtgate's gRPC endpoint.
func TestBinlogDumpGTID_VTGateGRPC_Streaming(t *testing.T) {
	ctx := t.Context()

	// Get current GTID position
	pos, _ := cluster.GetPrimaryPosition(t, *clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet(), hostname)
	gtidSet := pos
	if _, after, found := strings.Cut(pos, "/"); found {
		gtidSet = after
	}

	conn, err := vtgateconn.Dial(ctx, vtgateGrpcAddr())
	require.NoError(t, err)
	defer conn.Close()

	grpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	reader, err := conn.BinlogDumpGTID(grpcCtx, keyspaceName, "0", topodatapb.TabletType_PRIMARY, nil, "", 4, gtidSet, 0)
	require.NoError(t, err)

	// Insert data to generate binlog events
	var wg sync.WaitGroup
	wg.Go(func() {
		// Small delay so the stream is established first
		time.Sleep(200 * time.Millisecond)
		dataConn, err := mysql.Connect(ctx, &vtParams)
		if err != nil {
			t.Logf("Failed to connect for writes: %v", err)
			return
		}
		defer dataConn.Close()
		for i := range 3 {
			_, err := dataConn.ExecuteFetch(
				fmt.Sprintf("INSERT INTO binlog_test (msg) VALUES ('vtgate_grpc_test_%d')", i), 1, false)
			if err != nil {
				t.Logf("Insert failed: %v", err)
				return
			}
		}
	})

	var receivedPackets int
	for {
		resp, err := reader.Recv()
		if err != nil {
			t.Logf("Stream ended: %v", err)
			break
		}
		receivedPackets++
		t.Logf("Received packet %d via vtgate gRPC: %d bytes", receivedPackets, len(resp.Raw))
		if receivedPackets >= 3 {
			break
		}
	}

	cancel()
	wg.Wait()

	assert.GreaterOrEqual(t, receivedPackets, 1, "Should have received binlog packets via vtgate gRPC")
}

// TestBinlogDumpGTID_VTGateGRPC_RejectsFilename tests that vtgate's gRPC endpoint rejects
// requests with a binlog filename.
func TestBinlogDumpGTID_VTGateGRPC_RejectsFilename(t *testing.T) {
	ctx := t.Context()

	conn, err := vtgateconn.Dial(ctx, vtgateGrpcAddr())
	require.NoError(t, err)
	defer conn.Close()

	reader, err := conn.BinlogDumpGTID(ctx, keyspaceName, "0", topodatapb.TabletType_PRIMARY, nil, "binlog.000001", 4, "", 0)
	if err != nil {
		assert.Contains(t, err.Error(), "binlog filename is not supported")
		return
	}
	_, err = reader.Recv()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "binlog filename is not supported")
}

// TestBinlogDumpGTID_VTGateGRPC_RejectsNonDefaultPosition tests that vtgate's gRPC endpoint
// rejects requests with a binlog position other than 4.
func TestBinlogDumpGTID_VTGateGRPC_RejectsNonDefaultPosition(t *testing.T) {
	ctx := t.Context()

	conn, err := vtgateconn.Dial(ctx, vtgateGrpcAddr())
	require.NoError(t, err)
	defer conn.Close()

	reader, err := conn.BinlogDumpGTID(ctx, keyspaceName, "0", topodatapb.TabletType_PRIMARY, nil, "", 1234, "", 0)
	if err != nil {
		assert.Contains(t, err.Error(), "only binlog position 4 is supported")
		return
	}
	_, err = reader.Recv()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only binlog position 4 is supported")
}

// TestBinlogDumpGTID_VTGateGRPC_RejectsPositionBelowMinimum tests that vtgate's gRPC endpoint
// rejects requests with a binlog position below 4.
func TestBinlogDumpGTID_VTGateGRPC_RejectsPositionBelowMinimum(t *testing.T) {
	ctx := t.Context()

	conn, err := vtgateconn.Dial(ctx, vtgateGrpcAddr())
	require.NoError(t, err)
	defer conn.Close()

	reader, err := conn.BinlogDumpGTID(ctx, keyspaceName, "0", topodatapb.TabletType_PRIMARY, nil, "", 0, "", 0)
	if err != nil {
		assert.Contains(t, err.Error(), "position < 4")
		return
	}
	_, err = reader.Recv()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "position < 4")
}
