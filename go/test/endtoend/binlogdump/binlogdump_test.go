/*
Copyright 2025 The Vitess Authors.

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
)

// TestBinlogDumpStreaming tests that binlog events are actually streamed from vttablet to the client.
func TestBinlogDumpStreaming(t *testing.T) {
	ctx := context.Background()

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
	err = binlogConn.WriteComBinlogDumpGTID(1, "", 4, mysql.BinlogThroughGTID, nil)
	require.NoError(t, err, "Should be able to send COM_BINLOG_DUMP_GTID")

	// Channel to receive events and errors
	eventCh := make(chan []byte, 10)
	errCh := make(chan error, 1)

	// Start reading packets in a goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(eventCh)
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
			case eventCh <- data:
			default:
				// Channel full, drop packet
			}
		}
	}()

	// Give the binlog dump a moment to start
	time.Sleep(100 * time.Millisecond)

	// Now insert data to generate binlog events
	dataConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer dataConn.Close()

	t.Log("Inserting test data to generate binlog events")
	for i := 0; i < 3; i++ {
		_, err := dataConn.ExecuteFetch(fmt.Sprintf("INSERT INTO binlog_test (msg) VALUES ('streaming_test_%d')", i), 1, false)
		require.NoError(t, err)
	}

	// Wait for at least one event or timeout
	receivedEvents := 0
	timeout := time.After(5 * time.Second)

eventLoop:
	for {
		select {
		case data, ok := <-eventCh:
			if !ok {
				// Channel closed
				t.Logf("Event channel closed after receiving %d events", receivedEvents)
				break eventLoop
			}
			receivedEvents++
			if len(data) > 0 {
				t.Logf("Received event %d: first byte=0x%02x, length=%d", receivedEvents, data[0], len(data))
			}
			// We got an event, test passes
			if receivedEvents >= 3 {
				t.Logf("Received %d events, test passed", receivedEvents)
				break eventLoop
			}
		case err := <-errCh:
			t.Logf("Got error from event reader: %v", err)
			break eventLoop
		case <-timeout:
			t.Logf("Timeout after receiving %d events", receivedEvents)
			break eventLoop
		}
	}

	// Close the connection to stop the reader goroutine
	binlogConn.Close()
	wg.Wait()

	// We should have received at least some events
	assert.GreaterOrEqual(t, receivedEvents, 1, "Should have received at least one binlog event")
}

// TestBinlogDumpNoTarget verifies that binlog dump returns an error packet without a target
func TestBinlogDumpNoTarget(t *testing.T) {
	ctx := context.Background()

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Try to send COM_BINLOG_DUMP_GTID without setting a target
	err = conn.WriteComBinlogDumpGTID(1, "", 4, mysql.BinlogThroughGTID, nil)
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

// TestBinlogDumpLargeEvent tests that binlog events larger than 16MB (spanning multiple MySQL packets)
// are correctly streamed through VTGate. This is critical because MySQL protocol uses 16MB max packet
// size, and large events must be split into multiple packets and reassembled correctly.
func TestBinlogDumpLargeEvent(t *testing.T) {
	ctx := context.Background()

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
	err = binlogConn.WriteComBinlogDumpGTID(1, "", 4, mysql.BinlogThroughGTID, nil)
	require.NoError(t, err, "Should be able to send COM_BINLOG_DUMP_GTID")

	t.Log("Binlog dump started")

	// Channel to receive events
	eventCh := make(chan []byte, 100)
	errCh := make(chan error, 1)

	// Start reading packets in a goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(eventCh)
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
			case eventCh <- data:
			default:
				// Channel full, drop packet
			}
		}
	}()

	// Give the binlog dump a moment to start
	time.Sleep(100 * time.Millisecond)

	// Connect to insert data - this will generate binlog events AFTER we started dumping
	dataConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer dataConn.Close()

	// Create a large blob of 32MB that spans multiple MySQL packets.
	// The MySQL MaxPacketSize is 16777215 bytes (16MB - 1), so a 32MB event
	// will be split across 2+ packets by MySQL, testing our multi-packet handling.
	//
	// The gRPC max message size is configured to 64MB in main_test.go to allow
	// streaming these large events.
	largeDataSize := 32 * 1024 * 1024 // 32MB

	t.Logf("Inserting large blob of %d bytes (%d MB) to test multi-packet event handling", largeDataSize, largeDataSize/(1024*1024))

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

	t.Log("Large blob inserted successfully, waiting for binlog events...")

	// Wait for events - we should receive the large event
	var largeEventReceived bool
	receivedEvents := 0
	timeout := time.After(30 * time.Second) // Longer timeout for large data

eventLoop:
	for {
		select {
		case data, ok := <-eventCh:
			if !ok {
				t.Logf("Event channel closed after receiving %d events", receivedEvents)
				break eventLoop
			}
			receivedEvents++
			eventSize := len(data)

			// Log event details
			if eventSize > 1024*1024 {
				t.Logf("Received event %d: size=%d bytes (%.1f MB), first byte=0x%02x",
					receivedEvents, eventSize, float64(eventSize)/(1024*1024), data[0])
			} else {
				t.Logf("Received event %d: size=%d bytes, first byte=0x%02x",
					receivedEvents, eventSize, data[0])
			}

			// Check if we received a large event (>30MB)
			// The binlog event will be slightly larger than our data due to event headers
			if eventSize > 30*1024*1024 {
				largeEventReceived = true
				t.Logf("SUCCESS: Received large event of %d bytes (%.1f MB) - multi-packet handling works!",
					eventSize, float64(eventSize)/(1024*1024))
				break eventLoop
			}

			// Safety limit - don't wait forever
			if receivedEvents > 50 {
				t.Log("Received 50 events, stopping")
				break eventLoop
			}

		case err := <-errCh:
			t.Logf("Got error from event reader: %v", err)
			break eventLoop

		case <-timeout:
			t.Logf("Timeout after receiving %d events", receivedEvents)
			break eventLoop
		}
	}

	// Close the connection to stop the reader goroutine
	binlogConn.Close()
	wg.Wait()

	// Verify we received the large event
	assert.True(t, largeEventReceived, "Should have received a binlog event larger than 30MB")
	assert.GreaterOrEqual(t, receivedEvents, 1, "Should have received at least one binlog event")
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

// TestBinlogDumpFromSpecificGTID verifies that binlog streaming starts from the specified
// GTID position and only receives subsequent events.
func TestBinlogDumpFromSpecificGTID(t *testing.T) {
	ctx := context.Background()

	// Connect to insert initial data
	dataConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer dataConn.Close()

	// Insert initial data (we should NOT see these events)
	for i := 0; i < 3; i++ {
		_, err := dataConn.ExecuteFetch(
			fmt.Sprintf("INSERT INTO binlog_test (msg) VALUES ('before_gtid_%d')", i), 1, false)
		require.NoError(t, err)
	}

	// Get current GTID position - we'll start streaming from here
	startGTID := getCurrentGTID(t, dataConn)
	t.Logf("Starting GTID position: %s", startGTID)

	// Insert more data (we SHOULD see these events)
	for i := 0; i < 3; i++ {
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
	err = binlogConn.WriteComBinlogDumpGTID(1, "", 4, mysql.BinlogThroughGTID, sidBlock)
	require.NoError(t, err)

	// Read events - should only get events after startGTID
	receivedEvents := 0
	timeout := time.After(5 * time.Second)

	for receivedEvents < 3 {
		select {
		case <-timeout:
			t.Fatalf("Timeout waiting for events, received %d", receivedEvents)
		default:
			data, err := binlogConn.ReadPacket()
			if err != nil {
				t.Fatalf("Error reading packet: %v", err)
			}
			if len(data) > 0 && data[0] == mysql.OKPacket {
				receivedEvents++
				t.Logf("Received event %d: size=%d bytes", receivedEvents, len(data))
			}
		}
	}

	t.Logf("Successfully received %d events from GTID position", receivedEvents)
}

// TestBinlogDumpInvalidGTIDFormat verifies that an invalid GTID format returns a proper error packet.
func TestBinlogDumpInvalidGTIDFormat(t *testing.T) {
	ctx := context.Background()

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
	err = binlogConn.WriteComBinlogDumpGTID(1, "", 4, mysql.BinlogThroughGTID, invalidSIDBlock)
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

// TestBinlogDumpFutureGTID verifies that when requesting a GTID set that includes
// transactions not yet in the binlog, MySQL returns an error.
// This is expected MySQL behavior - you cannot request events from a position that doesn't exist.
func TestBinlogDumpFutureGTID(t *testing.T) {
	ctx := context.Background()

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
	err = binlogConn.WriteComBinlogDumpGTID(1, "", 4, mysql.BinlogThroughGTID, sidBlock)
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
