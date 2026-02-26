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
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/test/utils"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func TestComBinlogDump(t *testing.T) {
	_ = utils.LeakCheckContext(t)
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	// Try to write a ComBinlogDump packet with a position greater than 4 bytes.
	err := cConn.WriteComBinlogDump(1, "moofarm", math.MaxInt64, 0x0d0e)
	require.Error(t, err)

	// Write ComBinlogDump packet, read it, compare.
	if err := cConn.WriteComBinlogDump(0x01020304, "moofarm", 0x05060708, 0x090a); err != nil {
		t.Fatalf("WriteComBinlogDump failed: %v", err)
	}

	data, err := sConn.ReadPacket()
	require.NoError(t, err, "sConn.ReadPacket - ComBinlogDump failed: %v", err)

	expectedData := []byte{
		ComBinlogDump,
		0x08, 0x07, 0x06, 0x05, // binlog-pos
		0x0a, 0x09, // flags
		0x04, 0x03, 0x02, 0x01, // server-id
		'm', 'o', 'o', 'f', 'a', 'r', 'm', // binlog-filename
	}
	assert.True(t, reflect.DeepEqual(data, expectedData), "ComBinlogDump returned unexpected data:\n%v\nwas expecting:\n%v", data, expectedData)

	sConn.sequence = 0

	// Write ComBinlogDump packet with no filename, read it, compare.
	if err := cConn.WriteComBinlogDump(0x01020304, "", 0x05060708, 0x090a); err != nil {
		t.Fatalf("WriteComBinlogDump failed: %v", err)
	}

	data, err = sConn.ReadPacket()
	require.NoError(t, err, "sConn.ReadPacket - ComBinlogDump failed: %v", err)

	expectedData = []byte{
		ComBinlogDump,
		0x08, 0x07, 0x06, 0x05, // binlog-pos
		0x0a, 0x09, // flags
		0x04, 0x03, 0x02, 0x01, // server-id
	}
	assert.True(t, reflect.DeepEqual(data, expectedData), "ComBinlogDump returned unexpected data:\n%v\nwas expecting:\n%v", data, expectedData)
}

func TestComBinlogDumpGTID(t *testing.T) {
	_ = utils.LeakCheckContext(t)
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	t.Run("WriteComBinlogDumpGTIDEmptyGTID", func(t *testing.T) {
		// Write ComBinlogDumpGTID packet, read it, compare.
		var flags uint16 = 0x0d0e
		err := cConn.WriteComBinlogDumpGTID(0x01020304, "moofarm", 0x05060708090a0b0c, flags, []byte{})
		assert.NoError(t, err)
		data, err := sConn.ReadPacket()
		require.NoError(t, err, "sConn.ReadPacket - ComBinlogDumpGTID failed: %v", err)
		require.NotEmpty(t, data)
		require.EqualValues(t, data[0], ComBinlogDumpGTID)

		expectedData := []byte{
			ComBinlogDumpGTID,
			0x0e, 0x0d, // flags
			0x04, 0x03, 0x02, 0x01, // server-id
			0x07, 0x00, 0x00, 0x00, // binlog-filename-len
			'm', 'o', 'o', 'f', 'a', 'r', 'm', // binlog-filename
			0x0c, 0x0b, 0x0a, 0x09, 0x08, 0x07, 0x06, 0x05, // binlog-pos
			0x00, 0x00, 0x00, 0x00, // data-size is zero, no GTID payload
		}
		assert.Equal(t, expectedData, data)
		logFile, logPos, pos, nonBlock, err := sConn.parseComBinlogDumpGTID(data)
		require.NoError(t, err, "parseComBinlogDumpGTID failed: %v", err)
		assert.Equal(t, "moofarm", logFile)
		assert.Equal(t, uint64(0x05060708090a0b0c), logPos)
		assert.True(t, pos.IsZero())
		// flags 0x0d0e does not have BinlogDumpNonBlock (0x01) set
		assert.False(t, nonBlock)
	})

	sConn.sequence = 0

	t.Run("WriteComBinlogDumpGTID", func(t *testing.T) {
		// Write ComBinlogDumpGTID packet, read it, compare.
		var flags uint16 = 0x0d0e
		assert.Equal(t, flags, flags|BinlogThroughGTID)
		gtidSet, err := replication.ParseMysql56GTIDSet("16b1039f-22b6-11ed-b765-0a43f95f28a3:1-243")
		require.NoError(t, err)
		sidBlock := gtidSet.SIDBlock()
		assert.Len(t, sidBlock, 48)

		err = cConn.WriteComBinlogDumpGTID(0x01020304, "moofarm", 0x05060708090a0b0c, flags, sidBlock)
		assert.NoError(t, err)
		data, err := sConn.ReadPacket()
		require.NoError(t, err, "sConn.ReadPacket - ComBinlogDumpGTID failed: %v", err)
		require.NotEmpty(t, data)
		require.EqualValues(t, data[0], ComBinlogDumpGTID)

		expectedData := []byte{
			ComBinlogDumpGTID,
			0x0e, 0x0d, // flags
			0x04, 0x03, 0x02, 0x01, // server-id
			0x07, 0x00, 0x00, 0x00, // binlog-filename-len
			'm', 'o', 'o', 'f', 'a', 'r', 'm', // binlog-filename
			0x0c, 0x0b, 0x0a, 0x09, 0x08, 0x07, 0x06, 0x05, // binlog-pos
			0x30, 0x00, 0x00, 0x00, // data-size
		}
		expectedData = append(expectedData, sidBlock...) // data
		assert.Equal(t, expectedData, data)
		logFile, logPos, pos, nonBlock, err := sConn.parseComBinlogDumpGTID(data)
		require.NoError(t, err, "parseComBinlogDumpGTID failed: %v", err)
		assert.Equal(t, "moofarm", logFile)
		assert.Equal(t, uint64(0x05060708090a0b0c), logPos)
		assert.Equal(t, gtidSet, pos.GTIDSet)
		// flags 0x0d0e does not have BinlogDumpNonBlock (0x01) set
		assert.False(t, nonBlock)
	})

	sConn.sequence = 0

	t.Run("WriteComBinlogDumpGTID no filename", func(t *testing.T) {
		// Write ComBinlogDumpGTID packet with no filename, read it, compare.
		err := cConn.WriteComBinlogDumpGTID(0x01020304, "", 0x05060708090a0b0c, 0x0d0e, []byte{0xfa, 0xfb})
		assert.NoError(t, err)
		data, err := sConn.ReadPacket()
		require.NoError(t, err, "sConn.ReadPacket - ComBinlogDumpGTID failed: %v", err)

		expectedData := []byte{
			ComBinlogDumpGTID,
			0x0e, 0x0d, // flags
			0x04, 0x03, 0x02, 0x01, // server-id
			0x00, 0x00, 0x00, 0x00, // binlog-filename-len
			0x0c, 0x0b, 0x0a, 0x09, 0x08, 0x07, 0x06, 0x05, // binlog-pos
			0x02, 0x00, 0x00, 0x00, // data-size
			0xfa, 0xfb, // data
		}
		assert.Equal(t, expectedData, data)
	})
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	t.Run("Write rotate event", func(t *testing.T) {
		event := NewRotateEvent(f, s, 456, "mysql-bin.000123")
		err := cConn.WriteBinlogEvent(event, false)
		assert.NoError(t, err)
		data, err := sConn.ReadPacket()
		require.NoError(t, err)

		expectedData := []byte{
			0x0, 0x0, 0x0, 0x0, 0x0, 0x4, 0x1, 0x0, 0x0, 0x0, 0x2f,
			0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc8, 0x1, 0x0, 0x0, 0x0, 0x0,
			0x0, 0x0, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x2d, 0x62, 0x69, 0x6e, 0x2e, 0x30,
			0x30, 0x30, 0x31, 0x32, 0x33, 0xfd, 0x1c, 0x1d, 0x80,
		}
		assert.Equal(t, expectedData, data)
	})
	t.Run("Write query event", func(t *testing.T) {
		q := Query{
			Database: "my database",
			SQL:      "my query",
			Charset: &binlogdatapb.Charset{
				Client: 0x1234,
				Conn:   0x5678,
				Server: 0x9abc,
			},
		}
		event := NewQueryEvent(f, s, q)
		err := cConn.WriteBinlogEvent(event, false)
		assert.NoError(t, err)
		data, err := sConn.ReadPacket()
		require.NoError(t, err)

		expectedData := []byte{
			0x0, 0x98, 0x68, 0xe9, 0x53, 0x2, 0x1, 0x0, 0x0, 0x0,
			0x3f, 0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
			0x0, 0x0, 0x0, 0xb, 0x0, 0x0, 0x7, 0x0, 0x4, 0x34, 0x12, 0x78, 0x56, 0xbc,
			0x9a, 0x6d, 0x79, 0x20, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65,
			0x0, 0x6d, 0x79, 0x20, 0x71, 0x75, 0x65, 0x72, 0x79, 0x65, 0xaa, 0x33, 0xe,
		}
		assert.Equal(t, expectedData, data)
	})
}

func TestWritePacketPayload(t *testing.T) {
	_ = utils.LeakCheckContext(t)
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	t.Run("Write raw packet payload", func(t *testing.T) {
		// Simulate a packet payload with OK prefix (0x00) followed by binlog event data
		payload := []byte{
			0x00, // OK prefix
			// Binlog event header (19 bytes for MySQL 5.6+)
			0x00, 0x00, 0x00, 0x00, // timestamp
			0x04,                   // event type (ROTATE_EVENT)
			0x01, 0x00, 0x00, 0x00, // server_id
			0x2f, 0x00, 0x00, 0x00, // event_length
			0x04, 0x00, 0x00, 0x00, // next_position
			0x00, 0x00, // flags
			// Event data
			0xc8, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // position
			'm', 'y', 's', 'q', 'l', '-', 'b', 'i', 'n', '.', '0', '0', '0', '1', '2', '3', // filename
			// Checksum
			0xfd, 0x1c, 0x1d, 0x80,
		}

		err := cConn.WritePacketPayload(payload)
		require.NoError(t, err)

		data, err := sConn.ReadPacket()
		require.NoError(t, err)

		// The received data should be exactly the payload
		assert.Equal(t, payload, data)
	})

	sConn.sequence = 0
	cConn.sequence = 0

	t.Run("Write empty payload", func(t *testing.T) {
		err := cConn.WritePacketPayload([]byte{})
		require.NoError(t, err)

		data, err := sConn.ReadPacket()
		require.NoError(t, err)

		// Should be empty (nil or empty slice)
		assert.Empty(t, data)
	})

	sConn.sequence = 0
	cConn.sequence = 0

	t.Run("Write multiple payloads", func(t *testing.T) {
		payloads := [][]byte{
			{0x00, 0x01, 0x02, 0x03},       // OK packet with data
			{0x00, 0x04, 0x05, 0x06, 0x07}, // OK packet with data
			{0x00, 0x08},                   // OK packet with data
		}

		for _, payload := range payloads {
			err := cConn.WritePacketPayload(payload)
			require.NoError(t, err)

			data, err := sConn.ReadPacket()
			require.NoError(t, err)

			assert.Equal(t, payload, data)
		}
	})
}

func TestWritePacketDirect(t *testing.T) {
	_ = utils.LeakCheckContext(t)
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	t.Run("Write single packet", func(t *testing.T) {
		payload := []byte{0x00, 0x01, 0x02, 0x03}

		err := cConn.WritePacketDirect(payload)
		require.NoError(t, err)

		data, err := sConn.ReadPacket()
		require.NoError(t, err)
		assert.Equal(t, payload, data)
	})

	sConn.sequence = 0
	cConn.sequence = 0

	t.Run("Write multiple packets sequentially", func(t *testing.T) {
		packets := [][]byte{
			{0x00, 0x01, 0x02, 0x03},       // First packet
			{0x04, 0x05, 0x06, 0x07, 0x08}, // Second packet
			{0x09, 0x0a},                   // Third packet
		}

		for _, packet := range packets {
			err := cConn.WritePacketDirect(packet)
			require.NoError(t, err)
		}

		// Read each packet and verify
		for _, expected := range packets {
			data, err := sConn.ReadPacket()
			require.NoError(t, err)
			assert.Equal(t, expected, data)
		}
	})

	sConn.sequence = 0
	cConn.sequence = 0

	t.Run("Write zero-length packet", func(t *testing.T) {
		// Zero-length packets are valid terminators in MySQL protocol
		err := cConn.WritePacketDirect([]byte{})
		require.NoError(t, err)

		// ReadPacket returns nil for zero-length packets
		data, err := sConn.ReadPacket()
		require.NoError(t, err)
		assert.Empty(t, data)
	})
}

func TestWritePacketDirectSequenceNumbers(t *testing.T) {
	_ = utils.LeakCheckContext(t)
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	t.Run("Sequence numbers increment correctly", func(t *testing.T) {
		// Verify sequence numbers start at 0
		assert.Equal(t, uint8(0), cConn.sequence, "Writer sequence should start at 0")
		assert.Equal(t, uint8(0), sConn.sequence, "Reader sequence should start at 0")

		// Write first packet
		err := cConn.WritePacketDirect([]byte{0x01})
		require.NoError(t, err)
		assert.Equal(t, uint8(1), cConn.sequence, "Writer sequence should be 1 after first packet")

		// Write second packet
		err = cConn.WritePacketDirect([]byte{0x02})
		require.NoError(t, err)
		assert.Equal(t, uint8(2), cConn.sequence, "Writer sequence should be 2 after second packet")

		// Write third packet
		err = cConn.WritePacketDirect([]byte{0x03})
		require.NoError(t, err)
		assert.Equal(t, uint8(3), cConn.sequence, "Writer sequence should be 3 after third packet")

		// Read packets - reader validates sequence numbers internally
		// If sequence numbers are wrong, ReadPacket will return an error
		for i := range 3 {
			_, err := sConn.ReadPacket()
			require.NoError(t, err, "ReadPacket should succeed with correct sequence number %d", i)
		}
		assert.Equal(t, uint8(3), sConn.sequence, "Reader sequence should be 3 after reading all packets")
	})

	sConn.sequence = 0
	cConn.sequence = 0

	t.Run("Multi-packet sequence with zero-length terminator", func(t *testing.T) {
		// Simulate a multi-packet binlog event:
		// - First packet (max size would be 16MB, we use small for testing)
		// - Second packet (continuation)
		// - Zero-length terminator
		packets := [][]byte{
			{0x00, 0x01, 0x02, 0x03}, // First fragment
			{0x04, 0x05, 0x06, 0x07}, // Second fragment
			{},                       // Zero-length terminator
		}

		// Write all packets
		for i, packet := range packets {
			err := cConn.WritePacketDirect(packet)
			require.NoError(t, err)
			assert.Equal(t, uint8(i+1), cConn.sequence, "Writer sequence should be %d after packet %d", i+1, i)
		}

		// Read all packets - validates sequence numbers
		for i, expected := range packets {
			data, err := sConn.ReadPacket()
			require.NoError(t, err, "ReadPacket %d should succeed", i)
			if len(expected) == 0 {
				// ReadPacket returns nil for zero-length packets
				assert.Nil(t, data, "Zero-length packet should return nil")
			} else {
				assert.Equal(t, expected, data, "Packet %d content should match", i)
			}
		}
		assert.Equal(t, uint8(3), sConn.sequence, "Reader sequence should be 3 after all packets")
	})

	sConn.sequence = 0
	cConn.sequence = 0

	t.Run("Sequence wraps around at 256", func(t *testing.T) {
		// Write 256 packets to test sequence wraparound
		for i := range 256 {
			err := cConn.WritePacketDirect([]byte{byte(i)})
			require.NoError(t, err)
		}
		// After 256 packets, sequence should wrap to 0
		assert.Equal(t, uint8(0), cConn.sequence, "Writer sequence should wrap to 0 after 256 packets")

		// Write one more packet
		err := cConn.WritePacketDirect([]byte{0xFF})
		require.NoError(t, err)
		assert.Equal(t, uint8(1), cConn.sequence, "Writer sequence should be 1 after wraparound")

		// Read all 257 packets
		for i := range 257 {
			_, err := sConn.ReadPacket()
			require.NoError(t, err, "ReadPacket %d should succeed after wraparound", i)
		}
		assert.Equal(t, uint8(1), sConn.sequence, "Reader sequence should be 1 after reading all packets")
	})
}

func TestSendSemiSyncAck(t *testing.T) {
	_ = utils.LeakCheckContext(t)
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	// Write ComBinlogDumpGTID packet, read it, compare.
	logName := "moofarm"
	logPos := uint64(1852)
	if err := cConn.SendSemiSyncAck(logName, logPos); err != nil {
		t.Fatalf("SendSemiSyncAck failed: %v", err)
	}

	data, err := sConn.ReadPacket()
	require.NoError(t, err, "sConn.ReadPacket - SendSemiSyncAck failed: %v", err)

	expectedData := []byte{
		ComSemiSyncAck,
		0x3c, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log pos
		'm', 'o', 'o', 'f', 'a', 'r', 'm', // binlog-filename
	}
	assert.True(t, reflect.DeepEqual(data, expectedData), "SendSemiSyncAck returned unexpected data:\n%v\nwas expecting:\n%v", data, expectedData)
}
