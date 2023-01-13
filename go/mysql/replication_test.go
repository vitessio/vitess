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
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func TestComBinlogDump(t *testing.T) {
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	// Write ComBinlogDump packet, read it, compare.
	if err := cConn.WriteComBinlogDump(0x01020304, "moofarm", 0x05060708, 0x090a); err != nil {
		t.Fatalf("WriteComBinlogDump failed: %v", err)
	}

	data, err := sConn.ReadPacket()
	require.NoError(t, err, "sConn.ReadPacket - ComBinlogDump failed: %v", err)

	expectedData := []byte{
		ComBinlogDump,
		0x08, 0x07, 0x06, 0x05, // binlog-pos
		0x0a, 0x09, //flags
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
	listener, sConn, cConn := createSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	t.Run("WriteComBinlogDumpGTID", func(t *testing.T) {
		// Write ComBinlogDumpGTID packet, read it, compare.
		err := cConn.WriteComBinlogDumpGTID(0x01020304, "moofarm", 0x05060708090a0b0c, 0x0d0e, []byte{0xfa, 0xfb})
		assert.NoError(t, err)
		data, err := sConn.ReadPacket()
		require.NoError(t, err, "sConn.ReadPacket - ComBinlogDumpGTID failed: %v", err)

		expectedData := []byte{
			ComBinlogDumpGTID,
			0x0e, 0x0d, // flags
			0x04, 0x03, 0x02, 0x01, // server-id
			0x07, 0x00, 0x00, 0x00, // binlog-filename-len
			'm', 'o', 'o', 'f', 'a', 'r', 'm', // bilog-filename
			0x0c, 0x0b, 0x0a, 0x09, 0x08, 0x07, 0x06, 0x05, // binlog-pos
			0x02, 0x00, 0x00, 0x00, // data-size
			0xfa, 0xfb, // data
		}
		assert.Equal(t, expectedData, data)
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

		expectedData := []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x4, 0x1, 0x0, 0x0, 0x0, 0x2f,
			0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc8, 0x1, 0x0, 0x0, 0x0, 0x0,
			0x0, 0x0, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x2d, 0x62, 0x69, 0x6e, 0x2e, 0x30,
			0x30, 0x30, 0x31, 0x32, 0x33, 0xfd, 0x1c, 0x1d, 0x80}
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

		expectedData := []byte{0x0, 0x98, 0x68, 0xe9, 0x53, 0x2, 0x1, 0x0, 0x0, 0x0,
			0x3f, 0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
			0x0, 0x0, 0x0, 0xb, 0x0, 0x0, 0x7, 0x0, 0x4, 0x34, 0x12, 0x78, 0x56, 0xbc,
			0x9a, 0x6d, 0x79, 0x20, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65,
			0x0, 0x6d, 0x79, 0x20, 0x71, 0x75, 0x65, 0x72, 0x79, 0x65, 0xaa, 0x33, 0xe}
		assert.Equal(t, expectedData, data)
	})
}

func TestSendSemiSyncAck(t *testing.T) {
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
