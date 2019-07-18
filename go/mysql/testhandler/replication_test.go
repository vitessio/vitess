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
	"reflect"
	"testing"

	"vitess.io/vitess/go/mysql"
)

func TestComBinlogDump(t *testing.T) {
	listener, sConn, cConn := CreateSocketPair(t)
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
	if err != nil {
		t.Fatalf("sConn.ReadPacket - ComBinlogDump failed: %v", err)
	}

	expectedData := []byte{
		mysql.ComBinlogDump,
		0x08, 0x07, 0x06, 0x05, // binlog-pos
		0x0a, 0x09, //flags
		0x04, 0x03, 0x02, 0x01, // server-id
		'm', 'o', 'o', 'f', 'a', 'r', 'm', // binlog-filename
	}
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("ComBinlogDump returned unexpected data:\n%v\nwas expecting:\n%v", data, expectedData)
	}
	sConn.Sequence = 0

	// Write ComBinlogDump packet with no filename, read it, compare.
	if err := cConn.WriteComBinlogDump(0x01020304, "", 0x05060708, 0x090a); err != nil {
		t.Fatalf("WriteComBinlogDump failed: %v", err)
	}

	data, err = sConn.ReadPacket()
	if err != nil {
		t.Fatalf("sConn.ReadPacket - ComBinlogDump failed: %v", err)
	}

	expectedData = []byte{
		mysql.ComBinlogDump,
		0x08, 0x07, 0x06, 0x05, // binlog-pos
		0x0a, 0x09, // flags
		0x04, 0x03, 0x02, 0x01, // server-id
	}
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("ComBinlogDump returned unexpected data:\n%v\nwas expecting:\n%v", data, expectedData)
	}
}

func TestComBinlogDumpGTID(t *testing.T) {
	listener, sConn, cConn := CreateSocketPair(t)
	defer func() {
		listener.Close()
		sConn.Close()
		cConn.Close()
	}()

	// Write ComBinlogDumpGTID packet, read it, compare.
	if err := cConn.WriteComBinlogDumpGTID(0x01020304, "moofarm", 0x05060708090a0b0c, 0x0d0e, []byte{0xfa, 0xfb}); err != nil {
		t.Fatalf("WriteComBinlogDumpGTID failed: %v", err)
	}

	data, err := sConn.ReadPacket()
	if err != nil {
		t.Fatalf("sConn.ReadPacket - ComBinlogDumpGTID failed: %v", err)
	}

	expectedData := []byte{
		mysql.ComBinlogDumpGTID,
		0x0e, 0x0d, // flags
		0x04, 0x03, 0x02, 0x01, // server-id
		0x07, 0x00, 0x00, 0x00, // binlog-filename-len
		'm', 'o', 'o', 'f', 'a', 'r', 'm', // bilog-filename
		0x0c, 0x0b, 0x0a, 0x09, 0x08, 0x07, 0x06, 0x05, // binlog-pos
		0x02, 0x00, 0x00, 0x00, // data-size
		0xfa, 0xfb, // data
	}
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("ComBinlogDumpGTID returned unexpected data:\n%v\nwas expecting:\n%v", data, expectedData)
	}
	sConn.Sequence = 0

	// Write ComBinlogDumpGTID packet with no filename, read it, compare.
	if err := cConn.WriteComBinlogDumpGTID(0x01020304, "", 0x05060708090a0b0c, 0x0d0e, []byte{0xfa, 0xfb}); err != nil {
		t.Fatalf("WriteComBinlogDumpGTID failed: %v", err)
	}

	data, err = sConn.ReadPacket()
	if err != nil {
		t.Fatalf("sConn.ReadPacket - ComBinlogDumpGTID failed: %v", err)
	}

	expectedData = []byte{
		mysql.ComBinlogDumpGTID,
		0x0e, 0x0d, // flags
		0x04, 0x03, 0x02, 0x01, // server-id
		0x00, 0x00, 0x00, 0x00, // binlog-filename-len
		0x0c, 0x0b, 0x0a, 0x09, 0x08, 0x07, 0x06, 0x05, // binlog-pos
		0x02, 0x00, 0x00, 0x00, // data-size
		0xfa, 0xfb, // data
	}
	if !reflect.DeepEqual(data, expectedData) {
		t.Errorf("ComBinlogDumpGTID returned unexpected data:\n%v\nwas expecting:\n%v", data, expectedData)
	}
}
