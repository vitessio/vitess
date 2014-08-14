// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"reflect"
	"testing"
)

func TestMakeBinlogDumpCommand(t *testing.T) {
	want := []byte{
		// binlog_pos
		0x78, 0x56, 0x34, 0x12,
		// binlog_flags
		0xfe, 0xca,
		// server_id
		0xef, 0xbe, 0xad, 0xde,
		// binlog_filename
		0x6d, 0x6f, 0x6f, 0x66, 0x61, 0x72, 0x6d, // "moofarm"
	}

	got := makeBinlogDumpCommand(0x12345678, 0xcafe, 0xdeadbeef, "moofarm")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("makeBinlogDumpCommand() = %#v, want %#v", got, want)
	}
}

func TestMakeBinlogDumpCommandEmptyFilename(t *testing.T) {
	want := []byte{
		// binlog_pos
		0x78, 0x56, 0x34, 0x12,
		// binlog_flags
		0xfe, 0xca,
		// server_id
		0xef, 0xbe, 0xad, 0xde,
		// binlog_filename
		// ""
	}

	got := makeBinlogDumpCommand(0x12345678, 0xcafe, 0xdeadbeef, "")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("makeBinlogDumpCommand() = %#v, want %#v", got, want)
	}
}
