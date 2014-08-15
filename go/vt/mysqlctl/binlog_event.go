// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"bytes"
	"encoding/binary"
	"fmt"

	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
)

// binlogEvent wraps a raw packet buffer and provides methods to examine it
// by partially implementing blproto.BinlogEvent. These methods can be composed
// into flavor-specific event types to pull in common parsing code.
//
// The default v4 header format is:
//                  offset : size
//   +============================+
//   | timestamp         0 : 4    |
//   +----------------------------+
//   | type_code         4 : 1    |
//   +----------------------------+
//   | server_id         5 : 4    |
//   +----------------------------+
//   | event_length      9 : 4    |
//   +----------------------------+
//   | next_position    13 : 4    |
//   +----------------------------+
//   | flags            17 : 2    |
//   +----------------------------+
//   | extra_headers    19 : x-19 |
//   +============================+
//   http://dev.mysql.com/doc/internals/en/event-header-fields.html
type binlogEvent []byte

// IsValid implements BinlogEvent.IsValid().
func (ev binlogEvent) IsValid() bool {
	bufLen := len(ev.Bytes())

	// The buffer must be at least 19 bytes to contain a valid header.
	if bufLen < 19 {
		return false
	}

	// It's now safe to use methods that examine header fields.
	// Let's see if the event is right about its own size.
	evLen := ev.Length()
	if evLen < 19 || evLen != uint32(bufLen) {
		return false
	}

	// Everything's there, so we shouldn't have any out-of-bounds issues while
	// reading header fields or constant-offset data fields. We should still check
	// bounds any time we compute an offset based on values in the buffer itself.
	return true
}

// Bytes returns the underlying byte buffer.
func (ev binlogEvent) Bytes() []byte {
	return []byte(ev)
}

// Type returns the type_code field from the header.
func (ev binlogEvent) Type() byte {
	return ev.Bytes()[4]
}

// Flags returns the flags field from the header.
func (ev binlogEvent) Flags() uint16 {
	return binary.LittleEndian.Uint16(ev.Bytes()[17 : 17+2])
}

// Timestamp returns the timestamp field from the header.
func (ev binlogEvent) Timestamp() uint32 {
	return binary.LittleEndian.Uint32(ev.Bytes()[:4])
}

// ServerID returns the server_id field from the header.
func (ev binlogEvent) ServerID() uint32 {
	return binary.LittleEndian.Uint32(ev.Bytes()[5 : 5+4])
}

// Length returns the event_length field from the header.
func (ev binlogEvent) Length() uint32 {
	return binary.LittleEndian.Uint32(ev.Bytes()[9 : 9+4])
}

// IsFormatDescription implements BinlogEvent.IsFormatDescription().
func (ev binlogEvent) IsFormatDescription() bool {
	return ev.Type() == 15
}

// IsQuery implements BinlogEvent.IsQuery().
func (ev binlogEvent) IsQuery() bool {
	return ev.Type() == 2
}

// IsRotate implements BinlogEvent.IsRotate().
func (ev binlogEvent) IsRotate() bool {
	return ev.Type() == 4
}

// IsXID implements BinlogEvent.IsXID().
func (ev binlogEvent) IsXID() bool {
	return ev.Type() == 16
}

// IsIntVar implements BinlogEvent.IsIntVar().
func (ev binlogEvent) IsIntVar() bool {
	return ev.Type() == 5
}

// IsRand implements BinlogEvent.IsRand().
func (ev binlogEvent) IsRand() bool {
	return ev.Type() == 13
}

// Format implements BinlogEvent.Format().
//
// Expected format (L = total length of event data):
//   # bytes   field
//   2         format version
//   50        server version string, 0-padded but not necessarily 0-terminated
//   4         timestamp (same as timestamp header field)
//   1         header length
func (ev binlogEvent) Format() (f blproto.BinlogFormat, err error) {
	// FORMAT_DESCRIPTION_EVENT has a fixed header size of 19 because we have to
	// read it before we know the header_length.
	data := ev.Bytes()[19:]

	f.FormatVersion = binary.LittleEndian.Uint16(data[:2])
	if f.FormatVersion != 4 {
		return f, fmt.Errorf("format version = %d, we only support version 4", f.FormatVersion)
	}
	f.ServerVersion = string(bytes.TrimRight(data[2:2+50], "\x00"))
	f.HeaderLength = data[2+50+4]
	if f.HeaderLength < 19 {
		return f, fmt.Errorf("header length = %d, should be >= 19", f.HeaderLength)
	}
	return f, nil
}

// Query implements BinlogEvent.Query().
//
// Expected format (L = total length of event data):
//   # bytes   field
//   4         thread_id
//   4         execution time
//   1         length of db_name, not including NULL terminator (X)
//   2         error code
//   2         length of status vars block (Y)
//   Y         status vars block
//   X+1       db_name + NULL terminator
//   L-X-1-Y   SQL statement (no NULL terminator)
func (ev binlogEvent) Query(f blproto.BinlogFormat) (string, []byte, error) {
	data := ev.Bytes()[f.HeaderLength:]

	// length of database name
	dbNameLen := int(data[4+4])
	// length of status variables block
	varsLen := int(binary.LittleEndian.Uint16(data[4+4+1+2 : 4+4+1+2+2]))

	// position of database name
	dbPos := 4 + 4 + 1 + 2 + 2 + varsLen
	// position of SQL query
	sqlPos := dbPos + dbNameLen + 1 // +1 for NULL terminator
	if sqlPos > len(data) {
		return "", nil, fmt.Errorf("SQL query position = %v, which is outside buffer", sqlPos)
	}
	return string(data[dbPos : dbPos+dbNameLen]), data[sqlPos:], nil
}

// IntVar implements BinlogEvent.IntVar().
//
// Expected format (L = total length of event data):
//   # bytes   field
//   1         variable ID
//   8         variable value
func (ev binlogEvent) IntVar(f blproto.BinlogFormat) (name string, value uint64, err error) {
	data := ev.Bytes()[f.HeaderLength:]

	switch data[0] {
	case 1:
		name = "LAST_INSERT_ID"
	case 2:
		name = "INSERT_ID"
	default:
		return "", 0, fmt.Errorf("invalid IntVar ID: %v", data[0])
	}

	value = binary.LittleEndian.Uint64(data[1 : 1+8])
	return name, value, nil
}

// Rand implements BinlogEvent.Rand().
//
// Expected format (L = total length of event data):
//   # bytes   field
//   8         seed 1
//   8         seed 2
func (ev binlogEvent) Rand(f blproto.BinlogFormat) (seed1 uint64, seed2 uint64, err error) {
	data := ev.Bytes()[f.HeaderLength:]
	seed1 = binary.LittleEndian.Uint64(data[0:8])
	seed2 = binary.LittleEndian.Uint64(data[8 : 8+8])
	return seed1, seed2, nil
}
