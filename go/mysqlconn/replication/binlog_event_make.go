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

package replication

import "encoding/binary"

// This file contains utility methods to create binlog replication
// packets. They are mostly used for testing.

// NewMySQL56BinlogFormat returns a typical BinlogFormat for MySQL 5.6.
func NewMySQL56BinlogFormat() BinlogFormat {
	return BinlogFormat{
		FormatVersion:     4,
		ServerVersion:     "5.6.33-0ubuntu0.14.04.1-log",
		HeaderLength:      19,
		ChecksumAlgorithm: BinlogChecksumAlgCRC32, // most commonly used.
		HeaderSizes: []byte{
			56, 13, 0, 8, 0, 18, 0, 4, 4, 4,
			4, 18, 0, 0, 92, 0, 4, 26, 8, 0,
			0, 0, 8, 8, 8, 2, 0, 0, 0, 10,
			10, 10, 25, 25, 0},
	}
}

// NewMariaDBBinlogFormat returns a typical BinlogFormat for MariaDB 10.0.
func NewMariaDBBinlogFormat() BinlogFormat {
	return BinlogFormat{
		FormatVersion:     4,
		ServerVersion:     "10.0.13-MariaDB-1~precise-log",
		HeaderLength:      19,
		ChecksumAlgorithm: BinlogChecksumAlgOff,
		// HeaderSizes is very long because the MariaDB specific events are indexed at 160+
		HeaderSizes: []byte{
			56, 13, 0, 8, 0, 18, 0, 4, 4, 4,
			4, 18, 0, 0, 220, 0, 4, 26, 8, 0,
			0, 0, 8, 8, 8, 2, 0, 0, 0, 10,
			10, 10, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			4, 19, 4},
	}
}

// FakeBinlogStream is used to generate consistent BinlogEvent packets
// for a stream. It makes sure the ServerID and log positions are
// reasonable.
type FakeBinlogStream struct {
	// ServerID is the server ID of the originating mysql-server.
	ServerID uint32

	// LogPosition is an incrementing log position.
	LogPosition uint32

	// Timestamp is a uint32 of when the events occur. It is not changed.
	Timestamp uint32
}

// NewFakeBinlogStream returns a simple FakeBinlogStream.
func NewFakeBinlogStream() *FakeBinlogStream {
	return &FakeBinlogStream{
		ServerID:    1,
		LogPosition: 4,
		Timestamp:   1407805592,
	}
}

// Packetize adds the binlog event header to a packet, and optionally
// the checksum.
func (s *FakeBinlogStream) Packetize(f BinlogFormat, typ byte, flags uint16, data []byte) []byte {
	length := int(f.HeaderLength) + len(data)
	if typ == eFormatDescriptionEvent || f.ChecksumAlgorithm == BinlogChecksumAlgCRC32 {
		// Just add 4 zeroes to the end.
		length += 4
	}

	result := make([]byte, length)
	binary.LittleEndian.PutUint32(result[0:4], s.Timestamp)
	result[4] = typ
	binary.LittleEndian.PutUint32(result[5:9], s.ServerID)
	binary.LittleEndian.PutUint32(result[9:13], uint32(length))
	if f.HeaderLength >= 19 {
		binary.LittleEndian.PutUint32(result[13:17], s.LogPosition)
		binary.LittleEndian.PutUint16(result[17:19], flags)
	}
	copy(result[f.HeaderLength:], data)
	return result
}

// NewInvalidEvent returns an invalid event (its size is <19).
func NewInvalidEvent() BinlogEvent {
	return NewMysql56BinlogEvent([]byte{0})
}

// NewFormatDescriptionEvent creates a new FormatDescriptionEvent
// based on the provided BinlogFormat. It uses a mysql56BinlogEvent
// but could use a MariaDB one.
func NewFormatDescriptionEvent(f BinlogFormat, s *FakeBinlogStream) BinlogEvent {
	length := 2 + // binlog-version
		50 + // server version
		4 + // create timestamp
		1 + // event header length
		len(f.HeaderSizes) + // event type header lengths
		1 // (undocumented) checksum algorithm
	data := make([]byte, length)
	binary.LittleEndian.PutUint16(data[0:2], f.FormatVersion)
	copy(data[2:52], []byte(f.ServerVersion))
	binary.LittleEndian.PutUint32(data[52:56], s.Timestamp)
	data[56] = f.HeaderLength
	copy(data[57:], f.HeaderSizes)
	data[57+len(f.HeaderSizes)] = f.ChecksumAlgorithm

	ev := s.Packetize(f, eFormatDescriptionEvent, 0, data)
	return NewMysql56BinlogEvent(ev)
}

// NewInvalidFormatDescriptionEvent returns an invalid FormatDescriptionEvent.
// The binlog version is set to 3. It IsValid() though.
func NewInvalidFormatDescriptionEvent(f BinlogFormat, s *FakeBinlogStream) BinlogEvent {
	length := 75
	data := make([]byte, length)
	data[0] = 3

	ev := s.Packetize(f, eFormatDescriptionEvent, 0, data)
	return NewMysql56BinlogEvent(ev)
}

// NewRotateEvent returns a RotateEvent.
// The timestmap of such an event should be zero, so we patch it in.
func NewRotateEvent(f BinlogFormat, s *FakeBinlogStream, position uint64, filename string) BinlogEvent {
	length := 8 + // position
		len(filename)
	data := make([]byte, length)
	binary.LittleEndian.PutUint64(data[0:8], position)

	ev := s.Packetize(f, eRotateEvent, 0, data)
	ev[0] = 0
	ev[1] = 0
	ev[2] = 0
	ev[3] = 0
	return NewMysql56BinlogEvent(ev)
}

// NewQueryEvent makes up a QueryEvent based on the Query structure.
func NewQueryEvent(f BinlogFormat, s *FakeBinlogStream, q Query) BinlogEvent {
	statusVarLength := 0
	if q.Charset != nil {
		statusVarLength += 1 + 2 + 2 + 2
	}
	length := 4 + // slave proxy id
		4 + // execution time
		1 + // schema length
		2 + // error code
		2 + // status vars length
		statusVarLength +
		len(q.Database) + // schema
		1 + // [00]
		len(q.SQL) // query
	data := make([]byte, length)

	pos := 8
	data[pos] = byte(len(q.Database))
	pos += 1 + 2
	data[pos] = byte(statusVarLength)
	data[pos+1] = byte(statusVarLength >> 8)
	pos += 2
	if q.Charset != nil {
		data[pos] = QCharsetCode
		data[pos+1] = byte(q.Charset.Client)
		data[pos+2] = byte(q.Charset.Client >> 8)
		data[pos+3] = byte(q.Charset.Conn)
		data[pos+4] = byte(q.Charset.Conn >> 8)
		data[pos+5] = byte(q.Charset.Server)
		data[pos+6] = byte(q.Charset.Server >> 8)
		pos += 7
	}
	pos += copy(data[pos:pos+len(q.Database)], []byte(q.Database))
	data[pos] = 0
	pos++
	copy(data[pos:], q.SQL)

	ev := s.Packetize(f, eQueryEvent, 0, data)
	return NewMysql56BinlogEvent(ev)
}

// NewInvalidQueryEvent returns an invalid QueryEvent. IsValid is however true.
// sqlPos is out of bounds.
func NewInvalidQueryEvent(f BinlogFormat, s *FakeBinlogStream) BinlogEvent {
	length := 100
	data := make([]byte, length)
	data[4+4] = 200 // > 100

	ev := s.Packetize(f, eQueryEvent, 0, data)
	return NewMysql56BinlogEvent(ev)
}

// NewXIDEvent returns a XID event. We do not use the data, so keep it 0.
func NewXIDEvent(f BinlogFormat, s *FakeBinlogStream) BinlogEvent {
	length := 8
	data := make([]byte, length)

	ev := s.Packetize(f, eXIDEvent, 0, data)
	return NewMysql56BinlogEvent(ev)
}

// NewIntVarEvent returns an IntVar event.
func NewIntVarEvent(f BinlogFormat, s *FakeBinlogStream, typ byte, value uint64) BinlogEvent {
	length := 9
	data := make([]byte, length)

	data[0] = typ
	data[1] = byte(value)
	data[2] = byte(value >> 8)
	data[3] = byte(value >> 16)
	data[4] = byte(value >> 24)
	data[5] = byte(value >> 32)
	data[6] = byte(value >> 40)
	data[7] = byte(value >> 48)
	data[8] = byte(value >> 56)

	ev := s.Packetize(f, eIntVarEvent, 0, data)
	return NewMysql56BinlogEvent(ev)
}

// NewMariaDBGTIDEvent returns a MariaDB specific GTID event.
// It ignores the Server in the gtid, instead uses the FakeBinlogStream.ServerID.
func NewMariaDBGTIDEvent(f BinlogFormat, s *FakeBinlogStream, gtid MariadbGTID, hasBegin bool) BinlogEvent {
	length := 8 + // sequence
		4 + // domain
		1 // flags2
	data := make([]byte, length)

	data[0] = byte(gtid.Sequence)
	data[1] = byte(gtid.Sequence >> 8)
	data[2] = byte(gtid.Sequence >> 16)
	data[3] = byte(gtid.Sequence >> 24)
	data[4] = byte(gtid.Sequence >> 32)
	data[5] = byte(gtid.Sequence >> 40)
	data[6] = byte(gtid.Sequence >> 48)
	data[7] = byte(gtid.Sequence >> 56)
	data[8] = byte(gtid.Domain)
	data[9] = byte(gtid.Domain >> 8)
	data[10] = byte(gtid.Domain >> 16)
	data[11] = byte(gtid.Domain >> 24)

	const FLStandalone = 1
	var flags2 byte
	if !hasBegin {
		flags2 |= FLStandalone
	}
	data[12] = flags2

	ev := s.Packetize(f, eMariaGTIDEvent, 0, data)
	return NewMariadbBinlogEvent(ev)
}

// NewTableMapEvent returns a TableMap event.
// Only works with post_header_length=8.
func NewTableMapEvent(f BinlogFormat, s *FakeBinlogStream, tableID uint64, tm *TableMap) BinlogEvent {
	if f.HeaderSize(eTableMapEvent) != 8 {
		panic("Not implemented, post_header_length!=8")
	}

	metadataLength := metadataTotalLength(tm.Types)

	length := 6 + // table_id
		2 + // flags
		1 + // schema name length
		len(tm.Database) +
		1 + // [00]
		1 + // table name length
		len(tm.Name) +
		1 + // [00]
		1 + // column-count FIXME(alainjobart) len enc
		len(tm.Types) +
		1 + // lenenc-str column-meta-def FIXME(alainjobart) len enc
		metadataLength +
		len(tm.CanBeNull.data)
	data := make([]byte, length)

	data[0] = byte(tableID)
	data[1] = byte(tableID >> 8)
	data[2] = byte(tableID >> 16)
	data[3] = byte(tableID >> 24)
	data[4] = byte(tableID >> 32)
	data[5] = byte(tableID >> 40)
	data[6] = byte(tm.Flags)
	data[7] = byte(tm.Flags >> 8)
	data[8] = byte(len(tm.Database))
	pos := 6 + 2 + 1 + copy(data[9:], []byte(tm.Database))
	data[pos] = 0
	pos++
	data[pos] = byte(len(tm.Name))
	pos += 1 + copy(data[pos+1:], []byte(tm.Name))
	data[pos] = 0
	pos++

	data[pos] = byte(len(tm.Types)) // FIXME(alainjobart) lenenc
	pos++

	pos += copy(data[pos:], tm.Types)

	// Per-column meta data. Starting with len-enc length.
	// FIXME(alainjobart) lenenc
	data[pos] = byte(metadataLength)
	pos++
	for c, typ := range tm.Types {
		pos = metadataWrite(data, pos, typ, tm.Metadata[c])
	}

	pos += copy(data[pos:], tm.CanBeNull.data)
	if pos != len(data) {
		panic("bad encoding")
	}

	ev := s.Packetize(f, eTableMapEvent, 0, data)
	return NewMariadbBinlogEvent(ev)
}

// NewWriteRowsEvent returns a WriteRows event. Uses v2.
func NewWriteRowsEvent(f BinlogFormat, s *FakeBinlogStream, tableID uint64, rows Rows) BinlogEvent {
	return newRowsEvent(f, s, eWriteRowsEventV2, tableID, rows)
}

// NewUpdateRowsEvent returns an UpdateRows event. Uses v2.
func NewUpdateRowsEvent(f BinlogFormat, s *FakeBinlogStream, tableID uint64, rows Rows) BinlogEvent {
	return newRowsEvent(f, s, eUpdateRowsEventV2, tableID, rows)
}

// NewDeleteRowsEvent returns an DeleteRows event. Uses v2.
func NewDeleteRowsEvent(f BinlogFormat, s *FakeBinlogStream, tableID uint64, rows Rows) BinlogEvent {
	return newRowsEvent(f, s, eDeleteRowsEventV2, tableID, rows)
}

// newRowsEvent can create an event of type:
// eWriteRowsEventV1, eWriteRowsEventV2,
// eUpdateRowsEventV1, eUpdateRowsEventV2,
// eDeleteRowsEventV1, eDeleteRowsEventV2.
func newRowsEvent(f BinlogFormat, s *FakeBinlogStream, typ byte, tableID uint64, rows Rows) BinlogEvent {
	if f.HeaderSize(typ) == 6 {
		panic("Not implemented, post_header_length==6")
	}

	length := 6 + // table id
		2 + // flags
		2 + // extra data length, no extra data.
		1 + // num columns FIXME(alainjobart) len enc
		len(rows.IdentifyColumns.data) + // only > 0 for Update & Delete
		len(rows.DataColumns.data) // only > 0 for Write & Update
	for _, row := range rows.Rows {
		length += len(row.NullIdentifyColumns.data) +
			len(row.NullColumns.data) +
			len(row.Identify) +
			len(row.Data)
	}
	data := make([]byte, length)

	hasIdentify := typ == eUpdateRowsEventV1 || typ == eUpdateRowsEventV2 ||
		typ == eDeleteRowsEventV1 || typ == eDeleteRowsEventV2
	hasData := typ == eWriteRowsEventV1 || typ == eWriteRowsEventV2 ||
		typ == eUpdateRowsEventV1 || typ == eUpdateRowsEventV2

	data[0] = byte(tableID)
	data[1] = byte(tableID >> 8)
	data[2] = byte(tableID >> 16)
	data[3] = byte(tableID >> 24)
	data[4] = byte(tableID >> 32)
	data[5] = byte(tableID >> 40)
	data[6] = byte(rows.Flags)
	data[7] = byte(rows.Flags >> 8)
	data[8] = 0x02
	data[9] = 0x00

	if hasIdentify {
		data[10] = byte(rows.IdentifyColumns.Count()) // FIXME(alainjobart) len
	} else {
		data[10] = byte(rows.DataColumns.Count()) // FIXME(alainjobart) len
	}
	pos := 11

	if hasIdentify {
		pos += copy(data[pos:], rows.IdentifyColumns.data)
	}
	if hasData {
		pos += copy(data[pos:], rows.DataColumns.data)
	}

	for _, row := range rows.Rows {
		if hasIdentify {
			pos += copy(data[pos:], row.NullIdentifyColumns.data)
			pos += copy(data[pos:], row.Identify)
		}
		if hasData {
			pos += copy(data[pos:], row.NullColumns.data)
			pos += copy(data[pos:], row.Data)
		}
	}

	ev := s.Packetize(f, typ, 0, data)
	return NewMysql56BinlogEvent(ev)
}
