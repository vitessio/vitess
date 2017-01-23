package replication

import (
	"encoding/binary"
	"time"
)

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

// FakeBinlogStream is used to generate consistent BinlogEvent packets
// for a stream. It makes sure the ServerID and log positions are
// reasonable.
type FakeBinlogStream struct {
	// ServerID is the server ID of the originating mysql-server.
	ServerID uint32

	// LogPosition is an incrementing log position.
	LogPosition uint32
}

// NewFakeBinlogStream returns a simple FakeBinlogStream.
func NewFakeBinlogStream() *FakeBinlogStream {
	return &FakeBinlogStream{
		ServerID:    1,
		LogPosition: 4,
	}
}

// Packetize adds the binlog event header to a packet, and optionally
// the checksum.
func (s *FakeBinlogStream) Packetize(f BinlogFormat, typ byte, flags uint16, data []byte) []byte {
	timestamp := uint32(time.Now().Unix())

	length := int(f.HeaderLength) + len(data)
	if typ == eFormatDescriptionEvent || f.ChecksumAlgorithm == BinlogChecksumAlgCRC32 {
		// Just add 4 zeroes to the end.
		length += 4
	}

	result := make([]byte, length)
	binary.LittleEndian.PutUint32(result[0:4], timestamp)
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
	timestamp := uint32(time.Now().Unix())

	length := 2 + // binlog-version
		50 + // server version
		4 + // create timestamp
		1 + // event header length
		len(f.HeaderSizes) + // event type header lengths
		1 // (undocumented) checksum algorithm
	data := make([]byte, length)
	binary.LittleEndian.PutUint16(data[0:2], f.FormatVersion)
	copy(data[2:52], []byte(f.ServerVersion))
	binary.LittleEndian.PutUint32(data[52:56], timestamp)
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
	return nil
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
