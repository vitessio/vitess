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
	"encoding/binary"
	"fmt"
	"strconv"
)

// filePosBinlogEvent wraps a raw packet buffer and provides methods to examine
// it by implementing BinlogEvent. Some methods are pulled in from binlogEvent.
type filePosBinlogEvent struct {
	binlogEvent
}

func (*filePosBinlogEvent) GTID(BinlogFormat) (GTID, bool, error) {
	return nil, false, nil
}

func (*filePosBinlogEvent) IsGTID() bool {
	return false
}

func (*filePosBinlogEvent) PreviousGTIDs(BinlogFormat) (Position, error) {
	return Position{}, fmt.Errorf("filePos should not provide PREVIOUS_GTIDS_EVENT events")
}

// StripChecksum implements BinlogEvent.StripChecksum().
func (ev *filePosBinlogEvent) StripChecksum(f BinlogFormat) (BinlogEvent, []byte, error) {
	switch f.ChecksumAlgorithm {
	case BinlogChecksumAlgOff, BinlogChecksumAlgUndef:
		// There is no checksum.
		return ev, nil, nil
	default:
		// Checksum is the last 4 bytes of the event buffer.
		data := ev.Bytes()
		length := len(data)
		checksum := data[length-4:]
		data = data[:length-4]
		return &filePosBinlogEvent{binlogEvent: binlogEvent(data)}, checksum, nil
	}
}

// nextPosition returns the next file position of the binlog.
// If no information is available, it returns 0.
func (ev *filePosBinlogEvent) nextPosition(f BinlogFormat) int {
	if f.HeaderLength <= 13 {
		// Dead code. This is just a failsafe.
		return 0
	}
	return int(binary.LittleEndian.Uint32(ev.Bytes()[13:17]))
}

// rotate implements BinlogEvent.Rotate().
//
// Expected format (L = total length of event data):
//   # bytes  field
//   8        position
//   8:L      file
func (ev *filePosBinlogEvent) rotate(f BinlogFormat) (int, string) {
	data := ev.Bytes()[f.HeaderLength:]
	pos := binary.LittleEndian.Uint64(data[0:8])
	file := data[8:]
	return int(pos), string(file)
}

//----------------------------------------------------------------------------

// filePosGTIDEvent is a fake GTID event for filePos.
type filePosGTIDEvent struct {
	gtid      filePosGTID
	timestamp uint32
	binlogEvent
}

func newFilePosGTIDEvent(file string, pos int, timestamp uint32) filePosGTIDEvent {
	return filePosGTIDEvent{
		gtid: filePosGTID{
			file: file,
			pos:  strconv.Itoa(pos),
		},
		timestamp: timestamp,
	}
}

func (ev filePosGTIDEvent) IsPseudo() bool {
	return true
}

func (ev filePosGTIDEvent) IsGTID() bool {
	return false
}

func (ev filePosGTIDEvent) IsValid() bool {
	return true
}

func (ev filePosGTIDEvent) IsFormatDescription() bool {
	return false
}

func (ev filePosGTIDEvent) IsRotate() bool {
	return false
}

func (ev filePosGTIDEvent) Timestamp() uint32 {
	return ev.timestamp
}

func (ev filePosGTIDEvent) GTID(BinlogFormat) (GTID, bool, error) {
	return ev.gtid, false, nil
}

func (ev filePosGTIDEvent) PreviousGTIDs(BinlogFormat) (Position, error) {
	return Position{}, fmt.Errorf("filePos should not provide PREVIOUS_GTIDS_EVENT events")
}

// StripChecksum implements BinlogEvent.StripChecksum().
func (ev filePosGTIDEvent) StripChecksum(f BinlogFormat) (BinlogEvent, []byte, error) {
	return ev, nil, nil
}
