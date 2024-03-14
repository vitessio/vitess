/*
Copyright 2022 The Vitess Authors.

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
	vtrpcpb "github.com/dolthub/vitess/go/vt/proto/vtrpc"
	"github.com/dolthub/vitess/go/vt/vterrors"
	"io"
)

var (
	// BinglogMagicNumber is 4-byte number at the beginning of every binary log
	BinglogMagicNumber = []byte{0xfe, 0x62, 0x69, 0x6e}
	readPacketErr      = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "error reading BinlogDumpGTID packet")
)

const (
	BinlogDumpNonBlock    = 0x01
	BinlogThroughPosition = 0x02
	BinlogThroughGTID     = 0x04
)

func (c *Conn) parseComBinlogDump(data []byte) (logFile string, binlogPos uint32, err error) {
	pos := 1

	binlogPos, pos, ok := readUint32(data, pos)
	if !ok {
		return logFile, binlogPos, readPacketErr
	}

	pos += 2 // flags
	pos += 4 // server-id

	logFile = string(data[pos:])
	return logFile, binlogPos, nil
}

func (c *Conn) parseComBinlogDumpGTID(data []byte) (logFile string, logPos uint64, position Position, err error) {
	// see https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html
	pos := 1

	flags := binary.LittleEndian.Uint16(data[pos : pos+2])
	pos += 2 // flags
	pos += 4 // server-id

	fileNameLen, pos, ok := readUint32(data, pos)
	if !ok {
		return logFile, logPos, position, readPacketErr
	}
	logFile = string(data[pos : pos+int(fileNameLen)])
	pos += int(fileNameLen)

	logPos, pos, ok = readUint64(data, pos)
	if !ok {
		return logFile, logPos, position, readPacketErr
	}

	if flags&BinlogDumpNonBlock != 0 {
		return logFile, logPos, position, io.EOF
	}
	if flags&BinlogThroughGTID != 0 {
		dataSize, pos, ok := readUint32(data, pos)
		if !ok {
			return logFile, logPos, position, readPacketErr
		}

		// NOTE: In testing a replication connection with a MySQL 8.0 replica, the replica sends back an 8 byte
		//       GTID will all zero valued bytes, but Vitess was unable to parse it, so we added the nilGtid
		//       check below. We may be able to remove this check if we find the primary is not sending back
		//       some missing metadata, but documentation seems to indicate that the replica is allowed to send
		//       an empty GTID set when no GTID was specified in the start replica statement. Looking through
		//       MySQL primary <-> MySQL replica wire logs could help prove this out.
		gtidBytes := data[pos : pos+int(dataSize)]
		if gtid := string(gtidBytes); gtid != "" && !isNilGtid(gtidBytes) {
			position, err = DecodePosition(gtid)
			if err != nil {
				return logFile, logPos, position, err
			}
		}
	}

	return logFile, logPos, position, nil
}

// isNilGtid returns true if the specified |bytes| that represent a serialized GTID are all zero valued,
// otherwise returns false.
func isNilGtid(bytes []byte) bool {
	for _, b := range bytes {
		if uint(b) != 0 {
			return false
		}
	}
	return true
}
