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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var (
	// BinglogMagicNumber is 4-byte number at the beginning of every binary log
	BinglogMagicNumber = []byte{0xfe, 0x62, 0x69, 0x6e}
	readPacketErr      = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "error reading BinlogDumpGTID packet")
)

func (c *Conn) parseComBinlogDumpGTID(data []byte) (logFile string, logPos uint64, position Position, err error) {
	pos := 1
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

	dataSize, pos, ok := readUint32(data, pos)
	if !ok {
		return logFile, logPos, position, readPacketErr
	}
	if gtid := string(data[pos : pos+int(dataSize)]); gtid != "" {
		position, err = DecodePosition(gtid)
		if err != nil {
			return logFile, logPos, position, err
		}
	}

	return logFile, logPos, position, nil
}
