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

// This file contains the methods related to replication.

// WriteComBinlogDump writes a ComBinlogDump command.
// See http://dev.mysql.com/doc/internals/en/com-binlog-dump.html for syntax.
// Returns a SQLError.
func (c *Conn) WriteComBinlogDump(serverID uint32, binlogFilename string, binlogPos uint32, flags uint16) error {
	c.sequence = 0
	length := 1 + // ComBinlogDump
		4 + // binlog-pos
		2 + // flags
		4 + // server-id
		len(binlogFilename) // binlog-filename
	data := c.startEphemeralPacket(length)
	pos := writeByte(data, 0, ComBinlogDump)
	pos = writeUint32(data, pos, binlogPos)
	pos = writeUint16(data, pos, flags)
	pos = writeUint32(data, pos, serverID)
	_ = writeEOFString(data, pos, binlogFilename)
	if err := c.writeEphemeralPacket(); err != nil {
		return NewSQLError(CRServerGone, SSUnknownSQLState, "%v", err)
	}
	return nil
}

// WriteComBinlogDumpGTID writes a ComBinlogDumpGTID command.
// Only works with MySQL 5.6+ (and not MariaDB).
// See http://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html for syntax.
func (c *Conn) WriteComBinlogDumpGTID(serverID uint32, binlogFilename string, binlogPos uint64, flags uint16, gtidSet []byte) error {
	c.sequence = 0
	length := 1 + // ComBinlogDumpGTID
		2 + // flags
		4 + // server-id
		4 + // binlog-filename-len
		len(binlogFilename) + // binlog-filename
		8 + // binlog-pos
		4 + // data-size
		len(gtidSet) // data
	data := c.startEphemeralPacket(length)
	pos := writeByte(data, 0, ComBinlogDumpGTID)
	pos = writeUint16(data, pos, flags)
	pos = writeUint32(data, pos, serverID)
	pos = writeUint32(data, pos, uint32(len(binlogFilename)))
	pos = writeEOFString(data, pos, binlogFilename)
	pos = writeUint64(data, pos, binlogPos)
	pos = writeUint32(data, pos, uint32(len(gtidSet)))
	pos += copy(data[pos:], gtidSet)
	if err := c.writeEphemeralPacket(); err != nil {
		return NewSQLError(CRServerGone, SSUnknownSQLState, "%v", err)
	}
	return nil
}
