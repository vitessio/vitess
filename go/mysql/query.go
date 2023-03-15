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
	"fmt"
	"math"
	"strconv"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// This file contains the methods related to queries.

//
// Client side methods.
//

// WriteComQuery writes a query for the server to execute.
// Client -> Server.
// Returns SQLError(CRServerGone) if it can't.
func (c *Conn) WriteComQuery(query string) error {
	// This is a new command, need to reset the sequence.
	c.sequence = 0

	data, pos := c.startEphemeralPacketWithHeader(len(query) + 1)
	data[pos] = ComQuery
	pos++
	copy(data[pos:], query)
	if err := c.writeEphemeralPacket(); err != nil {
		return NewSQLError(CRServerGone, SSUnknownSQLState, err.Error())
	}
	return nil
}

// writeComInitDB changes the default database to use.
// Client -> Server.
// Returns SQLError(CRServerGone) if it can't.
func (c *Conn) writeComInitDB(db string) error {
	data, pos := c.startEphemeralPacketWithHeader(len(db) + 1)
	data[pos] = ComInitDB
	pos++
	copy(data[pos:], db)
	if err := c.writeEphemeralPacket(); err != nil {
		return NewSQLError(CRServerGone, SSUnknownSQLState, err.Error())
	}
	return nil
}

// writeComSetOption changes the connection's capability of executing multi statements.
// Returns SQLError(CRServerGone) if it can't.
func (c *Conn) writeComSetOption(operation uint16) error {
	data, pos := c.startEphemeralPacketWithHeader(16 + 1)
	data[pos] = ComSetOption
	pos++
	writeUint16(data, pos, operation)
	if err := c.writeEphemeralPacket(); err != nil {
		return NewSQLError(CRServerGone, SSUnknownSQLState, err.Error())
	}
	return nil
}

// readColumnDefinition reads the next Column Definition packet.
// Returns a SQLError.
func (c *Conn) readColumnDefinition(field *querypb.Field, index int) error {
	colDef, err := c.readEphemeralPacket()
	if err != nil {
		return NewSQLError(CRServerLost, SSUnknownSQLState, "%v", err)
	}
	defer c.recycleReadPacket()

	// Catalog is ignored, always set to "def"
	pos, ok := skipLenEncString(colDef, 0)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "skipping col %v catalog failed", index)
	}

	// schema, table, orgTable, name and OrgName are strings.
	field.Database, pos, ok = readLenEncString(colDef, pos)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "extracting col %v schema failed", index)
	}
	field.Table, pos, ok = readLenEncString(colDef, pos)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "extracting col %v table failed", index)
	}
	field.OrgTable, pos, ok = readLenEncString(colDef, pos)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "extracting col %v org_table failed", index)
	}
	field.Name, pos, ok = readLenEncString(colDef, pos)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "extracting col %v name failed", index)
	}
	field.OrgName, pos, ok = readLenEncString(colDef, pos)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "extracting col %v org_name failed", index)
	}

	// Skip length of fixed-length fields.
	pos++

	// characterSet is a uint16.
	characterSet, pos, ok := readUint16(colDef, pos)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "extracting col %v characterSet failed", index)
	}
	field.Charset = uint32(characterSet)

	// columnLength is a uint32.
	field.ColumnLength, pos, ok = readUint32(colDef, pos)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "extracting col %v columnLength failed", index)
	}

	// type is one byte.
	t, pos, ok := readByte(colDef, pos)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "extracting col %v type failed", index)
	}

	// flags is 2 bytes.
	flags, pos, ok := readUint16(colDef, pos)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "extracting col %v flags failed", index)
	}

	// Convert MySQL type to Vitess type.
	field.Type, err = sqltypes.MySQLToType(int64(t), int64(flags))
	if err != nil {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "MySQLToType(%v,%v) failed for column %v: %v", t, flags, index, err)
	}
	// Decimals is a byte.
	decimals, _, ok := readByte(colDef, pos)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "extracting col %v decimals failed", index)
	}
	field.Decimals = uint32(decimals)

	// If we didn't get column length or character set,
	// we assume the original row on the other side was encoded from
	// a Field without that data, so we don't return the flags.
	if field.ColumnLength != 0 || field.Charset != 0 {
		field.Flags = uint32(flags)

		// FIXME(alainjobart): This is something the MySQL
		// client library does: If the type is numerical, it
		// adds a NUM_FLAG to the flags.  We're doing it here
		// only to be compatible with the C library. Once
		// we're not using that library any more, we'll remove this.
		// See doc.go.
		if IsNum(t) {
			field.Flags |= uint32(querypb.MySqlFlag_NUM_FLAG)
		}
	}

	return nil
}

// readColumnDefinitionType is a faster version of
// readColumnDefinition that only fills in the Type.
// Returns a SQLError.
func (c *Conn) readColumnDefinitionType(field *querypb.Field, index int) error {
	colDef, err := c.readEphemeralPacket()
	if err != nil {
		return NewSQLError(CRServerLost, SSUnknownSQLState, "%v", err)
	}
	defer c.recycleReadPacket()

	// catalog, schema, table, orgTable, name and orgName are
	// strings, all skipped.
	pos, ok := skipLenEncString(colDef, 0)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "skipping col %v catalog failed", index)
	}
	pos, ok = skipLenEncString(colDef, pos)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "skipping col %v schema failed", index)
	}
	pos, ok = skipLenEncString(colDef, pos)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "skipping col %v table failed", index)
	}
	pos, ok = skipLenEncString(colDef, pos)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "skipping col %v org_table failed", index)
	}
	pos, ok = skipLenEncString(colDef, pos)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "skipping col %v name failed", index)
	}
	pos, ok = skipLenEncString(colDef, pos)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "skipping col %v org_name failed", index)
	}

	// Skip length of fixed-length fields.
	pos++

	// characterSet is a uint16.
	_, pos, ok = readUint16(colDef, pos)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "extracting col %v characterSet failed", index)
	}

	// columnLength is a uint32.
	_, pos, ok = readUint32(colDef, pos)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "extracting col %v columnLength failed", index)
	}

	// type is one byte
	t, pos, ok := readByte(colDef, pos)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "extracting col %v type failed", index)
	}

	// flags is 2 bytes
	flags, _, ok := readUint16(colDef, pos)
	if !ok {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "extracting col %v flags failed", index)
	}

	// Convert MySQL type to Vitess type.
	field.Type, err = sqltypes.MySQLToType(int64(t), int64(flags))
	if err != nil {
		return NewSQLError(CRMalformedPacket, SSUnknownSQLState, "MySQLToType(%v,%v) failed for column %v: %v", t, flags, index, err)
	}

	// skip decimals

	return nil
}

// parseRow parses an individual row.
// Returns a SQLError.
func (c *Conn) parseRow(data []byte, fields []*querypb.Field, reader func([]byte, int) ([]byte, int, bool), result []sqltypes.Value) ([]sqltypes.Value, error) {
	colNumber := len(fields)
	if result == nil {
		result = make([]sqltypes.Value, 0, colNumber)
	}
	pos := 0
	for i := 0; i < colNumber; i++ {
		if data[pos] == NullValue {
			result = append(result, sqltypes.Value{})
			pos++
			continue
		}
		var s []byte
		var ok bool
		s, pos, ok = reader(data, pos)
		if !ok {
			return nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "decoding string failed")
		}
		result = append(result, sqltypes.MakeTrusted(fields[i].Type, s))
	}
	return result, nil
}

// ExecuteFetch executes a query and returns the result.
// Returns a SQLError. Depending on the transport used, the error
// returned might be different for the same condition:
//
// 1. if the server closes the connection when no command is in flight:
//
//		1.1 unix: WriteComQuery will fail with a 'broken pipe', and we'll
//		    return CRServerGone(2006).
//
//		1.2 tcp: WriteComQuery will most likely work, but readComQueryResponse
//		    will fail, and we'll return CRServerLost(2013).
//
//		    This is because closing a TCP socket on the server side sends
//		    a FIN to the client (telling the client the server is done
//		    writing), but on most platforms doesn't send a RST.  So the
//		    client has no idea it can't write. So it succeeds writing data, which
//		    *then* triggers the server to send a RST back, received a bit
//		    later. By then, the client has already started waiting for
//		    the response, and will just return a CRServerLost(2013).
//		    So CRServerGone(2006) will almost never be seen with TCP.
//
//	 2. if the server closes the connection when a command is in flight,
//	    readComQueryResponse will fail, and we'll return CRServerLost(2013).
func (c *Conn) ExecuteFetch(query string, maxrows int, wantfields bool) (result *sqltypes.Result, err error) {
	result, _, err = c.ExecuteFetchMulti(query, maxrows, wantfields)
	return result, err
}

// ExecuteFetchMulti is for fetching multiple results from a multi-statement result.
// It returns an additional 'more' flag. If it is set, you must fetch the additional
// results using ReadQueryResult.
func (c *Conn) ExecuteFetchMulti(query string, maxrows int, wantfields bool) (result *sqltypes.Result, more bool, err error) {
	defer func() {
		if err != nil {
			if sqlerr, ok := err.(*SQLError); ok {
				sqlerr.Query = query
			}
		}
	}()

	// Send the query as a COM_QUERY packet.
	if err = c.WriteComQuery(query); err != nil {
		return nil, false, err
	}

	res, more, _, err := c.ReadQueryResult(maxrows, wantfields)
	if err != nil {
		return nil, false, err
	}
	return res, more, err
}

// ExecuteFetchWithWarningCount is for fetching results and a warning count
// Note: In a future iteration this should be abolished and merged into the
// ExecuteFetch API.
func (c *Conn) ExecuteFetchWithWarningCount(query string, maxrows int, wantfields bool) (result *sqltypes.Result, warnings uint16, err error) {
	defer func() {
		if err != nil {
			if sqlerr, ok := err.(*SQLError); ok {
				sqlerr.Query = query
			}
		}
	}()

	// Send the query as a COM_QUERY packet.
	if err = c.WriteComQuery(query); err != nil {
		return nil, 0, err
	}

	res, _, warnings, err := c.ReadQueryResult(maxrows, wantfields)
	return res, warnings, err
}

// ReadQueryResult gets the result from the last written query.
func (c *Conn) ReadQueryResult(maxrows int, wantfields bool) (*sqltypes.Result, bool, uint16, error) {
	// Get the result.
	colNumber, packetOk, err := c.readComQueryResponse()
	if err != nil {
		return nil, false, 0, err
	}
	more := packetOk.statusFlags&ServerMoreResultsExists != 0
	warnings := packetOk.warnings
	if colNumber == 0 {
		// OK packet, means no results. Just use the numbers.
		return &sqltypes.Result{
			RowsAffected:        packetOk.affectedRows,
			InsertID:            packetOk.lastInsertID,
			SessionStateChanges: packetOk.sessionStateData,
			StatusFlags:         packetOk.statusFlags,
			Info:                packetOk.info,
		}, more, warnings, nil
	}

	fields := make([]querypb.Field, colNumber)
	result := &sqltypes.Result{
		Fields: make([]*querypb.Field, colNumber),
	}

	// Read column headers. One packet per column.
	// Build the fields.
	for i := 0; i < colNumber; i++ {
		result.Fields[i] = &fields[i]

		if wantfields {
			if err := c.readColumnDefinition(result.Fields[i], i); err != nil {
				return nil, false, 0, err
			}
		} else {
			if err := c.readColumnDefinitionType(result.Fields[i], i); err != nil {
				return nil, false, 0, err
			}
		}
	}

	if c.Capabilities&CapabilityClientDeprecateEOF == 0 {
		// EOF is only present here if it's not deprecated.
		data, err := c.readEphemeralPacket()
		if err != nil {
			return nil, false, 0, NewSQLError(CRServerLost, SSUnknownSQLState, "%v", err)
		}
		if c.isEOFPacket(data) {

			// This is what we expect.
			// Warnings and status flags are ignored.
			c.recycleReadPacket()
			// goto: read row loop

		} else if isErrorPacket(data) {
			defer c.recycleReadPacket()
			return nil, false, 0, ParseErrorPacket(data)
		} else {
			defer c.recycleReadPacket()
			return nil, false, 0, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected packet after fields: %v", data)
		}
	}

	// read each row until EOF or OK packet.
	for {
		data, err := c.readEphemeralPacket()
		if err != nil {
			return nil, false, 0, NewSQLError(CRServerLost, SSUnknownSQLState, "%v", err)
		}

		if c.isEOFPacket(data) {
			defer c.recycleReadPacket()

			// Strip the partial Fields before returning.
			if !wantfields {
				result.Fields = nil
			}

			// The deprecated EOF packets change means that this is either an
			// EOF packet or an OK packet with the EOF type code.
			if c.Capabilities&CapabilityClientDeprecateEOF == 0 {
				var statusFlags uint16
				warnings, statusFlags, err = parseEOFPacket(data)
				if err != nil {
					return nil, false, 0, err
				}
				more = (statusFlags & ServerMoreResultsExists) != 0
				result.StatusFlags = statusFlags
			} else {
				packetOk, err := c.parseOKPacket(data)
				if err != nil {
					return nil, false, 0, err
				}
				warnings = packetOk.warnings
				more = (packetOk.statusFlags & ServerMoreResultsExists) != 0
				result.SessionStateChanges = packetOk.sessionStateData
				result.StatusFlags = packetOk.statusFlags
				result.Info = packetOk.info
			}
			return result, more, warnings, nil

		} else if isErrorPacket(data) {
			defer c.recycleReadPacket()
			// Error packet.
			return nil, false, 0, ParseErrorPacket(data)
		}

		// Check we're not over the limit before we add more.
		if len(result.Rows) == maxrows {
			c.recycleReadPacket()
			if err := c.drainResults(); err != nil {
				return nil, false, 0, err
			}
			return nil, false, 0, vterrors.Errorf(vtrpc.Code_ABORTED, "Row count exceeded %d", maxrows)
		}

		// Regular row.
		row, err := c.parseRow(data, result.Fields, readLenEncStringAsBytesCopy, nil)
		if err != nil {
			c.recycleReadPacket()
			return nil, false, 0, err
		}
		result.Rows = append(result.Rows, row)
		c.recycleReadPacket()
	}
}

// drainResults will read all packets for a result set and ignore them.
func (c *Conn) drainResults() error {
	for {
		data, err := c.readEphemeralPacket()
		if err != nil {
			return NewSQLError(CRServerLost, SSUnknownSQLState, "%v", err)
		}
		if c.isEOFPacket(data) {
			c.recycleReadPacket()
			return nil
		} else if isErrorPacket(data) {
			defer c.recycleReadPacket()
			return ParseErrorPacket(data)
		}
		c.recycleReadPacket()
	}
}

func (c *Conn) readComQueryResponse() (int, *PacketOK, error) {
	data, err := c.readEphemeralPacket()
	if err != nil {
		return 0, nil, NewSQLError(CRServerLost, SSUnknownSQLState, "%v", err)
	}
	defer c.recycleReadPacket()
	if len(data) == 0 {
		return 0, nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "invalid empty COM_QUERY response packet")
	}

	switch data[0] {
	case OKPacket:
		packetOk, err := c.parseOKPacket(data)
		return 0, packetOk, err
	case ErrPacket:
		// Error
		return 0, nil, ParseErrorPacket(data)
	case 0xfb:
		// Local infile
		return 0, nil, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "not implemented")
	}
	n, pos, ok := readLenEncInt(data, 0)
	if !ok {
		return 0, nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "cannot get column number")
	}
	if pos != len(data) {
		return 0, nil, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "extra data in COM_QUERY response")
	}
	return int(n), &PacketOK{}, nil
}

//
// Server side methods.
//

func (c *Conn) parseComQuery(data []byte) string {
	return string(data[1:])
}

func (c *Conn) parseComSetOption(data []byte) (uint16, bool) {
	val, _, ok := readUint16(data, 1)
	return val, ok
}

func (c *Conn) parseComPrepare(data []byte) string {
	return string(data[1:])
}

func (c *Conn) parseComStmtExecute(prepareData map[uint32]*PrepareData, data []byte) (uint32, byte, error) {
	pos := 0
	payload := data[1:]
	bitMap := make([]byte, 0)

	// statement ID
	stmtID, pos, ok := readUint32(payload, 0)
	if !ok {
		return 0, 0, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "reading statement ID failed")
	}
	prepare, ok := prepareData[stmtID]
	if !ok {
		return 0, 0, NewSQLError(CRCommandsOutOfSync, SSUnknownSQLState, "statement ID is not found from record")
	}

	// cursor type flags
	cursorType, pos, ok := readByte(payload, pos)
	if !ok {
		return stmtID, 0, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "reading cursor type flags failed")
	}

	// iteration count
	iterCount, pos, ok := readUint32(payload, pos)
	if !ok {
		return stmtID, 0, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "reading iteration count failed")
	}
	if iterCount != uint32(1) {
		return stmtID, 0, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "iteration count is not equal to 1")
	}

	if prepare.ParamsCount > 0 {
		bitMap, pos, ok = readBytes(payload, pos, (int(prepare.ParamsCount)+7)/8)
		if !ok {
			return stmtID, 0, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "reading NULL-bitmap failed")
		}
	}

	newParamsBoundFlag, pos, ok := readByte(payload, pos)
	if ok && newParamsBoundFlag == 0x01 {
		var mysqlType, flags byte
		for i := uint16(0); i < prepare.ParamsCount; i++ {
			mysqlType, pos, ok = readByte(payload, pos)
			if !ok {
				return stmtID, 0, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "reading parameter type failed")
			}

			flags, pos, ok = readByte(payload, pos)
			if !ok {
				return stmtID, 0, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "reading parameter flags failed")
			}

			// convert MySQL type to internal type.
			valType, err := sqltypes.MySQLToType(int64(mysqlType), int64(flags))
			if err != nil {
				return stmtID, 0, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "MySQLToType(%v,%v) failed: %v", mysqlType, flags, err)
			}

			prepare.ParamsType[i] = int32(valType)
		}
	}

	for i := 0; i < len(prepare.ParamsType); i++ {
		var val sqltypes.Value
		parameterID := fmt.Sprintf("v%d", i+1)
		if v, ok := prepare.BindVars[parameterID]; ok {
			if v != nil {
				continue
			}
		}

		if (bitMap[i/8] & (1 << uint(i%8))) > 0 {
			val, pos, ok = c.parseStmtArgs(nil, sqltypes.Null, pos)
		} else {
			val, pos, ok = c.parseStmtArgs(payload, querypb.Type(prepare.ParamsType[i]), pos)
		}
		if !ok {
			return stmtID, 0, NewSQLError(CRMalformedPacket, SSUnknownSQLState, "decoding parameter value failed: %v", prepare.ParamsType[i])
		}

		prepare.BindVars[parameterID] = sqltypes.ValueBindVariable(val)
	}

	return stmtID, cursorType, nil
}

func (c *Conn) parseStmtArgs(data []byte, typ querypb.Type, pos int) (sqltypes.Value, int, bool) {
	switch typ {
	case sqltypes.Null:
		return sqltypes.NULL, pos, true
	case sqltypes.Int8:
		val, pos, ok := readByte(data, pos)
		return sqltypes.NewInt64(int64(int8(val))), pos, ok
	case sqltypes.Uint8:
		val, pos, ok := readByte(data, pos)
		return sqltypes.NewUint64(uint64(val)), pos, ok
	case sqltypes.Uint16:
		val, pos, ok := readUint16(data, pos)
		return sqltypes.NewUint64(uint64(val)), pos, ok
	case sqltypes.Int16, sqltypes.Year:
		val, pos, ok := readUint16(data, pos)
		return sqltypes.NewInt64(int64(int16(val))), pos, ok
	case sqltypes.Uint24, sqltypes.Uint32:
		val, pos, ok := readUint32(data, pos)
		return sqltypes.NewUint64(uint64(val)), pos, ok
	case sqltypes.Int24, sqltypes.Int32:
		val, pos, ok := readUint32(data, pos)
		return sqltypes.NewInt64(int64(int32(val))), pos, ok
	case sqltypes.Float32:
		val, pos, ok := readUint32(data, pos)
		return sqltypes.NewFloat64(float64(math.Float32frombits(uint32(val)))), pos, ok
	case sqltypes.Uint64:
		val, pos, ok := readUint64(data, pos)
		return sqltypes.NewUint64(val), pos, ok
	case sqltypes.Int64:
		val, pos, ok := readUint64(data, pos)
		return sqltypes.NewInt64(int64(val)), pos, ok
	case sqltypes.Float64:
		val, pos, ok := readUint64(data, pos)
		return sqltypes.NewFloat64(math.Float64frombits(val)), pos, ok
	case sqltypes.Timestamp, sqltypes.Date, sqltypes.Datetime:
		size, pos, ok := readByte(data, pos)
		if !ok {
			return sqltypes.NULL, 0, false
		}
		switch size {
		case 0x00:
			return sqltypes.NewVarChar(" "), pos, ok
		case 0x0b:
			year, pos, ok := readUint16(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			month, pos, ok := readByte(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			day, pos, ok := readByte(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			hour, pos, ok := readByte(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			minute, pos, ok := readByte(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			second, pos, ok := readByte(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			microSecond, pos, ok := readUint32(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			val := strconv.Itoa(int(year)) + "-" +
				strconv.Itoa(int(month)) + "-" +
				strconv.Itoa(int(day)) + " " +
				strconv.Itoa(int(hour)) + ":" +
				strconv.Itoa(int(minute)) + ":" +
				strconv.Itoa(int(second)) + "." +
				fmt.Sprintf("%06d", microSecond)

			return sqltypes.NewVarChar(val), pos, ok
		case 0x07:
			year, pos, ok := readUint16(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			month, pos, ok := readByte(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			day, pos, ok := readByte(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			hour, pos, ok := readByte(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			minute, pos, ok := readByte(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			second, pos, ok := readByte(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			val := strconv.Itoa(int(year)) + "-" +
				strconv.Itoa(int(month)) + "-" +
				strconv.Itoa(int(day)) + " " +
				strconv.Itoa(int(hour)) + ":" +
				strconv.Itoa(int(minute)) + ":" +
				strconv.Itoa(int(second))

			return sqltypes.NewVarChar(val), pos, ok
		case 0x04:
			year, pos, ok := readUint16(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			month, pos, ok := readByte(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			day, pos, ok := readByte(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			val := strconv.Itoa(int(year)) + "-" +
				strconv.Itoa(int(month)) + "-" +
				strconv.Itoa(int(day))

			return sqltypes.NewVarChar(val), pos, ok
		default:
			return sqltypes.NULL, 0, false
		}
	case sqltypes.Time:
		size, pos, ok := readByte(data, pos)
		if !ok {
			return sqltypes.NULL, 0, false
		}
		switch size {
		case 0x00:
			return sqltypes.NewVarChar("00:00:00"), pos, ok
		case 0x0c:
			isNegative, pos, ok := readByte(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			days, pos, ok := readUint32(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			hour, pos, ok := readByte(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}

			hours := uint32(hour) + days*uint32(24)

			minute, pos, ok := readByte(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			second, pos, ok := readByte(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			microSecond, pos, ok := readUint32(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}

			val := ""
			if isNegative == 0x01 {
				val += "-"
			}
			val += strconv.Itoa(int(hours)) + ":" +
				strconv.Itoa(int(minute)) + ":" +
				strconv.Itoa(int(second)) + "." +
				fmt.Sprintf("%06d", microSecond)

			return sqltypes.NewVarChar(val), pos, ok
		case 0x08:
			isNegative, pos, ok := readByte(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			days, pos, ok := readUint32(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			hour, pos, ok := readByte(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}

			hours := uint32(hour) + days*uint32(24)

			minute, pos, ok := readByte(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}
			second, pos, ok := readByte(data, pos)
			if !ok {
				return sqltypes.NULL, 0, false
			}

			val := ""
			if isNegative == 0x01 {
				val += "-"
			}
			val += strconv.Itoa(int(hours)) + ":" +
				strconv.Itoa(int(minute)) + ":" +
				strconv.Itoa(int(second))

			return sqltypes.NewVarChar(val), pos, ok
		default:
			return sqltypes.NULL, 0, false
		}
	case sqltypes.Decimal, sqltypes.Text, sqltypes.Blob, sqltypes.VarChar, sqltypes.VarBinary, sqltypes.Char,
		sqltypes.Bit, sqltypes.Enum, sqltypes.Set, sqltypes.Geometry, sqltypes.Binary, sqltypes.TypeJSON:
		val, pos, ok := readLenEncStringAsBytesCopy(data, pos)
		return sqltypes.MakeTrusted(sqltypes.VarBinary, val), pos, ok
	default:
		return sqltypes.NULL, pos, false
	}
}

func (c *Conn) parseComStmtSendLongData(data []byte) (uint32, uint16, []byte, bool) {
	pos := 1
	statementID, pos, ok := readUint32(data, pos)
	if !ok {
		return 0, 0, nil, false
	}

	paramID, pos, ok := readUint16(data, pos)
	if !ok {
		return 0, 0, nil, false
	}

	chunkData := data[pos:]
	chunk := make([]byte, len(chunkData))
	copy(chunk, chunkData)

	return statementID, paramID, chunk, true
}

func (c *Conn) parseComStmtClose(data []byte) (uint32, bool) {
	val, _, ok := readUint32(data, 1)
	return val, ok
}

func (c *Conn) parseComStmtReset(data []byte) (uint32, bool) {
	val, _, ok := readUint32(data, 1)
	return val, ok
}

func (c *Conn) parseComInitDB(data []byte) string {
	return string(data[1:])
}

func (c *Conn) sendColumnCount(count uint64) error {
	length := lenEncIntSize(count)
	data, pos := c.startEphemeralPacketWithHeader(length)
	writeLenEncInt(data, pos, count)
	return c.writeEphemeralPacket()
}

func (c *Conn) writeColumnDefinition(field *querypb.Field) error {
	length := 4 + // lenEncStringSize("def")
		lenEncStringSize(field.Database) +
		lenEncStringSize(field.Table) +
		lenEncStringSize(field.OrgTable) +
		lenEncStringSize(field.Name) +
		lenEncStringSize(field.OrgName) +
		1 + // length of fixed length fields
		2 + // character set
		4 + // column length
		1 + // type
		2 + // flags
		1 + // decimals
		2 // filler

	// Get the type and the flags back. If the Field contains
	// non-zero flags, we use them. Otherwise use the flags we
	// derive from the type.
	typ, flags := sqltypes.TypeToMySQL(field.Type)
	if field.Flags != 0 {
		flags = int64(field.Flags)
	}

	data, pos := c.startEphemeralPacketWithHeader(length)

	pos = writeLenEncString(data, pos, "def") // Always the same.
	pos = writeLenEncString(data, pos, field.Database)
	pos = writeLenEncString(data, pos, field.Table)
	pos = writeLenEncString(data, pos, field.OrgTable)
	pos = writeLenEncString(data, pos, field.Name)
	pos = writeLenEncString(data, pos, field.OrgName)
	pos = writeByte(data, pos, 0x0c)
	pos = writeUint16(data, pos, uint16(field.Charset))
	pos = writeUint32(data, pos, field.ColumnLength)
	pos = writeByte(data, pos, byte(typ))
	pos = writeUint16(data, pos, uint16(flags))
	pos = writeByte(data, pos, byte(field.Decimals))
	pos = writeUint16(data, pos, uint16(0x0000))

	if pos != len(data) {
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "packing of column definition used %v bytes instead of %v", pos, len(data))
	}

	return c.writeEphemeralPacket()
}

func (c *Conn) writeRow(row []sqltypes.Value) error {
	length := 0
	for _, val := range row {
		if val.IsNull() {
			length++
		} else {
			l := len(val.Raw())
			length += lenEncIntSize(uint64(l)) + l
		}
	}

	data, pos := c.startEphemeralPacketWithHeader(length)
	for _, val := range row {
		if val.IsNull() {
			pos = writeByte(data, pos, NullValue)
		} else {
			l := len(val.Raw())
			pos = writeLenEncInt(data, pos, uint64(l))
			pos += copy(data[pos:], val.Raw())
		}
	}

	return c.writeEphemeralPacket()
}

// writeFields writes the fields of a Result. It should be called only
// if there are valid columns in the result.
func (c *Conn) writeFields(result *sqltypes.Result) error {
	// Send the number of fields first.
	if err := c.sendColumnCount(uint64(len(result.Fields))); err != nil {
		return err
	}

	// Now send each Field.
	for _, field := range result.Fields {
		if err := c.writeColumnDefinition(field); err != nil {
			return err
		}
	}

	// Now send an EOF packet.
	if c.Capabilities&CapabilityClientDeprecateEOF == 0 {
		// With CapabilityClientDeprecateEOF, we do not send this EOF.
		if err := c.writeEOFPacket(c.StatusFlags, 0); err != nil {
			return err
		}
	}
	return nil
}

// writeRows sends the rows of a Result.
func (c *Conn) writeRows(result *sqltypes.Result) error {
	for _, row := range result.Rows {
		if err := c.writeRow(row); err != nil {
			return err
		}
	}
	return nil
}

// writeEndResult concludes the sending of a Result.
// if more is set to true, then it means there are more results afterwords
func (c *Conn) writeEndResult(more bool, affectedRows, lastInsertID uint64, warnings uint16) error {
	// Send either an EOF, or an OK packet.
	// See doc.go.
	flags := c.StatusFlags
	if more {
		flags |= ServerMoreResultsExists
	}
	if c.Capabilities&CapabilityClientDeprecateEOF == 0 {
		if err := c.writeEOFPacket(flags, warnings); err != nil {
			return err
		}
	} else {
		// This will flush too.
		if err := c.writeOKPacketWithEOFHeader(&PacketOK{
			affectedRows: affectedRows,
			lastInsertID: lastInsertID,
			statusFlags:  flags,
			warnings:     warnings,
		}); err != nil {
			return err
		}
	}

	return nil
}

// PacketComStmtPrepareOK contains the COM_STMT_PREPARE_OK packet details
type PacketComStmtPrepareOK struct {
	status       uint8
	stmtID       uint32
	numCols      uint16
	numParams    uint16
	warningCount uint16
}

// writePrepare writes a prepare query response to the wire.
func (c *Conn) writePrepare(fld []*querypb.Field, prepare *PrepareData) error {
	paramsCount := prepare.ParamsCount
	columnCount := 0
	if len(fld) != 0 {
		columnCount = len(fld)
	}
	if columnCount > 0 {
		prepare.ColumnNames = make([]string, columnCount)
	}

	ok := PacketComStmtPrepareOK{
		status:       OKPacket,
		stmtID:       prepare.StatementID,
		numCols:      (uint16)(columnCount),
		numParams:    paramsCount,
		warningCount: 0,
	}
	bytes, pos := c.startEphemeralPacketWithHeader(12)
	data := &coder{data: bytes, pos: pos}
	data.writeByte(ok.status)
	data.writeUint32(ok.stmtID)
	data.writeUint16(ok.numCols)
	data.writeUint16(ok.numParams)
	data.writeByte(0x00) // reserved 1 byte
	data.writeUint16(ok.warningCount)

	if err := c.writeEphemeralPacket(); err != nil {
		return err
	}

	if paramsCount > 0 {
		for i := uint16(0); i < paramsCount; i++ {
			if err := c.writeColumnDefinition(&querypb.Field{
				Name:    "?",
				Type:    sqltypes.VarBinary,
				Charset: 63}); err != nil {
				return err
			}
		}

		// Now send an EOF packet.
		if c.Capabilities&CapabilityClientDeprecateEOF == 0 {
			// With CapabilityClientDeprecateEOF, we do not send this EOF.
			if err := c.writeEOFPacket(c.StatusFlags, 0); err != nil {
				return err
			}
		}
	}

	for i, field := range fld {
		field.Name = strings.Replace(field.Name, "'?'", "?", -1)
		prepare.ColumnNames[i] = field.Name
		if err := c.writeColumnDefinition(field); err != nil {
			return err
		}
	}

	if columnCount > 0 {
		// Now send an EOF packet.
		if c.Capabilities&CapabilityClientDeprecateEOF == 0 {
			// With CapabilityClientDeprecateEOF, we do not send this EOF.
			if err := c.writeEOFPacket(c.StatusFlags, 0); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *Conn) writeBinaryRow(fields []*querypb.Field, row []sqltypes.Value) error {
	length := 0
	nullBitMapLen := (len(fields) + 7 + 2) / 8
	for _, val := range row {
		if !val.IsNull() {
			l, err := val2MySQLLen(val)
			if err != nil {
				return fmt.Errorf("internal value %v get MySQL value length error: %v", val, err)
			}
			length += l
		}
	}

	length += nullBitMapLen + 1

	data, pos := c.startEphemeralPacketWithHeader(length)

	pos = writeByte(data, pos, 0x00)

	for i := 0; i < nullBitMapLen; i++ {
		pos = writeByte(data, pos, 0x00)
	}

	for i, val := range row {
		if val.IsNull() {
			bytePos := (i+2)/8 + 1 + packetHeaderSize
			bitPos := (i + 2) % 8
			data[bytePos] |= 1 << uint(bitPos)
		} else {
			v, err := val2MySQL(val)
			if err != nil {
				c.recycleWritePacket()
				return fmt.Errorf("internal value %v to MySQL value error: %v", val, err)
			}
			pos += copy(data[pos:], v)
		}
	}

	return c.writeEphemeralPacket()
}

// writeBinaryRows sends the rows of a Result with binary form.
func (c *Conn) writeBinaryRows(result *sqltypes.Result) error {
	for _, row := range result.Rows {
		if err := c.writeBinaryRow(result.Fields, row); err != nil {
			return err
		}
	}
	return nil
}

func val2MySQL(v sqltypes.Value) ([]byte, error) {
	var out []byte
	pos := 0
	switch v.Type() {
	case sqltypes.Null:
		// no-op
	case sqltypes.Int8:
		val, err := strconv.ParseInt(v.ToString(), 10, 8)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 1)
		writeByte(out, pos, uint8(val))
	case sqltypes.Uint8:
		val, err := strconv.ParseUint(v.ToString(), 10, 8)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 1)
		writeByte(out, pos, uint8(val))
	case sqltypes.Uint16:
		val, err := strconv.ParseUint(v.ToString(), 10, 16)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 2)
		writeUint16(out, pos, uint16(val))
	case sqltypes.Int16, sqltypes.Year:
		val, err := strconv.ParseInt(v.ToString(), 10, 16)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 2)
		writeUint16(out, pos, uint16(val))
	case sqltypes.Uint24, sqltypes.Uint32:
		val, err := strconv.ParseUint(v.ToString(), 10, 32)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 4)
		writeUint32(out, pos, uint32(val))
	case sqltypes.Int24, sqltypes.Int32:
		val, err := strconv.ParseInt(v.ToString(), 10, 32)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 4)
		writeUint32(out, pos, uint32(val))
	case sqltypes.Float32:
		val, err := strconv.ParseFloat(v.ToString(), 32)
		if err != nil {
			return []byte{}, err
		}
		bits := math.Float32bits(float32(val))
		out = make([]byte, 4)
		writeUint32(out, pos, bits)
	case sqltypes.Uint64:
		val, err := strconv.ParseUint(v.ToString(), 10, 64)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 8)
		writeUint64(out, pos, uint64(val))
	case sqltypes.Int64:
		val, err := strconv.ParseInt(v.ToString(), 10, 64)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 8)
		writeUint64(out, pos, uint64(val))
	case sqltypes.Float64:
		val, err := strconv.ParseFloat(v.ToString(), 64)
		if err != nil {
			return []byte{}, err
		}
		bits := math.Float64bits(val)
		out = make([]byte, 8)
		writeUint64(out, pos, bits)
	case sqltypes.Timestamp, sqltypes.Date, sqltypes.Datetime:
		if len(v.Raw()) > 19 {
			out = make([]byte, 1+11)
			out[pos] = 0x0b
			pos++
			year, err := strconv.ParseUint(string(v.Raw()[0:4]), 10, 16)
			if err != nil {
				return []byte{}, err
			}
			month, err := strconv.ParseUint(string(v.Raw()[5:7]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			day, err := strconv.ParseUint(string(v.Raw()[8:10]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			hour, err := strconv.ParseUint(string(v.Raw()[11:13]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			minute, err := strconv.ParseUint(string(v.Raw()[14:16]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			second, err := strconv.ParseUint(string(v.Raw()[17:19]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			val := make([]byte, 6)
			count := copy(val, v.Raw()[20:])
			for i := 0; i < (6 - count); i++ {
				val[count+i] = 0x30
			}
			microSecond, err := strconv.ParseUint(string(val), 10, 32)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint16(out, pos, uint16(year))
			pos = writeByte(out, pos, byte(month))
			pos = writeByte(out, pos, byte(day))
			pos = writeByte(out, pos, byte(hour))
			pos = writeByte(out, pos, byte(minute))
			pos = writeByte(out, pos, byte(second))
			writeUint32(out, pos, uint32(microSecond))
		} else if len(v.Raw()) > 10 {
			out = make([]byte, 1+7)
			out[pos] = 0x07
			pos++
			year, err := strconv.ParseUint(string(v.Raw()[0:4]), 10, 16)
			if err != nil {
				return []byte{}, err
			}
			month, err := strconv.ParseUint(string(v.Raw()[5:7]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			day, err := strconv.ParseUint(string(v.Raw()[8:10]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			hour, err := strconv.ParseUint(string(v.Raw()[11:13]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			minute, err := strconv.ParseUint(string(v.Raw()[14:16]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			second, err := strconv.ParseUint(string(v.Raw()[17:]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint16(out, pos, uint16(year))
			pos = writeByte(out, pos, byte(month))
			pos = writeByte(out, pos, byte(day))
			pos = writeByte(out, pos, byte(hour))
			pos = writeByte(out, pos, byte(minute))
			writeByte(out, pos, byte(second))
		} else if len(v.Raw()) > 0 {
			out = make([]byte, 1+4)
			out[pos] = 0x04
			pos++
			year, err := strconv.ParseUint(string(v.Raw()[0:4]), 10, 16)
			if err != nil {
				return []byte{}, err
			}
			month, err := strconv.ParseUint(string(v.Raw()[5:7]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			day, err := strconv.ParseUint(string(v.Raw()[8:]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint16(out, pos, uint16(year))
			pos = writeByte(out, pos, byte(month))
			writeByte(out, pos, byte(day))
		} else {
			out = make([]byte, 1)
			out[pos] = 0x00
		}
	case sqltypes.Time:
		if string(v.Raw()) == "00:00:00" {
			out = make([]byte, 1)
			out[pos] = 0x00
		} else if strings.Contains(string(v.Raw()), ".") {
			out = make([]byte, 1+12)
			out[pos] = 0x0c
			pos++

			sub1 := strings.Split(string(v.Raw()), ":")
			if len(sub1) != 3 {
				err := fmt.Errorf("incorrect time value, ':' is not found")
				return []byte{}, err
			}
			sub2 := strings.Split(sub1[2], ".")
			if len(sub2) != 2 {
				err := fmt.Errorf("incorrect time value, '.' is not found")
				return []byte{}, err
			}

			var total []byte
			if strings.HasPrefix(sub1[0], "-") {
				out[pos] = 0x01
				total = []byte(sub1[0])
				total = total[1:]
			} else {
				out[pos] = 0x00
				total = []byte(sub1[0])
			}
			pos++

			h, err := strconv.ParseUint(string(total), 10, 32)
			if err != nil {
				return []byte{}, err
			}

			days := uint32(h) / 24
			hours := uint32(h) % 24
			minute := sub1[1]
			second := sub2[0]
			microSecond := sub2[1]

			minutes, err := strconv.ParseUint(minute, 10, 8)
			if err != nil {
				return []byte{}, err
			}

			seconds, err := strconv.ParseUint(second, 10, 8)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint32(out, pos, uint32(days))
			pos = writeByte(out, pos, byte(hours))
			pos = writeByte(out, pos, byte(minutes))
			pos = writeByte(out, pos, byte(seconds))

			val := make([]byte, 6)
			count := copy(val, microSecond)
			for i := 0; i < (6 - count); i++ {
				val[count+i] = 0x30
			}
			microSeconds, err := strconv.ParseUint(string(val), 10, 32)
			if err != nil {
				return []byte{}, err
			}
			writeUint32(out, pos, uint32(microSeconds))
		} else if len(v.Raw()) > 0 {
			out = make([]byte, 1+8)
			out[pos] = 0x08
			pos++

			sub1 := strings.Split(string(v.Raw()), ":")
			if len(sub1) != 3 {
				err := fmt.Errorf("incorrect time value, ':' is not found")
				return []byte{}, err
			}

			var total []byte
			if strings.HasPrefix(sub1[0], "-") {
				out[pos] = 0x01
				total = []byte(sub1[0])
				total = total[1:]
			} else {
				out[pos] = 0x00
				total = []byte(sub1[0])
			}
			pos++

			h, err := strconv.ParseUint(string(total), 10, 32)
			if err != nil {
				return []byte{}, err
			}

			days := uint32(h) / 24
			hours := uint32(h) % 24
			minute := sub1[1]
			second := sub1[2]

			minutes, err := strconv.ParseUint(minute, 10, 8)
			if err != nil {
				return []byte{}, err
			}

			seconds, err := strconv.ParseUint(second, 10, 8)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint32(out, pos, uint32(days))
			pos = writeByte(out, pos, byte(hours))
			pos = writeByte(out, pos, byte(minutes))
			writeByte(out, pos, byte(seconds))
		} else {
			err := fmt.Errorf("incorrect time value")
			return []byte{}, err
		}
	case sqltypes.Decimal, sqltypes.Text, sqltypes.Blob, sqltypes.VarChar,
		sqltypes.VarBinary, sqltypes.Char, sqltypes.Bit, sqltypes.Enum,
		sqltypes.Set, sqltypes.Geometry, sqltypes.Binary, sqltypes.TypeJSON:
		l := len(v.Raw())
		length := lenEncIntSize(uint64(l)) + l
		out = make([]byte, length)
		pos = writeLenEncInt(out, pos, uint64(l))
		copy(out[pos:], v.Raw())
	default:
		out = make([]byte, len(v.Raw()))
		copy(out, v.Raw())
	}
	return out, nil
}

func val2MySQLLen(v sqltypes.Value) (int, error) {
	var length int
	var err error

	switch v.Type() {
	case sqltypes.Null:
		length = 0
	case sqltypes.Int8, sqltypes.Uint8:
		length = 1
	case sqltypes.Uint16, sqltypes.Int16, sqltypes.Year:
		length = 2
	case sqltypes.Uint24, sqltypes.Uint32, sqltypes.Int24, sqltypes.Int32, sqltypes.Float32:
		length = 4
	case sqltypes.Uint64, sqltypes.Int64, sqltypes.Float64:
		length = 8
	case sqltypes.Timestamp, sqltypes.Date, sqltypes.Datetime:
		if len(v.Raw()) > 19 {
			length = 12
		} else if len(v.Raw()) > 10 {
			length = 8
		} else if len(v.Raw()) > 0 {
			length = 5
		} else {
			length = 1
		}
	case sqltypes.Time:
		if string(v.Raw()) == "00:00:00" {
			length = 1
		} else if strings.Contains(string(v.Raw()), ".") {
			length = 13
		} else if len(v.Raw()) > 0 {
			length = 9
		} else {
			err = fmt.Errorf("incorrect time value")
		}
	case sqltypes.Decimal, sqltypes.Text, sqltypes.Blob, sqltypes.VarChar,
		sqltypes.VarBinary, sqltypes.Char, sqltypes.Bit, sqltypes.Enum,
		sqltypes.Set, sqltypes.Geometry, sqltypes.Binary, sqltypes.TypeJSON:
		l := len(v.Raw())
		length = lenEncIntSize(uint64(l)) + l
	default:
		length = len(v.Raw())
	}
	if err != nil {
		return 0, err
	}
	return length, nil
}
