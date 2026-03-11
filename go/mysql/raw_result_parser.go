/*
Copyright 2024 The Vitess Authors.

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
	"errors"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

const (
	rawParserStateColumnCount = iota
	rawParserStateColumnDefs
	rawParserStateMidEOF
	rawParserStateRows
	rawParserStateDone
)

// RawResultParser is a stateful parser that converts raw MySQL wire protocol
// bytes into *sqltypes.Result objects. It is used by the gRPC client to parse
// incoming raw chunks from StreamExecuteRaw.
type RawResultParser struct {
	buf          []byte
	state        int
	deprecateEOF bool
	colCount     int
	colsRead     int
	fields       []*querypb.Field
	fieldsSent   bool
	pendingRows  [][]sqltypes.Value
}

// NewRawResultParser creates a parser for raw MySQL wire protocol bytes.
func NewRawResultParser(deprecateEOF bool) *RawResultParser {
	return &RawResultParser{
		deprecateEOF: deprecateEOF,
		state:        rawParserStateColumnCount,
	}
}

// Feed appends a raw chunk and parses any complete packets, calling the callback
// for each complete result. The first result has Fields set. Subsequent results have Rows.
// All rows parsed from a single chunk are batched into a single callback.
func (p *RawResultParser) Feed(chunk []byte, callback func(*sqltypes.Result) error) error {
	p.buf = append(p.buf, chunk...)

	for {
		if p.state == rawParserStateDone {
			return p.flushPendingRows(callback)
		}

		// Need at least a header to proceed
		if len(p.buf) < PacketHeaderSize {
			return p.flushPendingRows(callback)
		}

		packetLength := int(uint32(p.buf[0]) | uint32(p.buf[1])<<8 | uint32(p.buf[2])<<16)
		totalLength := PacketHeaderSize + packetLength

		// Wait for complete packet
		if len(p.buf) < totalLength {
			return p.flushPendingRows(callback)
		}

		payload := p.buf[PacketHeaderSize:totalLength]
		p.buf = p.buf[totalLength:]

		if err := p.processPacket(payload, callback); err != nil {
			return err
		}
	}
}

func (p *RawResultParser) processPacket(payload []byte, callback func(*sqltypes.Result) error) error {
	switch p.state {
	case rawParserStateColumnCount:
		return p.handleColumnCount(payload, callback)
	case rawParserStateColumnDefs:
		return p.handleColumnDef(payload, callback)
	case rawParserStateMidEOF:
		return p.handleMidEOF(payload, callback)
	case rawParserStateRows:
		return p.handleRow(payload, callback)
	}
	return nil
}

func (p *RawResultParser) handleColumnCount(payload []byte, callback func(*sqltypes.Result) error) error {
	if len(payload) == 0 {
		return errors.New("empty column count packet")
	}

	// Check for ERR packet
	if payload[0] == ErrPacket {
		p.state = rawParserStateDone
		return ParseErrorPacket(payload)
	}

	// Check for OK packet (0 columns)
	if payload[0] == OKPacket {
		p.state = rawParserStateDone
		result := &sqltypes.Result{}
		pos := 1
		var ok bool
		result.RowsAffected, pos, ok = readLenEncInt(payload, pos)
		if !ok {
			return callback(result)
		}
		result.InsertID, pos, ok = readLenEncInt(payload, pos)
		if !ok {
			return callback(result)
		}
		if result.InsertID > 0 {
			result.InsertIDChanged = true
		}
		// status_flags (uint16)
		sf, pos, ok := readUint16(payload, pos)
		if !ok {
			return callback(result)
		}
		result.StatusFlags = sf
		// warnings (uint16) - skip
		_, pos, ok = readUint16(payload, pos)
		if !ok {
			return callback(result)
		}
		// info (remaining bytes as EOF string)
		if pos < len(payload) {
			result.Info = string(payload[pos:])
		}
		return callback(result)
	}

	// Parse column count
	colCount, _, ok := readLenEncInt(payload, 0)
	if !ok {
		return errors.New("failed to parse column count")
	}
	p.colCount = int(colCount)
	p.colsRead = 0
	p.fields = make([]*querypb.Field, p.colCount)
	for i := range p.fields {
		p.fields[i] = &querypb.Field{}
	}
	p.state = rawParserStateColumnDefs
	return nil
}

func (p *RawResultParser) handleColumnDef(payload []byte, _ func(*sqltypes.Result) error) error {
	if err := ParseColumnDefinition(payload, p.fields[p.colsRead], p.colsRead); err != nil {
		return err
	}
	p.colsRead++
	if p.colsRead == p.colCount {
		if p.deprecateEOF {
			// With deprecateEOF, there's no mid-stream EOF packet.
			// Don't emit fields yet - they'll be included with the first row.
			p.state = rawParserStateRows
		} else {
			p.state = rawParserStateMidEOF
		}
	}
	return nil
}

func (p *RawResultParser) handleMidEOF(_ []byte, _ func(*sqltypes.Result) error) error {
	// Consume the EOF packet. Don't emit fields yet - they'll be
	// included with the first row (matching StreamExecute behavior).
	p.state = rawParserStateRows
	return nil
}

func (p *RawResultParser) handleRow(payload []byte, callback func(*sqltypes.Result) error) error {
	if len(payload) == 0 {
		return nil
	}

	// Check for terminal packets
	if payload[0] == ErrPacket {
		p.state = rawParserStateDone
		// Flush accumulated rows before returning the error.
		if err := p.flushPendingRows(callback); err != nil {
			return err
		}
		return ParseErrorPacket(payload)
	}

	if payload[0] == EOFPacket {
		isEOF := false
		if p.deprecateEOF {
			isEOF = len(payload) < MaxPacketSize
		} else {
			isEOF = len(payload) < 9
		}
		if isEOF {
			p.state = rawParserStateDone
			// Rows will be flushed by Feed when it sees rawParserStateDone.
			return nil
		}
	}

	// Parse row data and accumulate. Rows are flushed as a batch
	// when Feed exits (end of chunk, done, or waiting for more data).
	row, err := ParseTextRow(payload, p.fields)
	if err != nil {
		return err
	}
	p.pendingRows = append(p.pendingRows, row)
	return nil
}

// flushPendingRows delivers accumulated rows (if any) via a single callback.
// The first result includes Fields, matching StreamExecute behavior.
func (p *RawResultParser) flushPendingRows(callback func(*sqltypes.Result) error) error {
	if len(p.pendingRows) == 0 {
		// No rows accumulated. If we're done and fields were never sent,
		// emit a fields-only result (empty result set).
		if p.state == rawParserStateDone && !p.fieldsSent && p.fields != nil {
			p.fieldsSent = true
			return callback(&sqltypes.Result{Fields: p.fields})
		}
		return nil
	}

	result := &sqltypes.Result{
		Rows: p.pendingRows,
	}
	if !p.fieldsSent {
		p.fieldsSent = true
		result.Fields = p.fields
	}
	p.pendingRows = nil
	return callback(result)
}

// ParseColumnDefinition parses a column definition packet from raw bytes.
// This is a standalone version of Conn.readColumnDefinition that doesn't
// require a Conn.
func ParseColumnDefinition(data []byte, field *querypb.Field, index int) error {
	// Catalog is ignored, always set to "def"
	pos, ok := skipLenEncString(data, 0)
	if !ok {
		return fmt.Errorf("skipping col %v catalog failed", index)
	}

	// schema, table, orgTable, name and OrgName are strings.
	field.Database, pos, ok = readLenEncString(data, pos)
	if !ok {
		return fmt.Errorf("extracting col %v schema failed", index)
	}
	field.Table, pos, ok = readLenEncString(data, pos)
	if !ok {
		return fmt.Errorf("extracting col %v table failed", index)
	}
	field.OrgTable, pos, ok = readLenEncString(data, pos)
	if !ok {
		return fmt.Errorf("extracting col %v org_table failed", index)
	}
	field.Name, pos, ok = readLenEncString(data, pos)
	if !ok {
		return fmt.Errorf("extracting col %v name failed", index)
	}
	field.OrgName, pos, ok = readLenEncString(data, pos)
	if !ok {
		return fmt.Errorf("extracting col %v org_name failed", index)
	}

	// Skip length of fixed-length fields.
	pos++

	// characterSet is a uint16.
	characterSet, pos, ok := readUint16(data, pos)
	if !ok {
		return fmt.Errorf("extracting col %v characterSet failed", index)
	}
	field.Charset = uint32(characterSet)

	// columnLength is a uint32.
	field.ColumnLength, pos, ok = readUint32(data, pos)
	if !ok {
		return fmt.Errorf("extracting col %v columnLength failed", index)
	}

	// type is one byte.
	t, pos, ok := readByte(data, pos)
	if !ok {
		return fmt.Errorf("extracting col %v type failed", index)
	}

	// flags is 2 bytes.
	flags, pos, ok := readUint16(data, pos)
	if !ok {
		return fmt.Errorf("extracting col %v flags failed", index)
	}

	// Convert MySQL type to Vitess type.
	var err error
	field.Type, err = sqltypes.MySQLToType(t, int64(flags))
	if err != nil {
		return fmt.Errorf("MySQLToType(%v,%v) failed for column %v: %v", t, flags, index, err)
	}

	// Decimals is a byte.
	decimals, _, ok := readByte(data, pos)
	if !ok {
		return fmt.Errorf("extracting col %v decimals failed", index)
	}
	field.Decimals = uint32(decimals)

	if field.ColumnLength != 0 || field.Charset != 0 {
		field.Flags = uint32(flags)
		if IsNum(t) {
			field.Flags |= uint32(querypb.MySqlFlag_NUM_FLAG)
		}
	}

	return nil
}

// ParseTextRow parses a text protocol row from raw bytes.
func ParseTextRow(data []byte, fields []*querypb.Field) ([]sqltypes.Value, error) {
	colNumber := len(fields)
	result := make([]sqltypes.Value, 0, colNumber)
	pos := 0
	for i := range colNumber {
		if pos >= len(data) {
			return nil, fmt.Errorf("unexpected end of row data at column %d", i)
		}
		if data[pos] == NullValue {
			result = append(result, sqltypes.Value{})
			pos++
			continue
		}
		var s []byte
		var ok bool
		s, pos, ok = readLenEncStringAsBytes(data, pos)
		if !ok {
			return nil, fmt.Errorf("decoding string failed at column %d", i)
		}
		result = append(result, sqltypes.MakeTrusted(fields[i].Type, s))
	}
	return result, nil
}

// EncodeResultToMySQLPackets converts a sequence of sqltypes.Result objects into
// raw MySQL wire protocol bytes suitable for feeding into RawResultParser.
// The first result should have Fields set. Subsequent results should have Rows.
// This is used for testing and by test doubles (e.g. SandboxConn).
func EncodeResultToMySQLPackets(results []*sqltypes.Result, deprecateEOF bool) []byte {
	var buf []byte
	var seq byte = 1

	// Find fields from first result that has them.
	var fields []*querypb.Field
	for _, r := range results {
		if len(r.Fields) > 0 {
			fields = r.Fields
			break
		}
	}

	if len(fields) == 0 {
		// No fields: encode an OK packet with result metadata.
		var r *sqltypes.Result
		for _, res := range results {
			if res != nil {
				r = res
				break
			}
		}
		if r == nil {
			r = &sqltypes.Result{}
		}
		buf = appendPacket(&buf, &seq, encodeOKPayload(r))
		return buf
	}

	// Column count.
	colCountPayload := make([]byte, lenEncIntSize(uint64(len(fields))))
	writeLenEncInt(colCountPayload, 0, uint64(len(fields)))
	buf = appendPacket(&buf, &seq, colCountPayload)

	// Column definitions.
	for _, field := range fields {
		buf = appendPacket(&buf, &seq, encodeColumnDefPayload(field))
	}

	// Mid-stream EOF if not deprecateEOF.
	if !deprecateEOF {
		buf = appendPacket(&buf, &seq, []byte{EOFPacket, 0, 0, 0, 0})
	}

	// Rows from all results.
	for _, r := range results {
		for _, row := range r.Rows {
			buf = appendPacket(&buf, &seq, encodeTextRowPayload(row))
		}
	}

	// Terminal EOF.
	buf = appendPacket(&buf, &seq, []byte{EOFPacket, 0, 0, 0, 0})

	return buf
}

func appendPacket(buf *[]byte, seq *byte, payload []byte) []byte {
	length := len(payload)
	header := [PacketHeaderSize]byte{
		byte(length),
		byte(length >> 8),
		byte(length >> 16),
		*seq,
	}
	*seq++
	*buf = append(*buf, header[:]...)
	*buf = append(*buf, payload...)
	return *buf
}

func encodeColumnDefPayload(field *querypb.Field) []byte {
	length := 4 + // lenEncStringSize("def")
		lenEncStringSize(field.Database) +
		lenEncStringSize(field.Table) +
		lenEncStringSize(field.OrgTable) +
		lenEncStringSize(field.Name) +
		lenEncStringSize(field.OrgName) +
		1 + 2 + 4 + 1 + 2 + 1 + 2

	data := make([]byte, length)
	pos := 0
	pos = writeLenEncString(data, pos, "def")
	pos = writeLenEncString(data, pos, field.Database)
	pos = writeLenEncString(data, pos, field.Table)
	pos = writeLenEncString(data, pos, field.OrgTable)
	pos = writeLenEncString(data, pos, field.Name)
	pos = writeLenEncString(data, pos, field.OrgName)
	pos = writeByte(data, pos, 0x0c)
	pos = writeUint16(data, pos, uint16(field.Charset))
	pos = writeUint32(data, pos, field.ColumnLength)
	typ, flags := sqltypes.TypeToMySQL(field.Type)
	if field.Flags != 0 {
		flags = int64(field.Flags)
	}
	pos = writeByte(data, pos, typ)
	pos = writeUint16(data, pos, uint16(flags))
	pos = writeByte(data, pos, byte(field.Decimals))
	writeUint16(data, pos, 0)

	return data
}

func encodeOKPayload(r *sqltypes.Result) []byte {
	length := 1 + // OK marker
		lenEncIntSize(r.RowsAffected) +
		lenEncIntSize(r.InsertID) +
		2 + // status_flags
		2 + // warnings
		len(r.Info) // info as EOF string

	data := make([]byte, length)
	pos := 0
	pos = writeByte(data, pos, OKPacket)
	pos = writeLenEncInt(data, pos, r.RowsAffected)
	pos = writeLenEncInt(data, pos, r.InsertID)
	pos = writeUint16(data, pos, r.StatusFlags)
	pos = writeUint16(data, pos, 0) // warnings
	copy(data[pos:], r.Info)
	return data
}

func encodeTextRowPayload(row []sqltypes.Value) []byte {
	length := 0
	for _, val := range row {
		if val.IsNull() {
			length++
		} else {
			l := len(val.Raw())
			length += lenEncIntSize(uint64(l)) + l
		}
	}
	data := make([]byte, length)
	pos := 0
	for _, val := range row {
		if val.IsNull() {
			pos = writeByte(data, pos, NullValue)
		} else {
			pos = writeLenEncString(data, pos, val.ToString())
		}
	}
	return data
}
