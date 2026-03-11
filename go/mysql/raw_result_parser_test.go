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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// makePacket creates a MySQL packet with the given sequence number and payload.
func makePacket(seq byte, payload []byte) []byte {
	pkt := make([]byte, PacketHeaderSize+len(payload))
	pkt[0] = byte(len(payload))
	pkt[1] = byte(len(payload) >> 8)
	pkt[2] = byte(len(payload) >> 16)
	pkt[3] = seq
	copy(pkt[PacketHeaderSize:], payload)
	return pkt
}

// makeColumnDefPacket builds a simplified column definition packet.
func makeColumnDefPacket(seq byte, name string, fieldType byte, flags uint16) []byte {
	var payload []byte

	writeLenEncStr := func(s string) {
		payload = append(payload, byte(len(s)))
		payload = append(payload, []byte(s)...)
	}

	// catalog
	writeLenEncStr("def")
	// schema
	writeLenEncStr("testdb")
	// table
	writeLenEncStr("testtable")
	// org_table
	writeLenEncStr("testtable")
	// name
	writeLenEncStr(name)
	// org_name
	writeLenEncStr(name)
	// length of fixed-length fields
	payload = append(payload, 0x0c)
	// character set (2 bytes) - utf8 = 33
	payload = append(payload, 33, 0)
	// column length (4 bytes)
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, 255)
	payload = append(payload, b...)
	// type (1 byte)
	payload = append(payload, fieldType)
	// flags (2 bytes)
	flagBytes := make([]byte, 2)
	binary.LittleEndian.PutUint16(flagBytes, flags)
	payload = append(payload, flagBytes...)
	// decimals (1 byte)
	payload = append(payload, 0)
	// filler (2 bytes)
	payload = append(payload, 0, 0)

	return makePacket(seq, payload)
}

// makeEOFPacket creates a MySQL EOF packet.
func makeEOFPacket(seq byte) []byte {
	payload := []byte{EOFPacket, 0, 0, 0, 0} // EOF marker + warnings(2) + status(2)
	return makePacket(seq, payload)
}

// makeRowPacket creates a text row packet with the given string values.
func makeRowPacket(seq byte, values ...string) []byte {
	var payload []byte
	for _, v := range values {
		payload = append(payload, byte(len(v)))
		payload = append(payload, []byte(v)...)
	}
	return makePacket(seq, payload)
}

// makeNullRowPacket creates a row with a NULL value.
func makeNullRowPacket(seq byte) []byte {
	return makePacket(seq, []byte{NullValue})
}

func TestRawResultParser_SimpleResultSet(t *testing.T) {
	// Build a result set: 1 column (VARCHAR), 2 rows, with deprecateEOF=true
	parser := NewRawResultParser(true)

	var results []*sqltypes.Result
	cb := func(r *sqltypes.Result) error {
		results = append(results, r)
		return nil
	}

	// Column count: 1
	chunk := makePacket(1, []byte{1})
	// Column def for "name" (VARCHAR = 0x0f)
	chunk = append(chunk, makeColumnDefPacket(2, "name", 0x0f, 0)...)
	// Row 1
	chunk = append(chunk, makeRowPacket(3, "alice")...)
	// Row 2
	chunk = append(chunk, makeRowPacket(4, "bob")...)
	// EOF (deprecateEOF style - OK packet with EOF marker)
	chunk = append(chunk, makeEOFPacket(5)...)

	err := parser.Feed(chunk, cb)
	require.NoError(t, err)

	// All rows from one chunk are batched into a single result with Fields.
	require.Len(t, results, 1, "expected 1 batched result")

	assert.NotNil(t, results[0].Fields)
	assert.Equal(t, "name", results[0].Fields[0].Name)
	assert.Len(t, results[0].Rows, 2)
	assert.Equal(t, "alice", results[0].Rows[0][0].ToString())
	assert.Equal(t, "bob", results[0].Rows[1][0].ToString())
}

func TestRawResultParser_WithMidEOF(t *testing.T) {
	parser := NewRawResultParser(false) // deprecateEOF=false, so mid-stream EOF expected

	var results []*sqltypes.Result
	cb := func(r *sqltypes.Result) error {
		results = append(results, r)
		return nil
	}

	var chunk []byte
	// Column count: 1
	chunk = append(chunk, makePacket(1, []byte{1})...)
	// Column def
	chunk = append(chunk, makeColumnDefPacket(2, "id", 0x03, 0)...) // LONG = 0x03
	// Mid-stream EOF
	chunk = append(chunk, makeEOFPacket(3)...)
	// Row 1
	chunk = append(chunk, makeRowPacket(4, "42")...)
	// Terminal EOF
	chunk = append(chunk, makeEOFPacket(5)...)

	err := parser.Feed(chunk, cb)
	require.NoError(t, err)

	// Single result: fields + row combined (matching StreamExecute behavior)
	require.Len(t, results, 1)
	assert.NotNil(t, results[0].Fields)
	assert.Equal(t, "id", results[0].Fields[0].Name)
	assert.Len(t, results[0].Rows, 1)
	assert.Equal(t, "42", results[0].Rows[0][0].ToString())
}

func TestRawResultParser_SplitAcrossChunks(t *testing.T) {
	parser := NewRawResultParser(false)

	var results []*sqltypes.Result
	cb := func(r *sqltypes.Result) error {
		results = append(results, r)
		return nil
	}

	// Build full result
	var full []byte
	full = append(full, makePacket(1, []byte{1})...)
	full = append(full, makeColumnDefPacket(2, "val", 0x0f, 0)...)
	full = append(full, makeEOFPacket(3)...)
	full = append(full, makeRowPacket(4, "hello")...)
	full = append(full, makeEOFPacket(5)...)

	// Feed in small chunks (simulating split across gRPC messages)
	for i := 0; i < len(full); i += 7 {
		end := min(i+7, len(full))
		err := parser.Feed(full[i:end], cb)
		require.NoError(t, err)
	}

	// Single result: fields + row combined
	require.Len(t, results, 1)
	assert.NotNil(t, results[0].Fields)
	assert.Equal(t, "val", results[0].Fields[0].Name)
	assert.Len(t, results[0].Rows, 1)
	assert.Equal(t, "hello", results[0].Rows[0][0].ToString())
}

func TestRawResultParser_ErrorPacket(t *testing.T) {
	parser := NewRawResultParser(true)

	// Send an error response instead of column count
	errPayload := []byte{
		ErrPacket,
		0x48, 0x04, // error code 1096
		'#',
		'H', 'Y', '0', '0', '0', // SQL state
		'T', 'e', 's', 't', // message
	}
	chunk := makePacket(1, errPayload)

	err := parser.Feed(chunk, func(r *sqltypes.Result) error {
		t.Fatal("should not receive result on error")
		return nil
	})
	require.Error(t, err)
}

func TestRawResultParser_EmptyResultSet(t *testing.T) {
	parser := NewRawResultParser(true)

	var results []*sqltypes.Result
	cb := func(r *sqltypes.Result) error {
		results = append(results, r)
		return nil
	}

	// OK packet (no columns)
	chunk := makePacket(1, []byte{OKPacket, 0, 0, 0, 0})
	err := parser.Feed(chunk, cb)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Empty(t, results[0].Fields)
	assert.Empty(t, results[0].Rows)
}

func TestRawResultParser_NullValues(t *testing.T) {
	parser := NewRawResultParser(false)

	var results []*sqltypes.Result
	cb := func(r *sqltypes.Result) error {
		results = append(results, r)
		return nil
	}

	var chunk []byte
	// Column count: 1
	chunk = append(chunk, makePacket(1, []byte{1})...)
	// Column def
	chunk = append(chunk, makeColumnDefPacket(2, "nullable", 0x0f, 0)...)
	// Mid-stream EOF
	chunk = append(chunk, makeEOFPacket(3)...)
	// Row with NULL value
	chunk = append(chunk, makeNullRowPacket(4)...)
	// Terminal EOF
	chunk = append(chunk, makeEOFPacket(5)...)

	err := parser.Feed(chunk, cb)
	require.NoError(t, err)

	// Find the row result
	var rowResult *sqltypes.Result
	for _, r := range results {
		if len(r.Rows) > 0 {
			rowResult = r
			break
		}
	}
	require.NotNil(t, rowResult)
	assert.True(t, rowResult.Rows[0][0].IsNull())
}

func TestParseColumnDefinition(t *testing.T) {
	// Build a column definition payload
	pkt := makeColumnDefPacket(0, "testcol", 0x0f, 0)
	// Extract just the payload (skip the 4-byte header)
	payload := pkt[PacketHeaderSize:]

	field := &querypb.Field{}
	err := ParseColumnDefinition(payload, field, 0)
	require.NoError(t, err)

	assert.Equal(t, "testcol", field.Name)
	assert.Equal(t, "testcol", field.OrgName)
	assert.Equal(t, "testdb", field.Database)
	assert.Equal(t, "testtable", field.Table)
}

func TestParseTextRow(t *testing.T) {
	fields := []*querypb.Field{
		{Name: "col1", Type: sqltypes.VarChar},
		{Name: "col2", Type: sqltypes.Int64},
	}

	// Build a row with "hello" and "42"
	var payload []byte
	payload = append(payload, 5)
	payload = append(payload, []byte("hello")...)
	payload = append(payload, 2)
	payload = append(payload, []byte("42")...)

	row, err := ParseTextRow(payload, fields)
	require.NoError(t, err)
	require.Len(t, row, 2)
	assert.Equal(t, "hello", row[0].ToString())
	assert.Equal(t, "42", row[1].ToString())
}

func TestParseTextRow_WithNull(t *testing.T) {
	fields := []*querypb.Field{
		{Name: "col1", Type: sqltypes.VarChar},
	}

	row, err := ParseTextRow([]byte{NullValue}, fields)
	require.NoError(t, err)
	require.Len(t, row, 1)
	assert.True(t, row[0].IsNull())
}

// makeTerminalOKPacket creates a terminal OK packet (0xFE marker) with the
// given metadata, as sent by MySQL when CLIENT_DEPRECATE_EOF is negotiated.
func makeTerminalOKPacket(seq byte, insertID uint64, statusFlags uint16) []byte {
	length := 1 + // 0xFE marker
		lenEncIntSize(0) + // affected_rows
		lenEncIntSize(insertID) +
		2 + // status_flags
		2 // warnings

	payload := make([]byte, length)
	pos := 0
	pos = writeByte(payload, pos, EOFPacket) // 0xFE
	pos = writeLenEncInt(payload, pos, 0)    // affected_rows
	pos = writeLenEncInt(payload, pos, insertID)
	pos = writeUint16(payload, pos, statusFlags)
	writeUint16(payload, pos, 0) // warnings
	return makePacket(seq, payload)
}

func TestRawResultParser_TerminalOKMetadata_DeprecateEOF(t *testing.T) {
	parser := NewRawResultParser(true)

	var results []*sqltypes.Result
	cb := func(r *sqltypes.Result) error {
		results = append(results, r)
		return nil
	}

	// Simulate SELECT LAST_INSERT_ID(42): result set with terminal OK containing insertID=42
	var chunk []byte
	chunk = append(chunk, makePacket(1, []byte{1})...)               // 1 column
	chunk = append(chunk, makeColumnDefPacket(2, "lid", 0x08, 0)...) // LONGLONG
	chunk = append(chunk, makeRowPacket(3, "42")...)                 // row
	chunk = append(chunk, makeTerminalOKPacket(4, 42, 0x0002)...)    // insertID=42, status=autocommit

	err := parser.Feed(chunk, cb)
	require.NoError(t, err)
	require.Len(t, results, 1)

	assert.Len(t, results[0].Rows, 1)
	assert.Equal(t, "42", results[0].Rows[0][0].ToString())
	assert.True(t, results[0].InsertIDChanged, "InsertIDChanged should be true")
	assert.Equal(t, uint64(42), results[0].InsertID)
	assert.Equal(t, uint16(0x0002), results[0].StatusFlags)
}

func TestRawResultParser_TerminalOKMetadata_SplitChunks(t *testing.T) {
	// Test that terminal metadata is delivered even when the terminal packet
	// arrives in a separate chunk from the rows.
	parser := NewRawResultParser(true)

	var results []*sqltypes.Result
	cb := func(r *sqltypes.Result) error {
		results = append(results, r)
		return nil
	}

	// Chunk 1: column def + row
	var chunk1 []byte
	chunk1 = append(chunk1, makePacket(1, []byte{1})...)
	chunk1 = append(chunk1, makeColumnDefPacket(2, "lid", 0x08, 0)...)
	chunk1 = append(chunk1, makeRowPacket(3, "7")...)

	err := parser.Feed(chunk1, cb)
	require.NoError(t, err)
	require.Len(t, results, 1, "first chunk should flush fields+rows")
	assert.False(t, results[0].InsertIDChanged, "InsertIDChanged should be false before terminal")

	// Chunk 2: terminal OK with insertID=7
	chunk2 := makeTerminalOKPacket(4, 7, 0)
	err = parser.Feed(chunk2, cb)
	require.NoError(t, err)
	require.Len(t, results, 2, "terminal metadata should be delivered as a separate result")
	assert.True(t, results[1].InsertIDChanged, "InsertIDChanged should be true from terminal OK")
	assert.Equal(t, uint64(7), results[1].InsertID)
}

func TestRawResultParser_NoInsertID(t *testing.T) {
	// Normal SELECT without LAST_INSERT_ID should not set InsertIDChanged.
	parser := NewRawResultParser(true)

	var results []*sqltypes.Result
	cb := func(r *sqltypes.Result) error {
		results = append(results, r)
		return nil
	}

	var chunk []byte
	chunk = append(chunk, makePacket(1, []byte{1})...)
	chunk = append(chunk, makeColumnDefPacket(2, "name", 0x0f, 0)...)
	chunk = append(chunk, makeRowPacket(3, "alice")...)
	chunk = append(chunk, makeTerminalOKPacket(4, 0, 0)...) // insertID=0

	err := parser.Feed(chunk, cb)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.False(t, results[0].InsertIDChanged, "InsertIDChanged should be false when insertID is 0")
	assert.Equal(t, uint64(0), results[0].InsertID)
}

func TestEncodeResultToMySQLPackets_TerminalOK_DeprecateEOF(t *testing.T) {
	// Verify that EncodeResultToMySQLPackets encodes InsertID in the terminal
	// OK packet when deprecateEOF=true.
	results := []*sqltypes.Result{
		{
			Fields: []*querypb.Field{
				{Name: "lid", Type: sqltypes.Int64, Charset: 33, ColumnLength: 20},
			},
			Rows: [][]sqltypes.Value{
				{sqltypes.NewInt64(5)},
			},
			InsertID:        5,
			InsertIDChanged: true,
		},
	}

	encoded := EncodeResultToMySQLPackets(results, true)
	parser := NewRawResultParser(true)

	var parsed []*sqltypes.Result
	err := parser.Feed(encoded, func(r *sqltypes.Result) error {
		parsed = append(parsed, r)
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, parsed)

	// Find the result with InsertIDChanged
	var found bool
	for _, r := range parsed {
		if r.InsertIDChanged {
			assert.Equal(t, uint64(5), r.InsertID)
			found = true
		}
	}
	assert.True(t, found, "should find a result with InsertIDChanged=true")
}
