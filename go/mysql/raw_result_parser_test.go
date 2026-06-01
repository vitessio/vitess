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
	// Build a result set: 1 column (VARCHAR), 2 rows
	parser := NewRawResultParser()

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
	// Terminal EOF (OK packet with EOF marker)
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

func TestRawResultParser_IntColumn(t *testing.T) {
	parser := NewRawResultParser()

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
	// Row 1
	chunk = append(chunk, makeRowPacket(3, "42")...)
	// Terminal EOF
	chunk = append(chunk, makeEOFPacket(4)...)

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
	parser := NewRawResultParser()

	var results []*sqltypes.Result
	cb := func(r *sqltypes.Result) error {
		results = append(results, r)
		return nil
	}

	// Build full result
	var full []byte
	full = append(full, makePacket(1, []byte{1})...)
	full = append(full, makeColumnDefPacket(2, "val", 0x0f, 0)...)
	full = append(full, makeRowPacket(3, "hello")...)
	full = append(full, makeEOFPacket(4)...)

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
	parser := NewRawResultParser()

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

func TestRawResultParser_LocalInfileRejected(t *testing.T) {
	parser := NewRawResultParser()

	// A 0xfb byte at the column-count position is a LOCAL INFILE request, which
	// the raw path does not support. It must error rather than be decoded as a
	// column count of 251.
	chunk := makePacket(1, []byte{0xfb, '/', 't', 'm', 'p', '/', 'f'})
	err := parser.Feed(chunk, func(*sqltypes.Result) error {
		require.Fail(t, "callback should not be invoked")
		return nil
	})
	require.ErrorContains(t, err, "LOCAL INFILE")
}

func TestRawResultParser_HugeColumnCountBounded(t *testing.T) {
	parser := NewRawResultParser()

	// A malformed column-count packet claiming ~16M columns (0xfd + 3-byte int)
	// must not trigger a large up-front allocation: the field slice is grown
	// lazily, so its initial capacity stays bounded by fieldsPreallocCap.
	chunk := makePacket(1, []byte{0xfd, 0xff, 0xff, 0xff})
	err := parser.Feed(chunk, func(*sqltypes.Result) error {
		require.Fail(t, "callback should not be invoked")
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, rawParserStateColumnDefs, parser.state)
	require.Equal(t, 0xffffff, parser.colCount)
	require.LessOrEqual(t, cap(parser.fields), fieldsPreallocCap)
}

func TestRawResultParser_EmptyResultSet(t *testing.T) {
	parser := NewRawResultParser()

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
	parser := NewRawResultParser()

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
	// Row with NULL value
	chunk = append(chunk, makeNullRowPacket(3)...)
	// Terminal EOF
	chunk = append(chunk, makeEOFPacket(4)...)

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
	parser := NewRawResultParser()

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
	parser := NewRawResultParser()

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
	parser := NewRawResultParser()

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

func TestEncodeResultToMySQLPackets_TerminalOK(t *testing.T) {
	// Verify that EncodeResultToMySQLPackets encodes InsertID in the terminal
	// OK packet.
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

	encoded := EncodeResultToMySQLPackets(results)
	parser := NewRawResultParser()

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

// TestRawResultParser_RetainedRowsSurviveBufferReuse feeds two chunks through a
// single reused backing array — exactly what the gRPC client (pooled resp.Raw)
// and the in-process vtcombo path (reused tablet send buffer) do — while
// retaining the parsed rows past each callback. If the parser returned
// Values aliasing the caller's buffer, the first row would read the bytes the
// second Feed overwrote.
func TestRawResultParser_RetainedRowsSurviveBufferReuse(t *testing.T) {
	parser := NewRawResultParser()

	var retained [][]sqltypes.Value
	cb := func(r *sqltypes.Result) error {
		retained = append(retained, r.Rows...) // retain rows past the callback
		return nil
	}

	// Chunk 1: column count + column def + first row (no terminal yet).
	var c1 []byte
	c1 = append(c1, makePacket(1, []byte{1})...)
	c1 = append(c1, makeColumnDefPacket(2, "name", 0x0f, 0)...)
	c1 = append(c1, makeRowPacket(3, "alice")...)

	// Chunk 2: a longer second row + terminal EOF.
	var c2 []byte
	c2 = append(c2, makeRowPacket(4, "bob-is-a-deliberately-longer-value")...)
	c2 = append(c2, makeEOFPacket(5)...)

	shared := make([]byte, max(len(c1), len(c2)))

	copy(shared, c1)
	require.NoError(t, parser.Feed(shared[:len(c1)], cb))

	// Reuse the backing array for chunk 2 (overwrite chunk 1's bytes), then
	// scribble any tail so stale aliases cannot accidentally read valid data.
	for i := range shared {
		shared[i] = 0xff
	}
	copy(shared, c2)
	require.NoError(t, parser.Feed(shared[:len(c2)], cb))

	require.Len(t, retained, 2)
	require.Equal(t, "alice", retained[0][0].ToString())
	require.Equal(t, "bob-is-a-deliberately-longer-value", retained[1][0].ToString())
	require.Equal(t, "name", parser.fields[0].Name)
}

// TestRawResultParser_LeftoverSurvivesBufferReuse exercises the cross-chunk
// leftover path: a packet is split across two Feed calls that reuse the same
// backing array. The retained row that spans the split must remain intact.
func TestRawResultParser_LeftoverSurvivesBufferReuse(t *testing.T) {
	parser := NewRawResultParser()

	var retained [][]sqltypes.Value
	cb := func(r *sqltypes.Result) error {
		retained = append(retained, r.Rows...)
		return nil
	}

	var full []byte
	full = append(full, makePacket(1, []byte{1})...)
	full = append(full, makeColumnDefPacket(2, "val", 0x0f, 0)...)
	full = append(full, makeRowPacket(3, "spanning-row-value")...)
	full = append(full, makeEOFPacket(4)...)

	split := len(full) - 6 // cut inside the row/terminal region

	shared := make([]byte, len(full))
	copy(shared, full[:split])
	require.NoError(t, parser.Feed(shared[:split], cb)) // leaves leftover in p.buf

	for i := range split {
		shared[i] = 0xff // reuse/overwrite the chunk-1 region
	}
	copy(shared, full[split:])
	require.NoError(t, parser.Feed(shared[:len(full)-split], cb))

	require.Len(t, retained, 1)
	require.Equal(t, "spanning-row-value", retained[0][0].ToString())
}

// lenEncStringBytes encodes a length-encoded string (MySQL text-protocol
// column value): a length-encoded integer prefix followed by the bytes.
func lenEncStringBytes(val []byte) []byte {
	n := uint64(len(val))
	var prefix []byte
	switch {
	case n < 251:
		prefix = []byte{byte(n)}
	case n < 1<<16:
		prefix = []byte{0xfc, byte(n), byte(n >> 8)}
	case n < 1<<24:
		prefix = []byte{0xfd, byte(n), byte(n >> 8), byte(n >> 16)}
	default:
		prefix = make([]byte, 9)
		prefix[0] = 0xfe
		binary.LittleEndian.PutUint64(prefix[1:], n)
	}
	return append(prefix, val...)
}

// frameLogical splits a logical payload into physical packets per the MySQL
// MaxPacketSize rule (a run of exactly-MaxPacketSize fragments terminated by
// one < MaxPacketSize, which is a 0-length packet when the length is an exact
// multiple). It returns the concatenated wire bytes and the next sequence id.
func frameLogical(seq byte, logical []byte) ([]byte, byte) {
	var out []byte
	for {
		n := min(len(logical), MaxPacketSize)
		out = append(out, makePacket(seq, logical[:n])...)
		seq++
		if n < MaxPacketSize {
			return out, seq
		}
		logical = logical[n:]
		if len(logical) == 0 {
			out = append(out, makePacket(seq, nil)...) // 0-length terminator
			return out, seq + 1
		}
	}
}

// TestRawResultParser_LargeRowMultiFragment verifies a result row whose payload
// exceeds MaxPacketSize (split by MySQL into multiple physical packets) is
// reassembled and parsed correctly. The value is filled with 0xff so the
// continuation fragment begins with 0xff (ErrPacket) and the row's lenenc
// prefix is 0xfe (EOFPacket) — neither may be mistaken for a terminal packet.
func TestRawResultParser_LargeRowMultiFragment(t *testing.T) {
	parser := NewRawResultParser()
	var results []*sqltypes.Result
	cb := func(r *sqltypes.Result) error {
		results = append(results, r)
		return nil
	}

	value := make([]byte, 1<<24) // 16MiB; lenenc prefix is 0xfe (8-byte length)
	for i := range value {
		value[i] = 0xff
	}

	var raw []byte
	raw = append(raw, makePacket(1, []byte{1})...)               // column count = 1
	raw = append(raw, makeColumnDefPacket(2, "val", 0x0f, 0)...) // VARCHAR
	rowBytes, nextSeq := frameLogical(3, lenEncStringBytes(value))
	raw = append(raw, rowBytes...)
	raw = append(raw, makeEOFPacket(nextSeq)...) // terminal

	require.NoError(t, parser.Feed(raw, cb))

	var fields []*querypb.Field
	var rows [][]sqltypes.Value
	for _, r := range results {
		if r.Fields != nil {
			fields = r.Fields
		}
		rows = append(rows, r.Rows...)
	}
	require.Len(t, fields, 1)
	require.Len(t, rows, 1)
	require.Equal(t, value, rows[0][0].Raw())
}
