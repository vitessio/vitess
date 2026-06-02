/*
Copyright 2026 The Vitess Authors.

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

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// buildColumnDefPacket builds a full ColumnDefinition41 packet (4-byte header +
// payload) with the given schema, table and column name.
func buildColumnDefPacket(t *testing.T, seq byte, schema, table, name string) []byte {
	t.Helper()

	var payload []byte
	writeStr := func(s string) {
		require.Less(t, len(s), 251, "test helper only supports short strings")
		payload = append(payload, byte(len(s)))
		payload = append(payload, s...)
	}

	writeStr("def")                  // catalog
	writeStr(schema)                 // schema
	writeStr(table)                  // table
	writeStr(table)                  // org_table
	writeStr(name)                   // name
	writeStr(name)                   // org_name
	payload = append(payload, 0x0c)  // length of fixed-length fields
	payload = append(payload, 33, 0) // character set
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, 255)
	payload = append(payload, b...) // column length
	payload = append(payload, 0x0f) // type
	payload = append(payload, 0, 0) // flags
	payload = append(payload, 0)    // decimals
	payload = append(payload, 0, 0) // filler

	header := make([]byte, PacketHeaderSize)
	header[0] = byte(len(payload))
	header[1] = byte(len(payload) >> 8)
	header[2] = byte(len(payload) >> 16)
	header[3] = seq

	return append(header, payload...)
}

// fieldAt parses the column-definition packet at buf[start:end].
func fieldAt(t *testing.T, buf []byte, start, end int) *querypb.Field {
	t.Helper()
	field := &querypb.Field{}
	require.NoError(t, ParseColumnDefinition(buf[start+PacketHeaderSize:end], field, 0))
	return field
}

// payloadLenAt decodes the 3-byte little-endian payload length from the header
// at buf[start:].
func payloadLenAt(buf []byte, start int) int {
	return int(uint32(buf[start]) | uint32(buf[start+1])<<8 | uint32(buf[start+2])<<16)
}

// rewriteInBuf copies packet into a fresh buffer with slack trailing capacity
// and rewrites it in place at offset 0.
func rewriteInBuf(packet []byte, slack int, oldDB, newKeyspace string) (buf []byte, newEnd int, changed bool, err error) {
	buf = make([]byte, len(packet)+slack)
	copy(buf, packet)
	newEnd, changed, err = RewriteColumnDefinitionSchemaInPlace(buf, 0, len(packet), oldDB, newKeyspace)
	return buf, newEnd, changed, err
}

func TestRewriteColumnDefinitionSchemaInPlace_ShorterKeyspace(t *testing.T) {
	const seq = 7
	packet := buildColumnDefPacket(t, seq, "vt_physical_db", "users", "id")

	buf, newEnd, changed, err := rewriteInBuf(packet, 0, "vt_physical_db", "customer")
	require.NoError(t, err)
	require.True(t, changed)
	require.Less(t, newEnd, len(packet), "shorter keyspace shrinks the packet")

	field := fieldAt(t, buf, 0, newEnd)
	assert.Equal(t, "customer", field.Database)
	assert.Equal(t, "users", field.Table)
	assert.Equal(t, "users", field.OrgTable)
	assert.Equal(t, "id", field.Name)
	assert.Equal(t, "id", field.OrgName)

	// The 3-byte length header matches the new payload, the sequence id survives.
	assert.Equal(t, newEnd-PacketHeaderSize, payloadLenAt(buf, 0))
	assert.Equal(t, byte(seq), buf[3])
}

func TestRewriteColumnDefinitionSchemaInPlace_LongerKeyspace(t *testing.T) {
	const ks = "a_much_longer_keyspace_name"
	packet := buildColumnDefPacket(t, 1, "db", "t", "c")

	// Reserve room for the growth.
	buf, newEnd, changed, err := rewriteInBuf(packet, len(ks), "db", ks)
	require.NoError(t, err)
	require.True(t, changed)
	require.Greater(t, newEnd, len(packet), "longer keyspace grows the packet")

	field := fieldAt(t, buf, 0, newEnd)
	assert.Equal(t, ks, field.Database)
	assert.Equal(t, "t", field.Table)
	assert.Equal(t, "c", field.Name)
	assert.Equal(t, newEnd-PacketHeaderSize, payloadLenAt(buf, 0))
}

func TestRewriteColumnDefinitionSchemaInPlace_LeavesOtherSchemasUntouched(t *testing.T) {
	// The parity corner: information_schema fields must NOT be rewritten.
	packet := buildColumnDefPacket(t, 3, "information_schema", "TABLES", "TABLE_NAME")

	buf, newEnd, changed, err := rewriteInBuf(packet, 0, "vt_physical_db", "customer")
	require.NoError(t, err)
	assert.False(t, changed)
	require.Equal(t, len(packet), newEnd, "no rewrite leaves the end offset unchanged")
	// The packet bytes are untouched.
	assert.Equal(t, packet, buf[:newEnd])
	assert.Equal(t, "information_schema", fieldAt(t, buf, 0, newEnd).Database)
}

func TestRewriteColumnDefinitionSchemaInPlace_EmptySchemaNotRewritten(t *testing.T) {
	// An empty schema is not the physical DB name, so it is left alone.
	packet := buildColumnDefPacket(t, 1, "", "t", "c")
	buf, newEnd, changed, err := rewriteInBuf(packet, 0, "vt_physical_db", "customer")
	require.NoError(t, err)
	assert.False(t, changed)
	assert.Equal(t, len(packet), newEnd)
	assert.Equal(t, "", fieldAt(t, buf, 0, newEnd).Database)
}

// TestRewriteColumnDefinitionSchemaInPlace_AtOffset rewrites a packet that is not
// at the start of the buffer and verifies the preceding bytes are untouched —
// the tablet batches several column definitions in one buffer.
func TestRewriteColumnDefinitionSchemaInPlace_AtOffset(t *testing.T) {
	prefix := []byte("PRECEDING-PACKET-BYTES")
	packet := buildColumnDefPacket(t, 9, "vt_physical_db", "t", "c")

	buf := make([]byte, len(prefix)+len(packet))
	copy(buf, prefix)
	copy(buf[len(prefix):], packet)

	start := len(prefix)
	end := start + len(packet)
	newEnd, changed, err := RewriteColumnDefinitionSchemaInPlace(buf, start, end, "vt_physical_db", "customer")
	require.NoError(t, err)
	require.True(t, changed)

	assert.Equal(t, prefix, buf[:start], "bytes before the packet must be untouched")
	assert.Equal(t, "customer", fieldAt(t, buf, start, newEnd).Database)
	assert.Equal(t, newEnd-start-PacketHeaderSize, payloadLenAt(buf, start))
}

func TestRewriteColumnDefinitionSchemaInPlace_DoesNotFit(t *testing.T) {
	const ks = "a_much_longer_keyspace_name"
	packet := buildColumnDefPacket(t, 1, "db", "t", "c")

	// No slack: the grown packet cannot fit in the buffer.
	_, _, changed, err := rewriteInBuf(packet, 0, "db", ks)
	require.ErrorContains(t, err, "does not fit")
	assert.False(t, changed)
}

func TestRewriteColumnDefinitionSchemaInPlace_Malformed(t *testing.T) {
	tests := []struct {
		name   string
		packet []byte
	}{
		{"too short for header", []byte{0, 0}},
		{"truncated catalog", []byte{0x05, 0, 0, 1, 0x03, 'd', 'e'}},
		{"truncated schema", []byte{0x06, 0, 0, 1, 0x03, 'd', 'e', 'f', 0x05}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, len(tc.packet)+64)
			copy(buf, tc.packet)
			_, changed, err := RewriteColumnDefinitionSchemaInPlace(buf, 0, len(tc.packet), "def", "ks")
			require.Error(t, err)
			assert.False(t, changed)
		})
	}
}
