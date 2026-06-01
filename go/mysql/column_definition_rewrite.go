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
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// RewriteColumnDefinitionSchemaInPlace rewrites the schema (database) field of
// the ColumnDefinition41 packet stored at buf[start:end] in place, but only when
// the schema is exactly oldDB. This mirrors the legacy, non-raw StreamExecute
// behaviour where the tablet replaces the physical MySQL DB name with the
// keyspace name in result-set field metadata, leaving other schemas (e.g.
// information_schema) untouched.
//
// buf[start:end] is a full MySQL packet: a 4-byte header (3-byte little-endian
// payload length + 1-byte sequence id) followed by the ColumnDefinition41
// payload, whose layout is: lenenc_str catalog, schema, table, org_table, name,
// org_name, then fixed-length fields. Only schema is replaced.
//
// The keyspace name rarely matches the physical DB name's length, so the bytes
// after the schema are shifted within buf to make room (or close the gap) and
// the 3-byte length header is recomputed; the sequence-id byte is preserved.
// The caller must leave enough room after end for any growth — at most
// LenEncStringSize(newKeyspace) bytes. The function returns the new end offset
// (== end when no rewrite happens) and whether a rewrite occurred. A malformed
// packet, or a rewrite that would not fit in buf, fails fast.
func RewriteColumnDefinitionSchemaInPlace(buf []byte, start, end int, oldDB, newKeyspace string) (int, bool, error) {
	if end-start < PacketHeaderSize {
		return end, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "column definition packet too short: %d bytes", end-start)
	}

	payloadStart := start + PacketHeaderSize
	payload := buf[payloadStart:end]

	// Skip the catalog ("def"), then locate the schema lenenc string. Offsets
	// are relative to the payload start.
	schemaRel, ok := skipLenEncString(payload, 0)
	if !ok {
		return end, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "malformed column definition packet: cannot skip catalog")
	}
	schema, schemaEndRel, ok := readLenEncString(payload, schemaRel)
	if !ok {
		return end, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "malformed column definition packet: cannot read schema")
	}

	if schema != oldDB {
		return end, false, nil
	}

	schemaPos := payloadStart + schemaRel
	schemaEnd := payloadStart + schemaEndRel
	delta := lenEncStringSize(newKeyspace) - (schemaEnd - schemaPos)
	newEnd := end + delta

	newPayloadLen := (end - payloadStart) + delta
	if newPayloadLen > MaxPacketSize {
		return end, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "rewritten column definition payload too large: %d bytes", newPayloadLen)
	}
	if newEnd > len(buf) {
		return end, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "rewritten column definition does not fit in buffer")
	}

	// Shift the bytes after the schema to their new position (copy handles
	// overlapping ranges in either direction), then write the new schema and
	// recompute the 3-byte payload length. The sequence-id byte is untouched.
	copy(buf[schemaEnd+delta:newEnd], buf[schemaEnd:end])
	writeLenEncString(buf, schemaPos, newKeyspace)
	buf[start] = byte(newPayloadLen)
	buf[start+1] = byte(newPayloadLen >> 8)
	buf[start+2] = byte(newPayloadLen >> 16)

	return newEnd, true, nil
}
