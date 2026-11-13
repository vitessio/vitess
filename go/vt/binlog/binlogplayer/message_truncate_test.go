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

package binlogplayer

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
)

// TestMessageTruncateWithBinaryData verifies that binary data (including
// invalid UTF-8 bytes) survives truncation and SQL encode/decode round-trip
// without expansion.
//
// Historically there was an encoding asymmetry: encodeBytesSQLBytes2 (used for
// INSERT queries) iterates []byte byte-by-byte with WriteByte, preserving raw
// high bytes. But EncodeStringSQL (used for UPDATE messages) used rune-by-rune
// iteration with WriteRune, causing invalid UTF-8 bytes to expand to 3-byte
// U+FFFD replacement characters. This could make a truncated message (>950B
// truncated value) exceed the varbinary(1000) column limit after round-trip.
//
// That asymmetry was fixed by PR #21248, which changed BufEncodeStringSQL to
// iterate byte-by-byte and use WriteByte, matching the behavior of
// encodeBytesSQLBytes2. The round-trip is now perfectly symmetric and
// truncation to 950 bytes guarantees the stored value fits in varbinary(1000).
func TestMessageTruncateWithBinaryData(t *testing.T) {
	// Build a realistic error message. The binary data in the INSERT query
	// was encoded by encodeBytesSQLBytes2 which preserves raw bytes. So the
	// Go string contains raw high bytes (0x80-0xFF) that are NOT valid UTF-8.
	var msg strings.Builder

	msg.WriteString("task error: failed inserting rows: Field 'workspace_id' doesn't have a default value (errno 1364) (sqlstate HY000) during query: insert into _vt_vrp_2354fd5f43b850e8a66b2375d6c09642_20260218201401_(chunk,type,`name`,`data`,environment_id) values ")

	// Simulate how encodeBytesSQLBytes2 writes binary values: raw bytes are
	// preserved as-is (not re-encoded as UTF-8 runes). Build values that
	// contain raw high bytes, similar to bitmap data.
	for i := range 30 {
		if i > 0 {
			msg.WriteString(", ")
		}
		fmt.Fprintf(&msg, "(0,%d,_binary'", i%4)
		msg.WriteString("id")
		msg.WriteString("',_binary'")
		// Write raw binary data the way encodeBytesSQLBytes2 does: raw bytes
		// including invalid UTF-8 sequences.
		msg.WriteByte(0x00) // null byte
		msg.WriteByte(0x80) // invalid UTF-8 standalone
		msg.WriteByte(0x0d) // carriage return
		msg.WriteByte(0xc2) // start of 2-byte UTF-8 sequence...
		msg.WriteByte(0xa6) // ...valid pair (U+00A6)
		msg.WriteByte(0xff) // invalid UTF-8
		msg.WriteByte(0x80) // invalid UTF-8 standalone
		msg.WriteByte(0xfe) // invalid UTF-8
		msg.WriteByte(0x90) // invalid UTF-8 standalone
		msg.WriteString("',0)")
	}

	fullMessage := msg.String()
	require.Greater(t, len(fullMessage), 950, "message should be longer than truncation limit")

	// MessageTruncate correctly limits the raw string to 950 bytes.
	truncated := MessageTruncate(fullMessage)
	assert.LessOrEqual(t, len(truncated), 950, "MessageTruncate should limit to 950 bytes")

	// Encode for SQL (as setState/setMessage does via encodeString).
	encoded := sqltypes.EncodeStringSQL(truncated)

	// Decode (simulating what MySQL stores after processing the UPDATE).
	decoded, err := sqltypes.DecodeStringSQL(encoded)
	require.NoError(t, err, "DecodeStringSQL should not error")

	// With the byte-preserving BufEncodeStringSQL (PR #21248), the round-trip
	// is perfectly symmetric. Invalid UTF-8 bytes are preserved as raw bytes
	// instead of being expanded to 3-byte U+FFFD replacement characters.
	assert.Equal(t, truncated, decoded,
		"encoding round-trip should preserve binary data exactly (byte-preserving BufEncodeStringSQL)")
	assert.LessOrEqual(t, len(decoded), 950,
		"decoded value must not exceed MessageTruncate limit of 950 bytes (and thus fits in varbinary(1000))")
}
