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

// TestMessageTruncateWithBinaryData covers MessageTruncate's bound for an error
// message carrying raw binary data: 950 bytes in has to still be 950 bytes
// after EncodeStringSQL, so the value fits the varbinary(1000) column that
// setMessage() and setVReplicationState() write it to.
//
// That did not hold before. EncodeStringSQL walked the string rune by rune, so
// each invalid UTF-8 byte came back as a 3-byte U+FFFD and an already-truncated
// message could still overflow the column -- the UPDATE failed, and the error
// path retried it forever (#19423). EncodeStringSQL walks bytes now, so the
// round-trip is symmetric and `maxLen = 950` is a real bound rather than an
// approximate one.
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

	// DecodeStringSQL rejects `\%` and `\_`, which the encoder passes through
	// untouched so MySQL keeps treating them as LIKE literals. The comparison
	// below is only well-defined without them, so assert that rather than
	// leaving it to luck.
	require.NotContains(t, truncated, `\%`, "payload must avoid the one sequence DecodeStringSQL rejects")
	require.NotContains(t, truncated, `\_`, "payload must avoid the one sequence DecodeStringSQL rejects")

	// Decode (simulating what MySQL stores after processing the UPDATE).
	decoded, err := sqltypes.DecodeStringSQL(encoded)
	require.NoError(t, err, "DecodeStringSQL should not error")

	// Invalid UTF-8 has to survive the round-trip byte for byte, so that
	// MessageTruncate's limit still holds once the message is encoded.
	assert.Equal(t, truncated, decoded,
		"encoding round-trip must be byte-symmetric for invalid UTF-8")
	assert.LessOrEqual(t, len(decoded), 1000,
		"a truncated message must fit varbinary(1000) after encoding")
}
