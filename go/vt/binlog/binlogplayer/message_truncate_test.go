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

// TestMessageTruncateWithBinaryData documents a known encoding asymmetry
// between how binary data is encoded into INSERT queries vs how error messages
// are re-encoded for storage in _vt.vreplication.message (varbinary(1000)).
//
// The INSERT query is built using encodeBytesSQLBytes2 which iterates over
// []byte (byte-by-byte) and uses WriteByte, preserving raw high bytes (128-255)
// as single bytes in the Go string. But when the error message is re-encoded by
// EncodeStringSQL for the UPDATE, it iterates over string (rune-by-rune with
// UTF-8 decoding) and uses WriteRune. Invalid UTF-8 bytes get expanded to
// 3-byte U+FFFD replacement characters, causing the stored value to potentially
// exceed the column limit.
//
// The controller's runBlp() handles this by falling back to a simplified error
// message when setState() fails, preventing an infinite retry loop.
// See: controller.go runBlp() and the fail-binary-no-default-value e2e test.
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
		msg.WriteString(fmt.Sprintf("(0,%d,_binary'", i%4))
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

	// Document the known asymmetry: EncodeStringSQL iterates the string by
	// rune (UTF-8 decoding), producing U+FFFD for each invalid byte. WriteRune
	// then re-encodes U+FFFD as 3 bytes. The round-trip is NOT symmetric for
	// strings containing invalid UTF-8, so the decoded value is larger than
	// the input. This can cause the stored value to exceed varbinary(1000).
	//
	// The controller handles this gracefully by falling back to a simplified
	// error message when setState() fails. See controller.go runBlp().
	assert.Greater(t, len(decoded), len(truncated),
		"encoding round-trip should expand invalid UTF-8 bytes (known asymmetry)")
	assert.Greater(t, len(decoded), 1000,
		"decoded value should exceed varbinary(1000) limit (known asymmetry that controller.runBlp handles)")
}
