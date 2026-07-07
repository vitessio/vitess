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

package evalengine

import (
	"bytes"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mysqlBase64Reference encodes the way MySQL does: standard base64 with a
// newline after every 76 output characters, and no trailing newline.
func mysqlBase64Reference(in []byte) []byte {
	encoded := base64.StdEncoding.EncodeToString(in)
	var out bytes.Buffer
	for len(encoded) > mysqlBase64OutLineLength {
		out.WriteString(encoded[:mysqlBase64OutLineLength])
		out.WriteByte('\n')
		encoded = encoded[mysqlBase64OutLineLength:]
	}
	out.WriteString(encoded)
	return out.Bytes()
}

func TestMysqlBase64Encode(t *testing.T) {
	// Inputs whose length is an exact non-zero multiple of 57 bytes used
	// to get a spurious trailing NUL byte appended, because the newline
	// count was over-estimated by one for the final full block.
	lengths := []int{0, 1, 56, 57, 58, 113, 114, 115, 171}

	for _, n := range lengths {
		in := bytes.Repeat([]byte{'a'}, n)
		encoded := mysqlBase64Encode(in)

		assert.Equal(t, string(mysqlBase64Reference(in)), string(encoded), "input length %d", n)
		assert.NotContains(t, string(encoded), "\x00", "input length %d: encoded output must not contain NUL", n)
	}
}

func TestMysqlBase64EncodeDecodeRoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 56, 57, 58, 113, 114, 115, 171, 500} {
		in := bytes.Repeat([]byte{'x'}, n)

		decoded, err := mysqlBase64Decode(mysqlBase64Encode(in))
		require.NoError(t, err, "input length %d", n)
		assert.Equal(t, in, decoded, "input length %d", n)
	}
}
