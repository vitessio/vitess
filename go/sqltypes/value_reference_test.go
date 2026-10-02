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

package sqltypes

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/bytes2"
)

// The four encoders below are the pre-rewrite versions, copied verbatim, so
// that a before-and-after comparison is one `go test -bench` run rather than
// two binaries built at different commits. The benchmarks in
// value_bench_test.go drive them through the `impl=main` cells; see the
// comment there for the benchstat invocation.
//
// TestReferenceEncodersMatchOnASCII keeps them honest: an unchanged reference
// is only a baseline if it still agrees with the current encoders everywhere
// the rewrite did not deliberately change the output.

// refEncodeBytesSQLBytes2 is encodeBytesSQLBytes2 before the rewrite: one
// WriteByte per input byte, and no handling of bytes above ASCII.
func refEncodeBytesSQLBytes2(val []byte, buf *bytes2.Buffer) {
	buf.WriteByte('\'')
	for idx, ch := range val {
		// If \% or \_ is present, we want to keep them as is, and don't want to escape \ again
		if ch == '\\' && idx+1 < len(val) && (val[idx+1] == '%' || val[idx+1] == '_') {
			buf.WriteByte(ch)
			continue
		}
		if encodedChar := SQLEncodeMap[ch]; encodedChar == DontEscape {
			buf.WriteByte(ch)
		} else {
			buf.WriteByte('\\')
			buf.WriteByte(encodedChar)
		}
	}
	buf.WriteByte('\'')
}

// refEncodeBytesSQLStringBuilder is the strings.Builder twin of the above.
func refEncodeBytesSQLStringBuilder(val []byte, buf *strings.Builder) {
	buf.WriteByte('\'')
	for idx, ch := range val {
		// If \% or \_ is present, we want to keep them as is, and don't want to escape \ again
		if ch == '\\' && idx+1 < len(val) && (val[idx+1] == '%' || val[idx+1] == '_') {
			buf.WriteByte(ch)
			continue
		}
		if encodedChar := SQLEncodeMap[ch]; encodedChar == DontEscape {
			buf.WriteByte(ch)
		} else {
			buf.WriteByte('\\')
			buf.WriteByte(encodedChar)
		}
	}
	buf.WriteByte('\'')
}

// refEncodeBinarySQLBytes2 is encodeBinarySQLBytes2 before the rewrite, which
// converted the introducer to a []byte on every call.
func refEncodeBinarySQLBytes2(val []byte, buf *bytes2.Buffer) {
	buf.Write([]byte("_binary"))
	refEncodeBytesSQLBytes2(val, buf)
}

// refBufEncodeStringSQL is BufEncodeStringSQL before the rewrite. It walks
// runes rather than bytes, so every non-ASCII character is decoded and then
// re-encoded through WriteRune -- and a byte sequence that is not valid UTF-8
// is replaced with U+FFFD rather than preserved.
func refBufEncodeStringSQL(buf *strings.Builder, val string) {
	buf.WriteByte('\'')
	for idx, ch := range val {
		if ch > 255 {
			buf.WriteRune(ch)
			continue
		}
		// If \% or \_ is present, we want to keep them as is, and don't want to escape \ again
		if ch == '\\' && idx+1 < len(val) && (val[idx+1] == '%' || val[idx+1] == '_') {
			buf.WriteRune(ch)
			continue
		}
		if encodedChar := SQLEncodeMap[ch]; encodedChar == DontEscape {
			buf.WriteRune(ch)
		} else {
			buf.WriteByte('\\')
			buf.WriteByte(encodedChar)
		}
	}
	buf.WriteByte('\'')
}

// TestReferenceEncodersMatchOnASCII pins the references to the current
// encoders on every input where the rewrite was not meant to change the
// output, which is everything holding no byte above ASCII. A reference that
// has drifted from what it claims to be makes its benchmark cell meaningless,
// so this runs over the same inputs the benchmarks use.
//
// Inputs that do hold a high byte are covered by
// TestReferenceEncodersDivergeOnHighBytes below, which states the difference
// rather than asserting equality.
func TestReferenceEncodersMatchOnASCII(t *testing.T) {
	for _, shape := range benchEncodeShapes {
		if benchShapeHasHighBytes(shape) {
			continue
		}
		for _, size := range benchEncodeSizes {
			in := benchEncodeInput(size, shape)
			t.Run(shape+"/"+itoa(size), func(t *testing.T) {
				var gotB2, wantB2 bytes2.Buffer
				encodeBytesSQLBytes2(in, &gotB2)
				refEncodeBytesSQLBytes2(in, &wantB2)
				require.Equal(t, wantB2.String(), gotB2.String(), "encodeBytesSQLBytes2")

				var gotSB, wantSB strings.Builder
				encodeBytesSQLStringBuilder(in, &gotSB)
				refEncodeBytesSQLStringBuilder(in, &wantSB)
				require.Equal(t, wantSB.String(), gotSB.String(), "encodeBytesSQLStringBuilder")

				var gotStr, wantStr strings.Builder
				BufEncodeStringSQL(&gotStr, string(in))
				refBufEncodeStringSQL(&wantStr, string(in))
				require.Equal(t, wantStr.String(), gotStr.String(), "BufEncodeStringSQL")
			})
		}
	}
}

// TestReferenceEncodersDivergeOnHighBytes states the two deliberate output
// changes, so that the `impl=main` benchmark cells are read as a speed
// baseline and not as the same work. Both are why the rewrite exists:
//
//   - a run of bytes above ASCII ahead of a `\` is escaped, and a quote after
//     such a run is doubled, so that no multi-byte connection charset can read
//     our escape as a trail byte
//   - BufEncodeStringSQL preserves bytes that are not valid UTF-8 instead of
//     replacing each with U+FFFD
func TestReferenceEncodersDivergeOnHighBytes(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
		ref  string
	}{
		{
			name: "quote after a high-byte run is doubled",
			in:   "\x81'",
			want: "'\x81''" + "'",
			ref:  "'\x81" + "\\'" + "'",
		},
		{
			name: "high-byte run ahead of a backslash is escaped",
			in:   "\x81\\",
			want: "'" + "\\\x81" + "\\\\" + "'",
			ref:  "'\x81" + "\\\\" + "'",
		},
		{
			name: "a high byte alone is untouched",
			in:   "\x81",
			want: "'\x81'",
			ref:  "'\x81'",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got, ref strings.Builder
			encodeBytesSQLStringBuilder([]byte(tc.in), &got)
			refEncodeBytesSQLStringBuilder([]byte(tc.in), &ref)
			assert.Equal(t, tc.want, got.String(), "current")
			assert.Equal(t, tc.ref, ref.String(), "reference")
		})
	}

	// The rune walk could not carry invalid UTF-8 through: each bad byte came
	// back as a 3-byte U+FFFD, which is what let an already-truncated
	// vreplication message overflow varbinary(1000).
	const invalid = "\x81\xfe"
	var got, ref strings.Builder
	BufEncodeStringSQL(&got, invalid)
	refBufEncodeStringSQL(&ref, invalid)
	assert.Equal(t, "'"+invalid+"'", got.String(), "current preserves the bytes")
	assert.Equal(t, "'\uFFFD\uFFFD'", ref.String(), "reference replaced them")
	assert.Greater(t, ref.Len(), got.Len(), "and grew the string doing it")
}

// itoa keeps the subtest names free of a strconv import in a file that needs
// nothing else from it.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits [20]byte
	i := len(digits)
	for n > 0 {
		i--
		digits[i] = byte('0' + n%10)
		n /= 10
	}
	return string(digits[i:])
}
