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
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/bytes2"
)

// encodeBytesSQLReference is the old encodeBytesSQLBytes2, verbatim: the
// byte-at-a-time loop the run-based encoders replaced, writing through the
// same bytes2.Buffer it did. It is kept as the definition of correct output,
// which every encoder must reproduce for every input, and as the benchmark's
// "today" cell, which then differs from the Bytes2 cell only in the loop:
// an append-based copy measured faster than the real loop, and a fresh
// slice per call measured slower, and neither was today's code.
func encodeBytesSQLReference(val []byte, buf *bytes2.Buffer) {
	buf.WriteByte('\'')
	for idx, ch := range val {
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

// referenceOutput runs val through encodeBytesSQLReference and returns the
// literal it produces.
func referenceOutput(val []byte) []byte {
	var buf bytes2.Buffer
	encodeBytesSQLReference(val, &buf)
	return buf.Bytes()
}

// encodeAll runs val through both run-based encoders and returns their
// output.
func encodeAll(val []byte) (bytes2Out, builderOut []byte) {
	var b2 bytes2.Buffer
	encodeBytesSQLBytes2(val, &b2)
	var sb strings.Builder
	encodeBytesSQLStringBuilder(val, &sb)
	return b2.Bytes(), []byte(sb.String())
}

func requireEncodesLikeReference(t *testing.T, name string, val []byte) {
	t.Helper()
	want := referenceOutput(val)
	gotB2, gotSB := encodeAll(val)
	require.Equal(t, string(want), string(gotB2), "%s: Bytes2", name)
	require.Equal(t, string(want), string(gotSB), "%s: StringBuilder", name)
}

func TestEncodeBytesSQLMatchesReference(t *testing.T) {
	escapes := []byte{0, '\'', '\b', '\n', '\r', '\t', 26, '\\'}
	clean := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = 'a' + byte(i%26)
		}
		return b
	}

	// Every escape byte at every position around the vector block
	// boundaries, so a hit lands in the last lane of a full block, the first
	// lane of the next, and inside the overlapping tail block.
	for _, n := range []int{0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65} {
		requireEncodesLikeReference(t, fmt.Sprintf("clean %d", n), clean(n))
		for pos := range n {
			for _, e := range escapes {
				in := clean(n)
				in[pos] = e
				requireEncodesLikeReference(t, fmt.Sprintf("byte %#x at %d of %d", e, pos, n), in)
			}
		}
	}

	// The \% and \_ carve-out: a backslash followed by a wildcard is
	// preserved, including when the backslash is the last byte of a block
	// and the wildcard the first of the next, and when the backslash is the
	// last byte of the input.
	for _, n := range []int{16, 32, 64} {
		for _, w := range []byte{'%', '_', 'x'} {
			in := clean(n + 8)
			in[n-1] = '\\'
			in[n] = w
			requireEncodesLikeReference(t, fmt.Sprintf("backslash-%c across %d", w, n), in)
		}
		in := clean(n)
		in[n-1] = '\\'
		requireEncodesLikeReference(t, fmt.Sprintf("trailing backslash at %d", n), in)
	}
	requireEncodesLikeReference(t, "only a backslash", []byte{'\\'})
	requireEncodesLikeReference(t, "backslash then percent", []byte{'\\', '%'})
	requireEncodesLikeReference(t, "runs of escapes", []byte("''''\\\\\\\\\n\n\r\r\x00\x00"))

	// Seeded random binary, which is what blob bind variables look like.
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range 10000 {
		n := rng.IntN(300)
		in := make([]byte, n)
		for j := range in {
			in[j] = byte(rng.Uint32())
		}
		requireEncodesLikeReference(t, fmt.Sprintf("random #%d", i), in)
	}
}

func FuzzEncodeBytesSQL(f *testing.F) {
	f.Add([]byte("hello"))
	f.Add([]byte("it's a \\% test\\_ with \\ and \n"))
	f.Add([]byte("aaaaaaaaaaaaaaaa\\%"))
	f.Add(append(make([]byte, 31), '\''))
	f.Fuzz(func(t *testing.T, in []byte) {
		want := referenceOutput(in)
		gotB2, gotSB := encodeAll(in)
		if string(gotB2) != string(want) {
			t.Fatalf("Bytes2 encoder diverged for %q:\n got %q\nwant %q", in, gotB2, want)
		}
		if string(gotSB) != string(want) {
			t.Fatalf("StringBuilder encoder diverged for %q:\n got %q\nwant %q", in, gotSB, want)
		}
	})
}
