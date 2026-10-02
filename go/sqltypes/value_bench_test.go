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

	"vitess.io/vitess/go/bytes2"
)

// These benchmarks cover the SQL string-literal encoders that substitute bind
// variables into query text: vttablet runs them through
// ParsedQuery.GenerateQuery on every query that carries a string or binary
// bind variable, and VReplication runs them for every row it applies.
//
// Every cell runs twice, as `impl=main` and `impl=branch`, so one run reports
// the byte-at-a-time loop next to the one that replaced it:
//
//	go test ./go/sqltypes/ -run='^$' -bench=BenchmarkEncode -count=10 | tee bench.txt
//	benchstat -col impl bench.txt
//
// The `impl=main` cells call the reference copies in value_reference_test.go,
// which TestReferenceEncodersMatchOnASCII pins to the current encoders
// everywhere the rewrite did not deliberately change the output. Where it did
// -- a high-byte run ahead of a `\`, and invalid UTF-8 through
// BufEncodeStringSQL -- the two are not doing the same work, and
// TestReferenceEncodersDivergeOnHighBytes says exactly how they differ.

// benchEncodeSizes spans the bind variable sizes the escaping loop sees in
// practice: short identifiers, typical column values, and text or blob
// payloads.
var benchEncodeSizes = []int{8, 32, 256, 4096}

// benchEncodeShapes are the byte distributions that matter to an escaping
// loop, since its cost is driven by how often it has to stop and emit an
// escape sequence rather than copy a clean run.
var benchEncodeShapes = []string{
	"clean", "sparse", "random", "zeros", "quotes", "backslashes", "wildcards",
	"highbytes", "highbytes-hazard",
}

// benchShapeHasHighBytes reports whether a shape holds any byte above ASCII,
// which is what decides whether the current encoders and the references are
// doing the same work in that cell.
func benchShapeHasHighBytes(shape string) bool {
	switch shape {
	case "random", "highbytes", "highbytes-hazard":
		return true
	}
	return false
}

// benchEncodeInput builds a deterministic input of the given size and shape:
// "random" has about one escape per 32 bytes; zeros, quotes and backslashes
// exercise consecutive escapes. Wildcards exercise the preserved \% and \_
// pairs. "highbytes" is ordinary non-ASCII prose, which is what most text
// carrying a high byte actually looks like, and "highbytes-hazard" is the
// worst case for the rewrite: a high byte before every `\`, so every run is
// escaped byte by byte and the literal grows by a third.
func benchEncodeInput(size int, shape string) []byte {
	const text = "The quick brown fox jumps over the lazy dog, then it does so again. "
	buf := make([]byte, size)
	switch shape {
	case "clean", "sparse":
		for i := range buf {
			buf[i] = text[i%len(text)]
		}
		if shape == "sparse" {
			for i := min(size/2, 128); i < size; i += 256 {
				buf[i] = '\''
			}
		}
	case "random":
		rng := rand.New(rand.NewPCG(0x5eed, uint64(size)))
		for i := range buf {
			buf[i] = byte(rng.Uint32())
		}
	case "zeros":
	case "quotes", "backslashes":
		ch := byte('\'')
		if shape == "backslashes" {
			ch = '\\'
		}
		for i := range buf {
			buf[i] = ch
		}
	case "wildcards":
		const pattern = `\%\_`
		for i := range buf {
			buf[i] = pattern[i%len(pattern)]
		}
	case "highbytes":
		const accented = "L'été où le renard brun sauta par-dessus le chien paresseux; à nouveau. "
		for i := range buf {
			buf[i] = accented[i%len(accented)]
		}
	case "highbytes-hazard":
		const pattern = "\xaa\\"
		for i := range buf {
			buf[i] = pattern[i%len(pattern)]
		}
	default:
		panic("unknown benchmark shape " + shape)
	}
	return buf
}

// BenchmarkEncodeSQL covers the two byte-slice encoders. Both cells call the
// encoder directly rather than through Value, so the two differ only in the
// loop and not in the type switch above it.
func BenchmarkEncodeSQL(b *testing.B) {
	for _, size := range benchEncodeSizes {
		for _, shape := range benchEncodeShapes {
			in := benchEncodeInput(size, shape)

			for _, impl := range []struct {
				name   string
				bytes2 func([]byte, *bytes2.Buffer)
				sb     func([]byte, *strings.Builder)
			}{
				{"main", refEncodeBytesSQLBytes2, refEncodeBytesSQLStringBuilder},
				{"branch", encodeBytesSQLBytes2, encodeBytesSQLStringBuilder},
			} {
				b.Run(fmt.Sprintf("Bytes2/%d/%s/impl=%s", size, shape, impl.name), func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(size))
					var buf bytes2.Buffer
					for b.Loop() {
						buf.Reset()
						impl.bytes2(in, &buf)
					}
				})
				b.Run(fmt.Sprintf("StringBuilder/%d/%s/impl=%s", size, shape, impl.name), func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(size))
					for b.Loop() {
						// GenerateQuery allocates a fresh, pre-grown builder
						// per query, so the benchmark does the same rather
						// than reuse one across iterations.
						var buf strings.Builder
						buf.Grow(size + 2)
						impl.sb(in, &buf)
					}
				})
			}
		}
	}
}

// BenchmarkEncodeStringSQL covers BufEncodeStringSQL, which walks the input as
// a string. The pre-rewrite version walked it rune by rune and wrote each one
// back through WriteRune, so its cost scales with how much of the input is
// non-ASCII; the current one walks bytes over a zero-copy view. This is the
// path vreplication takes for every workflow name, table name and error
// message it writes to _vt.vreplication.
func BenchmarkEncodeStringSQL(b *testing.B) {
	for _, size := range benchEncodeSizes {
		for _, shape := range benchEncodeShapes {
			in := string(benchEncodeInput(size, shape))

			for _, impl := range []struct {
				name   string
				encode func(*strings.Builder, string)
			}{
				{"main", refBufEncodeStringSQL},
				{"branch", BufEncodeStringSQL},
			} {
				b.Run(fmt.Sprintf("%d/%s/impl=%s", size, shape, impl.name), func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(size))
					for b.Loop() {
						var buf strings.Builder
						buf.Grow(size + 2)
						impl.encode(&buf, in)
					}
				})
			}
		}
	}
}

// BenchmarkEncodeBinarySQL isolates the `_binary` introducer write, which the
// rewrite changed from Write([]byte("_binary")) to WriteString. Only the
// smallest payloads say anything: at 4KB the introducer is lost in the value.
func BenchmarkEncodeBinarySQL(b *testing.B) {
	for _, size := range []int{8, 32} {
		in := benchEncodeInput(size, "clean")

		for _, impl := range []struct {
			name   string
			encode func([]byte, *bytes2.Buffer)
		}{
			{"main", refEncodeBinarySQLBytes2},
			{"branch", encodeBinarySQLBytes2},
		} {
			b.Run(fmt.Sprintf("%d/impl=%s", size, impl.name), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(size))
				var buf bytes2.Buffer
				for b.Loop() {
					buf.Reset()
					impl.encode(in, &buf)
				}
			})
		}
	}
}
