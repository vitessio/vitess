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

// These benchmarks cover the SQL string-literal encoders that substitute bind
// variables into query text: vttablet runs them through
// ParsedQuery.GenerateQuery on every query that carries a string or binary
// bind variable, and VReplication runs them for every row it applies. They
// give a stable baseline for before-and-after comparisons with benchstat when
// the escaping loop changes.

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"vitess.io/vitess/go/bytes2"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// benchEncodeSizes spans the bind variable sizes the escaping loop sees in
// practice: short identifiers, typical column values, and text or blob
// payloads.
var benchEncodeSizes = []int{8, 32, 256, 4096}

// benchEncodeShapes are the byte distributions that matter to an escaping
// loop, since its cost is driven by how often it has to stop and emit an
// escape sequence rather than copy a clean run.
var benchEncodeShapes = []string{"clean", "sparse", "dense"}

// benchEncodeInput builds a deterministic input of the given size and shape:
// "clean" is ASCII text with nothing to escape, "sparse" is the same text
// with a single quote roughly every 256 bytes, and "dense" is seeded random
// binary where about one byte in 32 needs escaping.
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
	case "dense":
		rng := rand.New(rand.NewPCG(0x5eed, uint64(size)))
		for i := range buf {
			buf[i] = byte(rng.Uint32())
		}
	default:
		panic("unknown benchmark shape " + shape)
	}
	return buf
}

func BenchmarkEncodeSQL(b *testing.B) {
	for _, size := range benchEncodeSizes {
		for _, shape := range benchEncodeShapes {
			val := MakeTrusted(querypb.Type_VARCHAR, benchEncodeInput(size, shape))
			b.Run(fmt.Sprintf("Bytes2/%d/%s", size, shape), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(size))
				var buf bytes2.Buffer
				for b.Loop() {
					buf.Reset()
					val.EncodeSQLBytes2(&buf)
				}
			})
			b.Run(fmt.Sprintf("StringBuilder/%d/%s", size, shape), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(size))
				for b.Loop() {
					// GenerateQuery allocates a fresh, pre-grown builder per
					// query, so the benchmark does the same rather than reuse
					// one across iterations.
					var buf strings.Builder
					buf.Grow(size + 2)
					val.EncodeSQLStringBuilder(&buf)
				}
			})
		}
	}
}

// BenchmarkEncodeSQLReference measures the byte-at-a-time loop the run-based
// encoders replaced, so a single test binary reports today's code next to
// the new scalar and SIMD paths. It drives the old loop through a reused
// bytes2.Buffer exactly as the Bytes2 cells drive the new one, so the two
// differ only in the loop. Measured against a binary built at the baseline
// commit it is at parity from 256 bytes up and reads 8-12ns under it on the
// 8 and 32 byte cells; the Value.EncodeSQLBytes2 type switch the Bytes2
// cells go through and this does not measured about 1ns of that, and the
// rest, the same loop compiled into two binaries, is not traced.
func BenchmarkEncodeSQLReference(b *testing.B) {
	for _, size := range benchEncodeSizes {
		for _, shape := range benchEncodeShapes {
			in := benchEncodeInput(size, shape)
			b.Run(fmt.Sprintf("%d/%s", size, shape), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(size))
				var buf bytes2.Buffer
				for b.Loop() {
					buf.Reset()
					encodeBytesSQLReference(in, &buf)
				}
			})
		}
	}
}
