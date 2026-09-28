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

package colldata_test

// These benchmarks cover the public charset and collation operations over a
// fixed corpus, one sub-benchmark for each charset or collation. They give a
// stable baseline for before-and-after comparisons with benchstat when the
// decoders, the conversion paths, or the matchers change.

import (
	"strings"
	"testing"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/vt/vthash"
)

var benchDataUTF8 = []byte(strings.Repeat("The quick brown fox 日本語のテキストと漢字が混ざった文字列 ascii again ", 16))

func benchCollation(b *testing.B, name string) colldata.Collation {
	coll := colldata.Lookup(collations.MySQL8().LookupByName(name))
	if coll == nil {
		b.Fatalf("missing collation %s", name)
	}
	return coll
}

var benchDataLatinUTF8 = []byte(strings.Repeat("The quick brown fox jumps över the lazy dög, très vite indeed! ", 16))

func benchNativeData(b *testing.B, cs charset.Charset) []byte {
	data, err := charset.ConvertFromUTF8(nil, cs, benchDataUTF8)
	if err == nil {
		return data
	}
	data, err = charset.ConvertFromUTF8(nil, cs, benchDataLatinUTF8)
	if err != nil {
		b.Fatalf("cannot convert benchmark data to %s: %v", cs.Name(), err)
	}
	return data
}

var benchCharsets = []charset.Charset{
	charset.Charset_latin1{},
	charset.Charset_sjis{},
	charset.Charset_euckr{},
	charset.Charset_gb18030{},
	charset.Charset_utf16{},
	charset.Charset_utf8mb4{},
}

func BenchmarkCharsetConvert(b *testing.B) {
	for _, cs := range benchCharsets {
		data := benchNativeData(b, cs)
		b.Run(cs.Name(), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				_, _ = charset.Convert(nil, charset.Charset_utf8mb3{}, data, cs)
			}
		})
	}
}

func BenchmarkCharsetValidate(b *testing.B) {
	for _, cs := range benchCharsets {
		data := benchNativeData(b, cs)
		b.Run(cs.Name(), func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				_ = charset.Validate(cs, data)
			}
		})
	}
}

func BenchmarkCharsetLength(b *testing.B) {
	for _, cs := range benchCharsets {
		data := benchNativeData(b, cs)
		b.Run(cs.Name(), func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				_ = charset.Length(cs, data)
			}
		})
	}
}

var benchCollationNames = []string{
	"utf8mb4_0900_ai_ci", "utf8mb4_0900_bin", "utf8mb4_general_ci",
	"utf8mb4_swedish_ci", "utf8mb4_bin", "latin1_swedish_ci",
	"sjis_japanese_ci", "utf16_unicode_ci",
}

func BenchmarkCollationWeightString(b *testing.B) {
	for _, name := range benchCollationNames {
		coll := benchCollation(b, name)
		data := benchNativeData(b, coll.Charset())
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			var dst []byte
			for i := 0; i < b.N; i++ {
				dst = coll.WeightString(dst[:0], data, 0)
			}
		})
	}
}

func BenchmarkCollationCollate(b *testing.B) {
	for _, name := range benchCollationNames {
		coll := benchCollation(b, name)
		data := benchNativeData(b, coll.Charset())
		other := append([]byte{}, data...)
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				_ = coll.Collate(data, other, false)
			}
		})
	}
}

func BenchmarkCollationHash(b *testing.B) {
	for _, name := range benchCollationNames {
		coll := benchCollation(b, name)
		data := benchNativeData(b, coll.Charset())
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				hasher := vthash.New()
				coll.Hash(&hasher, data, 0)
			}
		})
	}
}

func BenchmarkWildcardLiteral(b *testing.B) {
	for _, name := range benchCollationNames {
		coll := benchCollation(b, name)
		data := benchNativeData(b, coll.Charset())
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				if !coll.Wildcard(data, 0, 0, 0).Match(data) {
					b.Fatal("literal pattern must match itself")
				}
			}
		})
	}
}

func BenchmarkWildcardContains(b *testing.B) {
	for _, name := range benchCollationNames {
		coll := benchCollation(b, name)
		data := benchNativeData(b, coll.Charset())
		pct, err := charset.ConvertFromUTF8(nil, coll.Charset(), []byte("%"))
		if err != nil {
			b.Fatal(err)
		}
		pattern := append(append([]byte{}, pct...), charset.Slice(coll.Charset(), data, 0, 100)...)
		pattern = append(pattern, pct...)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				if !coll.Wildcard(pattern, 0, 0, 0).Match(data) {
					b.Fatal("contains pattern must match")
				}
			}
		})
	}
}

// benchSharedPrefix returns an n-byte ASCII buffer built from a fixed
// alphanumeric alphabet, with the byte at differAt bumped to the next letter
// so that two buffers built with different differAt values share the longest
// possible prefix and then differ in primary weight, never just in case. The
// alphabet has no punctuation so no byte has a zero (ignorable) weight, which
// keeps the UCA900 fast path engaged for the whole comparison.
func benchSharedPrefix(n, differAt int) []byte {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = alphabet[i%len(alphabet)]
	}
	if differAt >= 0 && differAt < n {
		buf[differAt] = alphabet[(differAt+1)%len(alphabet)]
	}
	return buf
}

// BenchmarkCollateSharedPrefix measures Collate on utf8mb4_0900_ai_ci for the
// inputs that reach it on the vtgate query path: ORDER BY, GROUP BY, DISTINCT
// and hash joins compare values through the tiny-weight prefix first, so the
// full collation only runs for strings that share their first four weight
// bytes. The shared-prefix cases differ in the last byte; the short case
// differs in the first four bytes and bounds the fixed cost of a compare that
// gets nothing from the fast path.
func BenchmarkCollateSharedPrefix(b *testing.B) {
	coll := benchCollation(b, "utf8mb4_0900_ai_ci")
	cases := []struct {
		name  string
		left  []byte
		right []byte
	}{
		{"16", benchSharedPrefix(16, -1), benchSharedPrefix(16, 15)},
		{"64", benchSharedPrefix(64, -1), benchSharedPrefix(64, 63)},
		{"256", benchSharedPrefix(256, -1), benchSharedPrefix(256, 255)},
		{"1024", benchSharedPrefix(1024, -1), benchSharedPrefix(1024, 1023)},
		{"short-16", benchSharedPrefix(16, -1), benchSharedPrefix(16, 2)},
	}
	for _, tc := range cases {
		b.Run("utf8mb4_0900_ai_ci/"+tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(tc.left)))
			for b.Loop() {
				if coll.Collate(tc.left, tc.right, false) == 0 {
					b.Fatal("inputs differ but Collate returned 0")
				}
			}
		})
	}
}
