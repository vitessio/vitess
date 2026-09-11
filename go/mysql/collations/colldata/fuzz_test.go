/*
Copyright 2021 The Vitess Authors.

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

package colldata

import (
	"bytes"
	"testing"
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/vt/vthash"
)

func FuzzUCACollate(f *testing.F) {
	for _, left := range AllTestStrings {
		for _, right := range AllTestStrings {
			f.Add([]byte(left.Content), []byte(right.Content))
		}
	}

	coll := testcollation(f, "utf8mb4_0900_ai_ci")

	f.Fuzz(func(t *testing.T, left, right []byte) {
		_ = coll.Collate(left, right, false)
	})
}

func FuzzUCAWeightStrings(f *testing.F) {
	for _, input := range AllTestStrings {
		f.Add([]byte(input.Content))
	}

	coll := testcollation(f, "utf8mb4_0900_ai_ci")

	f.Fuzz(func(t *testing.T, input []byte) {
		_ = coll.WeightString(nil, input, 0)
	})
}

// fuzzCharsets is every charset reachable through a collation, plus the
// charsets that have no colldata implementation yet.
func fuzzCharsets() []charset.Charset {
	seen := map[string]bool{}
	var all []charset.Charset
	for _, coll := range testall() {
		cs := coll.Charset()
		if !seen[cs.Name()] {
			seen[cs.Name()] = true
			all = append(all, cs)
		}
	}
	all = append(all, charset.Charset_gb18030{})
	return all
}

// decodeWalk mirrors what byte-walking callers of DecodeRune do and checks the
// contract along the way: non-empty input always advances by 1..MaxWidth bytes
// within the input, and malformed input is reported as RuneError.
func decodeWalk(t *testing.T, cs charset.Charset, input []byte) (runes int, valid bool) {
	valid = true
	for in := input; len(in) > 0; {
		r, width, ok := cs.DecodeRune(in)
		if width < 1 || width > len(in) || width > cs.MaxWidth() {
			t.Fatalf("%s.DecodeRune(%#v) = %d, %d, %v: width out of range 1..min(%d, %d)",
				cs.Name(), in, r, width, ok, len(in), cs.MaxWidth())
		}
		if !ok {
			valid = false
			if r != charset.RuneError {
				t.Fatalf("%s.DecodeRune(%#v) = %d, %d, %v: rune is not RuneError for malformed input",
					cs.Name(), in, r, width, ok)
			}
		} else {
			var enc [8]byte
			encWidth := cs.EncodeRune(enc[:], r)
			if encWidth < 1 {
				t.Fatalf("%s.EncodeRune(%U) = %d: decoded rune does not encode",
					cs.Name(), r, encWidth)
			}
			r2, width2, ok2 := cs.DecodeRune(enc[:encWidth])
			if r2 != r || width2 != encWidth || !ok2 {
				t.Fatalf("%s: decode(encode(%U)) = %U, %d, %v: want %U, %d, true",
					cs.Name(), r, r2, width2, ok2, r, encWidth)
			}
		}
		in = in[width:]
		runes++
	}
	return runes, valid
}

func FuzzCharsetInvariants(f *testing.F) {
	for _, input := range AllTestStrings {
		f.Add([]byte(input.Content))
	}
	for _, seed := range [][]byte{
		{0x80},
		{0x81},
		{0xA0},
		{0xFF},
		{0x00},
		{0x81, 0x20, 0x41},
		{0x81, 0xAD},
		{0x80, 0x41},
		{0xD8, 0x00},
		{0xD8, 0x00, 0x00, 0x31},
		{0x00, 0xD8},
		{0xDC, 0x00},
		{0xFF, 0xFF, 0xFF, 0xFF},
		{0x00, 0x00, 0xD8, 0x00},
		{0x00, 0x11, 0x00, 0x00},
		{0xA1, 0x21},
		{0xA1, 0x20},
		{0xF8, 0xA1, 0xA1},
		{0x81, 0x3A, 0x81, 0x30},
		{0x81, 0x30, 0xFF, 0x30},
		{0x84, 0x31, 0xA5, 0x30},
		{0x8E, 0xE5},
		{0x8F, 0xA1, 0x20},
		{0x8F, 0xA1},
		{0xA2, 0xF1},
		{0xB0, 0xFF},
		{0xB0, 0x5B},
		{0xC9, 0x41},
		{0xF0, 0x9F, 0x98, 0x8A},
		{0xED, 0xA0, 0x80},
		{0xC2},
	} {
		f.Add(seed)
	}

	charsets := fuzzCharsets()
	utf8mb4 := charset.Charset_utf8mb4{}

	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 64 {
			t.Skip()
		}
		for _, cs := range charsets {
			runes, walkValid := decodeWalk(t, cs, input)

			if fastValid := charset.Validate(cs, input); fastValid != walkValid {
				t.Fatalf("%s: Validate(%#v) = %v, but walking DecodeRune says %v",
					cs.Name(), input, fastValid, walkValid)
			}

			if walkValid {
				if length := charset.Length(cs, input); length != runes {
					t.Fatalf("%s: Length(%#v) = %d, but walking DecodeRune counts %d",
						cs.Name(), input, length, runes)
				}
				for to := 0; to <= runes; to++ {
					sliced := charset.Slice(cs, input, 0, to)
					if !bytes.HasPrefix(input, sliced) {
						t.Fatalf("%s: Slice(%#v, 0, %d) = %#v: not a prefix of the input",
							cs.Name(), input, to, sliced)
					}
				}
			}

			if !utf8mb4.IsSuperset(cs) {
				// Converting from binary relabels the bytes and validates them
				// against the destination charset instead of walking the source.
				wantErr := !walkValid
				if _, isBinary := cs.(charset.Charset_binary); isBinary {
					wantErr = !charset.Validate(utf8mb4, input)
				}
				out, err := charset.Convert(nil, utf8mb4, input, cs)
				if !utf8.Valid(out) {
					t.Fatalf("%s: Convert(%#v) to utf8mb4 = %#v: not valid UTF-8",
						cs.Name(), input, out)
				}
				if gotErr := err != nil; gotErr != wantErr {
					t.Fatalf("%s: Convert(%#v) to utf8mb4 error = %v, want error = %v",
						cs.Name(), input, err, wantErr)
				}
			}
		}
	})
}

func FuzzCollationSafety(f *testing.F) {
	for _, input := range AllTestStrings {
		f.Add([]byte(input.Content), []byte(input.Content))
	}
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF}, []byte{0xD8, 0x00})
	f.Add([]byte{0x81, 0x20, 0x41}, []byte{0x84, 0x31, 0xA5, 0x30})

	all := testall()

	f.Fuzz(func(t *testing.T, left, right []byte) {
		if len(left) > 24 || len(right) > 24 {
			t.Skip()
		}
		for _, coll := range all {
			_ = coll.WeightString(nil, left, 0)
			_ = coll.WeightString(nil, left, 2)
			_ = coll.WeightString(nil, left, PadToMax)

			hasher := vthash.New()
			coll.Hash(&hasher, left, 0)

			if cmp := coll.Collate(left, left, false); cmp != 0 {
				t.Fatalf("%s: Collate(x, x) = %d, want 0 (x = %#v)", coll.Name(), cmp, left)
			}
			lr := coll.Collate(left, right, false)
			rl := coll.Collate(right, left, false)
			if (lr > 0 && rl > 0) || (lr < 0 && rl < 0) || (lr == 0) != (rl == 0) {
				t.Fatalf("%s: Collate(a, b) = %d but Collate(b, a) = %d (a = %#v, b = %#v)",
					coll.Name(), lr, rl, left, right)
			}

			_ = coll.Wildcard(left, 0, 0, 0).Match(right)
		}
	})
}
