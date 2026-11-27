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

package colldata

import (
	"bytes"
	"encoding/hex"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/vt/vthash"
)

// TestTIS620ThaiCI checks tis620_thai_ci against values measured on MySQL
// 8.4.13 and 8.0.46 with a tis620_thai_ci column: WEIGHT_STRING('เก') is
// A1 E0, the consonant before the leading vowel.
func TestTIS620ThaiCI(t *testing.T) {
	coll := testcollation(t, "tis620_thai_ci")
	tis620 := func(s string) []byte {
		converted, err := charset.ConvertFromUTF8(nil, coll.Charset(), []byte(s))
		require.NoError(t, err)
		return converted
	}

	t.Run("weight string", func(t *testing.T) {
		for input, weights := range map[string]string{
			"กา": "A1D2",
			// The leading vowel changes places with the consonant.
			"เก": "A1E0",
			// The tone mark moves to the end.
			"ก่า": "A1D2F3",
			"ก้า": "A1D2F4",
			"ไก่": "A1E4FB",
		} {
			expected, err := hex.DecodeString(weights)
			require.NoError(t, err)
			assert.Equal(t, expected, coll.WeightString(nil, tis620(input), 0), "WEIGHT_STRING(%q)", input)
		}

		// WEIGHT_STRING(X'A6A2E85FC1CEE3' AS CHAR(5)) cuts the string to
		// five characters first: the weight of the mark E8 is the last
		// byte, in the place of C1.
		input, err := hex.DecodeString("A6A2E85FC1CEE3")
		require.NoError(t, err)
		expected, err := hex.DecodeString("A6A25FC1EB")
		require.NoError(t, err)
		assert.Equal(t, expected, coll.WeightString(nil, input, 5))
	})

	t.Run("comparison", func(t *testing.T) {
		for _, tc := range []struct {
			left, right string
			cmp         int
		}{
			{"เก", "กา", 1},
			{"เก", "แก", -1},
			{"ก่า", "กา", 1},
			{"ก่า", "ก้า", -1},
			{"ไก่", "แก", 1},
			{"ก่า", "แก", -1},
		} {
			got := coll.Collate(tis620(tc.left), tis620(tc.right), false)
			assert.Equal(t, tc.cmp, min(max(got, -1), 1), "%q <=> %q", tc.left, tc.right)
		}
	})

	t.Run("like", func(t *testing.T) {
		for _, tc := range []struct {
			input, pattern string
			match          bool
		}{
			// A pattern is not rewritten: the tone mark is a
			// character at its place.
			{"ก่า", "ก_า", true},
			{"ก่า", "กา", false},
			{"กา", "กา", true},
			{"เก", "ก_า", false},
		} {
			pattern := coll.Wildcard(tis620(tc.pattern), 0, 0, 0)
			assert.Equal(t, tc.match, pattern.Match(tis620(tc.input)), "%q LIKE %q", tc.input, tc.pattern)
		}
	})
}

// TestTIS620ThaiCIZeroByte checks tis620_thai_ci for strings with a zero
// byte against values measured on MySQL 8.4.13 and 8.0.46. MySQL compares
// the bytes after a zero byte as well: STRCMP(x'00E7', x'E700') is -1. Its
// weight string ends at the zero byte, so it does not have that order.
func TestTIS620ThaiCIZeroByte(t *testing.T) {
	coll := testcollation(t, "tis620_thai_ci")
	decode := func(s string) []byte {
		decoded, err := hex.DecodeString(s)
		require.NoError(t, err)
		return decoded
	}

	comparisons := []struct {
		left, right string
		cmp         int
	}{
		// The mark E7 has another byte at the second place of a string
		// than at the first.
		{"00E7", "E700", -1},
		{"0061E7", "00E761", -1},
		{"6100E7", "61E700", -1},
		{"6100A1E7", "6100E7A1", -1},
		// The letters after a zero byte compare as the ones before it:
		// without case, and a leading vowel after its consonant.
		{"0061", "0041", 0},
		{"610061", "610041", 0},
		{"00E0A1", "00A1E0", 0},
		{"6100E0A1", "6100A1E0", 0},
		// The zero byte is before every other byte.
		{"00", "01", -1},
		{"6100", "6120", -1},
		{"6101", "6100", 1},
		// The first of two marks is lost, also when a zero byte is
		// between them.
		{"E700E8", "E900E8", 0},
		// The mark of the left string moves behind its zero byte.
		{"E7610062", "610063", -1},
	}
	for _, tc := range comparisons {
		left, right := decode(tc.left), decode(tc.right)
		got := coll.Collate(left, right, false)
		assert.Equal(t, tc.cmp, min(max(got, -1), 1), "STRCMP(x'%s', x'%s')", tc.left, tc.right)
		reverse := coll.Collate(right, left, false)
		assert.Equal(t, -tc.cmp, min(max(reverse, -1), 1), "STRCMP(x'%s', x'%s')", tc.right, tc.left)

		// What MySQL has no function for: strings that are equal have
		// one hash, and the short weight of the smaller string is not
		// the greater one.
		var leftHash, rightHash vthash.Hasher
		coll.Hash(&leftHash, left, 0)
		coll.Hash(&rightHash, right, 0)
		tiny := coll.(TinyWeightCollation)
		switch tc.cmp {
		case 0:
			assert.Equal(t, leftHash.Sum128(), rightHash.Sum128(), "hash of x'%s' and x'%s'", tc.left, tc.right)
		case -1:
			assert.LessOrEqual(t, tiny.TinyWeightString(left), tiny.TinyWeightString(right), "short weight of x'%s' and x'%s'", tc.left, tc.right)
		case 1:
			assert.GreaterOrEqual(t, tiny.TinyWeightString(left), tiny.TinyWeightString(right), "short weight of x'%s' and x'%s'", tc.left, tc.right)
		}
	}

	// WEIGHT_STRING() has spaces from the zero byte on.
	for input, weights := range map[string]string{
		"00E7": "2020",
		"E700": "FA20",
	} {
		assert.Equal(t, decode(weights), coll.WeightString(nil, decode(input), 0), "WEIGHT_STRING(x'%s')", input)
	}
}

// thaiSortableMySQL is thai2sortable() of MySQL's strings/ctype-tis620.cc,
// which moves each mark to the end of the string by shifting the rest of the
// string. thaiSortable must write the same bytes in one pass.
func thaiSortableMySQL(buf []byte, tolower *[256]byte) {
	n := len(buf)
	bias := byte(256 - 8)

	p := 0
	for left := n; left > 0; p, left = p+1, left-1 {
		c := buf[p]
		if c < 128 {
			bias -= 8
			buf[p] = tolower[c]
			continue
		}

		if isThaiConsonant(c) {
			bias -= 8
		}
		if isThaiLeadingVowel(c) && left != 1 && isThaiConsonant(buf[p+1]) {
			buf[p], buf[p+1] = buf[p+1], c
			p++
			left--
			continue
		}
		if rank := thaiSecondLevel(c); rank != 0 {
			copy(buf[p:], buf[p+1:p+left])
			buf[n-1] = bias + rank
			p--
		}
	}
}

// TestTIS620ThaiCISortableMatchesMySQL checks thaiSortable and
// TinyWeightString against thaiSortableMySQL on random strings of letters,
// leading vowels, marks, ASCII and zero bytes.
func TestTIS620ThaiCISortableMatchesMySQL(t *testing.T) {
	coll := testcollation(t, "tis620_thai_ci").(*Collation_tis620_thai_ci)
	alphabet := []byte{'a', 'B', 0, ' ', 0x80, 0xA1, 0xA2, 0xCE, 0xD1, 0xD2, 0xE0, 0xE1, 0xE4, 0xE7, 0xE8, 0xEB, 0xEC, 0xFF}
	rng := rand.New(rand.NewPCG(1, 2))
	for range 200000 {
		src := make([]byte, rng.IntN(40))
		for i := range src {
			src[i] = alphabet[rng.IntN(len(alphabet))]
		}

		want := bytes.Clone(src)
		thaiSortableMySQL(want, coll.tolower)
		got := bytes.Clone(src)
		thaiSortable(got, coll.tolower)
		require.Equal(t, want, got, "thaiSortable(%x)", src)

		var w32 [4]byte
		copy(w32[:], want)
		wantTiny := uint32(w32[0])<<24 | uint32(w32[1])<<16 | uint32(w32[2])<<8 | uint32(w32[3])
		require.Equal(t, wantTiny, coll.TinyWeightString(src), "TinyWeightString(%x)", src)
	}
}

// TestTIS620ThaiCIDoesNotAllocate checks that Collate, Hash and
// TinyWeightString of short strings, which a sort calls for every row, put
// the sortable forms on the stack.
func TestTIS620ThaiCIDoesNotAllocate(t *testing.T) {
	coll := testcollation(t, "tis620_thai_ci")
	left, right := []byte("\xA1\xE8\xD2 abc"), []byte("\xE0\xA1\xE9 abd")
	hasher := vthash.New()
	allocs := testing.AllocsPerRun(100, func() {
		coll.Collate(left, right, false)
		coll.Hash(&hasher, left, 0)
		coll.(TinyWeightCollation).TinyWeightString(left)
	})
	assert.Zero(t, allocs)
}
