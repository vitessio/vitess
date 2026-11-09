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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/vt/vthash"
)

// padSpaceCollations are collations of each kind that pad with spaces in
// MySQL.
var padSpaceCollations = []string{
	"latin1_swedish_ci", "latin1_bin", "ascii_general_ci",
	"utf8mb3_general_ci", "utf8mb3_bin", "utf8mb4_general_ci", "utf8mb4_bin",
	"utf8mb4_unicode_ci", "utf8mb4_unicode_520_ci",
	"sjis_japanese_ci", "sjis_bin",
	"ucs2_general_ci", "utf16_bin", "utf32_general_ci",
}

// encode returns s, which is UTF-8, in the character set of coll.
func encode(t *testing.T, coll Collation, s string) []byte {
	out, err := charset.Convert(nil, coll.Charset(), []byte(s), charset.Charset_utf8mb4{})
	require.NoError(t, err)
	return out
}

func sign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	default:
		return 0
	}
}

// TestPadSpaceCollate tests that the collations that pad with spaces compare
// two strings as if the shorter one ended in spaces, and the others do not, with the results of
// STRCMP in MySQL 8.4.13. A tab weighs less than a space. Every PAD SPACE
// collation was also checked against MySQL with 158,400 pairs of strings that
// differ in their ends.
func TestPadSpaceCollate(t *testing.T) {
	testcases := []struct {
		left, right string
		want        int
	}{
		{"a", "a ", 0},
		{"a ", "a  ", 0},
		{"", " ", 0},
		{"a\t", "a", -1},
		{"a", "a\t", 1},
		{"a b", "a", 1},
		{"a", "a b", -1},
		{"ab", "a ", 1},
	}
	for _, name := range padSpaceCollations {
		coll := testcollation(t, name)
		for _, tc := range testcases {
			got := sign(coll.Collate(encode(t, coll, tc.left), encode(t, coll, tc.right), false))
			assert.Equal(t, tc.want, got, "%s: STRCMP(%q, %q)", name, tc.left, tc.right)
		}
	}
	// Binary and the utf8mb4 UCA 9.0.0 collations do not pad with spaces.
	for _, name := range []string{"binary", "utf8mb4_0900_ai_ci", "utf8mb4_0900_bin", "utf8mb4_0900_as_cs"} {
		coll := testcollation(t, name)
		assert.Negative(t, coll.Collate([]byte("a"), []byte("a "), false), name)
	}
}

// TestPadSpaceIgnorable tests that a NUL weighs less than a space in the
// collations that weigh it, and that the UCA collations, which ignore it,
// find 'a\0' and 'a' equal, as MySQL does.
func TestPadSpaceIgnorable(t *testing.T) {
	for _, name := range []string{"latin1_swedish_ci", "utf8mb4_general_ci", "utf8mb4_bin"} {
		coll := testcollation(t, name)
		assert.Negative(t, coll.Collate([]byte("a\x00"), []byte("a"), false), name)
	}
	for _, name := range []string{"utf8mb4_unicode_ci", "utf8mb4_unicode_520_ci"} {
		coll := testcollation(t, name)
		assert.Zero(t, coll.Collate([]byte("a\x00"), []byte("a"), false), name)
	}
}

// TestPadSpaceHash tests that strings that a collation that pads with spaces
// finds equal hash equally, and that a weight string keeps their trailing
// spaces, as WEIGHT_STRING does in MySQL.
func TestPadSpaceHash(t *testing.T) {
	hash := func(coll Collation, s []byte) vthash.Hash {
		h := vthash.New()
		coll.Hash(&h, s, 0)
		return h.Sum128()
	}
	for _, name := range padSpaceCollations {
		coll := testcollation(t, name)
		a := hash(coll, encode(t, coll, "a"))
		assert.Equal(t, a, hash(coll, encode(t, coll, "a ")), name)
		assert.Equal(t, a, hash(coll, encode(t, coll, "a   ")), name)
		assert.NotEqual(t, a, hash(coll, encode(t, coll, "a\t")), name)
		assert.NotEqual(t, a, hash(coll, encode(t, coll, "a a")), name)
		assert.Equal(t, hash(coll, encode(t, coll, "a b")), hash(coll, encode(t, coll, "a b  ")), name)
	}
	latin1 := testcollation(t, "latin1_swedish_ci")
	assert.Equal(t, []byte("A "), latin1.WeightString(nil, []byte("a "), 0))
}

// TestPadSpaceTinyWeight tests that the tiny weights of a collation that pads
// with spaces never order two strings otherwise than Collate does.
func TestPadSpaceTinyWeight(t *testing.T) {
	coll := testcollation(t, "latin1_swedish_ci").(TinyWeightCollation)
	assert.Equal(t, coll.TinyWeightString([]byte("a")), coll.TinyWeightString([]byte("a ")))
	assert.Less(t, coll.TinyWeightString([]byte("a\t")), coll.TinyWeightString([]byte("a")))
	assert.Less(t, coll.TinyWeightString([]byte("a")), coll.TinyWeightString([]byte("ab")))
}

// TestPadSpacePrefixAndLike tests that a prefix comparison, which LOCATE uses,
// and LIKE do not pad with spaces, as in MySQL: 'a' LIKE 'a ' is false.
func TestPadSpacePrefixAndLike(t *testing.T) {
	for _, name := range padSpaceCollations {
		coll := testcollation(t, name)
		a, aSpace := encode(t, coll, "a"), encode(t, coll, "a ")
		assert.NotZero(t, coll.Collate(a, aSpace, true), "%s: 'a ' is not a prefix of 'a'", name)
		assert.Zero(t, coll.Collate(aSpace, a, true), "%s: 'a' is a prefix of 'a '", name)

		assert.False(t, coll.Wildcard(aSpace, 0, 0, 0).Match(a), "%s: 'a' LIKE 'a '", name)
		assert.False(t, coll.Wildcard(a, 0, 0, 0).Match(aSpace), "%s: 'a ' LIKE 'a'", name)
		assert.True(t, coll.Wildcard(a, 0, 0, 0).Match(a), "%s: 'a' LIKE 'a'", name)
	}
}
