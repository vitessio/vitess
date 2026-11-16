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
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/vt/vthash"
)

// TestCzechCollations checks latin2_czech_cs and cp1250_czech_cs against
// values measured on MySQL 8.4.13 and 8.0.46 with a column of the collation.
// In both, "ch" is a letter after "h": 'chata' > 'hrad' and 'cihla' < 'hrad'.
func TestCzechCollations(t *testing.T) {
	type comparison struct {
		left, right string
		cmp         int
	}
	type like struct {
		input, pattern string
		match          bool
	}
	common := []comparison{
		{"chata", "hrad", 1},
		{"cihla", "hrad", -1},
		{"car", "cz", -1},
		{"chata", "cz", 1},
		{"čaj", "cihla", 1},
		{"čaj", "chata", -1},
		{"a b", "ach", -1},
	}

	testcases := []struct {
		collation   string
		weights     map[string]string
		comparisons []comparison
		likes       []like
	}{
		{
			collation: "latin2_czech_cs",
			weights: map[string]string{
				"ach":   "030C01031901032F01032F00",
				"a b":   "030203020401090103020F0103020F00",
				"chata": "0C031B030119033503012F036803012F03680300",
				"Chata": "0C031B0301190335030130036803013003680300",
				"čaj":   "06030E010B031D011303380113033800",
			},
			// An upper-case letter is after the lower-case letter.
			comparisons: append([]comparison{{"Chata", "chata", 1}}, common...),
			// LIKE compares the bytes.
			likes: []like{
				{"chata", "CHATA", false},
				{"chata", "ch%", true},
				{"čaj", "c_j", false},
				{"čaj", "č_j", true},
			},
		},
		{
			collation: "cp1250_czech_cs",
			weights: map[string]string{
				"ach":   "A4AD0203",
				"a b":   "A482A5020102",
				"chata": "ADA4BCA403020202",
				"Chata": "ADA4BCA402020202",
				"čaj":   "A7A4AF020202",
			},
			// An upper-case letter is before the lower-case letter.
			comparisons: append([]comparison{{"Chata", "chata", -1}}, common...),
			likes: []like{
				{"chata", "CHATA", false},
				{"chata", "ch%", true},
				{"čaj", "c_j", false},
				{"čaj", "č_j", true},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.collation, func(t *testing.T) {
			coll := testcollation(t, tc.collation)
			encode := func(s string) []byte {
				converted, err := charset.ConvertFromUTF8(nil, coll.Charset(), []byte(s))
				require.NoError(t, err)
				return converted
			}

			for input, weights := range tc.weights {
				expected, err := hex.DecodeString(weights)
				require.NoError(t, err)
				assert.Equal(t, expected, coll.WeightString(nil, encode(input), 0), "WEIGHT_STRING(%q)", input)
			}
			for _, cmp := range tc.comparisons {
				got := coll.Collate(encode(cmp.left), encode(cmp.right), false)
				assert.Equal(t, cmp.cmp, min(max(got, -1), 1), "%q <=> %q", cmp.left, cmp.right)
			}
			for _, like := range tc.likes {
				pattern := coll.Wildcard(encode(like.pattern), 0, 0, 0)
				assert.Equal(t, like.match, pattern.Match(encode(like.input)), "%q LIKE %q", like.input, like.pattern)
			}
		})
	}
}

// TestCP1250CzechCSTrailingSpaces checks that cp1250_czech_cs does not look
// at the spaces at the end of a string, against values measured on MySQL
// 8.4.13 and 8.0.46: 'a' = 'a ' is true, and x'6119' is after 'a ' although
// the byte 19 has a lower value than the space.
func TestCP1250CzechCSTrailingSpaces(t *testing.T) {
	coll := testcollation(t, "cp1250_czech_cs")
	decode := func(s string) []byte {
		decoded, err := hex.DecodeString(s)
		require.NoError(t, err)
		return decoded
	}

	for _, tc := range []struct {
		left, right string
		cmp         int
	}{
		{"61", "6120", 0},
		{"20", "", 0},
		{"611920", "6119", 0},
		// x'6119' is after 'a', and after 'a ' as well.
		{"6119", "61", 1},
		{"6119", "6120", 1},
		{"19", "20", 1},
		// A space that is not at the end is a character: the byte 19
		// is before it.
		{"611962", "612062", -1},
		// Only a space is left out: 'a' with a zero byte is after 'a'.
		{"6100", "61", 1},
	} {
		left, right := decode(tc.left), decode(tc.right)
		got := coll.Collate(left, right, false)
		assert.Equal(t, tc.cmp, min(max(got, -1), 1), "STRCMP(x'%s', x'%s')", tc.left, tc.right)
		reverse := coll.Collate(right, left, false)
		assert.Equal(t, -tc.cmp, min(max(reverse, -1), 1), "STRCMP(x'%s', x'%s')", tc.right, tc.left)

		// What MySQL has no function for: strings that are equal have
		// one hash.
		if tc.cmp == 0 {
			var leftHash, rightHash vthash.Hasher
			coll.Hash(&leftHash, left, 0)
			coll.Hash(&rightHash, right, 0)
			assert.Equal(t, leftHash.Sum128(), rightHash.Sum128(), "hash of x'%s' and x'%s'", tc.left, tc.right)
		}
	}

	// WEIGHT_STRING() has the values of the space at the end.
	for input, weights := range map[string]string{
		"61":   "A402",
		"6120": "A4820201",
	} {
		assert.Equal(t, decode(weights), coll.WeightString(nil, decode(input), 0), "WEIGHT_STRING(x'%s')", input)
	}
}
