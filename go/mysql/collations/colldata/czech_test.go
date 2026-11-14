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
