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

type chineseCICase struct {
	collation string
	// weights are WEIGHT_STRING() of the strings, in hex.
	weights map[string]string
	// comparisons are the sign of left <=> right.
	comparisons []struct {
		left, right string
		cmp         int
	}
	// likes are input LIKE pattern.
	likes []struct {
		input, pattern string
		match          bool
	}
}

// TestChineseCI checks gbk_chinese_ci and big5_chinese_ci against values
// measured on MySQL 8.4.13 and 8.0.46 with a column of the collation:
// WEIGHT_STRING('a中') is 41 D3 21 in gbk_chinese_ci.
func TestChineseCI(t *testing.T) {
	testcases := []chineseCICase{
		{
			collation: "gbk_chinese_ci",
			weights: map[string]string{
				"a中":     "41D321",
				"啊":      "8454",
				"ａ":      "82B1",
				"aBc ａＡ": "4142432082B182B0",
			},
			comparisons: []struct {
				left, right string
				cmp         int
			}{
				{"a中", "A中", 0},
				{"a中", "啊", -1},
				// "中" (zhong) is after "啊" (a) in pinyin.
				{"中", "啊", 1},
				// Only ASCII is case-insensitive.
				{"ａ", "Ａ", 1},
			},
			likes: []struct {
				input, pattern string
				match          bool
			}{
				{"a中", "A%", true},
				{"a中", "_中", true},
				{"ａ", "Ａ", false},
				{"aBc ａＡ", "A%", true},
			},
		},
		{
			collation: "big5_chinese_ci",
			weights: map[string]string{
				// "一" and "乙" have one stroke.
				"一":      "A440",
				"乙":      "A440",
				"a中":     "41A4A1",
				"aBc ａＡ": "41424320A140A140",
			},
			comparisons: []struct {
				left, right string
				cmp         int
			}{
				// A comparison is by the bytes of a character
				// and not by its weight.
				{"一", "乙", -1},
				{"a中", "A中", 0},
				{"ａ", "Ａ", 1},
			},
			likes: []struct {
				input, pattern string
				match          bool
			}{
				{"一", "乙", false},
				{"乙", "乙", true},
				{"a中", "A_", true},
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
