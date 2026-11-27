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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/vt/vthash"
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
// WEIGHT_STRING('a中') is 41 D3 21 in gbk_chinese_ci. The weight strings of
// big5_chinese_ci are the exception, see TestBig5ChineseCIFollowsComparison.
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
			// The weight of a two-byte character is its bytes, as
			// in a comparison. MySQL writes the first character with
			// the same number of strokes instead, A440 for "一" and
			// "乙", A4A1 for "中" and A140 for "ａ" and "Ａ", see
			// Collation_chinese_ci.
			weights: map[string]string{
				"一":      "A440",
				"乙":      "A441",
				"a中":     "41A4A4",
				"aBc ａＡ": "41424320A2E9A2CF",
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

// TestBig5ChineseCIFollowsComparison checks that the weight string and the
// hash of big5_chinese_ci follow its comparison, which orders two-byte
// characters by their bytes. MySQL's WEIGHT_STRING() orders them by strokes
// (https://bugs.mysql.com/bug.php?id=65899): "一" (A4 40) and "乙" (A4 41)
// both have the weight A4 40 there, and "乂" (C9 40) has A4 42 and sorts
// before "中" (A4 A4). With such weights, DISTINCT would take "一" and "乙"
// for one value, and a sort by weight would not be the order of the
// comparison.
func TestBig5ChineseCIFollowsComparison(t *testing.T) {
	coll := testcollation(t, "big5_chinese_ci")
	encode := func(s string) []byte {
		converted, err := charset.ConvertFromUTF8(nil, coll.Charset(), []byte(s))
		require.NoError(t, err)
		return converted
	}
	hash := func(b []byte) vthash.Hash {
		hasher := vthash.New()
		coll.Hash(&hasher, b, 0)
		return hasher.Sum128()
	}
	sign := func(n int) int {
		switch {
		case n < 0:
			return -1
		case n > 0:
			return 1
		}
		return 0
	}

	testcases := []struct {
		left, right string
		cmp         int
	}{
		{"一", "乙", -1},
		{"中", "乂", -1},
		{"a一", "A一", 0},
		{"上午好", "下午好", 1},
	}
	for _, tc := range testcases {
		left, right := encode(tc.left), encode(tc.right)
		assert.Equal(t, tc.cmp, sign(coll.Collate(left, right, false)), "Collate(%q, %q)", tc.left, tc.right)
		assert.Equal(t, tc.cmp, sign(bytes.Compare(coll.WeightString(nil, left, 0), coll.WeightString(nil, right, 0))), "WeightString(%q) vs WeightString(%q)", tc.left, tc.right)
		assert.Equal(t, tc.cmp == 0, hash(left) == hash(right), "Hash(%q) == Hash(%q)", tc.left, tc.right)
	}
}
