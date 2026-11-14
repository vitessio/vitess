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

// TestGB18030ChineseCI checks gb18030_chinese_ci against values measured on
// MySQL 8.4.13 and 8.0.46 with a gb18030_chinese_ci column:
// WEIGHT_STRING('阿') is FF A0 00 01, the first position in the pinyin order.
func TestGB18030ChineseCI(t *testing.T) {
	coll := testcollation(t, "gb18030_chinese_ci")
	gb18030 := func(s string) []byte {
		converted, err := charset.ConvertFromUTF8(nil, coll.Charset(), []byte(s))
		require.NoError(t, err)
		return converted
	}

	t.Run("weight string", func(t *testing.T) {
		for input, weights := range map[string]string{
			"阿":   "FFA00001",
			"波":   "FFA00729",
			"ａ":   "A3C1",
			"aａ阿": "41A3C1FFA00001",
			"aａд": "41A3C1A7A5",
			// "ǆ" and "Ǆ" are four bytes each, and have the weight of "Ǆ".
			"ǆ": "FF000128",
			"Ǆ": "FF000128",
			// "à" is two bytes and its upper case "À" is four.
			"à": "FF00003A",
			"À": "FF00003A",
		} {
			expected, err := hex.DecodeString(weights)
			require.NoError(t, err)
			assert.Equal(t, expected, coll.WeightString(nil, gb18030(input), 0), "WEIGHT_STRING(%q)", input)
		}
	})

	t.Run("comparison", func(t *testing.T) {
		for _, tc := range []struct {
			left, right string
			cmp         int
		}{
			{"ǆ", "Ǆ", 0},
			{"à", "À", 0},
			{"ａ", "Ａ", 0},
			{"ａ", "a", 1},
			{"阿", "波", -1},
			{"aａд", "波", -1},
		} {
			got := coll.Collate(gb18030(tc.left), gb18030(tc.right), false)
			assert.Equal(t, tc.cmp, min(max(got, -1), 1), "%q <=> %q", tc.left, tc.right)
		}
	})

	t.Run("like", func(t *testing.T) {
		for _, tc := range []struct {
			input, pattern string
			match          bool
		}{
			{"ǆ", "Ǆ", true},
			{"à", "À", true},
			{"ａ", "Ａ", true},
			{"ａ", "a", false},
			{"阿", "波", false},
			{"aａ阿", "A_阿", true},
			{"aａ阿", "a%", true},
		} {
			pattern := coll.Wildcard(gb18030(tc.pattern), 0, 0, 0)
			assert.Equal(t, tc.match, pattern.Match(gb18030(tc.input)), "%q LIKE %q", tc.input, tc.pattern)
		}
	})
}
