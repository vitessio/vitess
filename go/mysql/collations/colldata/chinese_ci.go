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
	"math"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/charset/simplifiedchinese"
	"vitess.io/vitess/go/vt/vthash"
)

// Collation_chinese_ci is gbk_chinese_ci and big5_chinese_ci. It sorts a
// single byte by the sort table of the collation, which makes ASCII
// case-insensitive, and a two-byte character by its order value: its position
// in the pinyin order of gbk_chinese_ci, and its bytes in big5_chinese_ci.
//
// Collate, WeightString and Hash all use the order value. In
// big5_chinese_ci this is not what MySQL's WEIGHT_STRING() and an ORDER BY
// without an index do: they sort a two-byte character by the first character
// of big5 with the same number of strokes, so '一' (A4 40) and '乙' (A4 41)
// both have the weight string A4 40 although '一' = '乙' is false. A
// comparison, GROUP BY, DISTINCT, a unique key and an ORDER BY that reads an
// index all order big5 characters by their bytes. MySQL verified the sort by
// strokes as a bug (https://bugs.mysql.com/bug.php?id=65899), and a weight
// string that does not follow the comparison would give rows that are not
// equal the same hash, so Vitess sorts big5_chinese_ci by the bytes of its
// characters everywhere.
type Collation_chinese_ci struct {
	id      collations.ID
	name    string
	sort    *[256]byte
	charset charset.Charset

	// charLen returns 2 when src starts with a two-byte character, and 0
	// when it starts with a single byte. It checks the range of each byte
	// and not that the character set has the character.
	charLen func(src []byte) int

	// order returns the order value of a two-byte character, which
	// Collate compares and WeightString and Hash write.
	order func(code uint16) uint16
}

func (c *Collation_chinese_ci) ID() collations.ID {
	return c.id
}

func (c *Collation_chinese_ci) Name() string {
	return c.name
}

func (c *Collation_chinese_ci) Charset() charset.Charset {
	return c.charset
}

func (c *Collation_chinese_ci) IsBinary() bool {
	return false
}

// Collate compares left and right up to the length of the shorter one. Where
// both have a two-byte character, it compares the order values of the two
// characters. Everywhere else it compares one byte with one byte by the sort
// table, also when one of the two bytes starts a two-byte character.
func (c *Collation_chinese_ci) Collate(left, right []byte, isPrefix bool) int {
	length := min(len(left), len(right))
	for i := 0; i < length; i++ {
		if i+1 < length && c.charLen(left[i:i+2]) == 2 && c.charLen(right[i:i+2]) == 2 {
			l := uint16(left[i])<<8 | uint16(left[i+1])
			r := uint16(right[i])<<8 | uint16(right[i+1])
			if l != r {
				return int(c.order(l)) - int(c.order(r))
			}
			i++
			continue
		}
		if l, r := c.sort[left[i]], c.sort[right[i]]; l != r {
			return int(l) - int(r)
		}
	}

	if isPrefix {
		return length - len(right)
	}
	return len(left) - len(right)
}

// next returns the weight of the first character of src and its length in
// bytes.
func (c *Collation_chinese_ci) next(src []byte) (weight uint16, width int) {
	if c.charLen(src) == 2 {
		return c.order(uint16(src[0])<<8 | uint16(src[1])), 2
	}
	return uint16(c.sort[src[0]]), 1
}

// WeightString appends one byte for a single byte and two bytes for a
// two-byte character: in gbk_chinese_ci, WEIGHT_STRING('a中') is 41 D3 21.
func (c *Collation_chinese_ci) WeightString(dst, src []byte, numCodepoints int) []byte {
	left := numCodepoints
	if numCodepoints == 0 || numCodepoints == PadToMax {
		left = math.MaxInt32
	}
	for len(src) > 0 && left > 0 {
		weight, width := c.next(src)
		if width == 2 {
			dst = append(dst, byte(weight>>8))
		}
		dst = append(dst, byte(weight))
		src = src[width:]
		left--
	}

	if numCodepoints == 0 || numCodepoints == PadToMax {
		left = 0
	}
	return weightStringPadingSimple(' ', dst, left, numCodepoints == PadToMax)
}

func (c *Collation_chinese_ci) Hash(hasher *vthash.Hasher, src []byte, numCodepoints int) {
	hasher.Write64(uint64(c.id))
	left := numCodepoints
	if left == 0 {
		left = math.MaxInt32
	}
	for len(src) > 0 && left > 0 {
		weight, width := c.next(src)
		if width == 2 {
			hasher.Write16(weight)
		} else {
			hasher.Write8(byte(weight))
		}
		src = src[width:]
		left--
	}
	if numCodepoints > 0 {
		for ; left > 0; left-- {
			hasher.Write8(' ')
		}
	}
}

// WeightStringLen returns two bytes for each character.
func (c *Collation_chinese_ci) WeightStringLen(numCodepoints int) int {
	return numCodepoints * 2
}

// Wildcard returns a matcher for LIKE that matches a single byte by the sort
// table and a two-byte character only with itself, as MySQL does for
// gbk_chinese_ci and big5_chinese_ci: 'a中' LIKE 'A%' is true, and a
// full-width "ａ" does not match "Ａ".
func (c *Collation_chinese_ci) Wildcard(pat []byte, matchOne rune, matchMany rune, escape rune) WildcardPattern {
	return newMultibyteWildcardMatcher(c.charset, c.sort, c.Collate, pat, matchOne, matchMany, escape)
}

// gbkSortOrder returns the value of a two-byte gbk character in
// gbk_chinese_ci: 0x8100 plus its position in gbkOrder. "中" (D6 D0) is
// 0xD321. The order has the Chinese characters of GB 2312 by pinyin.
func gbkSortOrder(code uint16) uint16 {
	idx, _ := simplifiedchinese.TwoByteIndex(byte(code>>8), byte(code))
	return 0x8100 + gbkOrder[idx]
}

// charLenGBK is for gbk: a lead byte 81-FE, and a second byte 40-7E or 80-FE.
func charLenGBK(src []byte) int {
	if len(src) < 2 {
		return 0
	}
	if _, ok := simplifiedchinese.TwoByteIndex(src[0], src[1]); !ok {
		return 0
	}
	return 2
}

// charLenBig5 is for big5: a lead byte A1-F9, and a second byte 40-7E or
// A1-FE.
func charLenBig5(src []byte) int {
	if len(src) < 2 {
		return 0
	}
	lead, trail := src[0], src[1]
	if lead < 0xA1 || lead > 0xF9 {
		return 0
	}
	if (0x40 <= trail && trail <= 0x7E) || (0xA1 <= trail && trail <= 0xFE) {
		return 2
	}
	return 0
}

// big5Code is the order value of a two-byte big5 character in
// big5_chinese_ci: the bytes of the character, see Collation_chinese_ci.
func big5Code(code uint16) uint16 {
	return code
}
