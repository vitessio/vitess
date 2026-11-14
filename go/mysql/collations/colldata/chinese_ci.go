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
	"slices"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/vt/vthash"
)

// Collation_chinese_ci is gbk_chinese_ci and big5_chinese_ci. It sorts a
// single byte by the sort table of the collation, which makes ASCII
// case-insensitive, and a two-byte character by a value from a table of the
// character set.
//
// The comparison and the weight string can use different values, as they do
// in MySQL for big5_chinese_ci: a comparison orders two big5 characters by
// their bytes, and the weight string has the first character with the same
// number of strokes. '一' = '乙' is false, and their weight strings are both
// A4 40.
type Collation_chinese_ci struct {
	id      collations.ID
	name    string
	sort    *[256]byte
	charset charset.Charset

	// charLen returns 2 when src starts with a two-byte character, and 0
	// when it starts with a single byte. It checks the range of each byte
	// and not that the character set has the character.
	charLen func(src []byte) int

	// order returns the value that Collate compares for a two-byte
	// character, and weight the value that WeightString writes.
	order, weight func(code uint16) uint16
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
		return c.weight(uint16(src[0])<<8 | uint16(src[1])), 2
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

	switch numCodepoints {
	case 0:
	case PadToMax:
		for len(dst) < cap(dst) {
			dst = append(dst, ' ')
		}
	default:
		for ; left > 0; left-- {
			dst = append(dst, ' ')
		}
	}
	return dst
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

func (c *Collation_chinese_ci) Wildcard(pat []byte, matchOne rune, matchMany rune, escape rune) WildcardPattern {
	return newMultibyteWildcardMatcher(c.charset, c.sort, c.Collate, pat, matchOne, matchMany, escape)
}

// gbkSortOrder returns the value of a two-byte gbk character in
// gbk_chinese_ci: 0x8100 and its position in gbkOrder. "中" (D6 D0) is
// 0xD321. The order has the Chinese characters of GB 2312 by pinyin.
func gbkSortOrder(code uint16) uint16 {
	idx := int(code & 0xFF)
	if idx > 0x7F {
		idx -= 0x41
	} else {
		idx -= 0x40
	}
	idx += (int(code>>8) - 0x81) * 0xBE
	return 0x8100 + gbkOrder[idx]
}

// charLenGBK is for gbk: a lead byte 81-FE, and a second byte 40-7E or 80-FE.
func charLenGBK(src []byte) int {
	if len(src) < 2 {
		return 0
	}
	lead, trail := src[0], src[1]
	if lead < 0x81 || lead > 0xFE {
		return 0
	}
	if (0x40 <= trail && trail <= 0x7E) || (0x80 <= trail && trail <= 0xFE) {
		return 2
	}
	return 0
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
// big5_chinese_ci: the bytes of the character.
func big5Code(code uint16) uint16 {
	return code
}

// big5StrokeWeight returns the weight of a two-byte big5 character in
// big5_chinese_ci: the first character of big5 with the same number of
// strokes, and 0xA140 for a character that is not a Chinese character. "一"
// (A4 40) and "乙" (A4 41) have one stroke and the weight 0xA440.
func big5StrokeWeight(code uint16) uint16 {
	i, found := slices.BinarySearchFunc(big5StrokeRanges[:], code, func(r [3]uint16, code uint16) int {
		switch {
		case r[1] < code:
			return -1
		case r[0] > code:
			return 1
		}
		return 0
	})
	if !found {
		return 0xA140
	}
	return big5StrokeRanges[i][2]
}

// big5StrokeRanges has {first character, last character, weight} for each
// range of big5 characters with the same number of strokes, in the order of
// the characters. With these values, the weight string of every big5
// character is the one of MySQL 8.4.13 and 8.0.46.
var big5StrokeRanges = [...][3]uint16{
	{0xA259, 0xA259, 0xAB45},
	{0xA25A, 0xA25A, 0xADBC},
	{0xA25B, 0xA25B, 0xB0AE},
	{0xA25C, 0xA25C, 0xB0AE},
	{0xA25D, 0xA25D, 0xB6C3},
	{0xA25E, 0xA25E, 0xBEA7},
	{0xA25F, 0xA25F, 0xB6C3},
	{0xA260, 0xA260, 0xA8C3},
	{0xA261, 0xA261, 0xBBF5},
	{0xA440, 0xA440, 0xA440},
	{0xA441, 0xA441, 0xA440},
	{0xA442, 0xA453, 0xA442},
	{0xA454, 0xA47E, 0xA454},
	{0xA4A1, 0xA4FD, 0xA4A1},
	{0xA4FE, 0xA5DF, 0xA4FE},
	{0xA5E0, 0xA6E9, 0xA5E0},
	{0xA6EA, 0xA8C2, 0xA6EA},
	{0xA8C3, 0xAB44, 0xA8C3},
	{0xAB45, 0xADBB, 0xAB45},
	{0xADBC, 0xB0AD, 0xADBC},
	{0xB0AE, 0xB3C2, 0xB0AE},
	{0xB3C3, 0xB6C2, 0xB3C3},
	{0xB6C3, 0xB9AB, 0xB6C3},
	{0xB9AC, 0xBBF4, 0xB9AC},
	{0xBBF5, 0xBEA6, 0xBBF5},
	{0xBEA7, 0xC074, 0xBEA7},
	{0xC075, 0xC24E, 0xC075},
	{0xC24F, 0xC35E, 0xC24F},
	{0xC35F, 0xC454, 0xC35F},
	{0xC455, 0xC4D6, 0xC455},
	{0xC4D7, 0xC56A, 0xC4D7},
	{0xC56B, 0xC5C7, 0xC56B},
	{0xC5C8, 0xC5F0, 0xC5C8},
	{0xC5F1, 0xC654, 0xC5F1},
	{0xC655, 0xC664, 0xC655},
	{0xC665, 0xC66B, 0xC665},
	{0xC66C, 0xC675, 0xC66C},
	{0xC676, 0xC678, 0xC676},
	{0xC679, 0xC67C, 0xC679},
	{0xC67D, 0xC67D, 0xC67D},
	{0xC67E, 0xC67E, 0xC67E},
	{0xC6A1, 0xC6A1, 0xB6C3},
	{0xC940, 0xC944, 0xA442},
	{0xC945, 0xC94C, 0xA454},
	{0xC94D, 0xC962, 0xA4A1},
	{0xC963, 0xC9AA, 0xA4FE},
	{0xC9AB, 0xCA59, 0xA5E0},
	{0xCA5A, 0xCBB0, 0xA6EA},
	{0xCBB1, 0xCDDC, 0xA8C3},
	{0xCDDD, 0xD0C7, 0xAB45},
	{0xD0C8, 0xD44A, 0xADBC},
	{0xD44B, 0xD850, 0xB0AE},
	{0xD851, 0xDCB0, 0xB3C3},
	{0xDCB1, 0xE0EF, 0xB6C3},
	{0xE0F0, 0xE4E5, 0xB9AC},
	{0xE4E6, 0xE8F3, 0xBBF5},
	{0xE8F4, 0xECB8, 0xBEA7},
	{0xECB9, 0xEFB6, 0xC075},
	{0xEFB7, 0xF1EA, 0xC24F},
	{0xF1EB, 0xF3FC, 0xC35F},
	{0xF3FD, 0xF5BF, 0xC455},
	{0xF5C0, 0xF6D5, 0xC4D7},
	{0xF6D6, 0xF7CF, 0xC56B},
	{0xF7D0, 0xF8A4, 0xC5C8},
	{0xF8A5, 0xF8ED, 0xC5F1},
	{0xF8EE, 0xF96A, 0xC655},
	{0xF96B, 0xF9A1, 0xC665},
	{0xF9A2, 0xF9B9, 0xC66C},
	{0xF9BA, 0xF9C5, 0xC676},
	{0xF9C6, 0xF9C6, 0xF9C6},
	{0xF9C7, 0xF9CB, 0xC679},
	{0xF9CC, 0xF9CF, 0xC67D},
	{0xF9D0, 0xF9D0, 0xF9D0},
	{0xF9D1, 0xF9D1, 0xC67E},
	{0xF9D2, 0xF9D2, 0xF9C6},
	{0xF9D3, 0xF9D3, 0xF9D3},
	{0xF9D4, 0xF9D4, 0xF9D4},
	{0xF9D5, 0xF9D5, 0xF9D5},
	{0xF9D6, 0xF9D6, 0xB6C3},
	{0xF9D7, 0xF9D7, 0xBEA7},
	{0xF9D8, 0xF9D8, 0xB6C3},
	{0xF9D9, 0xF9D9, 0xBEA7},
	{0xF9DA, 0xF9DA, 0xAB45},
	{0xF9DB, 0xF9DB, 0xB3C3},
	{0xF9DC, 0xF9DC, 0xB9AC},
}
