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
	"vitess.io/vitess/go/vt/vthash"
)

// Collation_chinese_ci is gbk_chinese_ci. It sorts a single byte by the
// sort table of the collation, which makes ASCII case-insensitive, and a
// two-byte character by a value from a table of the character set.
//
// The comparison and the weight string can use different values, as they do
// in MySQL.
type Collation_chinese_ci struct {
	id      collations.ID
	name    string
	sort    *[256]byte
	charset charset.Charset

	// caseinfo is the MySQL case conversion of the character set, for
	// ToUpper and ToLower.
	caseinfo caseConverter

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

// ToUpper appends the upper case of src to dst, see multibyteCase.
func (c *Collation_chinese_ci) ToUpper(dst, src []byte) []byte {
	return c.caseinfo.changeCase(dst, src, true)
}

// ToLower appends the lower case of src to dst, see multibyteCase.
func (c *Collation_chinese_ci) ToLower(dst, src []byte) []byte {
	return c.caseinfo.changeCase(dst, src, false)
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
