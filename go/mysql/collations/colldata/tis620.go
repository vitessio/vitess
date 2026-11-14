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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/vt/vthash"
)

// Collation_tis620_thai_ci is tis620_thai_ci. It compares two strings after
// it rewrites each of them to a form that sorts in the Thai dictionary order,
// see thaiSortable.
//
// A LIKE pattern is not rewritten. It compares one byte with one byte by the
// sort table of the collation.
type Collation_tis620_thai_ci struct {
	id   collations.ID
	name string
	simpletables
}

func (c *Collation_tis620_thai_ci) Name() string {
	return c.name
}

func (c *Collation_tis620_thai_ci) ID() collations.ID {
	return c.id
}

func (c *Collation_tis620_thai_ci) Charset() charset.Charset {
	return charset.Charset_tis620{}
}

func (c *Collation_tis620_thai_ci) IsBinary() bool {
	return false
}

// isThaiConsonant reports whether a tis620 byte is one of the 46 Thai
// consonants, A1 "ก" to CE "ฮ".
func isThaiConsonant(b byte) bool {
	return 0xA1 <= b && b <= 0xCE
}

// isThaiLeadingVowel reports whether a tis620 byte is one of the five vowels
// that are written before the consonant they follow in speech, E0 "เ" to
// E4 "ไ".
func isThaiLeadingVowel(b byte) bool {
	return 0xE0 <= b && b <= 0xE4
}

// thaiSecondLevel returns the rank of a tone mark or another mark that the
// Thai order looks at only after the letters, and 0 for every other byte. The
// marks are E7 to EC, with the rank that MySQL gives them.
func thaiSecondLevel(b byte) byte {
	switch b {
	case 0xEC:
		return 1
	case 0xE7, 0xE8, 0xE9, 0xEA, 0xEB:
		return b - 0xE7 + 2
	}
	return 0
}

// thaiSortable rewrites a tis620 string in place to the form that
// tis620_thai_ci compares byte by byte. It is thai2sortable in MySQL.
//
//   - A leading vowel changes places with the consonant after it: "เก"
//     (E0 A1) becomes A1 E0, so that the word sorts by its consonant.
//   - A tone mark or one of the other marks of thaiSecondLevel moves to the
//     end of the string, as a byte that has its rank and its position among
//     the letters: "ก่า" (A1 E8 D2) becomes A1 D2 F3.
//   - An ASCII letter becomes lower case.
//
// The function does what MySQL does also where that is not the Thai order:
// every mark is written to the last byte, so with two marks in a string the
// first is lost and the byte before the last is a copy of a letter.
func thaiSortable(buf []byte, tolower *[256]byte) {
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

// sortable returns a copy of src in the form of thaiSortable.
func (c *Collation_tis620_thai_ci) sortable(src []byte) []byte {
	buf := bytes.Clone(src)
	thaiSortable(buf, c.tolower)
	return buf
}

// Collate compares the sortable forms of left and right up to the first zero
// byte, as MySQL does with strcmp.
func (c *Collation_tis620_thai_ci) Collate(left, right []byte, rightIsPrefix bool) int {
	if rightIsPrefix && len(left) > len(right) {
		left = left[:len(right)]
	}
	l, r := c.sortable(left), c.sortable(right)

	for i := 0; ; i++ {
		var lb, rb byte
		if i < len(l) {
			lb = l[i]
		}
		if i < len(r) {
			rb = r[i]
		}
		if lb != rb {
			return int(lb) - int(rb)
		}
		if lb == 0 {
			return 0
		}
	}
}

// weights returns the weight string of src without padding: the sortable form
// of src up to its first zero byte.
func (c *Collation_tis620_thai_ci) weights(src []byte) []byte {
	if i := bytes.IndexByte(src, 0); i >= 0 {
		src = src[:i]
	}
	return c.sortable(src)
}

func (c *Collation_tis620_thai_ci) TinyWeightString(src []byte) uint32 {
	var w32 [4]byte
	copy(w32[:], c.weights(src))
	return uint32(w32[0])<<24 | uint32(w32[1])<<16 | uint32(w32[2])<<8 | uint32(w32[3])
}

// WeightString appends the sortable form of src: WEIGHT_STRING('เก') is
// A1 E0. The weight string has one byte for each character of src. Where src
// has a zero byte, the weight string has spaces from that byte on.
//
// For a CHAR(n) that is shorter than src, MySQL shortens src first: a mark
// in the part that is cut off does not move into the weight string.
func (c *Collation_tis620_thai_ci) WeightString(dst, src []byte, numCodepoints int) []byte {
	padToMax := false
	switch numCodepoints {
	case 0:
		numCodepoints = len(src)
	case PadToMax:
		padToMax = true
		numCodepoints = len(src)
	}

	weights := c.weights(src[:min(len(src), numCodepoints)])
	dst = append(dst, weights...)
	return weightStringPadingSimple(' ', dst, numCodepoints-len(weights), padToMax)
}

func (c *Collation_tis620_thai_ci) Hash(hasher *vthash.Hasher, src []byte, numCodepoints int) {
	if numCodepoints == 0 {
		numCodepoints = len(src)
	}
	weights := c.weights(src[:min(len(src), numCodepoints)])

	hasher.Write64(uint64(c.id))
	hasher.Write(weights)
	for i := len(weights); i < numCodepoints; i++ {
		hasher.Write8(' ')
	}
}

func (c *Collation_tis620_thai_ci) WeightStringLen(numBytes int) int {
	return numBytes
}

// Wildcard returns a matcher that compares one byte with one byte by the sort
// table, as LIKE does in MySQL for this collation. It does not use Collate.
func (c *Collation_tis620_thai_ci) Wildcard(pat []byte, matchOne rune, matchMany rune, escape rune) WildcardPattern {
	simple := &Collation_8bit_simple_ci{simpletables: c.simpletables}
	return newEightbitWildcardMatcher(c.sort, simple.Collate, pat, matchOne, matchMany, escape)
}

func (c *Collation_tis620_thai_ci) ToLower(dst, src []byte) []byte {
	lowerTable := c.tolower

	for _, c := range src {
		dst = append(dst, lowerTable[c])
	}
	return dst
}

func (c *Collation_tis620_thai_ci) ToUpper(dst, src []byte) []byte {
	upperTable := c.toupper

	for _, c := range src {
		dst = append(dst, upperTable[c])
	}
	return dst
}
