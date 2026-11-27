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
// tis620_thai_ci compares byte by byte.
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
// first is lost, and the bytes between the letters and the last byte are
// copies of the last byte of src.
//
// MySQL moves each mark to the end by shifting the rest of the string, which
// takes time quadratic in the number of marks. thaiSortable writes the same
// bytes in one pass.
func thaiSortable(buf []byte, tolower *[256]byte) {
	n := len(buf)
	// A mark gets bias plus its rank as its byte. bias goes down by 8 with
	// each ASCII byte and each consonant before the mark, as in MySQL, so a
	// mark after more of them sorts before one after fewer.
	bias := byte(256 - 8)

	// w is where the next letter goes. The letters stay in their order, so
	// w is never after r.
	w, marks := 0, 0
	var last byte
	for r := 0; r < n; r++ {
		c := buf[r]
		if c < 128 {
			bias -= 8
			buf[w] = tolower[c]
			w++
			continue
		}

		if isThaiConsonant(c) {
			bias -= 8
		}
		if isThaiLeadingVowel(c) && r+1 < n && isThaiConsonant(buf[r+1]) {
			buf[w], buf[w+1] = buf[r+1], c
			w += 2
			r++
			continue
		}
		if rank := thaiSecondLevel(c); rank != 0 {
			marks++
			last = bias + rank
			continue
		}
		buf[w] = c
		w++
	}
	if marks > 0 {
		// The last byte of src is still at n-1, because each mark leaves
		// w one byte further behind r.
		tail := buf[n-1]
		for i := w; i < n-1; i++ {
			buf[i] = tail
		}
		buf[n-1] = last
	}
}

// thaiSortableBuffer is the size of the buffers on the stack that Collate,
// Hash and TinyWeightString put the sortable form of a short string into.
const thaiSortableBuffer = 64

// sortable appends the sortable form of src to dst[:0], see thaiSortable.
func (c *Collation_tis620_thai_ci) sortable(dst, src []byte) []byte {
	dst = append(dst[:0], src...)
	thaiSortable(dst, c.tolower)
	return dst
}

// Collate compares the sortable forms of left and right byte by byte. A zero
// byte is a character as every other, as in MySQL: STRCMP(x'00E7', x'E700')
// is -1, because the mark E7 gets another byte at the second place of a
// string (F2) than at the first (FA).
//
// The weight string of MySQL ends at the first zero byte, see WeightString,
// so it does not have the order of Collate for such strings: those two
// strings have the weight strings 20 20 and FA 20.
func (c *Collation_tis620_thai_ci) Collate(left, right []byte, rightIsPrefix bool) int {
	if rightIsPrefix && len(left) > len(right) {
		left = left[:len(right)]
	}
	var lbuf, rbuf [thaiSortableBuffer]byte
	return bytes.Compare(c.sortable(lbuf[:0], left), c.sortable(rbuf[:0], right))
}

// appendWeights appends the weight string of src without padding to dst: the
// sortable form of src up to its first zero byte. It returns dst and the
// length of the weight string.
func (c *Collation_tis620_thai_ci) appendWeights(dst, src []byte) ([]byte, int) {
	if i := bytes.IndexByte(src, 0); i >= 0 {
		src = src[:i]
	}
	start := len(dst)
	dst = append(dst, src...)
	thaiSortable(dst[start:], c.tolower)
	return dst, len(src)
}

// TinyWeightString returns the first four bytes of the sortable form of src,
// which is what Collate compares. It is not the start of the weight string
// when src has a zero byte.
//
// The first four bytes are the first four letters, in the order of
// thaiSortable, when src has four letters. Only a shorter src needs the
// whole sortable form, for its marks and the copies of its last byte.
func (c *Collation_tis620_thai_ci) TinyWeightString(src []byte) uint32 {
	var w32 [4]byte
	w := 0
	for r := 0; r < len(src) && w < len(w32); r++ {
		ch := src[r]
		switch {
		case ch < 128:
			w32[w] = c.tolower[ch]
			w++
		case isThaiLeadingVowel(ch) && r+1 < len(src) && isThaiConsonant(src[r+1]):
			w32[w] = src[r+1]
			w++
			if w < len(w32) {
				w32[w] = ch
				w++
			}
			r++
		case thaiSecondLevel(ch) != 0:
		default:
			w32[w] = ch
			w++
		}
	}
	if w < len(w32) {
		var buf [thaiSortableBuffer]byte
		w32 = [4]byte{}
		copy(w32[:], c.sortable(buf[:0], src))
	}
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

	dst, n := c.appendWeights(dst, src[:min(len(src), numCodepoints)])
	return weightStringPadingSimple(' ', dst, numCodepoints-n, padToMax)
}

// Hash hashes the sortable form of src, which is what Collate compares, so
// that two strings that Collate finds equal have one hash. The weight string
// would not do: x'E700E8' and x'E900E8' are equal, because the first of two
// marks is lost, and their weight strings FA 20 20 and FC 20 20 end before
// the second mark.
func (c *Collation_tis620_thai_ci) Hash(hasher *vthash.Hasher, src []byte, numCodepoints int) {
	if numCodepoints == 0 {
		numCodepoints = len(src)
	}
	var buf [thaiSortableBuffer]byte
	sortable := c.sortable(buf[:0], src[:min(len(src), numCodepoints)])

	hasher.Write64(uint64(c.id))
	hasher.Write(sortable)
	for i := len(sortable); i < numCodepoints; i++ {
		hasher.Write8(' ')
	}
}

func (c *Collation_tis620_thai_ci) WeightStringLen(numBytes int) int {
	return numBytes
}

// Wildcard returns a matcher that compares one byte with one byte by the sort
// table, as LIKE does in MySQL for this collation. It does not use Collate.
func (c *Collation_tis620_thai_ci) Wildcard(pat []byte, matchOne rune, matchMany rune, escape rune) WildcardPattern {
	return c.sortWildcard(pat, matchOne, matchMany, escape)
}
