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
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/vt/vthash"
)

// Collation_latin1_german2_ci is latin1_german2_ci, the German phone book
// order (DIN 5007-2). It sorts an umlaut as its vowel and an "E", and "ß" as
// "SS": 'Müller' = 'MUELLER' and 'Straße' = 'strasse' are true.
//
// A LIKE pattern does not expand a character: 'ä' LIKE 'ae' is false. In
// LIKE, "ä" matches only "ä" and "Ä", the characters with its value in the
// sort table of the collation.
type Collation_latin1_german2_ci struct {
	id   collations.ID
	name string
	simpletables
	charset charset.Charset
}

func (c *Collation_latin1_german2_ci) Name() string {
	return c.name
}

func (c *Collation_latin1_german2_ci) ID() collations.ID {
	return c.id
}

func (c *Collation_latin1_german2_ci) Charset() charset.Charset {
	return c.charset
}

func (c *Collation_latin1_german2_ci) IsBinary() bool {
	return false
}

// weights returns the weights of a latin1 character: two for "Ä", "Ö", "Ü",
// "ä", "ö", "ü" and "ß", and for every other character its sort value and 0.
func (c *Collation_latin1_german2_ci) weights(ch byte) (byte, byte) {
	switch ch {
	case 0xC4, 0xE4:
		return 'A', 'E'
	case 0xD6, 0xF6:
		return 'O', 'E'
	case 0xDC, 0xFC:
		return 'U', 'E'
	case 0xDF:
		return 'S', 'S'
	}
	return c.sort[ch], 0
}

// Collate compares the weights of left and right. A character with two
// weights compares equal to the two characters it stands for: "ä" (E4) and
// "ae" are equal, and "ä" is greater than "ad" and less than "af".
func (c *Collation_latin1_german2_ci) Collate(left, right []byte, rightIsPrefix bool) int {
	var leftExtra, rightExtra byte

	for (len(left) > 0 || leftExtra != 0) && (len(right) > 0 || rightExtra != 0) {
		var l, r byte
		if leftExtra != 0 {
			l, leftExtra = leftExtra, 0
		} else {
			l, leftExtra = c.weights(left[0])
			left = left[1:]
		}
		if rightExtra != 0 {
			r, rightExtra = rightExtra, 0
		} else {
			r, rightExtra = c.weights(right[0])
			right = right[1:]
		}
		if l != r {
			return int(l) - int(r)
		}
	}

	switch {
	case len(left) > 0 || leftExtra != 0:
		if rightIsPrefix {
			return 0
		}
		return 1
	case len(right) > 0 || rightExtra != 0:
		return -1
	}
	return 0
}

func (c *Collation_latin1_german2_ci) TinyWeightString(src []byte) uint32 {
	var w32 uint32
	var n int
	for _, ch := range src {
		first, second := c.weights(ch)
		w32 = w32<<8 | uint32(first)
		n++
		if second != 0 && n < 4 {
			w32 = w32<<8 | uint32(second)
			n++
		}
		if n == 4 {
			return w32
		}
	}
	return w32 << (8 * (4 - n))
}

// WeightString appends the weights of src: WEIGHT_STRING('Äb') is "AEB"
// (41 45 42). The padding is one space for each character that src is
// shorter than numCodepoints, as for the other 8-bit collations.
func (c *Collation_latin1_german2_ci) WeightString(dst, src []byte, numCodepoints int) []byte {
	padToMax := false
	copyCodepoints := len(src)

	switch numCodepoints {
	case 0:
		numCodepoints = copyCodepoints
	case PadToMax:
		padToMax = true
	default:
		copyCodepoints = min(copyCodepoints, numCodepoints)
	}

	for _, ch := range src[:copyCodepoints] {
		first, second := c.weights(ch)
		dst = append(dst, first)
		if second != 0 {
			dst = append(dst, second)
		}
	}
	return weightStringPadingSimple(' ', dst, numCodepoints-copyCodepoints, padToMax)
}

func (c *Collation_latin1_german2_ci) Hash(hasher *vthash.Hasher, src []byte, numCodepoints int) {
	tocopy := len(src)
	if numCodepoints > 0 {
		tocopy = min(tocopy, numCodepoints)
	}

	hasher.Write64(uint64(c.id))
	for _, ch := range src[:tocopy] {
		first, second := c.weights(ch)
		hasher.Write8(first)
		if second != 0 {
			hasher.Write8(second)
		}
	}

	if numCodepoints > 0 {
		numCodepoints -= tocopy
		for numCodepoints > 0 {
			hasher.Write8(' ')
			numCodepoints--
		}
	}
}

// WeightStringLen returns two bytes for each character, the length of the
// weights of a string of umlauts.
func (c *Collation_latin1_german2_ci) WeightStringLen(numBytes int) int {
	return numBytes * 2
}

// Wildcard returns a matcher that compares one character with one character
// by the sort table, as LIKE does in MySQL for this collation. It does not
// use Collate: 'ä' = 'ae' is true and 'ä' LIKE 'ae' is false.
func (c *Collation_latin1_german2_ci) Wildcard(pat []byte, matchOne rune, matchMany rune, escape rune) WildcardPattern {
	return c.sortWildcard(pat, matchOne, matchMany, escape)
}
