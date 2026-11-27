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

// Collation_latin2_czech_cs is latin2_czech_cs, the Czech dictionary order
// for the latin2 character set. It reads a string in four passes and compares
// the values of each pass in turn:
//
//  1. the letters without their accents and without case, so that "c" is
//     before "č" only when the words are otherwise the same,
//  2. the accents,
//  3. the case,
//  4. every character, which also tells apart what the first three passes
//     ignore.
//
// The first two passes go word by word: both passes of the first word, then
// both passes of the second word. "ch" is one letter, after "h".
//
// A LIKE pattern compares byte by byte, as in MySQL.
type Collation_latin2_czech_cs struct {
	id   collations.ID
	name string
	simpletables
	charset charset.Charset
}

func (c *Collation_latin2_czech_cs) Name() string {
	return c.name
}

func (c *Collation_latin2_czech_cs) ID() collations.ID {
	return c.id
}

func (c *Collation_latin2_czech_cs) Charset() charset.Charset {
	return c.charset
}

func (c *Collation_latin2_czech_cs) IsBinary() bool {
	return false
}

var latin2CzechPasses = [4]*[256]byte{&latin2CzechPass0, &latin2CzechPass1, &latin2CzechPass2, &latin2CzechPass3}

// latin2CzechDoubles has the values of "c", "C" and the letter "ch" in each
// of the four passes. The scanner takes the first entry that the string has
// at its position.
var latin2CzechDoubles = [...]struct {
	word   string
	values [4]byte
}{
	{"ch", [4]byte{12, 25, 47, 47}},
	{"Ch", [4]byte{12, 25, 48, 48}},
	{"CH", [4]byte{12, 25, 49, 49}},
	{"c", [4]byte{5, 10, 17, 17}},
	{"C", [4]byte{5, 10, 18, 18}},
}

// latin2CzechScanner returns the values of a string for latin2_czech_cs one
// at a time.
type latin2CzechScanner struct {
	src []byte
	// pos is the position of the next character, and store the position
	// where the other of the first two passes goes on.
	pos, store int
	pass       int
	// runEnd is the end of the last run of spaces that the pass found, so
	// that the fourth pass, which returns a value for each space of a run,
	// does not look for the end of the run again at each space. MySQL does,
	// which takes time quadratic in the length of the run.
	runEnd int
}

// next returns the next value: 1 at the end of a pass, and 0 at the end of
// the fourth pass, which is the end of the values.
func (s *latin2CzechScanner) next() byte {
	for {
		if s.pos >= len(s.src) {
			if s.pass == 3 {
				return 0
			}
			if s.pass == 0 {
				s.pos = s.store
			} else {
				s.pos = 0
			}
			s.pass++
			s.runEnd = 0
			return 1
		}

		table := latin2CzechPasses[s.pass]
		value := table[s.src[s.pos]]
		switch value {
		case 0:
			// The pass ignores the character.
			s.pos++
			continue

		case 2:
			// A space ends a word. The scanner skips the spaces
			// after it, and in the first two passes it changes to
			// the other pass for the word it has just read.
			s.pos++
			runner := s.runEnd
			if runner < s.pos {
				runner = s.pos
				for runner < len(s.src) && table[s.src[runner]] == 2 {
					runner++
				}
				s.runEnd = runner
			}
			if runner >= len(s.src) || s.pass <= 2 {
				s.pos = runner
			}
			if s.pos >= len(s.src) {
				continue
			}
			if s.pass > 1 {
				return value
			}
			s.pass = 1 - s.pass
			s.pos, s.store = s.store, s.pos
			s.runEnd = 0
			return value

		case 255:
			for _, double := range latin2CzechDoubles {
				if bytes.HasPrefix(s.src[s.pos:], []byte(double.word)) {
					value = double.values[s.pass]
					s.pos += len(double.word) - 1
					break
				}
			}
		}

		s.pos++
		return value
	}
}

// Collate compares the values of left and right, see
// Collation_latin2_czech_cs.
func (c *Collation_latin2_czech_cs) Collate(left, right []byte, rightIsPrefix bool) int {
	if rightIsPrefix && len(left) > len(right) {
		left = left[:len(right)]
	}
	l, r := latin2CzechScanner{src: left}, latin2CzechScanner{src: right}
	for {
		lv, rv := l.next(), r.next()
		if lv != rv {
			return int(lv) - int(rv)
		}
		if lv == 0 {
			return 0
		}
	}
}

// weights appends the values of src, with the 1 at the end of each pass and
// the 0 at the end: for "ach", 03 0C 01 03 19 01 03 2F 01 03 2F 00. Each
// pass has a value for "a" and one for the letter "ch".
func (c *Collation_latin2_czech_cs) weights(dst, src []byte) []byte {
	s := latin2CzechScanner{src: src}
	for {
		value := s.next()
		dst = append(dst, value)
		if value == 0 {
			return dst
		}
	}
}

// WeightString appends the values of src. The weight string has no padding
// for a CHAR(n): MySQL shortens src to n characters and does not pad it.
func (c *Collation_latin2_czech_cs) WeightString(dst, src []byte, numCodepoints int) []byte {
	if numCodepoints > 0 && numCodepoints != PadToMax {
		src = src[:min(len(src), numCodepoints)]
	}
	dst = c.weights(dst, src)
	if numCodepoints == PadToMax {
		for len(dst) < cap(dst) {
			dst = append(dst, ' ')
		}
	}
	return dst
}

func (c *Collation_latin2_czech_cs) Hash(hasher *vthash.Hasher, src []byte, numCodepoints int) {
	if numCodepoints > 0 {
		src = src[:min(len(src), numCodepoints)]
	}
	hasher.Write64(uint64(c.id))
	hasher.Write(c.weights(nil, src))
}

// WeightStringLen returns four values for each character, one in each pass,
// and the four values that end the passes.
func (c *Collation_latin2_czech_cs) WeightStringLen(numBytes int) int {
	return numBytes*4 + 4
}

func (c *Collation_latin2_czech_cs) Wildcard(pat []byte, matchOne rune, matchMany rune, escape rune) WildcardPattern {
	return newEightbitWildcardMatcher(&sortOrderIdentity, collationBinary, pat, matchOne, matchMany, escape)
}

func (c *Collation_latin2_czech_cs) ToLower(dst, src []byte) []byte {
	lowerTable := c.tolower

	for _, c := range src {
		dst = append(dst, lowerTable[c])
	}
	return dst
}

func (c *Collation_latin2_czech_cs) ToUpper(dst, src []byte) []byte {
	upperTable := c.toupper

	for _, c := range src {
		dst = append(dst, upperTable[c])
	}
	return dst
}

// Collation_cp1250_czech_cs is cp1250_czech_cs, the Czech dictionary order
// for the cp1250 character set. It reads a string in two passes and compares
// the values of the first pass, and then of the second: first the letters
// without accents and case, then the accents and the case. "ch" is one
// letter, after "h".
//
// A LIKE pattern compares one byte with one byte by the sort table of the
// collation.
type Collation_cp1250_czech_cs struct {
	id   collations.ID
	name string
	simpletables
	charset charset.Charset
}

func (c *Collation_cp1250_czech_cs) Name() string {
	return c.name
}

func (c *Collation_cp1250_czech_cs) ID() collations.ID {
	return c.id
}

func (c *Collation_cp1250_czech_cs) Charset() charset.Charset {
	return c.charset
}

func (c *Collation_cp1250_czech_cs) IsBinary() bool {
	return false
}

// cp1250CzechDoubles has the values of "c", "C" and the letter "ch" in the
// two passes. The scanner takes the first entry that the string has at its
// position.
var cp1250CzechDoubles = [...]struct {
	word   string
	values [2]byte
}{
	{"ch", [2]byte{0xAD, 0x03}},
	{"c", [2]byte{0xA6, 0x02}},
	{"Ch", [2]byte{0xAD, 0x02}},
	{"CH", [2]byte{0xAD, 0x01}},
	{"C", [2]byte{0xA6, 0x01}},
}

// cp1250CzechScanner returns the values of a string for cp1250_czech_cs one
// at a time.
type cp1250CzechScanner struct {
	src  []byte
	pos  int
	pass int
}

// next returns the next value, and 0 at the end of the second pass. Nothing
// marks the end of the first pass.
func (s *cp1250CzechScanner) next() byte {
	if s.pos >= len(s.src) {
		if s.pass != 0 || len(s.src) == 0 {
			return 0
		}
		s.pos = 0
		s.pass++
	}

	table := &cp1250CzechPass0
	if s.pass != 0 {
		table = &cp1250CzechPass1
	}
	value := table[s.src[s.pos]]
	if value == 255 {
		for _, double := range cp1250CzechDoubles {
			if bytes.HasPrefix(s.src[s.pos:], []byte(double.word)) {
				value = double.values[s.pass]
				s.pos += len(double.word) - 1
				break
			}
		}
	}
	s.pos++
	return value
}

// Collate compares the values of left and right, see
// Collation_cp1250_czech_cs. MySQL does not look at the spaces at the end of
// either string: 'a' = 'a ' is true, and x'6119' is after 'a ' although the
// byte 19 has a lower value than the space, as it is after 'a'. A space
// inside a string is a character as every other.
//
// A weight string has the values of the spaces at the end, also in MySQL, so
// it does not have the order of Collate for such strings.
func (c *Collation_cp1250_czech_cs) Collate(left, right []byte, rightIsPrefix bool) int {
	if rightIsPrefix {
		if len(left) > len(right) {
			left = left[:len(right)]
		}
	} else {
		left, right = bytes.TrimRight(left, " "), bytes.TrimRight(right, " ")
	}
	l, r := cp1250CzechScanner{src: left}, cp1250CzechScanner{src: right}
	for {
		lv, rv := l.next(), r.next()
		if lv != rv {
			return int(lv) - int(rv)
		}
		if lv == 0 {
			return 0
		}
	}
}

// weights appends the values of src: for "ach", A4 AD 02 03. Each pass has
// a value for "a" and one for the letter "ch".
func (c *Collation_cp1250_czech_cs) weights(dst, src []byte) []byte {
	s := cp1250CzechScanner{src: src}
	for {
		value := s.next()
		if value == 0 {
			return dst
		}
		dst = append(dst, value)
	}
}

// WeightString appends the values of src. The weight string has no padding
// for a CHAR(n): MySQL shortens src to n characters and does not pad it.
// With PadToMax, the weight string is filled with zero bytes and not with
// the values of spaces.
func (c *Collation_cp1250_czech_cs) WeightString(dst, src []byte, numCodepoints int) []byte {
	if numCodepoints > 0 && numCodepoints != PadToMax {
		src = src[:min(len(src), numCodepoints)]
	}
	dst = c.weights(dst, src)
	if numCodepoints == PadToMax {
		for len(dst) < cap(dst) {
			dst = append(dst, 0)
		}
	}
	return dst
}

// Hash hashes the values of src without the spaces at its end, which Collate
// does not look at either.
func (c *Collation_cp1250_czech_cs) Hash(hasher *vthash.Hasher, src []byte, numCodepoints int) {
	if numCodepoints > 0 {
		src = src[:min(len(src), numCodepoints)]
	}
	hasher.Write64(uint64(c.id))
	hasher.Write(c.weights(nil, bytes.TrimRight(src, " ")))
}

// WeightStringLen returns two values for each character, one in each pass.
func (c *Collation_cp1250_czech_cs) WeightStringLen(numBytes int) int {
	return numBytes * 2
}

// Wildcard returns a matcher that compares one byte with one byte by the sort
// table, as LIKE does in MySQL for this collation. It does not use Collate.
func (c *Collation_cp1250_czech_cs) Wildcard(pat []byte, matchOne rune, matchMany rune, escape rune) WildcardPattern {
	simple := &Collation_8bit_simple_ci{simpletables: c.simpletables}
	return newEightbitWildcardMatcher(c.sort, simple.Collate, pat, matchOne, matchMany, escape)
}

func (c *Collation_cp1250_czech_cs) ToLower(dst, src []byte) []byte {
	lowerTable := c.tolower

	for _, c := range src {
		dst = append(dst, lowerTable[c])
	}
	return dst
}

func (c *Collation_cp1250_czech_cs) ToUpper(dst, src []byte) []byte {
	upperTable := c.toupper

	for _, c := range src {
		dst = append(dst, upperTable[c])
	}
	return dst
}
