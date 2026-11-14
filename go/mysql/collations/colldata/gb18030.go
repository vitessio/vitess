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

// gb18030CharLen returns the length in bytes of the multi-byte character at
// the start of src: 2 for a byte 81-FE and a byte 40-7E or 80-FE, 4 for
// bytes 81-FE, 30-39, 81-FE, 30-39, and 0 when src starts with a single
// byte. It checks the range of each byte and not that gb18030 has the
// character.
func gb18030CharLen(src []byte) int {
	if len(src) < 2 || src[0] < 0x81 || src[0] > 0xFE {
		return 0
	}
	isDigit := func(b byte) bool { return 0x30 <= b && b <= 0x39 }
	switch {
	case (0x40 <= src[1] && src[1] <= 0x7E) || (0x80 <= src[1] && src[1] <= 0xFE):
		return 2
	case len(src) > 3 && isDigit(src[1]) && 0x81 <= src[2] && src[2] <= 0xFE && isDigit(src[3]):
		return 4
	}
	return 0
}

// gb18030Diff returns the position of a four-byte character counted from
// the first one, 81 30 81 30. The position of 81 30 81 31 is 1.
func gb18030Diff(char []byte) uint32 {
	return uint32(char[0]-0x81)*12600 + uint32(char[1]-0x30)*1260 + uint32(char[2]-0x81)*10 + uint32(char[3]-0x30)
}

// gb18030FromDiff returns the four-byte character at a position, as a number.
func gb18030FromDiff(diff uint32) uint32 {
	b3 := diff%10 + 0x30
	diff /= 10
	b2 := diff%126 + 0x81
	diff /= 126
	b1 := diff%10 + 0x30
	b0 := diff/10 + 0x81
	return b0<<24 | b1<<16 | b2<<8 | b3
}

// gb18030Code returns the bytes of a character as a number: 0xA3E1 for the
// full-width "ａ" (A3 E1).
func gb18030Code(char []byte) uint32 {
	var code uint32
	for _, b := range char {
		code = code<<8 | uint32(b)
	}
	return code
}

// gb18030AppendCode appends a number as its bytes without the leading zero
// bytes: 0xA3E1 is A3 E1.
func gb18030AppendCode(dst []byte, code uint32) []byte {
	switch {
	case code > 0xFFFFFF:
		dst = append(dst, byte(code>>24), byte(code>>16), byte(code>>8), byte(code))
	case code > 0xFFFF:
		dst = append(dst, byte(code>>16), byte(code>>8), byte(code))
	case code > 0xFF:
		dst = append(dst, byte(code>>8), byte(code))
	default:
		dst = append(dst, byte(code))
	}
	return dst
}

// gb18030CaseCode returns the upper-case or lower-case form of a multi-byte
// character as a number, and 0 when the character has none. The upper case
// of the full-width "ａ" (A3 E1) is 0xA3C1, "Ａ".
//
// The case table has 256 pages of 256 case codes. A case code is
// one of three things. A two-byte character with a first byte A0-DF is its
// own case code. A four-byte character at a position below 0x9F80 has the
// case code position + 0x80. A four-byte character at a position
// 0x2E600-0x2E6FF has the case code 0xE600-0xE6FF. No other character has a
// case code, so no other character changes.
func gb18030CaseCode(char []byte, upper bool) uint32 {
	var caseCode uint32
	switch len(char) {
	case 2:
		if char[0] < 0xA0 || char[0] > 0xDF {
			return 0
		}
		caseCode = gb18030Code(char)
	case 4:
		switch diff := gb18030Diff(char); {
		case diff < 0xA000-0x80:
			caseCode = diff + 0x80
		case 0x2E600 <= diff && diff <= 0x2E6FF:
			caseCode = diff & 0xFFFF
		default:
			return 0
		}
	default:
		return 0
	}

	page := gb18030CasePages[caseCode>>8]
	if page == nil {
		return 0
	}
	converted := uint32((*page)[caseCode&0xFF].ToLower)
	if upper {
		converted = uint32((*page)[caseCode&0xFF].ToUpper)
	}

	switch {
	case converted < 0x80, 0xA000 <= converted && converted <= 0xDFFF:
		// 0 is an empty entry of a page: the character has no case.
		return converted
	case converted < 0xA000:
		return gb18030FromDiff(converted - 0x80)
	default:
		return gb18030FromDiff(converted + 0x20000)
	}
}

const (
	gb18030PinyinWeightBase = 0xFFA00000
	gb18030CommonWeightBase = 0xFF000000
)

// gb18030Weight returns the weight of a multi-byte character in
// gb18030_chinese_ci.
//
// A Chinese character sorts by its pinyin: its weight is 0xFFA00000 and its
// position in the pinyin order, so "阿" (B0 A2, "a") is 0xFFA00001 and less
// than "波" (B2 A8, "bo"). Any other character sorts by its upper case: a
// two-byte upper case is the weight, so the full-width "ａ" (A3 E1) and "Ａ"
// (A3 C1) both have 0xA3C1, and a four-byte upper case has 0xFF000000 and
// its position. All Chinese characters sort after all other characters.
func gb18030Weight(char []byte) uint32 {
	code := gb18030Code(char)
	if code == 0xFE39FE39 {
		// The last character of gb18030 has the highest weight.
		return 0xFFFFFFFF
	}

	var pinyin uint16
	switch {
	case 0x8140 <= code && code <= 0xFE9F:
		idx := uint32(char[0]-0x81)*0xBE + uint32(char[1]) - 0x40
		if char[1] > 0x7F {
			idx--
		}
		pinyin = gb18030PinyinWeights2[idx]
	case 0x8138FD38 <= code && code <= 0x82359232:
		pinyin = gb18030PinyinWeights4a[gb18030Diff(char)-11328]
	case 0x95328236 <= code && code <= 0x98399836:
		pinyin = gb18030PinyinWeights4b[gb18030Diff(char)-254536]
	}
	if pinyin != 0 {
		return gb18030PinyinWeightBase + uint32(pinyin)
	}

	upper := gb18030CaseCode(char, true)
	if upper == 0 {
		upper = code
	}
	if upper <= 0xFFFF {
		return upper
	}
	return gb18030CommonWeightBase + gb18030DiffOfCode(upper)
}

// gb18030DiffOfCode is gb18030Diff for a four-byte character that is given
// as a number.
func gb18030DiffOfCode(code uint32) uint32 {
	return gb18030Diff([]byte{byte(code >> 24), byte(code >> 16), byte(code >> 8), byte(code)})
}

// Collation_gb18030_chinese_ci is gb18030_chinese_ci. It sorts Chinese
// characters by pinyin and every other character by its upper case, see
// gb18030Weight.
type Collation_gb18030_chinese_ci struct {
	id   collations.ID
	name string
	sort *[256]byte
}

func (c *Collation_gb18030_chinese_ci) ID() collations.ID {
	return c.id
}

func (c *Collation_gb18030_chinese_ci) Name() string {
	return c.name
}

func (c *Collation_gb18030_chinese_ci) Charset() charset.Charset {
	return charset.Charset_gb18030{}
}

func (c *Collation_gb18030_chinese_ci) IsBinary() bool {
	return false
}

// next returns the weight of the first character of src and its length in
// bytes. A single byte has its sort value as the weight.
func (c *Collation_gb18030_chinese_ci) next(src []byte) (weight uint32, width int, multibyte bool) {
	if width := gb18030CharLen(src); width > 0 {
		return gb18030Weight(src[:width]), width, true
	}
	return uint32(c.sort[src[0]]), 1, false
}

// Collate compares left and right by the weights of their characters. A
// single byte is less than a multi-byte character.
//
// Two strings with the same weights are equal also when their lengths in
// bytes differ: "à" is two bytes in gb18030 (A8 A4) and its upper case "À"
// is four (81 30 86 38), and 'à' = 'À' is true.
func (c *Collation_gb18030_chinese_ci) Collate(left, right []byte, isPrefix bool) int {
	for len(left) > 0 && len(right) > 0 {
		lWeight, lWidth, lMultibyte := c.next(left)
		rWeight, rWidth, rMultibyte := c.next(right)

		if lMultibyte != rMultibyte {
			if rMultibyte {
				return -1
			}
			return 1
		}
		if lWeight != rWeight {
			if lWeight < rWeight {
				return -1
			}
			return 1
		}
		left, right = left[lWidth:], right[rWidth:]
	}

	if isPrefix && len(left) > 0 {
		return 0
	}
	return len(left) - len(right)
}

// WeightString appends the weight of each character as its bytes without the
// leading zero bytes: WEIGHT_STRING('aａ阿') is 41 A3 C1 FF A0 00 01.
func (c *Collation_gb18030_chinese_ci) WeightString(dst, src []byte, numCodepoints int) []byte {
	left := numCodepoints
	if numCodepoints == 0 || numCodepoints == PadToMax {
		left = math.MaxInt32
	}
	for len(src) > 0 && left > 0 {
		weight, width, _ := c.next(src)
		dst = gb18030AppendCode(dst, weight)
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

func (c *Collation_gb18030_chinese_ci) Hash(hasher *vthash.Hasher, src []byte, numCodepoints int) {
	hasher.Write64(uint64(c.id))
	left := numCodepoints
	if left == 0 {
		left = math.MaxInt32
	}
	for len(src) > 0 && left > 0 {
		weight, width, multibyte := c.next(src)
		if multibyte {
			hasher.Write32(weight)
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

// WeightStringLen returns four bytes for each character, the length of the
// weight of a Chinese character.
func (c *Collation_gb18030_chinese_ci) WeightStringLen(numCodepoints int) int {
	return numCodepoints * 4
}

// Wildcard returns a matcher that compares one character with one character
// by weight, as LIKE does in MySQL for this collation.
func (c *Collation_gb18030_chinese_ci) Wildcard(pat []byte, matchOne rune, matchMany rune, escape rune) WildcardPattern {
	cs := charset.Charset_gb18030{}
	weight := func(r rune) (uint32, bool) {
		var buf [4]byte
		n := cs.EncodeRune(buf[:], r)
		if n <= 0 {
			return 0, false
		}
		w, _, _ := c.next(buf[:n])
		return w, true
	}
	equals := func(a, b rune) bool {
		if a == b {
			return true
		}
		aWeight, aOK := weight(a)
		bWeight, bOK := weight(b)
		return aOK && bOK && aWeight == bWeight
	}
	return newUnicodeWildcardMatcher(cs, equals, nil, pat, matchOne, matchMany, escape)
}
