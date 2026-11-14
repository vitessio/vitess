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
	"cmp"
	"slices"
)

// multibyteCaseChar is the upper-case and lower-case form of a multi-byte
// character. The three values are the bytes of a character as a number: in
// sjis, {0x8281, 0x8260, 0x8281} is the full-width "ａ" (82 81), with the
// upper case "Ａ" (82 60).
type multibyteCaseChar struct {
	code, upper, lower uint32
}

// multibyteCase is the case conversion of a multi-byte character set that is
// not Unicode. MySQL converts these by the bytes of a character and not by
// its Unicode code point, with a table for each character set.
type multibyteCase struct {
	// charLen returns the length in bytes of the multi-byte character at
	// the start of src, and 0 when src starts with a single byte. It is
	// the ismbchar function of the character set in MySQL. It checks the
	// range of each byte and not that the character set has the character.
	charLen func(src []byte) int

	// chars has the characters that the conversion changes, in the order
	// of code.
	chars []multibyteCaseChar
}

// changeCase appends src to dst with every character converted to upper or
// lower case, the way UPPER() and LOWER() do it in MySQL. In sjis, the upper
// case of "aｂд" (61 82 82 84 74) is "AＢД" (41 82 61 84 44).
//
// A single byte, which includes a byte that does not start a valid
// multi-byte character, is converted as ASCII: MySQL has no other mapping for
// a single byte in these character sets.
//
// The result can be shorter or longer than src. In euckr, the upper case of
// the dotless "ı" (A9 A5) is "I" (49). In ujis, the lower case of the
// Angstrom sign "Å" (A2 F2) is "å", which is three bytes long (8F AB A9).
func (mb *multibyteCase) changeCase(dst, src []byte, upper bool) []byte {
	for len(src) > 0 {
		width := mb.charLen(src)
		if width == 0 {
			b := src[0]
			switch {
			case upper && 'a' <= b && b <= 'z':
				b -= 'a' - 'A'
			case !upper && 'A' <= b && b <= 'Z':
				b += 'a' - 'A'
			}
			dst = append(dst, b)
			src = src[1:]
			continue
		}

		var code uint32
		for _, b := range src[:width] {
			code = code<<8 | uint32(b)
		}
		i, found := slices.BinarySearchFunc(mb.chars, code, func(char multibyteCaseChar, code uint32) int {
			return cmp.Compare(char.code, code)
		})
		if !found {
			dst = append(dst, src[:width]...)
			src = src[width:]
			continue
		}

		converted := mb.chars[i].lower
		if upper {
			converted = mb.chars[i].upper
		}
		if converted > 0xFFFF {
			dst = append(dst, byte(converted>>16))
		}
		if converted > 0xFF {
			dst = append(dst, byte(converted>>8))
		}
		dst = append(dst, byte(converted))
		src = src[width:]
	}
	return dst
}

// charLenSJIS is for sjis and cp932: a lead byte 81-9F or E0-FC, and a
// second byte 40-7E or 80-FC.
func charLenSJIS(src []byte) int {
	if len(src) < 2 {
		return 0
	}
	lead, trail := src[0], src[1]
	if !(0x81 <= lead && lead <= 0x9F) && !(0xE0 <= lead && lead <= 0xFC) {
		return 0
	}
	if (0x40 <= trail && trail <= 0x7E) || (0x80 <= trail && trail <= 0xFC) {
		return 2
	}
	return 0
}

// charLenEUCKR is for euckr: a lead byte 81-FE, and a second byte 41-5A,
// 61-7A or 81-FE.
func charLenEUCKR(src []byte) int {
	if len(src) < 2 {
		return 0
	}
	lead, trail := src[0], src[1]
	if lead < 0x81 || lead > 0xFE {
		return 0
	}
	if (0x41 <= trail && trail <= 0x5A) || (0x61 <= trail && trail <= 0x7A) || (0x81 <= trail && trail <= 0xFE) {
		return 2
	}
	return 0
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

// charLenGB2312 is for gb2312: a lead byte A1-F7, and a second byte A1-FE.
func charLenGB2312(src []byte) int {
	if len(src) < 2 {
		return 0
	}
	lead, trail := src[0], src[1]
	if 0xA1 <= lead && lead <= 0xF7 && 0xA1 <= trail && trail <= 0xFE {
		return 2
	}
	return 0
}

// charLenUJIS is for ujis and eucjpms: two bytes A1-FE, or 8E and a byte
// A1-DF (half-width katakana), or 8F and two bytes A1-FE (JIS X 0212).
func charLenUJIS(src []byte) int {
	if len(src) < 2 {
		return 0
	}
	isEUC := func(b byte) bool { return 0xA1 <= b && b <= 0xFE }
	switch lead := src[0]; {
	case isEUC(lead) && isEUC(src[1]):
		return 2
	case lead == 0x8E && 0xA1 <= src[1] && src[1] <= 0xDF:
		return 2
	case lead == 0x8F && len(src) > 2 && isEUC(src[1]) && isEUC(src[2]):
		return 3
	}
	return 0
}

var (
	multibyteCase_sjis    = &multibyteCase{charLen: charLenSJIS, chars: multibyteCaseChars_sjis}
	multibyteCase_cp932   = &multibyteCase{charLen: charLenSJIS, chars: multibyteCaseChars_cp932}
	multibyteCase_ujis    = &multibyteCase{charLen: charLenUJIS, chars: multibyteCaseChars_ujis}
	multibyteCase_eucjpms = &multibyteCase{charLen: charLenUJIS, chars: multibyteCaseChars_eucjpms}
	multibyteCase_euckr   = &multibyteCase{charLen: charLenEUCKR, chars: multibyteCaseChars_euckr}
	multibyteCase_gb2312  = &multibyteCase{charLen: charLenGB2312, chars: multibyteCaseChars_gb2312}
	multibyteCase_gbk     = &multibyteCase{charLen: charLenGBK, chars: multibyteCaseChars_gbk}
	multibyteCase_big5    = &multibyteCase{charLen: charLenBig5, chars: multibyteCaseChars_big5}
)
