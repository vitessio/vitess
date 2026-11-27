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

package simplifiedchinese

import (
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/charset/types"
)

// Charset_gbk is the gbk character set of MySQL: ASCII, and two-byte
// characters with a first byte 81-FE and a second byte 40-7E or 80-FE. "中"
// (U+4E2D) is D6 D0.
//
// It uses the two-byte tables of gb18030. MySQL's gbk has the same
// characters, except for the 82 two-byte characters that GB18030 added
// later, such as the euro sign at A2 E3. The codes in gbkAbsent are not gbk
// characters, and their code points have no gbk form. This was measured on
// MySQL 8.4.13 and 8.0.46 for every two-byte sequence and every code point.
type Charset_gbk struct{}

// gbkAbsent has the ranges of two-byte codes that gb18030 has and MySQL's gbk
// does not, in the order of the codes.
var gbkAbsent = [...][2]uint16{
	{0xA2E3, 0xA2E3},
	{0xA3A0, 0xA3A0},
	{0xA8BF, 0xA8BF},
	{0xA989, 0xA995},
	{0xFE50, 0xFE50},
	{0xFE54, 0xFE58},
	{0xFE5A, 0xFE60},
	{0xFE62, 0xFE65},
	{0xFE68, 0xFE6B},
	{0xFE6E, 0xFE75},
	{0xFE77, 0xFE7D},
	{0xFE80, 0xFE8F},
	{0xFE92, 0xFE9F},
}

func isGBKAbsent(code uint16) bool {
	for _, run := range gbkAbsent {
		if code < run[0] {
			return false
		}
		if code <= run[1] {
			return true
		}
	}
	return false
}

func (Charset_gbk) Name() string {
	return "gbk"
}

func (Charset_gbk) IsSuperset(other types.Charset) bool {
	switch other.(type) {
	case Charset_gbk:
		return true
	default:
		return false
	}
}

func (Charset_gbk) SupportsSupplementaryChars() bool {
	return false
}

func (Charset_gbk) MaxWidth() int {
	return 2
}

func (Charset_gbk) EncodeRune(dst []byte, r rune) int {
	var code uint16
	switch {
	case r < utf8.RuneSelf:
		dst[0] = byte(r)
		return 1
	case encode0Low <= r && r < encode0High:
		code = encode0[r-encode0Low]
	case encode1Low <= r && r < encode1High:
		code = encode1[r-encode1Low]
	case encode2Low <= r && r < encode2High:
		code = encode2[r-encode2Low]
	case encode3Low <= r && r < encode3High:
		code = encode3[r-encode3Low]
	case encode4Low <= r && r < encode4High:
		code = encode4[r-encode4Low]
	}
	if code == 0 || isGBKAbsent(code) {
		return -1
	}
	_ = dst[1]
	dst[0] = byte(code >> 8)
	dst[1] = byte(code)
	return 2
}

func (Charset_gbk) DecodeRune(src []byte) (rune, int, bool) {
	if len(src) < 1 {
		return utf8.RuneError, 0, false
	}

	c0 := src[0]
	if c0 < utf8.RuneSelf {
		return rune(c0), 1, true
	}
	if c0 < 0x81 || c0 > 0xFE || len(src) < 2 {
		return utf8.RuneError, 1, false
	}

	c1 := src[1]
	idx, ok := TwoByteIndex(c0, c1)
	if !ok {
		return utf8.RuneError, 1, false
	}

	// The second byte is valid, so the two bytes are one character also
	// when gbk has no character at that code.
	if idx >= len(decode) || decode[idx] == 0 || isGBKAbsent(uint16(c0)<<8|uint16(c1)) {
		return utf8.RuneError, 2, false
	}
	return rune(decode[idx]), 2, true
}

// TwoByteIndex returns the position of a two-byte character of gbk and
// gb18030 among all of them, in the order of their bytes: 0 for 81 40, 190
// for 82 40. It returns false when the bytes are not a two-byte character,
// which has a first byte 81-FE and a second byte 40-7E or 80-FE. It does not
// check that the character set has a character there.
func TwoByteIndex(lead, trail byte) (int, bool) {
	if lead < 0x81 || lead > 0xFE {
		return 0, false
	}
	idx := int(lead-0x81) * 190
	switch {
	case 0x40 <= trail && trail <= 0x7E:
		return idx + int(trail-0x40), true
	case 0x80 <= trail && trail <= 0xFE:
		return idx + int(trail-0x41), true
	}
	return 0, false
}
