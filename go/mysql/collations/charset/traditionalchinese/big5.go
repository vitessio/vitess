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

// Package traditionalchinese has the Traditional Chinese character sets of
// MySQL.
package traditionalchinese

import (
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/charset/types"
)

// Charset_big5 is the big5 character set of MySQL: ASCII, and two-byte
// characters with a first byte A1-F9 and a second byte 40-7E or A1-FE. "中"
// (U+4E2D) is A4 A4.
type Charset_big5 struct{}

func (Charset_big5) Name() string {
	return "big5"
}

func (Charset_big5) IsSuperset(other types.Charset) bool {
	switch other.(type) {
	case Charset_big5:
		return true
	default:
		return false
	}
}

func (Charset_big5) SupportsSupplementaryChars() bool {
	return false
}

func (Charset_big5) MaxWidth() int {
	return 2
}

func (Charset_big5) EncodeRune(dst []byte, r rune) int {
	if r < utf8.RuneSelf {
		dst[0] = byte(r)
		return 1
	}
	if r > 0xFFFF {
		return -1
	}
	page := big5Encode[r>>8]
	if page == nil || page[r&0xFF] == 0 {
		return -1
	}
	code := page[r&0xFF]
	_ = dst[1]
	dst[0] = byte(code >> 8)
	dst[1] = byte(code)
	return 2
}

// DecodeRune decodes the first character of src. Seven two-byte characters,
// A1 5A among them, decode to U+FFFD in MySQL. For those the function
// returns RuneError and true.
func (Charset_big5) DecodeRune(src []byte) (rune, int, bool) {
	if len(src) < 1 {
		return utf8.RuneError, 0, false
	}

	c0 := src[0]
	if c0 < utf8.RuneSelf {
		return rune(c0), 1, true
	}
	if c0 < 0xA1 || c0 > 0xF9 || len(src) < 2 {
		return utf8.RuneError, 1, false
	}

	c1 := src[1]
	idx := int(c0-0xA1) * 157
	switch {
	case 0x40 <= c1 && c1 <= 0x7E:
		idx += int(c1 - 0x40)
	case 0xA1 <= c1 && c1 <= 0xFE:
		idx += int(c1-0xA1) + 63
	default:
		return utf8.RuneError, 1, false
	}

	// The second byte is valid, so the two bytes are one character also
	// when big5 has no character at that code.
	if big5Decode[idx] == 0 {
		return utf8.RuneError, 2, false
	}
	return rune(big5Decode[idx]), 2, true
}
