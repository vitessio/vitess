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

// Package thai has the Thai character sets of MySQL.
package thai

import (
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/charset/types"
)

// Charset_tis620 is the tis620 character set of MySQL, one byte for each
// character: 00-9F are U+0000-U+009F, A1-DA are the Thai U+0E01-U+0E3A and
// DF-FB are U+0E3F-U+0E5B. "ก" (U+0E01) is A1.
//
// The conversions follow MySQL 8.4.13 and 8.0.46, measured for every byte
// and every code point, in two things that a conversion table for TIS-620
// does not have:
//
//   - The nine bytes without a character, A0, DB-DE and FC-FF, decode to
//     U+FFFD and are valid.
//   - A code point above U+FFFF encodes as the code point with the same
//     two lowest bytes does: U+10041 encodes to "A" (41), as U+0041. A code
//     point whose two lowest bytes are 00 00, such as U+10000, has no
//     tis620 form, since only U+0000 encodes to 00.
//   - U+FFFD encodes to FF.
type Charset_tis620 struct{}

func (Charset_tis620) Name() string {
	return "tis620"
}

func (Charset_tis620) IsSuperset(other types.Charset) bool {
	switch other.(type) {
	case Charset_tis620:
		return true
	default:
		return false
	}
}

func (Charset_tis620) SupportsSupplementaryChars() bool {
	return false
}

func (Charset_tis620) MaxWidth() int {
	return 1
}

func (Charset_tis620) DecodeRune(src []byte) (rune, int, bool) {
	if len(src) < 1 {
		return utf8.RuneError, 0, false
	}
	switch b := src[0]; {
	case b <= 0x9F:
		return rune(b), 1, true
	case 0xA1 <= b && b <= 0xDA, 0xDF <= b && b <= 0xFB:
		return 0x0E00 + rune(b-0xA0), 1, true
	default:
		return utf8.RuneError, 1, true
	}
}

func (Charset_tis620) EncodeRune(dst []byte, r rune) int {
	if r < 0 {
		return -1
	}
	low := byte(r)
	switch (r >> 8) & 0xFF {
	case 0x00:
		if low <= 0x9F {
			// U+0000 is the only code point with the byte 00.
			if low == 0 && r != 0 {
				return -1
			}
			dst[0] = low
			return 1
		}
	case 0x0E:
		if (0x01 <= low && low <= 0x3A) || (0x3F <= low && low <= 0x5B) {
			dst[0] = low + 0xA0
			return 1
		}
	case 0xFF:
		if low == 0xFD {
			dst[0] = 0xFF
			return 1
		}
	}
	return -1
}
