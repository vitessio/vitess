/*
Copyright 2021 The Vitess Authors.

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

package japanese

import (
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/charset/types"
)

type Charset_sjis struct{}

func (Charset_sjis) Name() string {
	return "sjis"
}

func (Charset_sjis) IsSuperset(other types.Charset) bool {
	switch other.(type) {
	case Charset_sjis:
		return true
	default:
		return false
	}
}

func (Charset_sjis) SupportsSupplementaryChars() bool {
	return false
}

func encodeSJIS(dst []byte, r rune, table *[65536]uint16) int {
	_ = dst[1]

	if r > 0xFFFF {
		return -1
	}

	var sj = uint16(r)
	if sj < utf8.RuneSelf {
		if sj == 0x5c && table == &table_sjisEncode {
			// COMPAT: this appears to be a difference between SJIS and CP32,
			// all the other characters map properly
			sj = 0x815f
			goto write2
		}
		goto write1
	}
	if sj = table[r]; sj == 0 {
		return -1
	}
	if sj > 0xFF {
		goto write2
	}

write1:
	dst[0] = byte(sj)
	return 1

write2:
	dst[0] = byte(sj >> 8)
	dst[1] = byte(sj)
	return 2
}

func (Charset_sjis) EncodeRune(dst []byte, r rune) int {
	return encodeSJIS(dst, r, &table_sjisEncode)
}

func decodeSJIS(src []byte, table *[65536]uint16) (rune, int) {
	if len(src) < 1 {
		return utf8.RuneError, 0
	}
	c0 := src[0]
	if c0 < utf8.RuneSelf {
		return rune(c0), 1
	}
	if c0 >= 0xA1 && c0 <= 0xDF {
		return rune(table[c0]), 1
	}
	if len(src) >= 2 {
		sj := uint16(c0)<<8 | uint16(src[1])
		if cp := table[sj]; cp != 0 {
			return rune(cp), 2
		}
	}
	return utf8.RuneError, 2
}

func (Charset_sjis) DecodeRune(src []byte) (rune, int) {
	return decodeSJIS(src, &table_sjisDecode)
}

func (Charset_sjis) MaxWidth() int {
	return 2
}

type Charset_cp932 struct{}

func (Charset_cp932) Name() string {
	return "cp932"
}

func (Charset_cp932) SupportsSupplementaryChars() bool {
	return false
}

func (Charset_cp932) IsSuperset(other types.Charset) bool {
	switch other.(type) {
	case Charset_cp932:
		return true
	default:
		return false
	}
}

func (Charset_cp932) EncodeRune(dst []byte, r rune) int {
	return encodeSJIS(dst, r, &table_cp932Encode)
}

func (Charset_cp932) DecodeRune(src []byte) (rune, int) {
	return decodeSJIS(src, &table_cp932Decode)
}

func (Charset_cp932) MaxWidth() int {
	return 2
}
