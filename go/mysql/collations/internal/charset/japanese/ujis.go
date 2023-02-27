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
	_ "unsafe"

	"vitess.io/vitess/go/mysql/collations/internal/charset/types"
)

func ujisEncodeRune(dst []byte, r rune, table208, table212 *[65536]uint16) int {
	_ = dst[2]

	switch {
	case r < utf8.RuneSelf:
		dst[0] = byte(r)
		return 1

	case r >= 0xff61 && r <= 0xff9f:
		dst[0] = 0x8e
		dst[1] = uint8(r - (0xff61 - 0xa1))
		return 2

	case r <= 0xFFFF:
		uj := table208[r]
		if uj != 0 {
			dst[0] = byte(uj >> 8)
			dst[1] = byte(uj)
			return 2
		}

		uj = table212[r]
		if uj != 0 {
			dst[0] = 0x8f
			dst[1] = byte(uj >> 8)
			dst[2] = byte(uj)
			return 3
		}
	}
	return -1
}

func ujisDecodeRune(src []byte, table208, table212 *[65536]uint16) (rune, int) {
	if len(src) < 1 {
		return utf8.RuneError, 0
	}

	switch c0 := src[0]; {
	case c0 < utf8.RuneSelf:
		return rune(c0), 1

	case c0 == 0x8e:
		if len(src) < 2 {
			return utf8.RuneError, 1
		}
		c1 := src[1]
		switch {
		case c1 < 0xa1:
			return utf8.RuneError, 1
		case c1 > 0xdf:
			if c1 == 0xff {
				return utf8.RuneError, 1
			}
			return utf8.RuneError, 2
		default:
			return rune(c1) + (0xff61 - 0xa1), 2
		}
	case c0 == 0x8f:
		if len(src) < 3 {
			if len(src) == 2 && 0xa1 <= src[1] && src[1] < 0xfe {
				return utf8.RuneError, 2
			}
			return utf8.RuneError, 1
		}
		c1 := src[1]
		if c1 < 0xa1 || 0xfe < c1 {
			return utf8.RuneError, 1
		}
		c2 := src[2]
		if c2 < 0xa1 || 0xfe < c2 {
			return utf8.RuneError, 2
		}
		r := rune(table212[uint16(c1)<<8|uint16(c2)])
		if r == 0 {
			r = utf8.RuneError
		}
		return r, 3

	case 0xa1 <= c0 && c0 <= 0xfe:
		if len(src) < 2 {
			return utf8.RuneError, 1
		}
		c1 := src[1]
		if c1 < 0xa1 || 0xfe < c1 {
			return utf8.RuneError, 1
		}
		r := rune(table208[uint16(c0)<<8|uint16(c1)])
		if r == 0 {
			r = utf8.RuneError
		}
		return r, 2

	default:
		return utf8.RuneError, 1
	}
}

type Charset_ujis struct{}

func (Charset_ujis) Name() string {
	return "ujis"
}

func (Charset_ujis) IsSuperset(other types.Charset) bool {
	switch other.(type) {
	case Charset_ujis:
		return true
	default:
		return false
	}
}

func (Charset_ujis) SupportsSupplementaryChars() bool {
	return false
}

func (Charset_ujis) EncodeRune(dst []byte, r rune) int {
	return ujisEncodeRune(dst, r, &table_jis208Encode, &table_jis212Encode)
}

func (Charset_ujis) DecodeRune(src []byte) (rune, int) {
	return ujisDecodeRune(src, &table_jis208Decode, &table_jis212Decode)
}

func (Charset_ujis) MaxWidth() int {
	return 3
}

type Charset_eucjpms struct{}

func (Charset_eucjpms) Name() string {
	return "eucjpms"
}

func (Charset_eucjpms) IsSuperset(other types.Charset) bool {
	switch other.(type) {
	case Charset_eucjpms:
		return true
	default:
		return false
	}
}

func (Charset_eucjpms) SupportsSupplementaryChars() bool {
	return false
}

func (Charset_eucjpms) EncodeRune(dst []byte, r rune) int {
	return ujisEncodeRune(dst, r, &table_jis208_eucjpmsEncode, &table_jis212_eucjpmsEncode)
}

func (Charset_eucjpms) DecodeRune(src []byte) (rune, int) {
	return ujisDecodeRune(src, &table_jis208_eucjpmsDecode, &table_jis212_eucjpmsDecode)
}

func (Charset_eucjpms) MaxWidth() int {
	return 3
}
