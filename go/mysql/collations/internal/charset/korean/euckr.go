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

package korean

import (
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/internal/charset/types"
)

type Charset_euckr struct{}

func (Charset_euckr) Name() string {
	return "euckr"
}

func (Charset_euckr) IsSuperset(other types.Charset) bool {
	switch other.(type) {
	case Charset_euckr:
		return true
	default:
		return false
	}
}

func (Charset_euckr) SupportsSupplementaryChars() bool {
	return false
}

func (Charset_euckr) EncodeRune(dst []byte, r rune) int {
	_ = dst[1]

	switch {
	case r < utf8.RuneSelf:
		dst[0] = byte(r)
		return 1
	case encode0Low <= r && r < encode0High:
		if r = rune(encode0[r-encode0Low]); r != 0 {
			goto write2
		}
	case encode1Low <= r && r < encode1High:
		if r = rune(encode1[r-encode1Low]); r != 0 {
			goto write2
		}
	case encode2Low <= r && r < encode2High:
		if r = rune(encode2[r-encode2Low]); r != 0 {
			goto write2
		}
	case encode3Low <= r && r < encode3High:
		if r = rune(encode3[r-encode3Low]); r != 0 {
			goto write2
		}
	case encode4Low <= r && r < encode4High:
		if r = rune(encode4[r-encode4Low]); r != 0 {
			goto write2
		}
	case encode5Low <= r && r < encode5High:
		if r = rune(encode5[r-encode5Low]); r != 0 {
			goto write2
		}
	case encode6Low <= r && r < encode6High:
		if r = rune(encode6[r-encode6Low]); r != 0 {
			goto write2
		}
	}
	return -1

write2:
	dst[0] = uint8(r >> 8)
	dst[1] = uint8(r)
	return 2
}

func (Charset_euckr) DecodeRune(src []byte) (rune, int) {
	if len(src) < 1 {
		return utf8.RuneError, 0
	}

	switch c0 := src[0]; {
	case c0 < utf8.RuneSelf:
		return rune(c0), 1

	case 0x81 <= c0 && c0 < 0xff:
		if len(src) < 2 {
			return utf8.RuneError, 1
		}
		var r rune
		c1 := src[1]
		if c0 < 0xc7 {
			r = 178 * rune(c0-0x81)
			switch {
			case 0x41 <= c1 && c1 < 0x5b:
				r += rune(c1) - (0x41 - 0*26)
			case 0x61 <= c1 && c1 < 0x7b:
				r += rune(c1) - (0x61 - 1*26)
			case 0x81 <= c1 && c1 < 0xff:
				r += rune(c1) - (0x81 - 2*26)
			default:
				goto decError
			}
		} else if 0xa1 <= c1 && c1 < 0xff {
			r = 178*(0xc7-0x81) + rune(c0-0xc7)*94 + rune(c1-0xa1)
		} else {
			goto decError
		}
		if int(r) < len(decode) {
			r = rune(decode[r])
			if r != 0 {
				return r, 2
			}
		}

	decError:
		if c1 < utf8.RuneSelf {
			return utf8.RuneError, 1
		}
		return utf8.RuneError, 2

	default:
		return utf8.RuneError, 1
	}

}

func (Charset_euckr) MaxWidth() int {
	return 2
}
