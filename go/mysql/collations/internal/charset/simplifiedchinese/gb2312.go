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

package simplifiedchinese

import (
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/internal/charset/types"
)

type Charset_gb2312 struct{}

func (Charset_gb2312) Name() string {
	return "gb2312"
}

func (Charset_gb2312) IsSuperset(other types.Charset) bool {
	switch other.(type) {
	case Charset_gb2312:
		return true
	default:
		return false
	}
}

func (Charset_gb2312) SupportsSupplementaryChars() bool {
	return false
}

func (Charset_gb2312) EncodeRune(dst []byte, r rune) int {
	switch {
	case r < utf8.RuneSelf:
		dst[0] = byte(r)
		return 1
	case gb2312Encode0min <= r && r <= gb2312Encode0max:
		if r = rune(gb2312Encode0[r-gb2312Encode0min]); r != 0 {
			goto writeGB
		}
	case gb2312Encode1min <= r && r <= gb2312Encode1max:
		if r = rune(gb2312Encode1[r-gb2312Encode1min]); r != 0 {
			goto writeGB
		}
	case gb2312Encode2min <= r && r <= gb2312Encode2max:
		if r = rune(gb2312Encode2[r-gb2312Encode2min]); r != 0 {
			goto writeGB
		}
	case gb2312Encode3min <= r && r <= gb2312Encode3max:
		if r = rune(gb2312Encode3[r-gb2312Encode3min]); r != 0 {
			goto writeGB
		}
	case gb2312Encode4min <= r && r <= gb2312Encode4max:
		if r = rune(gb2312Encode4[r-gb2312Encode4min]); r != 0 {
			goto writeGB
		}
	case gb2312Encode5min <= r && r <= gb2312Encode5max:
		if r = rune(gb2312Encode5[r-gb2312Encode5min]); r != 0 {
			goto writeGB
		}
	case gb2312Encode6min <= r && r <= gb2312Encode6max:
		if r = rune(gb2312Encode6[r-gb2312Encode6min]); r != 0 {
			goto writeGB
		}
	case gb2312Encode7min <= r && r <= gb2312Encode7max:
		if r = rune(gb2312Encode7[r-gb2312Encode7min]); r != 0 {
			goto writeGB
		}
	case gb2312Encode8min <= r && r <= gb2312Encode8max:
		if r = rune(gb2312Encode8[r-gb2312Encode8min]); r != 0 {
			goto writeGB
		}
	case gb2312Encode9min <= r && r <= gb2312Encode9max:
		if r = rune(gb2312Encode9[r-gb2312Encode9min]); r != 0 {
			goto writeGB
		}
	}
	return -1

writeGB:
	r |= 0x8080
	dst[0] = byte(r >> 8)
	dst[1] = byte(r)
	return 2
}

func (Charset_gb2312) DecodeRune(src []byte) (rune, int) {
	if len(src) < 1 {
		return utf8.RuneError, 0
	}

	c0 := src[0]
	if c0 < utf8.RuneSelf {
		return rune(c0), 1
	}

	if len(src) < 2 {
		return utf8.RuneError, 1
	}

	r := (uint16(c0)<<8 | uint16(src[1])) & 0x7f7f
	switch {
	case gb2312Decode0min <= r && r <= gb2312Decode0max:
		r = gb2312Decode0[r-gb2312Decode0min]
	case gb2312Decode1min <= r && r <= gb2312Decode1max:
		r = gb2312Decode1[r-gb2312Decode1min]
	case gb2312Decode2min <= r && r <= gb2312Decode2max:
		r = gb2312Decode2[r-gb2312Decode2min]
	default:
		r = 0
	}
	if r == 0 {
		return utf8.RuneError, 2
	}
	return rune(r), 2
}

func (Charset_gb2312) MaxWidth() int {
	return 2
}
