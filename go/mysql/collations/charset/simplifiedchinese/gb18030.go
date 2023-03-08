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
	_ "unsafe"

	"vitess.io/vitess/go/mysql/collations/charset/types"
)

type Charset_gb18030 struct{}

func (Charset_gb18030) Name() string {
	return "gb18030"
}

func (Charset_gb18030) IsSuperset(other types.Charset) bool {
	switch other.(type) {
	case Charset_gb18030:
		return true
	default:
		return false
	}
}

const isgb18030 = true

func (Charset_gb18030) EncodeRune(dst []byte, r rune) int {
	_ = dst[3]

	var r2 rune
	switch {
	case r < utf8.RuneSelf:
		goto write1
	case encode0Low <= r && r < encode0High:
		if r2 = rune(encode0[r-encode0Low]); r2 != 0 {
			goto write2
		}
	case encode1Low <= r && r < encode1High:
		// Microsoft's Code Page 936 extends GBK 1.0 to encode the euro sign U+20AC
		// as 0x80. The HTML5 specification at http://encoding.spec.whatwg.org/#gbk
		// says to treat "gbk" as Code Page 936.
		if r == '€' {
			r = 0x80
			goto write1
		}
		if r2 = rune(encode1[r-encode1Low]); r2 != 0 {
			goto write2
		}
	case encode2Low <= r && r < encode2High:
		if r2 = rune(encode2[r-encode2Low]); r2 != 0 {
			goto write2
		}
	case encode3Low <= r && r < encode3High:
		if r2 = rune(encode3[r-encode3Low]); r2 != 0 {
			goto write2
		}
	case encode4Low <= r && r < encode4High:
		if r2 = rune(encode4[r-encode4Low]); r2 != 0 {
			goto write2
		}
	}

	if isgb18030 {
		if r < 0x10000 {
			i, j := 0, len(gb18030)
			for i < j {
				h := i + (j-i)/2
				if r >= rune(gb18030[h][1]) {
					i = h + 1
				} else {
					j = h
				}
			}
			dec := &gb18030[i-1]
			r += rune(dec[0]) - rune(dec[1])
			goto write4
		} else if r < 0x110000 {
			r += 189000 - 0x10000
			goto write4
		}
	}
	return -1

write1:
	dst[0] = uint8(r)
	return 1

write2:
	dst[0] = uint8(r2 >> 8)
	dst[1] = uint8(r2)
	return 2

write4:
	dst[3] = uint8(r%10 + 0x30)
	r /= 10
	dst[2] = uint8(r%126 + 0x81)
	r /= 126
	dst[1] = uint8(r%10 + 0x30)
	r /= 10
	dst[0] = uint8(r + 0x81)
	return 4
}

func (Charset_gb18030) DecodeRune(src []byte) (rune, int) {
	if len(src) < 1 {
		return utf8.RuneError, 0
	}

	switch c0 := src[0]; {
	case c0 < utf8.RuneSelf:
		return rune(c0), 1

	// Microsoft's Code Page 936 extends GBK 1.0 to encode the euro sign U+20AC
	// as 0x80. The HTML5 specification at http://encoding.spec.whatwg.org/#gbk
	// says to treat "gbk" as Code Page 936.
	case c0 == 0x80:
		return '€', 1

	case c0 < 0xff:
		if len(src) < 2 {
			return utf8.RuneError, 1
		}

		c1 := src[1]
		switch {
		case 0x40 <= c1 && c1 < 0x7f:
			c1 -= 0x40
		case 0x80 <= c1 && c1 < 0xff:
			c1 -= 0x41
		case isgb18030 && 0x30 <= c1 && c1 < 0x40:
			if len(src) < 4 {
				// The second byte here is always ASCII, so we can set size
				// to 1 in all cases.
				return utf8.RuneError, 1
			}
			c2 := src[2]
			if c2 < 0x81 || 0xff <= c2 {
				return utf8.RuneError, 1
			}
			c3 := src[3]
			if c3 < 0x30 || 0x3a <= c3 {
				return utf8.RuneError, 1
			}
			var r = ((rune(c0-0x81)*10+rune(c1-0x30))*126+rune(c2-0x81))*10 + rune(c3-0x30)
			if r < 39420 {
				i, j := 0, len(gb18030)
				for i < j {
					h := i + (j-i)/2
					if r >= rune(gb18030[h][0]) {
						i = h + 1
					} else {
						j = h
					}
				}
				dec := &gb18030[i-1]
				r += rune(dec[1]) - rune(dec[0])
				return r, 4
			}
			r -= 189000
			if 0 <= r && r < 0x100000 {
				r += 0x10000
			} else {
				return utf8.RuneError, 1
			}
			return r, 4
		default:
			return utf8.RuneError, 1
		}
		r := utf8.RuneError
		if i := int(c0-0x81)*190 + int(c1); i < len(decode) {
			r = rune(decode[i])
			if r == 0 {
				r = utf8.RuneError
			}
		}
		return r, 2

	default:
		return utf8.RuneError, 1
	}
}

func (c Charset_gb18030) SupportsSupplementaryChars() bool {
	return false
}

func (Charset_gb18030) MaxWidth() int {
	return 4
}
