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

	"golang.org/x/text/encoding/japanese"
)

type Charset_ujis struct{}

func (Charset_ujis) Name() string {
	return "ujis"
}

func (Charset_ujis) SupportsSupplementaryChars() bool {
	return false
}

func (Charset_ujis) DecodeRune(src []byte) (rune, int) {
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
		r := utf8.RuneError
		if i := int(c1-0xa1)*94 + int(c2-0xa1); i < len(jis0212Decode) {
			r = rune(jis0212Decode[i])
			if r == 0 {
				r = utf8.RuneError
			}
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
		r := utf8.RuneError
		if i := int(c0-0xa1)*94 + int(c1-0xa1); i < len(jis0208Decode) {
			r = rune(jis0208Decode[i])
			if r == 0 {
				r = utf8.RuneError
			}
		}
		return r, 2

	default:
		return utf8.RuneError, 1
	}
}

func (Charset_ujis) EncodeFromUTF8(in []byte) ([]byte, error) {
	return japanese.EUCJP.NewEncoder().Bytes(in)
}

type Charset_sjis struct{}

func (Charset_sjis) Name() string {
	return "sjis"
}

func (Charset_sjis) SupportsSupplementaryChars() bool {
	return false
}

func shiftjsHead(b byte) bool {
	switch {
	case b >= 0x81 && b <= 0x9f:
		return true
	case b >= 0xe0 && b <= 0xfc:
		return true
	default:
		return false
	}
}

func shiftjsTail(b byte) bool {
	switch {
	case b >= 0x40 && b <= 0x7e:
		return true
	case b >= 0x80 && b <= 0xfc:
		return true
	default:
		return false
	}
}

func (Charset_sjis) DecodeRune(in []byte) (rune, int) {
	if len(in) < 1 {
		return utf8.RuneError, 0
	}
	if shiftjsHead(in[0]) {
		if len(in) >= 2 && shiftjsTail(in[1]) {
			// TODO: actual mapping to Unicode
			return 0, 2
		}
		return utf8.RuneError, 1
	}
	return rune(in[0]), 1
}

func (Charset_sjis) EncodeFromUTF8(in []byte) ([]byte, error) {
	return japanese.ShiftJIS.NewEncoder().Bytes(in)
}
