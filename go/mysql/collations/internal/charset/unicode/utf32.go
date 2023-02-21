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

package unicode

import (
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/internal/charset/types"
)

type Charset_utf32 struct{}

func (Charset_utf32) Name() string {
	return "utf32"
}

func (Charset_utf32) IsSuperset(other types.Charset) bool {
	switch other.(type) {
	case Charset_utf32:
		return true
	default:
		return false
	}
}

func (Charset_utf32) EncodeRune(dst []byte, r rune) int {
	_ = dst[3]

	dst[0] = uint8(r >> 24)
	dst[1] = uint8(r >> 16)
	dst[2] = uint8(r >> 8)
	dst[3] = uint8(r)
	return 4
}

func (Charset_utf32) DecodeRune(p []byte) (rune, int) {
	if len(p) < 4 {
		return utf8.RuneError, 0
	}
	return (rune(p[0]) << 24) | (rune(p[1]) << 16) | (rune(p[2]) << 8) | rune(p[3]), 4
}

func (Charset_utf32) SupportsSupplementaryChars() bool {
	return true
}

func (Charset_utf32) CharLen(src []byte) int {
	cnt := len(src)
	if cnt%4 != 0 {
		return cnt/4 + 1
	}
	return cnt / 4
}

func (Charset_utf32) MaxWidth() int {
	return 4
}
