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

package eightbit

import (
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/internal/charset/types"
)

type UnicodeMapping struct {
	From, To uint16
	Range    []byte `json:"Tab"`
}

type Charset_8bit struct {
	Name_       string
	ToUnicode   *[256]uint16
	FromUnicode []UnicodeMapping
}

func (e *Charset_8bit) Name() string {
	return e.Name_
}

func (e *Charset_8bit) IsSuperset(other types.Charset) bool {
	switch other := other.(type) {
	case *Charset_8bit:
		return e.Name_ == other.Name_
	default:
		return false
	}
}

func (e *Charset_8bit) SupportsSupplementaryChars() bool {
	return false
}

func (e *Charset_8bit) DecodeRune(bytes []byte) (rune, int) {
	if len(bytes) < 1 {
		return utf8.RuneError, 0
	}
	return rune(e.ToUnicode[bytes[0]]), 1
}

func (e *Charset_8bit) EncodeRune(dst []byte, r rune) int {
	if r > 0xFFFF {
		return -1
	}
	cp := uint16(r)
	for _, mapping := range e.FromUnicode {
		if cp >= mapping.From && cp <= mapping.To {
			dst[0] = mapping.Range[cp-mapping.From]
			if dst[0] == 0 && cp != 0 {
				return -1
			}
			return 1
		}
	}
	return -1
}

func (Charset_8bit) Length(src []byte) int {
	return len(src)
}
