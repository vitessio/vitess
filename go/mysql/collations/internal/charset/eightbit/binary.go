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

type Charset_binary struct{}

func (Charset_binary) Name() string {
	return "binary"
}

func (Charset_binary) IsSuperset(_ types.Charset) bool {
	return true
}

func (Charset_binary) SupportsSupplementaryChars() bool {
	return true
}

func (c Charset_binary) EncodeRune(dst []byte, r rune) int {
	if r > 0xFF {
		return -1
	}
	dst[0] = byte(r)
	return 1
}

func (c Charset_binary) DecodeRune(bytes []byte) (rune, int) {
	if len(bytes) < 1 {
		return utf8.RuneError, 0
	}
	return rune(bytes[0]), 1
}

func (c Charset_binary) Convert(_, in []byte, _ types.Charset) ([]byte, error) {
	return in, nil
}

func (Charset_binary) Length(src []byte) int {
	return len(src)
}

func (Charset_binary) MaxWidth() int {
	return 1
}
