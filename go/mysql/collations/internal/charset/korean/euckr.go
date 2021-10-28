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

	"golang.org/x/text/encoding/korean"
)

type Charset_euckr struct{}

func (Charset_euckr) Name() string {
	return "euckr"
}

func (Charset_euckr) SupportsSupplementaryChars() bool {
	return false
}

func euckrHead(b byte) bool {
	return b >= 0x81 && b <= 0xfe
}

func euckrTail(b byte) bool {
	switch {
	case b >= 0x41 && b <= 0x5a:
		return true
	case b >= 0x61 && b <= 0x7a:
		return true
	case b >= 0x81 && b <= 0xfe:
		return true
	default:
		return false
	}
}

func (Charset_euckr) DecodeRune(in []byte) (rune, int) {
	if len(in) < 1 {
		return utf8.RuneError, 0
	}
	if euckrHead(in[0]) {
		if len(in) >= 2 && euckrTail(in[0]) {
			// TODO: map to Unicode
			return 0, 2
		}
		return utf8.RuneError, 1
	}
	return rune(in[0]), 1
}

func (Charset_euckr) EncodeFromUTF8(in []byte) ([]byte, error) {
	return korean.EUCKR.NewEncoder().Bytes(in)
}
