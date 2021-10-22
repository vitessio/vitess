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

package charset

import (
	"errors"
)

type UnicodeMapping struct {
	From, To uint16
	Range    []byte `json:"Tab"`
}

type Charset_8bit struct {
	ToUnicode   []uint16
	FromUnicode []UnicodeMapping
}

func (e *Charset_8bit) Name() string {
	return "generic_8bit_charset"
}

func (e *Charset_8bit) SupportsSupplementaryChars() bool {
	return false
}

func (e *Charset_8bit) DecodeRune(bytes []byte) (rune, int) {
	if len(bytes) < 1 {
		return RuneError, 0
	}
	return rune(e.ToUnicode[bytes[0]]), 1
}

var ErrNoMapping = errors.New("cannot encode all codepoints in this encoding")

func (e *Charset_8bit) encodeRune(r rune) byte {
	if r > 0xFFFF {
		return 0
	}
	cp := uint16(r)
	for _, mapping := range e.FromUnicode {
		if cp >= mapping.From && cp <= mapping.To {
			return mapping.Range[cp-mapping.From]
		}
	}
	return 0
}

func (e *Charset_8bit) EncodeFromUTF8(in []byte) ([]byte, error) {
	var output = make([]byte, 0, len(in))
	var b byte
	for _, cp := range string(in) {
		if b = e.encodeRune(cp); b == 0 {
			return nil, ErrNoMapping
		}
		output = append(output, b)
	}
	return output, nil
}
