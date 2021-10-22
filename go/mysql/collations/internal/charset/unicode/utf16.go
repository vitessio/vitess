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

import "golang.org/x/text/encoding/unicode"

var defaultUTF16be = unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM)
var defaultUTF16le = unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM)

// 0xd800-0xdc00 encodes the high 10 bits of a pair.
// 0xdc00-0xe000 encodes the low 10 bits of a pair.
// the value is those 20 bits plus 0x10000.
const (
	surr1    = 0xd800
	surr2    = 0xdc00
	surr3    = 0xe000
	surrSelf = 0x10000
)

type Charset_utf16be struct{}

func (Charset_utf16be) Name() string {
	return "utf16be"
}

func (Charset_utf16be) DecodeRune(b []byte) (rune, int) {
	if len(b) < 2 {
		return RuneError, 0
	}

	r1 := uint16(b[1]) | uint16(b[0])<<8
	if r1 < surr1 || surr3 <= r1 {
		return rune(r1), 2
	}

	if len(b) < 4 {
		return RuneError, 0
	}

	r2 := uint16(b[3]) | uint16(b[2])<<8
	if surr1 <= r1 && r1 < surr2 && surr2 <= r2 && r2 < surr3 {
		return (rune(r1)-surr1)<<10 | (rune(r2) - surr2) + surrSelf, 4
	}

	return RuneError, 1
}

func (Charset_utf16be) SupportsSupplementaryChars() bool {
	return true
}

func (Charset_utf16be) EncodeFromUTF8(in []byte) ([]byte, error) {
	return defaultUTF16be.NewEncoder().Bytes(in)
}

type Charset_utf16le struct{}

func (Charset_utf16le) Name() string {
	return "utf16le"
}

func (Charset_utf16le) DecodeRune(b []byte) (rune, int) {
	if len(b) < 2 {
		return RuneError, 0
	}

	r1 := uint16(b[0]) | uint16(b[1])<<8
	if r1 < surr1 || surr3 <= r1 {
		return rune(r1), 2
	}

	if len(b) < 4 {
		return RuneError, 0
	}

	r2 := uint16(b[2]) | uint16(b[3])<<8
	if surr1 <= r1 && r1 < surr2 && surr2 <= r2 && r2 < surr3 {
		return (rune(r1)-surr1)<<10 | (rune(r2) - surr2) + surrSelf, 4
	}

	return RuneError, 1
}

func (Charset_utf16le) SupportsSupplementaryChars() bool {
	return true
}

func (Charset_utf16le) EncodeFromUTF8(in []byte) ([]byte, error) {
	return defaultUTF16le.NewEncoder().Bytes(in)
}

type Charset_ucs2 struct{}

func (Charset_ucs2) Name() string {
	return "ucs2"
}

func (Charset_ucs2) DecodeRune(p []byte) (rune, int) {
	if len(p) < 2 {
		return RuneError, 0
	}
	return rune(p[0])<<8 | rune(p[1]), 2
}

func (Charset_ucs2) SupportsSupplementaryChars() bool {
	return false
}

func (Charset_ucs2) EncodeFromUTF8(in []byte) ([]byte, error) {
	if err := ensureBMPRange(in); err != nil {
		return nil, err
	}
	return defaultUTF16be.NewEncoder().Bytes(in)
}
