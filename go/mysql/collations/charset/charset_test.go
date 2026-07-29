/*
Copyright 2024 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsMultibyteByName(t *testing.T) {
	testCases := []struct {
		csname string
		want   bool
	}{
		{"euckr", true},
		{"gb2312", true},
		{"sjis", true},
		{"cp932", true},
		{"eucjpms", true},
		{"ujis", true},
		{"utf16", false},
		{"latin1", false},
		{"binary", false},
	}

	for _, tc := range testCases {
		t.Run(tc.csname, func(t *testing.T) {
			assert.Equal(t, tc.want, IsMultibyteByName(tc.csname))
		})
	}
}

func TestIsUnicode(t *testing.T) {
	testCases := []struct {
		cs   Charset
		want bool
	}{
		{Charset_utf8mb3{}, true},
		{Charset_utf8mb4{}, true},
		{Charset_utf16{}, true},
		{Charset_utf16le{}, true},
		{Charset_ucs2{}, true},
		{Charset_utf32{}, true},
		{&testCharset1{}, false},
	}

	for _, tc := range testCases {
		t.Run(tc.cs.Name(), func(t *testing.T) {
			assert.Equal(t, tc.want, IsUnicode(tc.cs))
		})
	}
}

func TestIsUnicodeByName(t *testing.T) {
	testCases := []struct {
		csname string
		want   bool
	}{
		{"utf8", true},
		{"utf8mb3", true},
		{"utf8mb4", true},
		{"utf16", true},
		{"utf16le", true},
		{"ucs2", true},
		{"utf32", true},
		{"binary", false},
	}

	for _, tc := range testCases {
		t.Run(tc.csname, func(t *testing.T) {
			assert.Equal(t, tc.want, IsUnicodeByName(tc.csname))
		})
	}
}

func TestIsBackslashSafe(t *testing.T) {
	testCases := []struct {
		cs   Charset
		want bool
	}{
		{Charset_sjis{}, false},
		{Charset_cp932{}, false},
		{Charset_gb18030{}, false},
		{Charset_utf16le{}, true},
		{&testCharset1{}, true},
	}

	for _, tc := range testCases {
		t.Run(tc.cs.Name(), func(t *testing.T) {
			assert.Equal(t, tc.want, IsBackslashSafe(tc.cs))
		})
	}
}

func TestDecodeRuneWidthContract(t *testing.T) {
	charsets := []Charset{
		Charset_binary{},
		Charset_latin1{},
		Charset_utf8mb3{},
		Charset_utf8mb4{},
		Charset_utf16{},
		Charset_utf16le{},
		Charset_ucs2{},
		Charset_utf32{},
		Charset_gb18030{},
		Charset_gb2312{},
		Charset_ujis{},
		Charset_sjis{},
		Charset_cp932{},
		Charset_eucjpms{},
		Charset_euckr{},
	}

	var inputs [][]byte
	for b := range 256 {
		inputs = append(inputs, []byte{byte(b)})
	}
	for hi := range 256 {
		for lo := range 256 {
			inputs = append(inputs, []byte{byte(hi), byte(lo)})
		}
	}
	// Three and four byte tails behind the byte values that lead longer
	// sequences somewhere: UTF-16 surrogates, UTF-8 continuations and
	// multibyte lead bytes.
	leads := []byte{0x00, 0x31, 0x81, 0x8E, 0x8F, 0xA1, 0xC2, 0xD8, 0xDB, 0xDC, 0xDF, 0xE0, 0xED, 0xF0, 0xFF}
	tails := []byte{0x00, 0x31, 0x80, 0xA0, 0xD8, 0xDC, 0xFF}
	for _, l := range leads {
		for _, m := range tails {
			for _, e := range tails {
				inputs = append(inputs, []byte{l, m, e})
				inputs = append(inputs, []byte{l, m, e, 0x31})
				inputs = append(inputs, []byte{0x31, l, m, e})
			}
		}
	}

	for _, cs := range charsets {
		t.Run(cs.Name(), func(t *testing.T) {
			r, width := cs.DecodeRune(nil)
			require.Equal(t, RuneError, r)
			require.Zero(t, width)

			for _, in := range inputs {
				_, width := cs.DecodeRune(in)
				size := width
				if size < 0 {
					size = -size
				}
				if size < 1 || size > len(in) {
					require.Failf(t, "DecodeRune width out of range",
						"%s.DecodeRune(%#v) returned width %d, want an absolute width in 1..%d", cs.Name(), in, width, len(in))
				}
			}
		})
	}
}

func TestDecodeRuneWidthSign(t *testing.T) {
	testCases := []struct {
		name      string
		cs        Charset
		input     []byte
		wantRune  rune
		wantWidth int
	}{
		{"utf8mb3 valid RuneError", Charset_utf8mb3{}, []byte{0xEF, 0xBF, 0xBD}, RuneError, 3},
		{"utf8mb3 invalid", Charset_utf8mb3{}, []byte{0xFF}, RuneError, -1},
		{"utf8mb4 valid RuneError", Charset_utf8mb4{}, []byte{0xEF, 0xBF, 0xBD}, RuneError, 3},
		{"utf8mb4 invalid", Charset_utf8mb4{}, []byte{0xFF}, RuneError, -1},
		{"utf16 valid RuneError", Charset_utf16{}, []byte{0xFF, 0xFD}, RuneError, 2},
		{"utf16 unpaired surrogate", Charset_utf16{}, []byte{0xD8, 0x00}, RuneError, -2},
		{"utf16 broken surrogate pair", Charset_utf16{}, []byte{0xD8, 0x00, 0x00, 0x31}, RuneError, -2},
		{"utf16le valid RuneError", Charset_utf16le{}, []byte{0xFD, 0xFF}, RuneError, 2},
		{"utf16le unpaired surrogate", Charset_utf16le{}, []byte{0x00, 0xD8}, RuneError, -2},
		{"ucs2 valid RuneError", Charset_ucs2{}, []byte{0xFF, 0xFD}, RuneError, 2},
		{"ucs2 invalid", Charset_ucs2{}, []byte{0x00}, RuneError, -1},
		{"utf32 valid RuneError", Charset_utf32{}, []byte{0x00, 0x00, 0xFF, 0xFD}, RuneError, 4},
		{"utf32 invalid", Charset_utf32{}, []byte{0x00}, RuneError, -1},
		{"gb18030 invalid", Charset_gb18030{}, []byte{0xFF}, RuneError, -1},
		{"gb2312 invalid", Charset_gb2312{}, []byte{0xFF}, RuneError, -1},
		{"ujis invalid", Charset_ujis{}, []byte{0xFF}, RuneError, -1},
		{"sjis invalid", Charset_sjis{}, []byte{0x81}, RuneError, -1},
		{"cp932 invalid", Charset_cp932{}, []byte{0x81}, RuneError, -1},
		{"eucjpms invalid", Charset_eucjpms{}, []byte{0xFF}, RuneError, -1},
		{"euckr invalid", Charset_euckr{}, []byte{0xFF}, RuneError, -1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotRune, gotWidth := tc.cs.DecodeRune(tc.input)
			require.Equal(t, tc.wantRune, gotRune)
			require.Equal(t, tc.wantWidth, gotWidth)
		})
	}
}
