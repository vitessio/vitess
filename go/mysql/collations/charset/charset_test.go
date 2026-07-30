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
			r, width, ok := cs.DecodeRune(nil)
			require.Equal(t, RuneError, r)
			require.Zero(t, width)
			require.False(t, ok)

			for _, in := range inputs {
				_, width, _ := cs.DecodeRune(in)
				if width < 1 || width > len(in) {
					require.Failf(t, "DecodeRune width out of range",
						"%s.DecodeRune(%#v) returned width %d, want a width in 1..%d", cs.Name(), in, width, len(in))
				}
			}
		})
	}
}

func TestDecodeRuneValidity(t *testing.T) {
	testCases := []struct {
		name      string
		cs        Charset
		input     []byte
		wantRune  rune
		wantWidth int
		wantOK    bool
	}{
		{"utf8mb3 valid RuneError", Charset_utf8mb3{}, []byte{0xEF, 0xBF, 0xBD}, RuneError, 3, true},
		{"utf8mb3 invalid", Charset_utf8mb3{}, []byte{0xFF}, RuneError, 1, false},
		{"utf8mb4 valid RuneError", Charset_utf8mb4{}, []byte{0xEF, 0xBF, 0xBD}, RuneError, 3, true},
		{"utf8mb4 invalid", Charset_utf8mb4{}, []byte{0xFF}, RuneError, 1, false},
		{"utf16 valid RuneError", Charset_utf16{}, []byte{0xFF, 0xFD}, RuneError, 2, true},
		{"utf16 unpaired surrogate", Charset_utf16{}, []byte{0xD8, 0x00}, RuneError, 2, false},
		{"utf16 broken surrogate pair", Charset_utf16{}, []byte{0xD8, 0x00, 0x00, 0x31}, RuneError, 2, false},
		{"utf16le valid RuneError", Charset_utf16le{}, []byte{0xFD, 0xFF}, RuneError, 2, true},
		{"utf16le unpaired surrogate", Charset_utf16le{}, []byte{0x00, 0xD8}, RuneError, 2, false},
		{"ucs2 valid RuneError", Charset_ucs2{}, []byte{0xFF, 0xFD}, RuneError, 2, true},
		{"ucs2 invalid", Charset_ucs2{}, []byte{0x00}, RuneError, 1, false},
		{"utf32 valid RuneError", Charset_utf32{}, []byte{0x00, 0x00, 0xFF, 0xFD}, RuneError, 4, true},
		{"utf32 invalid", Charset_utf32{}, []byte{0x00}, RuneError, 1, false},
		{"utf32 surrogate", Charset_utf32{}, []byte{0x00, 0x00, 0xD8, 0x00}, RuneError, 4, false},
		{"utf32 beyond max rune", Charset_utf32{}, []byte{0x00, 0x11, 0x00, 0x00}, RuneError, 4, false},
		{"utf32 negative rune", Charset_utf32{}, []byte{0xFF, 0xFF, 0xFF, 0xFF}, RuneError, 4, false},
		{"gb18030 invalid", Charset_gb18030{}, []byte{0xFF}, RuneError, 1, false},
		{"gb18030 invalid lead 0x80", Charset_gb18030{}, []byte{0x80, 0x41}, RuneError, 1, false},
		{"gb18030 second byte beyond digits", Charset_gb18030{}, []byte{0x81, 0x3A, 0x81, 0x30}, RuneError, 1, false},
		{"gb18030 third byte beyond 0xFE", Charset_gb18030{}, []byte{0x81, 0x30, 0xFF, 0x30}, RuneError, 1, false},
		{"gb18030 reserved pointer", Charset_gb18030{}, []byte{0x84, 0x31, 0xA5, 0x30}, RuneError, 4, false},
		{"gb18030 four byte minimum", Charset_gb18030{}, []byte{0x81, 0x30, 0x81, 0x30}, 0x80, 4, true},
		{"gb18030 four byte maximum", Charset_gb18030{}, []byte{0xE3, 0x32, 0x9A, 0x35}, 0x10FFFF, 4, true},
		{"gb2312 invalid", Charset_gb2312{}, []byte{0xFF}, RuneError, 1, false},
		{"gb2312 aliasing trail", Charset_gb2312{}, []byte{0xA1, 0x21}, RuneError, 1, false},
		{"gb2312 invalid trail", Charset_gb2312{}, []byte{0xA1, 0x20}, RuneError, 1, false},
		{"gb2312 invalid lead", Charset_gb2312{}, []byte{0xF8, 0xA1}, RuneError, 1, false},
		{"ujis invalid", Charset_ujis{}, []byte{0xFF}, RuneError, 1, false},
		{"ujis kana invalid trail", Charset_ujis{}, []byte{0x8E, 0xE5}, RuneError, 1, false},
		{"ujis plane2 invalid trail", Charset_ujis{}, []byte{0x8F, 0xA1, 0x20}, RuneError, 1, false},
		{"ujis plane2 truncated", Charset_ujis{}, []byte{0x8F, 0xA1}, RuneError, 1, false},
		{"ujis unassigned pair", Charset_ujis{}, []byte{0xA2, 0xF1}, RuneError, 2, false},
		{"sjis invalid", Charset_sjis{}, []byte{0x81}, RuneError, 1, false},
		{"sjis invalid lead", Charset_sjis{}, []byte{0x80, 0x41}, RuneError, 1, false},
		{"sjis invalid trail", Charset_sjis{}, []byte{0x81, 0x20}, RuneError, 1, false},
		{"sjis unassigned pair", Charset_sjis{}, []byte{0x81, 0xAD}, RuneError, 2, false},
		{"cp932 invalid", Charset_cp932{}, []byte{0x81}, RuneError, 1, false},
		{"cp932 invalid trail", Charset_cp932{}, []byte{0x81, 0x20}, RuneError, 1, false},
		{"eucjpms invalid", Charset_eucjpms{}, []byte{0xFF}, RuneError, 1, false},
		{"euckr invalid", Charset_euckr{}, []byte{0xFF}, RuneError, 1, false},
		{"euckr invalid trail", Charset_euckr{}, []byte{0xB0, 0xFF}, RuneError, 1, false},
		{"euckr gap trail", Charset_euckr{}, []byte{0xB0, 0x5B}, RuneError, 1, false},
		{"euckr unassigned pair", Charset_euckr{}, []byte{0xC9, 0x41}, RuneError, 2, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotRune, gotWidth, gotOK := tc.cs.DecodeRune(tc.input)
			require.Equal(t, tc.wantRune, gotRune)
			require.Equal(t, tc.wantWidth, gotWidth)
			require.Equal(t, tc.wantOK, gotOK)
		})
	}
}
