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
