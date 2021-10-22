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
	"vitess.io/vitess/go/mysql/collations/internal/charset/japanese"
	"vitess.io/vitess/go/mysql/collations/internal/charset/korean"
	"vitess.io/vitess/go/mysql/collations/internal/charset/simplifiedchinese"
	"vitess.io/vitess/go/mysql/collations/internal/charset/unicode"
)

type Charset interface {
	Name() string
	SupportsSupplementaryChars() bool
	DecodeRune([]byte) (rune, int)
	EncodeFromUTF8(in []byte) ([]byte, error)
}

// Unicode encodings

type Charset_utf8 = unicode.Charset_utf8mb3
type Charset_utf8mb4 = unicode.Charset_utf8mb4
type Charset_utf16 = unicode.Charset_utf16be
type Charset_utf16le = unicode.Charset_utf16le
type Charset_ucs2 = unicode.Charset_ucs2
type Charset_utf32 = unicode.Charset_utf32

// Simplified Chinese encodings

type Charset_gb18030 = simplifiedchinese.Charset_gb18030
type Charset_gb2312 = simplifiedchinese.Charset_gb2312

// Japanese encodings

type Charset_ujis = japanese.Charset_ujis
type Charset_sjis = japanese.Charset_sjis
type Charset_cp932 = japanese.Charset_sjis // TODO: this is not correct, see https://en.wikipedia.org/wiki/Code_page_932_(Microsoft_Windows)#Differences_from_standard_Shift_JIS

// Korean encodings

type Charset_euckr = korean.Charset_euckr

const RuneError = unicode.RuneError

func IsUnicode(csname string) bool {
	switch csname {
	case "utf8", "utf8mb4", "utf16", "utf16le", "ucs2", "utf32":
		return true
	default:
		return false
	}
}

func IsMultibyte(csname string) bool {
	switch csname {
	case "cp932", "euckr", "gb2312", "sjis", "ujis":
		return true
	case "eucjpms":
		// TODO: These multibyte encodings are not supported yet
		return false
	default:
		return false
	}
}
