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
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/charset/eightbit"
	"vitess.io/vitess/go/mysql/collations/charset/japanese"
	"vitess.io/vitess/go/mysql/collations/charset/korean"
	"vitess.io/vitess/go/mysql/collations/charset/simplifiedchinese"
	"vitess.io/vitess/go/mysql/collations/charset/types"
	"vitess.io/vitess/go/mysql/collations/charset/unicode"
)

const RuneError = utf8.RuneError

type Charset = types.Charset

// 8-bit encodings

type Charset_8bit = eightbit.Charset_8bit
type Charset_binary = eightbit.Charset_binary
type Charset_latin1 = eightbit.Charset_latin1
type UnicodeMapping = eightbit.UnicodeMapping

// Unicode encodings

type Charset_utf8mb3 = unicode.Charset_utf8mb3
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
type Charset_cp932 = japanese.Charset_cp932
type Charset_eucjpms = japanese.Charset_eucjpms

// Korean encodings

type Charset_euckr = korean.Charset_euckr

func IsMultibyteByName(csname string) bool {
	switch csname {
	case "euckr", "gb2312", "sjis", "cp932", "eucjpms", "ujis":
		return true

	default:
		return false
	}
}

func IsUnicode(charset Charset) bool {
	switch charset.(type) {
	case Charset_utf8mb3, Charset_utf8mb4:
		return true
	case Charset_utf16, Charset_utf16le, Charset_ucs2:
		return true
	case Charset_utf32:
		return true
	default:
		return false
	}
}

func IsUnicodeByName(csname string) bool {
	switch csname {
	case "utf8", "utf8mb3", "utf8mb4", "utf16", "utf16le", "ucs2", "utf32":
		return true
	default:
		return false
	}
}

func IsBackslashSafe(charset Charset) bool {
	switch charset.(type) {
	case Charset_sjis, Charset_cp932, Charset_gb18030 /*, Charset_gbk, Charset_big5 */ :
		return false
	default:
		return true
	}
}
