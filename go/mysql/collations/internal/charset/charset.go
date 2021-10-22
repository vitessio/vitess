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
	"vitess.io/vitess/go/mysql/collations/internal/charset/simplifiedchinese"
	"vitess.io/vitess/go/mysql/collations/internal/charset/unicode"
)

type Charset interface {
	Name() string
	SupportsSupplementaryChars() bool
	DecodeRune([]byte) (rune, int)
	EncodeFromUTF8(in []byte) ([]byte, error)
}

type Charset_utf8 = unicode.Charset_utf8mb3
type Charset_utf8mb4 = unicode.Charset_utf8mb4
type Charset_utf16 = unicode.Charset_utf16be
type Charset_utf16le = unicode.Charset_utf16le
type Charset_ucs2 = unicode.Charset_ucs2
type Charset_utf32 = unicode.Charset_utf32
type Charset_gb18030 = simplifiedchinese.Charset_gb18030

const RuneError = unicode.RuneError

type CharsetAware interface {
	Charset() Charset
}

func IsUnicode(csname string) bool {
	switch csname {
	case "utf8", "utf8mb4", "utf16", "utf16le", "ucs2", "utf32":
		return true
	default:
		return false
	}
}
