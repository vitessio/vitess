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

package encoding

import (
	"vitess.io/vitess/go/mysql/collations/internal/encoding/simplifiedchinese"
	"vitess.io/vitess/go/mysql/collations/internal/encoding/unicode"
)

type Encoding interface {
	Name() string
	SupportsSupplementaryChars() bool
	DecodeRune([]byte) (rune, int)
	EncodeFromUTF8(in []byte) ([]byte, error)
}

type Encoding_utf8 = unicode.Encoding_utf8
type Encoding_utf8mb4 = unicode.Encoding_utf8mb4
type Encoding_utf16 = unicode.Encoding_utf16
type Encoding_ucs2 = unicode.Encoding_ucs2
type Encoding_utf32 = unicode.Encoding_utf32
type Encoding_gb18030 = simplifiedchinese.Encoding_gb18030

const RuneError = unicode.RuneError

type EncodingAware interface {
	Encoding() Encoding
}
