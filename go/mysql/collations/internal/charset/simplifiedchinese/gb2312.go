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

package simplifiedchinese

import (
	"unicode/utf8"

	"golang.org/x/text/encoding/simplifiedchinese"
)

type Charset_gb2312 struct{}

func (Charset_gb2312) Name() string {
	return "gb2312"
}

func (Charset_gb2312) SupportsSupplementaryChars() bool {
	return false
}

func (Charset_gb2312) DecodeRune(in []byte) (rune, int) {
	if len(in) < 1 {
		return utf8.RuneError, 0
	}
	if in[0] >= 0xa1 && in[0] <= 0xf7 {
		if len(in) >= 2 && in[1] >= 0xa1 && in[1] <= 0xfe {
			return 0, 2
		}
		return utf8.RuneError, 1
	}
	return rune(in[0]), 1
}

func (Charset_gb2312) EncodeFromUTF8(in []byte) ([]byte, error) {
	return simplifiedchinese.HZGB2312.NewEncoder().Bytes(in)
}
