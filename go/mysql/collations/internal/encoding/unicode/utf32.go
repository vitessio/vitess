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

import "golang.org/x/text/encoding/unicode/utf32"

var defaultUTF32 = utf32.UTF32(utf32.BigEndian, utf32.IgnoreBOM)

type Encoding_utf32 struct{}

func (Encoding_utf32) Name() string {
	return "utf32"
}

func (Encoding_utf32) DecodeRune(p []byte) (rune, int) {
	if len(p) < 4 {
		return RuneError, 0
	}
	return (rune(p[0]) << 24) | (rune(p[1]) << 16) | (rune(p[2]) << 8) | rune(p[3]), 4
}

func (Encoding_utf32) SupportsSupplementaryChars() bool {
	return true
}

func (Encoding_utf32) EncodeFromUTF8(in []byte) ([]byte, error) {
	return defaultUTF32.NewEncoder().Bytes(in)
}
