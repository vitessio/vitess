/*
Copyright 2022 The Vitess Authors.

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

func Slice(charset Charset, input []byte, from, to int) []byte {
	if charset, ok := charset.(interface{ Slice([]byte, int, int) []byte }); ok {
		return charset.Slice(input, from, to)
	}
	iter := input
	start := 0
	for i := range to {
		_, size, ok := charset.DecodeRune(iter)
		if !ok {
			break
		}
		if i < from {
			start += size
		}
		iter = iter[size:]
	}
	return input[start : len(input)-len(iter)]
}

// IsASCIIIdentical reports whether text of the characters of ASCII has the
// bytes of ASCII in charset, and those bytes are that text: each of the bytes
// 0x00 to 0x7F is the character of that code point, and the other way round.
// Text that is all such bytes then needs no conversion from or to charset.
//
// It is false for ucs2, utf16, utf16le and utf32, where a character of ASCII
// has more than one byte, for swe7, which has Ä at the byte of "[" and eight
// more letters in place of ASCII punctuation, and for sjis, where MySQL
// writes a backslash as 81 5F.
func IsASCIIIdentical(charset Charset) bool {
	var buf [4]byte
	for b := range byte(0x80) {
		r, width, ok := charset.DecodeRune([]byte{b})
		if !ok || width != 1 || r != rune(b) || charset.EncodeRune(buf[:], r) != 1 || buf[0] != b {
			return false
		}
	}
	return true
}

func Validate(charset Charset, input []byte) bool {
	if charset, ok := charset.(interface{ Validate([]byte) bool }); ok {
		return charset.Validate(input)
	}
	for len(input) > 0 {
		_, size, ok := charset.DecodeRune(input)
		if !ok {
			return false
		}
		input = input[size:]
	}
	return true
}

func Length(charset Charset, input []byte) int {
	if charset, ok := charset.(interface{ Length([]byte) int }); ok {
		return charset.Length(input)
	}
	var count int
	for len(input) > 0 {
		_, size, _ := charset.DecodeRune(input)
		input = input[size:]
		count++
	}
	return count
}
