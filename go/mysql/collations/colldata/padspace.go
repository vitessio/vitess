/*
Copyright 2026 The Vitess Authors.

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

package colldata

import (
	"bytes"

	"vitess.io/vitess/go/mysql/collations/charset"
)

// MySQL's collations pad with spaces (PAD SPACE), except binary and the
// utf8mb4 UCA 9.0.0 collations (NO PAD): they compare two strings as if the
// shorter one ended in as many spaces as the longer one needs. 'a' and 'a '
// are equal, and 'a\t' is less than 'a', because a tab weighs less than a
// space. The PAD SPACE collations here compare the characters of the longer
// string that the shorter one does not have with a space, and leave out a
// string's trailing characters that weigh as a space when they hash it, so
// that two strings that compare as equal hash equally. A weight string keeps
// those characters, as WEIGHT_STRING does in MySQL.
//
// A prefix comparison, which LOCATE uses, does not pad.

// collationBinaryPadSpace compares left and right byte by byte, as the binary
// collation of an 8-bit or multibyte character set does, and compares the
// bytes of the longer string that the shorter one does not have with a space.
// Every byte of a multibyte character is above a space, as is every byte of a
// UTF-8 character of more than one byte.
func collationBinaryPadSpace(left, right []byte, rightIsPrefix bool) int {
	if rightIsPrefix {
		return collationBinary(left, right, true)
	}
	minLen := min(len(left), len(right))
	if diff := bytes.Compare(left[:minLen], right[:minLen]); diff != 0 {
		return diff
	}
	if len(left) > minLen {
		return compareBytesWithSpace(left[minLen:])
	}
	return -compareBytesWithSpace(right[minLen:])
}

// compareBytesWithSpace returns how rest compares with as many spaces as it
// has bytes.
func compareBytesWithSpace(rest []byte) int {
	for _, c := range rest {
		if c != ' ' {
			return int(c) - ' '
		}
	}
	return 0
}

// compareRunesWithSpace returns how rest, characters in cs, compares with as
// many spaces as it has characters, by the weight that weight gives each
// character. A byte that starts no valid character is above a space.
func compareRunesWithSpace(cs charset.Charset, rest []byte, weight func(rune) rune) int {
	space := weight(' ')
	for len(rest) > 0 {
		r, width, ok := cs.DecodeRune(rest)
		if !ok {
			return 1
		}
		if w := weight(r); w != space {
			return int(w) - int(space)
		}
		rest = rest[width:]
	}
	return 0
}

// trailingSortSpaces returns a function that counts the bytes at the end of a
// string that weigh as a space by sort, a collation's weight for each byte.
func trailingSortSpaces(sort *[256]byte) func([]byte) int {
	space := sort[' ']
	return func(s []byte) int {
		n := 0
		for i := len(s) - 1; i >= 0 && sort[s[i]] == space; i-- {
			n++
		}
		return n
	}
}

// trailingMultibyteSpaces returns a function that counts the bytes at the end
// of a string that weigh as a space in a multibyte collation with sort, its
// weights of the ASCII bytes, or with no weights, which compares bytes. A byte
// of a multibyte character never weighs as a space.
func trailingMultibyteSpaces(sort *[256]byte) func([]byte) int {
	if sort == nil {
		sort = &sortOrderIdentity
	}
	space := sort[' ']
	return func(s []byte) int {
		n := 0
		for i := len(s) - 1; i >= 0 && s[i] <= 127 && sort[s[i]] == space; i-- {
			n++
		}
		return n
	}
}

// trailingCharsetSpaces returns a function that counts the spaces at the end
// of a string in cs, in a collation in which only a space weighs as one, as in
// MySQL's general and binary Unicode collations.
func trailingCharsetSpaces(cs charset.Charset) func([]byte) int {
	return func(s []byte) int {
		trimmed := trimTrailingSpaces(cs, s)
		return charset.Length(cs, s[len(trimmed):])
	}
}

// trimTrailingSpaces returns s, a string in cs, without its trailing spaces.
// The last bytes of a string that are a space in cs are always a space: in
// UTF-8 and the 8-bit and multibyte character sets a byte 0x20 is always a
// space, and a character of UTF-16 or UTF-32 that ends in a space's bytes is
// one.
func trimTrailingSpaces(cs charset.Charset, s []byte) []byte {
	var buf [4]byte
	width := cs.EncodeRune(buf[:], ' ')
	if width == 1 {
		return bytes.TrimRight(s, " ")
	}
	space := buf[:width]
	for bytes.HasSuffix(s, space) {
		s = s[:len(s)-width]
	}
	return s
}
