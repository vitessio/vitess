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

package sqlparser

import (
	"unicode/utf8"

	"vitess.io/vitess/go/mysql/collations/unicase"
)

// identEqual reports whether a and b are equal under MySQL's identifier
// comparison rules: case-insensitive but accent-sensitive, using the
// utf8mb3_general_ci unicase ToLower tables.
func identEqual(a, b string) bool {
	for len(a) > 0 && len(b) > 0 {
		var aRune, bRune rune
		if a[0] < utf8.RuneSelf {
			aRune, a = rune(a[0]), a[1:]
		} else {
			r, size := utf8.DecodeRuneInString(a)
			aRune, a = r, a[size:]
		}
		if b[0] < utf8.RuneSelf {
			bRune, b = rune(b[0]), b[1:]
		} else {
			r, size := utf8.DecodeRuneInString(b)
			bRune, b = r, b[size:]
		}

		if unicase.ToLowerRune(aRune) != unicase.ToLowerRune(bRune) {
			return false
		}
	}
	return len(a) == len(b)
}

// identNormalize returns the normalized form of s for MySQL identifier
// comparison. Each rune is lowercased using the utf8mb3_general_ci unicase
// tables, producing case-insensitive but accent-sensitive results.
func identNormalize(s string) string {
	// Single pass: scan for the first byte that needs transformation.
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= utf8.RuneSelf {
			b := make([]byte, len(s))
			copy(b[:i], s[:i])
			n := identNormalizeUnicode(b[i:], s[i:])
			return string(b[:i+n])
		}
		if 'A' <= c && c <= 'Z' {
			b := make([]byte, len(s))
			copy(b[:i], s[:i])
			n := identNormalizeASCII(b[i:], s[i:])
			return string(b[:i+n])
		}
	}
	// All ASCII, already lowercase — return as-is, zero allocation.
	return s
}

// identNormalizeASCII lowercases ASCII bytes from src into dst. If a non-ASCII
// byte is encountered, it falls through to identNormalizeUnicode for the
// remainder. Returns the number of bytes written to dst.
func identNormalizeASCII(dst []byte, src string) int {
	for i := 0; i < len(src); i++ {
		c := src[i]
		if c >= utf8.RuneSelf {
			return i + identNormalizeUnicode(dst[i:], src[i:])
		}
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		dst[i] = c
	}
	return len(src)
}

// identNormalizeUnicode normalizes runes from src into dst using the unicase
// ToLower tables. Invalid UTF-8 bytes are copied through verbatim to
// guarantee the output is never longer than the input. Returns the number
// of bytes written to dst.
func identNormalizeUnicode(dst []byte, src string) int {
	var pos, i int
	for i < len(src) {
		if src[i] < utf8.RuneSelf {
			c := src[i]
			if 'A' <= c && c <= 'Z' {
				c += 'a' - 'A'
			}
			dst[pos] = c
			pos++
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(src[i:])
		if r == utf8.RuneError && size == 1 {
			dst[pos] = src[i]
			pos++
			i++
			continue
		}
		pos += utf8.EncodeRune(dst[pos:], unicase.ToLowerRune(r))
		i += size
	}
	return pos
}
