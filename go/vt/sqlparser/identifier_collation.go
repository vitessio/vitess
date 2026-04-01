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

// identSanitize sanitizes a SQL identifier string to match MySQL's behavior
// when processing identifiers with invalid UTF-8:
//   - Invalid bytes (lone continuations, overlong encodings, surrogates) → '?'
//   - Trailing truncated sequences (valid lead + insufficient continuations at EOF) → dropped
//
// For valid UTF-8 input (the common case), returns the string unchanged with
// zero allocation.
func identSanitize(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return identSanitizeSlow(s, i)
		}
	}
	return s
}

// identSanitizeSlow handles the non-ASCII path for identSanitize. The caller
// has verified that s[:firstNonASCII] is clean ASCII.
func identSanitizeSlow(s string, firstNonASCII int) string {
	// Check if the string is valid UTF-8 from the first non-ASCII byte.
	if utf8.ValidString(s[firstNonASCII:]) {
		return s
	}

	// Has invalid bytes — rebuild with '?' replacements, dropping any
	// trailing truncated sequence (matching MySQL's behavior).
	b := make([]byte, len(s))
	copy(b[:firstNonASCII], s[:firstNonASCII])
	pos := firstNonASCII
	i := firstNonASCII
	for i < len(s) {
		if s[i] < utf8.RuneSelf {
			b[pos] = s[i]
			pos++
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			// If this invalid byte and everything after it form a
			// truncated sequence at EOF, drop them (matching MySQL).
			if isTrailingTruncatedUTF8(s[i:]) {
				break
			}
			b[pos] = '?'
			pos++
			i++
			continue
		}
		copy(b[pos:], s[i:i+size])
		pos += size
		i += size
	}
	return string(b[:pos])
}

// isTrailingTruncatedUTF8 reports whether s is a truncated UTF-8 sequence:
// a valid multi-byte lead byte (C2-F4) followed by zero or more continuation
// bytes (80-BF), but fewer than the lead byte requires. MySQL silently drops
// these when they appear at the end of an identifier.
func isTrailingTruncatedUTF8(s string) bool {
	if len(s) == 0 {
		return false
	}
	lead := s[0]
	var expectedLen int
	switch {
	case lead >= 0xC2 && lead <= 0xDF:
		expectedLen = 2
	case lead&0xF0 == 0xE0:
		expectedLen = 3
	case lead >= 0xF0 && lead <= 0xF4:
		expectedLen = 4
	default:
		return false
	}
	if len(s) >= expectedLen {
		return false // not truncated, enough bytes present
	}
	// Check all following bytes are continuations.
	for j := 1; j < len(s); j++ {
		if s[j]&0xC0 != 0x80 {
			return false
		}
	}
	return true
}

// identEqual reports whether a and b are equal under MySQL's identifier
// comparison rules: case-insensitive but accent-sensitive, using the
// utf8mb3_general_ci unicase ToLower tables. Inputs are expected to be
// sanitized (valid UTF-8) via identSanitize.
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
// tables, producing case-insensitive but accent-sensitive results. Input
// is expected to be sanitized (valid UTF-8) via identSanitize.
func identNormalize(s string) string {
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
	return s
}

// identNormalizeASCII lowercases ASCII bytes from src into dst. Falls through
// to identNormalizeUnicode if a non-ASCII byte is encountered.
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

// identNormalizeUnicode lowercases runes using the unicase ToLower tables.
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
		pos += utf8.EncodeRune(dst[pos:], unicase.ToLowerRune(r))
		i += size
	}
	return pos
}
