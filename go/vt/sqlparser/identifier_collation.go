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
// utf8mb3_general_ci unicase ToLower tables. Invalid UTF-8 bytes are
// treated as '?' (matching MySQL's charset sanitization).
func identEqual(a, b string) bool {
	for len(a) > 0 && len(b) > 0 {
		var aRune, bRune rune
		if a[0] < utf8.RuneSelf {
			aRune, a = rune(a[0]), a[1:]
		} else {
			r, size := utf8.DecodeRuneInString(a)
			if r == utf8.RuneError && size == 1 {
				aRune = '?'
			} else {
				aRune = r
			}
			a = a[size:]
		}
		if b[0] < utf8.RuneSelf {
			bRune, b = rune(b[0]), b[1:]
		} else {
			r, size := utf8.DecodeRuneInString(b)
			if r == utf8.RuneError && size == 1 {
				bRune = '?'
			} else {
				bRune = r
			}
			b = b[size:]
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
// ToLower tables. Invalid UTF-8 bytes are replaced with '?' to match
// MySQL's charset sanitization behavior. Trailing truncated UTF-8 sequences
// are silently dropped (also matching MySQL). Returns the number of bytes
// written to dst.
func identNormalizeUnicode(dst []byte, src string) int {
	// Strip any trailing truncated UTF-8 sequence before processing.
	// MySQL silently drops incomplete sequences at the end of identifiers.
	src = trimTrailingTruncatedUTF8(src)

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
			dst[pos] = '?'
			pos++
			i++
			continue
		}
		pos += utf8.EncodeRune(dst[pos:], unicase.ToLowerRune(r))
		i += size
	}
	return pos
}

// trimTrailingTruncatedUTF8 removes a trailing incomplete UTF-8 sequence
// from s. A truncated sequence is a valid multi-byte lead byte (C2-F4)
// followed by zero or more continuation bytes (80-BF) at the end of the
// string, where the total byte count is less than what the lead byte
// requires. MySQL silently drops these from identifier names.
func trimTrailingTruncatedUTF8(s string) string {
	// Scan backwards past continuation bytes (10xxxxxx).
	end := len(s)
	for end > 0 && s[end-1]&0xC0 == 0x80 {
		end--
	}
	if end == 0 {
		// The string is all continuation bytes — those become '?'
		// individually, not a truncated sequence.
		return s
	}
	lead := s[end-1]
	var expectedLen int
	switch {
	case lead >= 0xC2 && lead <= 0xDF: // C2-DF: valid 2-byte lead
		expectedLen = 2
	case lead&0xF0 == 0xE0: // E0-EF: 3-byte sequence
		expectedLen = 3
	case lead >= 0xF0 && lead <= 0xF4: // F0-F4: valid 4-byte lead
		expectedLen = 4
	default:
		// Not a valid lead byte (C0/C1 are overlong, F5+ are invalid,
		// 00-7F are ASCII). These become '?', not truncated.
		return s
	}
	actualLen := len(s) - end + 1 // lead byte + trailing continuations
	if actualLen < expectedLen {
		// Truncated: strip the incomplete sequence.
		return s[:end-1]
	}
	return s
}
