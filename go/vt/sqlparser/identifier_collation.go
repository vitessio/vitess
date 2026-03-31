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
	"strings"
	"unicode/utf8"
)

// identNormalizedRune returns the utf8mb4_general_ci normalized form of a
// single rune using the sparse page table in identifier_normalized.go.
// Codepoints in pages where every entry is identity (no case/accent folding)
// are returned as-is.
func identNormalizedRune(r rune) rune {
	if r <= 0xFFFF {
		if page := normalizedPages[uint16(r)>>8]; page != nil {
			return page[uint16(r)&0xFF]
		}
	}
	return r
}

// identEqual reports whether a and b are equal under utf8mb4_general_ci.
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

		if identNormalizedRune(aRune) != identNormalizedRune(bRune) {
			return false
		}
	}
	return len(a) == len(b)
}

// identNormalize returns the utf8mb4_general_ci normalized form of a string.
// Each rune is mapped to the canonical lowercase representative of its
// equivalence class under the collation. For ASCII-only input, this produces
// the same result as strings.ToLower.
func identNormalize(s string) string {
	// Single pass: check if ASCII and already normalized simultaneously.
	isASCII, isNormalized := true, true
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= utf8.RuneSelf {
			isASCII = false
			break
		}
		isNormalized = isNormalized && !('A' <= c && c <= 'Z')
	}

	if isASCII {
		if isNormalized {
			return s
		}
		return strings.ToLower(s)
	}

	// Slow path: look up each rune's precomputed normalized form.
	var buf strings.Builder
	buf.Grow(len(s))
	for _, r := range s {
		buf.WriteRune(identNormalizedRune(r))
	}
	return buf.String()
}
