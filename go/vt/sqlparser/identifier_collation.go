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

	"vitess.io/vitess/go/mysql/collations/unicase"
)

// identCollate compares two strings using utf8mb4_general_ci semantics.
// Returns a negative value if a < b, 0 if equal, a positive value if a > b.
func identCollate(a, b string) int {
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

		aWeight := unicase.SortWeight(aRune)
		bWeight := unicase.SortWeight(bRune)
		if aWeight != bWeight {
			if aWeight < bWeight {
				return -1
			}
			return 1
		}
	}
	return len(a) - len(b)
}

// identNormalize returns the utf8mb4_general_ci normalized form of a string.
// Each rune is mapped to the canonical lowercase representative of its
// equivalence class under the collation. For ASCII-only input, this produces
// the same result as strings.ToLower.
//
// The normalizedRune table is defined in identifier_normalized.go.
func identNormalize(s string) string {
	// Fast path: all ASCII.
	isASCII := true
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			isASCII = false
			break
		}
	}
	if isASCII {
		return strings.ToLower(s)
	}

	// Slow path: look up each rune's precomputed normalized form.
	var buf strings.Builder
	buf.Grow(len(s))
	for _, r := range s {
		if r <= 0xFFFF {
			buf.WriteRune(normalizedRune[r])
		} else {
			buf.WriteRune(r)
		}
	}
	return buf.String()
}
