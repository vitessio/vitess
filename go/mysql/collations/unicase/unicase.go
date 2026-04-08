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

// Package unicase contains the Unicode case mapping and sort weight tables
// for the utf8mb4_general_ci collation family. These tables are shared
// between the collation implementation (go/mysql/collations/colldata) and
// the SQL parser's identifier normalization (go/vt/sqlparser).
package unicase

import "unicode/utf8"

// Char holds the Unicode case mapping and sort weight for a single
// codepoint under the general_ci collation family.
type Char struct {
	ToUpper, ToLower, Sort rune
}

const maxChar = 0xFFFF

// ToLowerRune returns the lowercase form of a codepoint under the
// utf8mb4_general_ci unicase tables. This is used for MySQL identifier
// comparison, which is case-insensitive but accent-sensitive.
func ToLowerRune(codepoint rune) rune {
	if codepoint > maxChar || (codepoint >= 0xD800 && codepoint <= 0xDFFF) {
		return utf8.RuneError
	}
	if page := pages[codepoint>>8]; page != nil {
		return (*page)[codepoint&0xFF].ToLower
	}
	return codepoint
}

// SortWeight returns the sort weight for a codepoint under the
// utf8mb4_general_ci collation. Codepoints outside the Basic
// Multilingual Plane (> U+FFFF) and UTF-16 surrogates (U+D800..U+DFFF)
// return utf8.RuneError.
func SortWeight(codepoint rune) rune {
	if codepoint > maxChar || (codepoint >= 0xD800 && codepoint <= 0xDFFF) {
		return utf8.RuneError
	}
	if page := pages[codepoint>>8]; page != nil {
		return (*page)[codepoint&0xFF].Sort
	}
	return codepoint
}
