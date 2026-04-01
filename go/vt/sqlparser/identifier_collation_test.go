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
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestIdentEqual verifies that identEqual matches MySQL's identifier comparison
// behavior: case-insensitive but accent-sensitive.
func TestIdentEqual(t *testing.T) {
	tests := []struct {
		a, b  string
		equal bool
	}{
		// Case-insensitive
		{"hello", "HELLO", true},
		{"Hello", "hello", true},
		{"abc", "ABC", true},
		{"z", "Z", true},
		{"A", "a", true},

		// Accent-sensitive: accented chars are NOT equal to base chars
		{"café", "cafe", false},
		{"café", "CAFE", false},
		{"résumé", "resume", false},
		{"naïve", "naive", false},
		{"über", "uber", false},
		{"piñata", "pinata", false},
		{"Ä", "A", false},

		// Accented chars ARE equal to their case variants
		{"café", "Café", true},
		{"café", "CAFÉ", true},
		{"résumé", "RÉSUMÉ", true},
		{"über", "ÜBER", true},
		{"Ä", "ä", true},
		{"É", "é", true},
		{"Ñ", "ñ", true},
		{"Ü", "ü", true},

		// Different strings
		{"hello", "world", false},
		{"abc", "abd", false},
		{"a", "b", false},

		// Empty
		{"", "", true},
		{"a", "", false},
		{"", "a", false},

		// Greek case folding
		{"ω", "Ω", true},

		// Cases where MySQL's unicase ToLower differs from Go's strings.ToLower:
		// U+0130 İ: MySQL lowercases to i, Go keeps as İ. The old strings.ToLower
		// behavior would incorrectly treat İ and i as different identifiers.
		{"İ", "i", true},
		// U+0220 Ƞ: MySQL has no lowercase mapping (identity), Go lowercases to ƞ.
		// The old strings.ToLower behavior would incorrectly treat Ƞ and ƞ as equal.
		{"Ƞ", "ƞ", false},

		// Longer strings
		{"thé café", "THÉ CAFÉ", true},
		{"thé café", "the cafe", false},
	}

	for _, tt := range tests {
		t.Run(tt.a+"_vs_"+tt.b, func(t *testing.T) {
			got := identEqual(tt.a, tt.b)
			assert.Equal(t, tt.equal, got,
				"identEqual(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.equal)
		})
	}
}

// TestIdentNormalize verifies normalization properties.
func TestIdentNormalize(t *testing.T) {
	// Case-insensitive: different cases produce the same normalized form.
	assert.Equal(t, identNormalize("hello"), identNormalize("HELLO"))
	assert.Equal(t, identNormalize("café"), identNormalize("CAFÉ"))
	assert.Equal(t, identNormalize("Café"), identNormalize("café"))

	// Accent-sensitive: accented and base chars produce different normalized forms.
	assert.NotEqual(t, identNormalize("café"), identNormalize("cafe"))
	assert.NotEqual(t, identNormalize("Ä"), identNormalize("A"))
	assert.NotEqual(t, identNormalize("é"), identNormalize("e"))

	// Already lowercase ASCII returns the original string (zero allocation).
	s := "my_table_name"
	norm := identNormalize(s)
	assert.Equal(t, s, norm)
}

// TestNormalizeConsistentWithEqual verifies that for all BMP codepoints,
// identNormalize(a) == identNormalize(b) iff identEqual(a, b).
func TestNormalizeConsistentWithEqual(t *testing.T) {
	type entry struct {
		cp   rune
		norm string
	}
	seen := make(map[string]entry)

	var inconsistencies int
	for cp := rune(1); cp <= 0xFFFF; cp++ {
		if cp >= 0xD800 && cp <= 0xDFFF {
			continue
		}
		s := string(cp)
		norm := identNormalize(s)

		if prev, ok := seen[norm]; ok {
			// Same normalization — they should be identEqual.
			if !identEqual(string(prev.cp), s) {
				if inconsistencies < 10 {
					t.Errorf("U+%04X and U+%04X normalize to same %q but identEqual returns false",
						prev.cp, cp, norm)
				}
				inconsistencies++
			}
		} else {
			seen[norm] = entry{cp: cp, norm: norm}
		}
	}
	if inconsistencies > 10 {
		t.Errorf("... and %d more inconsistencies", inconsistencies-10)
	}
	assert.Zero(t, inconsistencies)
}

// TestIdentSanitizeInvalidUTF8 verifies that identSanitize (called by
// NewIdentifierCI) matches MySQL's identifier sanitization: invalid bytes
// become '?' and trailing truncated sequences are dropped.
func TestIdentSanitizeInvalidUTF8(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Lone continuation bytes → '?'
		{"single invalid byte", string([]byte{0x80}), "?"},
		{"multiple invalid bytes", string([]byte{0x80, 0x81, 0x82}), "???"},
		{"ASCII then invalid", string([]byte{'h', 'e', 'l', 'l', 'o', 0x80}), "hello?"},
		{"invalid then ASCII", string([]byte{0x80, 'h', 'e', 'l', 'l', 'o'}), "?hello"},
		{"mixed valid and invalid", string([]byte{'c', 'a', 'f', 0xC3, 0xA9, 0x80, 0x81}), "caf\xc3\xa9??"},
		// Invalid bytes mid-string → each byte becomes '?'
		{"invalid mid then ASCII", string([]byte{0xE4, 0x80, 'e', 'n', 'd'}), "??end"},
		// Trailing truncated sequences → silently dropped (matching MySQL)
		{"truncated 2-byte at EOF", string([]byte{0xC3}), ""},
		{"truncated 3-byte at EOF", string([]byte{0xE4, 0x80}), ""},
		{"truncated 4-byte at EOF", string([]byte{0xF0, 0x80, 0x80}), ""},
		{"ASCII then truncated at EOF", string([]byte{'c', 'o', 'l', 0xE4, 0x80}), "col"},
		// Invalid lead bytes C0/C1 → '?' (not truncated, just invalid)
		{"C0 at EOF", string([]byte{0xC0}), "?"},
		{"C0 80 overlong", string([]byte{0xC0, 0x80}), "??"},
		// F5-FF at EOF are dropped (MySQL treats them like truncated leads)
		{"0xFF at EOF", string([]byte{0xFF}), ""},
		{"0xFF 0xFF 0xFF at EOF", string([]byte{0xFF, 0xFF, 0xFF}), ""},
		// F5-FF mid-string → '?'
		{"0xFF mid-string", string([]byte{0xFF, 'e', 'n', 'd'}), "?end"},
		// Valid UTF-8 passes through unchanged
		{"valid ASCII", "hello", "hello"},
		{"valid Unicode", "café", "café"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := NewIdentifierCI(tt.input)
			assert.Equal(t, tt.expected, id.String(),
				"NewIdentifierCI(%q).String()", tt.input)
		})
	}
}

// TestIdentEqualAfterSanitize verifies that identifiers with invalid UTF-8
// compare correctly after sanitization.
func TestIdentEqualAfterSanitize(t *testing.T) {
	// Different invalid bytes both become '?' → equal after sanitization.
	a := NewIdentifierCI(string([]byte{'x', 0x80}))
	b := NewIdentifierCI(string([]byte{'x', 0x81}))
	assert.True(t, a.Equal(b),
		"different invalid bytes should be equal after sanitization to '?'")

	// Invalid byte becomes '?', should equal literal '?'.
	c := NewIdentifierCI("x?")
	assert.True(t, a.Equal(c))

	// Valid U+FFFD should NOT equal '?' — it's a real character.
	d := NewIdentifierCI("x\uFFFD")
	assert.False(t, c.Equal(d),
		"literal '?' and valid U+FFFD should not be equal")
}

func BenchmarkIdentNormalize(b *testing.B) {
	b.Run("ASCII_lower/identNormalize", func(b *testing.B) {
		for b.Loop() {
			identNormalize("my_table_name")
		}
	})
	b.Run("ASCII_lower/strings.ToLower", func(b *testing.B) {
		for b.Loop() {
			strings.ToLower("my_table_name")
		}
	})
	b.Run("ASCII_upper/identNormalize", func(b *testing.B) {
		for b.Loop() {
			identNormalize("MY_TABLE_NAME")
		}
	})
	b.Run("ASCII_upper/strings.ToLower", func(b *testing.B) {
		for b.Loop() {
			strings.ToLower("MY_TABLE_NAME")
		}
	})
	b.Run("Unicode/identNormalize", func(b *testing.B) {
		for b.Loop() {
			identNormalize("café_résumé")
		}
	})
	b.Run("Unicode/strings.ToLower", func(b *testing.B) {
		for b.Loop() {
			strings.ToLower("café_résumé")
		}
	})
}

func BenchmarkNewIdentifierCI(b *testing.B) {
	b.Run("ASCII_lower", func(b *testing.B) {
		for b.Loop() {
			NewIdentifierCI("my_table_name")
		}
	})
	b.Run("ASCII_upper", func(b *testing.B) {
		for b.Loop() {
			NewIdentifierCI("MY_TABLE_NAME")
		}
	})
	b.Run("Unicode", func(b *testing.B) {
		for b.Loop() {
			NewIdentifierCI("café_résumé")
		}
	})
}

func BenchmarkIdentEqual(b *testing.B) {
	b.Run("ASCII_equal", func(b *testing.B) {
		for b.Loop() {
			identEqual("my_table_name", "MY_TABLE_NAME")
		}
	})
	b.Run("ASCII_unequal", func(b *testing.B) {
		for b.Loop() {
			identEqual("my_table_name", "other_table")
		}
	})
	b.Run("Unicode_equal", func(b *testing.B) {
		for b.Loop() {
			identEqual("café_résumé", "CAFÉ_RÉSUMÉ")
		}
	})
}
