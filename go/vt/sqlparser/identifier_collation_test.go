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
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/colldata"
)

// TestIdentEqualMatchesColldata verifies that identEqual produces the same
// equality results as the canonical utf8mb4_general_ci collation.
func TestIdentEqualMatchesColldata(t *testing.T) {
	coll := colldata.Lookup(collations.ID(45) /* utf8mb4_general_ci */)
	require.NotNil(t, coll, "utf8mb4_general_ci collation not found")

	pairs := [][2]string{
		// ASCII case
		{"hello", "HELLO"},
		{"Hello", "hello"},
		{"abc", "ABC"},
		{"z", "Z"},
		// Accented characters
		{"café", "CAFE"},
		{"café", "cafe"},
		{"résumé", "RESUME"},
		{"naïve", "NAIVE"},
		{"über", "UBER"},
		{"piñata", "PINATA"},
		// Different strings
		{"hello", "world"},
		{"abc", "abd"},
		{"a", "b"},
		// Empty
		{"", ""},
		{"a", ""},
		{"", "a"},
		// Single characters
		{"A", "a"},
		{"É", "e"},
		{"é", "E"},
		{"Ñ", "n"},
		{"ñ", "N"},
		{"Ü", "u"},
		{"ü", "U"},
		// Extended Latin
		{"Ā", "a"},
		{"ā", "A"},
		{"Ç", "c"},
		{"ç", "C"},
		// Longer strings
		{"thé café résumé", "THE CAFE RESUME"},
	}

	for _, pair := range pairs {
		a, b := pair[0], pair[1]
		expected := coll.Collate([]byte(a), []byte(b), false) == 0
		got := identEqual(a, b)

		assert.Equal(t, expected, got,
			"identEqual(%q, %q) = %v, want %v", a, b, got, expected)
	}
}

// TestNormalizeMatchesColldata verifies that identNormalize produces the same
// output for two strings iff the canonical utf8mb4_general_ci collation
// considers them equal, for all BMP codepoints.
func TestNormalizeMatchesColldata(t *testing.T) {
	coll := colldata.Lookup(collations.ID(45) /* utf8mb4_general_ci */)
	require.NotNil(t, coll, "utf8mb4_general_ci collation not found")

	// For each codepoint, compute its weight via colldata's WeightString.
	// Then verify that two codepoints normalize to the same string iff
	// they have the same weight.
	type entry struct {
		cp     rune
		weight uint16
	}
	normalized := make(map[string]entry)

	var falseMatches int
	for cp := rune(1); cp <= 0xFFFF; cp++ {
		s := string(cp)
		dst := coll.WeightString(nil, []byte(s), 0)
		if len(dst) < 2 {
			continue
		}
		weight := uint16(dst[0])<<8 | uint16(dst[1])
		norm := identNormalize(s)

		if prev, ok := normalized[norm]; ok {
			if prev.weight != weight {
				if falseMatches < 10 {
					t.Errorf("false match: U+%04X (weight 0x%04X) and U+%04X (weight 0x%04X) both normalize to %q",
						cp, weight, prev.cp, prev.weight, norm)
				}
				falseMatches++
			}
		} else {
			normalized[norm] = entry{cp: cp, weight: weight}
		}
	}
	if falseMatches > 10 {
		t.Errorf("... and %d more false matches", falseMatches-10)
	}
	assert.Zero(t, falseMatches, "identNormalize must not merge characters with different sort weights")
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
			identEqual("café_résumé", "CAFE_RESUME")
		}
	})
}
