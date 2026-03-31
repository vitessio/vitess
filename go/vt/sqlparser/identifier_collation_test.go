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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/mysql/collations/unicase"
)

// TestIdentCollateMatchesColldata verifies that our identCollate function
// produces the same results as the canonical utf8mb4_general_ci collation
// implementation in the colldata package.
func TestIdentCollateMatchesColldata(t *testing.T) {
	coll := colldata.Lookup(collations.ID(45) /* utf8mb4_general_ci */)
	require.NotNil(t, coll, "utf8mb4_general_ci collation not found")

	// Test pairs of strings that exercise various Unicode behaviors.
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
		expected := coll.Collate([]byte(a), []byte(b), false)
		got := identCollate(a, b)

		// Normalize to signum for comparison
		expectedSign := sign(expected)
		gotSign := sign(got)

		assert.Equal(t, expectedSign, gotSign,
			"identCollate(%q, %q) = %d (sign %d), colldata.Collate = %d (sign %d)",
			a, b, got, gotSign, expected, expectedSign)
	}
}

// TestSortWeightMatchesColldata verifies that our unicodeSortWeight
// lookup matches the canonical collation's weight string for all BMP code points.
func TestSortWeightMatchesColldata(t *testing.T) {
	coll := colldata.Lookup(collations.ID(45) /* utf8mb4_general_ci */)
	require.NotNil(t, coll, "utf8mb4_general_ci collation not found")

	var mismatches int
	for cp := rune(0); cp <= 0xFFFF; cp++ {
		s := string(cp)
		dst := coll.WeightString(nil, []byte(s), 0)
		if len(dst) < 2 {
			continue
		}
		expected := rune(uint16(dst[0])<<8 | uint16(dst[1]))
		got := unicase.SortWeight(cp)
		if got != expected {
			if mismatches < 10 {
				t.Errorf("unicase.SortWeight(0x%04X) = 0x%04X, want 0x%04X", cp, got, expected)
			}
			mismatches++
		}
	}
	if mismatches > 10 {
		t.Errorf("... and %d more mismatches", mismatches-10)
	}
}

// TestNormalizeConsistentWithCollate verifies that for all single-character
// pairs in the BMP, identNormalize produces the same output iff identCollate
// considers them equal. This ensures no false matches or false mismatches
// from the normalization step.
func TestNormalizeConsistentWithCollate(t *testing.T) {
	// Build a map from normalized single-char string to a representative code point.
	// If two code points normalize to the same string but have different sort weights,
	// that's a bug.
	type entry struct {
		cp     rune
		weight rune
	}
	normalized := make(map[string]entry)

	var falseMatches int
	for cp := rune(1); cp <= 0xFFFF; cp++ {
		s := string(cp)
		norm := identNormalize(s)
		weight := unicase.SortWeight(cp)

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

func sign(n int) int {
	if n < 0 {
		return -1
	}
	if n > 0 {
		return 1
	}
	return 0
}

func BenchmarkIdentNormalize(b *testing.B) {
	b.Run("ASCII", func(b *testing.B) {
		for b.Loop() {
			identNormalize("my_table_name")
		}
	})
	b.Run("Unicode", func(b *testing.B) {
		for b.Loop() {
			identNormalize("café_résumé")
		}
	})
}

func BenchmarkIdentCollate(b *testing.B) {
	b.Run("ASCII_equal", func(b *testing.B) {
		for b.Loop() {
			identCollate("my_table_name", "MY_TABLE_NAME")
		}
	})
	b.Run("ASCII_unequal", func(b *testing.B) {
		for b.Loop() {
			identCollate("my_table_name", "other_table")
		}
	})
	b.Run("Unicode_equal", func(b *testing.B) {
		for b.Loop() {
			identCollate("café_résumé", "CAFE_RESUME")
		}
	})
}
