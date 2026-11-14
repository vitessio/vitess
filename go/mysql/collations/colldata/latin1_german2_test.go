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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations/charset"
)

// TestLatin1German2 checks latin1_german2_ci against values measured on
// MySQL 8.4.13 and 8.0.46 with a latin1_german2_ci column: 'ä' = 'ae' is
// true and 'ä' LIKE 'ae' is false.
func TestLatin1German2(t *testing.T) {
	coll := testcollation(t, "latin1_german2_ci")
	latin1 := func(s string) []byte {
		converted, err := charset.ConvertFromUTF8(nil, coll.Charset(), []byte(s))
		require.NoError(t, err)
		return converted
	}

	t.Run("comparison", func(t *testing.T) {
		for _, tc := range []struct {
			left, right string
			cmp         int
		}{
			{"ä", "ae", 0},
			{"ä", "Áe", 0},
			{"ä", "a", 1},
			{"ä", "ad", 1},
			{"ä", "af", -1},
			{"ß", "ss", 0},
			{"ß", "s", 1},
			{"Müller Straße", "MUELLER STRASSE", 0},
		} {
			got := coll.Collate(latin1(tc.left), latin1(tc.right), false)
			assert.Equal(t, tc.cmp, min(max(got, -1), 1), "%q <=> %q", tc.left, tc.right)
		}
	})

	t.Run("weight string", func(t *testing.T) {
		assert.Equal(t, "MUELLER STRASSE", string(coll.WeightString(nil, latin1("Müller Straße"), 0)))
		// WEIGHT_STRING(col AS CHAR(3)) pads the one character to three.
		assert.Equal(t, "AE  ", string(coll.WeightString(nil, latin1("ä"), 3)))
	})

	t.Run("like", func(t *testing.T) {
		for _, tc := range []struct {
			input, pattern string
			match          bool
		}{
			{"ä", "ae", false},
			{"ä", "Ä", true},
			{"ä", "a", false},
			{"ä", "ae%", false},
			{"ä", "ä%", true},
			{"ß", "ss", false},
			{"ß", "ß", true},
			{"Müller Straße", "MUELLER%", false},
			{"Müller Straße", "MÜLLER STRAßE", true},
			{"Müller Straße", "mÜl%", true},
		} {
			pattern := coll.Wildcard(latin1(tc.pattern), 0, 0, 0)
			assert.Equal(t, tc.match, pattern.Match(latin1(tc.input)), "%q LIKE %q", tc.input, tc.pattern)
		}
	})

	t.Run("case", func(t *testing.T) {
		caseAware, ok := coll.(CaseAwareCollation)
		require.True(t, ok)
		// "ß" has no upper case in latin1.
		assert.Equal(t, latin1("MÜLLER STRAßE"), caseAware.ToUpper(nil, latin1("Müller Straße")))
		assert.Equal(t, latin1("müller straße"), caseAware.ToLower(nil, latin1("Müller Straße")))
	})
}
