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
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations/charset"
)

// TestGeneralCIVariants checks the collations that are utf8mb3_general_ci
// or ucs2_general_ci with a different sort value for some characters. The
// expected weight strings and comparisons were measured on MySQL 8.4.13 and
// 8.0.46: WEIGHT_STRING('aAéÉß') is 0041 0041 0045 0045 0053 in
// utf8mb3_general_ci, where "ß" sorts as "S".
func TestGeneralCIVariants(t *testing.T) {
	testcases := []struct {
		collation string
		input     string
		weights   string
		equal     map[string]bool
	}{
		{
			// "ß" is its own sort value, as it was in MySQL 5.0.
			collation: "utf8mb3_general_mysql500_ci",
			input:     "aAéÉß",
			weights:   "004100410045004500DF",
			equal:     map[string]bool{"aaees": false, "aaeeß": true},
		},
		{
			collation: "ucs2_general_mysql500_ci",
			input:     "aß",
			weights:   "004100DF",
			equal:     map[string]bool{"as": false, "Aß": true},
		},
		{
			// A character sorts by its lower case, so "é" is not "e".
			collation: "utf8mb3_tolower_ci",
			input:     "aAéÉß",
			weights:   "0061006100E900E900DF",
			equal:     map[string]bool{"aaees": false, "AAÉÉß": true, "aaeeß": false},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.collation, func(t *testing.T) {
			coll := testcollation(t, tc.collation)
			input, err := charset.ConvertFromUTF8(nil, coll.Charset(), []byte(tc.input))
			require.NoError(t, err)

			weights, err := hex.DecodeString(tc.weights)
			require.NoError(t, err)
			assert.Equal(t, weights, coll.WeightString(nil, input, 0))

			for other, equal := range tc.equal {
				converted, err := charset.ConvertFromUTF8(nil, coll.Charset(), []byte(other))
				require.NoError(t, err)
				assert.Equal(t, equal, coll.Collate(input, converted, false) == 0, "%q = %q", tc.input, other)
			}
		})
	}
}
