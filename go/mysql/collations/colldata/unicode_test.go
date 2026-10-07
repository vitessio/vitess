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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
)

// TestUnicodeBinChangeCase checks that the _bin Unicode collations change case
// with MySQL's default unicase table rather than current Unicode data, e.g.
// MySQL leaves U+0180 (ƀ) unchanged while Unicode maps it to U+0243 (Ƀ).
func TestUnicodeBinChangeCase(t *testing.T) {
	env := collations.MySQL8()
	for _, name := range []string{"utf8mb4_bin", "utf16_bin"} {
		t.Run(name, func(t *testing.T) {
			coll, ok := Lookup(env.LookupByName(name)).(CaseAwareCollation)
			require.True(t, ok)

			encode := func(s string) []byte {
				out, err := charset.ConvertFromUTF8(nil, coll.Charset(), []byte(s))
				require.NoError(t, err)
				return out
			}

			assert.Equal(t, encode(`{"A": "ÉƀЖ😀"}`), coll.ToUpper(nil, encode(`{"a": "éƀж😀"}`)))
			assert.Equal(t, encode(`{"a": "éƀж😀"}`), coll.ToLower(nil, encode(`{"A": "ÉƀЖ😀"}`)))
		})
	}
}
