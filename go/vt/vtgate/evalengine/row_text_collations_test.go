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

package evalengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vthash"
)

// TestRowTextCollations checks that VTGate does not compare, hash or give tiny
// weights to the text of rows in the collations of rowTextCollationNames,
// whose text arrives from the tablets in utf8mb4, so that its callers use the
// weight strings of MySQL. In latin1_german2_ci, 'ä' is before 'af', but its
// utf8mb4 bytes C3 A4 are the latin1 text 'Ã¤', which is after 'af'.
func TestRowTextCollations(t *testing.T) {
	env := collations.MySQL8()
	left, right := sqltypes.NewVarChar("ä"), sqltypes.NewVarChar("af")

	for _, name := range rowTextCollationNames {
		t.Run(name, func(t *testing.T) {
			id := env.LookupByName(name)
			require.True(t, env.IsSupported(id))
			require.True(t, NeedsServerWeightString(id))

			_, err := NullsafeCompare(left, right, env, id, nil)
			require.ErrorAs(t, err, &UnsupportedCollationError{})

			_, err = NullsafeHashcode(left, id, sqltypes.VarChar, 0, nil)
			require.ErrorIs(t, err, UnsupportedCollationHashError)

			hasher := vthash.New()
			err = NullsafeHashcode128(&hasher, left, id, sqltypes.VarChar, 0, nil)
			require.ErrorIs(t, err, UnsupportedCollationHashError)

			assert.Nil(t, TinyWeighter(&querypb.Field{Type: sqltypes.VarChar}, id))

			// The weight strings that MySQL sends are VARBINARY, which
			// VTGate compares byte by byte.
			cmp, err := NullsafeCompare(sqltypes.MakeTrusted(sqltypes.VarBinary, []byte{0x41, 0x45}), sqltypes.MakeTrusted(sqltypes.VarBinary, []byte{0x41, 0x46}), env, id, nil)
			require.NoError(t, err)
			assert.Equal(t, -1, cmp)
		})
	}

	// The collations that VTGate compared the text of rows in before keep
	// doing so.
	for _, name := range []string{"latin1_swedish_ci", "utf8mb4_0900_ai_ci", "utf8mb3_tolower_ci", "utf8mb3_general_mysql500_ci"} {
		id := env.LookupByName(name)
		assert.False(t, NeedsServerWeightString(id), name)
		_, err := NullsafeCompare(left, right, env, id, nil)
		assert.NoError(t, err, name)
	}
}
