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

package engine

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// TestRowTextCollationsUseWeightStrings checks that a sort and a DISTINCT over
// a latin1_german2_ci column use the weight strings that MySQL sends with the
// rows, and not the text, which a tablet sends in utf8mb4. The weight strings
// were measured on MySQL 8.4.13: 'ä' is 41 45, before 'af' (41 46), and
// 'Müller' and 'MUELLER' are both 4D 55 45 4C 4C 45 52. The utf8mb4 bytes of
// 'ä' would sort after 'af' in latin1_german2_ci, and those of 'Müller' would
// not equal 'MUELLER'.
func TestRowTextCollationsUseWeightStrings(t *testing.T) {
	german2 := collations.MySQL8().LookupByName("latin1_german2_ci")
	typ := evalengine.NewType(sqltypes.VarChar, german2)
	fields := sqltypes.MakeTestFields("name|weight_string(name)", "varchar|varbinary")
	row := func(name, weights string) sqltypes.Row {
		ws, err := hex.DecodeString(weights)
		require.NoError(t, err)
		return sqltypes.Row{sqltypes.NewVarChar(name), sqltypes.MakeTrusted(sqltypes.VarBinary, ws)}
	}

	t.Run("sort", func(t *testing.T) {
		input := &fakePrimitive{results: []*sqltypes.Result{{
			Fields: fields,
			Rows:   []sqltypes.Row{row("af", "4146"), row("ä", "4145")},
		}}}
		ms := &MemorySort{
			OrderBy: []evalengine.OrderByParams{{Col: 0, WeightStringCol: 1, Type: typ, CollationEnv: collations.MySQL8()}},
			Input:   input,
		}
		result, err := ms.TryExecute(t.Context(), &noopVCursor{}, nil, false)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2)
		assert.Equal(t, "ä", result.Rows[0][0].ToString())
		assert.Equal(t, "af", result.Rows[1][0].ToString())
	})

	t.Run("distinct", func(t *testing.T) {
		input := &fakePrimitive{results: []*sqltypes.Result{{
			Fields: fields,
			Rows:   []sqltypes.Row{row("Müller", "4D55454C4C4552"), row("MUELLER", "4D55454C4C4552")},
		}}}
		wsCol := 1
		distinct := &Distinct{
			Source:    input,
			CheckCols: []CheckCol{{Col: 0, WsCol: &wsCol, Type: typ, CollationEnv: collations.MySQL8()}},
		}
		result, err := distinct.TryExecute(t.Context(), &noopVCursor{}, nil, false)
		require.NoError(t, err)
		assert.Len(t, result.Rows, 1)
	})
}
