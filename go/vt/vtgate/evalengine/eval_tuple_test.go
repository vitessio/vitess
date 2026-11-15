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
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// TestValueToEvalTupleMemberError pins that a tuple member valueToEval cannot
// decode fails the whole conversion instead of being dropped. The closure's
// error used to shadow the outer one, so a HEXNUM member without its prefix,
// or a nested tuple with corrupt bytes, produced a shorter tuple and no error.
func TestValueToEvalTupleMemberError(t *testing.T) {
	coll := typedCoercionCollation(sqltypes.VarChar, collations.CollationUtf8mb4ID)

	t.Run("well-formed members convert", func(t *testing.T) {
		e, err := valueToEval(sqltypes.TestTuple(sqltypes.NewInt64(1), sqltypes.NewHexNum([]byte("0x41")), sqltypes.NewInt64(3)), coll, nil)
		require.NoError(t, err)
		require.Len(t, e.(*evalTuple).t, 3)
	})

	t.Run("undecodable scalar member", func(t *testing.T) {
		e, err := valueToEval(sqltypes.TestTuple(sqltypes.NewInt64(1), sqltypes.MakeTrusted(sqltypes.HexNum, []byte("zz")), sqltypes.NewInt64(3)), coll, nil)
		require.Error(t, err)
		assert.Equal(t, vtrpc.Code_INVALID_ARGUMENT, vterrors.Code(err))
		require.ErrorContains(t, err, "malformed hex literal")
		assert.Nil(t, e)
	})

	t.Run("corrupt nested tuple member", func(t *testing.T) {
		inner := sqltypes.TestTuple(sqltypes.NewInt64(1))
		raw := append([]byte{}, inner.Raw()...)
		// protowire layout: varint type, varint length, bytes. Inflate the
		// length byte that precedes the single payload byte '1'.
		idx := bytes.LastIndexByte(raw, '1') - 1
		require.Equal(t, byte(1), raw[idx], "expected the member length byte")
		raw[idx] = 0x7f
		corrupt := sqltypes.MakeTrusted(sqltypes.Tuple, raw)

		e, err := valueToEval(sqltypes.TestTuple(sqltypes.NewInt64(1), corrupt), coll, nil)
		require.Error(t, err)
		require.ErrorContains(t, err, "bad tuple encoding")
		assert.Nil(t, e)
	})
}
