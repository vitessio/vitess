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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
)

// TestArenaReusedEntriesAreCleared tests that a value handed out by the arena
// carries nothing over from the value that used the same entry before it. An
// arena is reset once per evaluation and then hands the same entries out again,
// so a field a constructor takes no argument for still has to be cleared for the
// caller to get the value it asked for.
func TestArenaReusedEntriesAreCleared(t *testing.T) {
	t.Run("bytes", func(t *testing.T) {
		var a Arena

		// A hex literal reaches an entry by having the whole literal copied over
		// it, which is how an entry ends up holding flags at all.
		stale := a.newEvalBytesEmpty()
		*stale = *newEvalBytesHex([]byte("A")).(*evalBytes)
		require.True(t, stale.isHexLiteral())

		a.reset()

		fresh := a.newEvalBinary([]byte("A"))
		require.Same(t, stale, fresh, "the entry must be reused for this test to mean anything")
		require.False(t, fresh.isHexLiteral())
		require.False(t, fresh.isBitLiteral())
		require.Equal(t, sqltypes.VarBinary, fresh.SQLType())
		require.Equal(t, collationBinary, fresh.col)
	})

	t.Run("int64", func(t *testing.T) {
		var a Arena

		stale := a.newEvalInt64(1)
		stale.bitLiteral = true

		a.reset()

		fresh := a.newEvalInt64(2)
		require.Same(t, stale, fresh, "the entry must be reused for this test to mean anything")
		require.False(t, fresh.bitLiteral)
		require.EqualValues(t, 2, fresh.i)
	})

	t.Run("uint64", func(t *testing.T) {
		var a Arena

		stale := a.newEvalUint64(1)
		stale.hexLiteral = true

		a.reset()

		fresh := a.newEvalUint64(2)
		require.Same(t, stale, fresh, "the entry must be reused for this test to mean anything")
		require.False(t, fresh.hexLiteral)
		require.EqualValues(t, 2, fresh.u)
	})
}
