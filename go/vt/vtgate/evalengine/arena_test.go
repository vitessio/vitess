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

// TestArenaReusedBytesEntriesAreCleared tests that a value handed out by the
// arena carries nothing over from the value that used the same entry before it.
// An arena is reset once per evaluation and then hands the same entries out
// again, while the constructors set only the type, the collation and the bytes,
// so anything else has to be cleared for the caller to get the value it asked
// for.
func TestArenaReusedBytesEntriesAreCleared(t *testing.T) {
	var a Arena

	// A hex literal is pushed onto the stack by copying the whole literal, which
	// is how an entry ends up holding flags at all.
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
}
