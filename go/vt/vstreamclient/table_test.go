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

package vstreamclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResetBatch_ReuseBatchSlice pins the batch reuse contract deterministically: the test holds a
// reference into the previous batch's backing array, so with reuse enabled the next batch must
// land at the same address, and with reuse disabled it cannot (the held reference keeps the old
// array alive, so the allocator cannot hand the same memory back).
func TestResetBatch_ReuseBatchSlice(t *testing.T) {
	newTable := func(reuse bool) *TableConfig {
		table := &TableConfig{
			Keyspace:        "ks",
			Table:           "t",
			MaxRowsPerFlush: 4,
			ReuseBatchSlice: reuse,
		}
		table.resetBatch()
		return table
	}

	t.Run("reuses the backing array when enabled", func(t *testing.T) {
		table := newTable(true)
		table.currentBatch = append(table.currentBatch, Row{Data: "first"})
		held := &table.currentBatch[0]

		table.resetBatch()
		require.Empty(t, table.currentBatch)

		table.currentBatch = append(table.currentBatch, Row{Data: "second"})
		assert.Same(t, held, &table.currentBatch[0])
	})

	t.Run("allocates a fresh array when disabled", func(t *testing.T) {
		table := newTable(false)
		table.currentBatch = append(table.currentBatch, Row{Data: "first"})
		held := &table.currentBatch[0]

		table.resetBatch()
		require.Empty(t, table.currentBatch)

		table.currentBatch = append(table.currentBatch, Row{Data: "second"})
		assert.NotSame(t, held, &table.currentBatch[0])
	})

	t.Run("clears retained row data when reusing", func(t *testing.T) {
		table := newTable(true)
		table.currentBatch = append(table.currentBatch, Row{Data: "first"})

		table.resetBatch()

		// slices.Delete must zero the vacated elements so reused batches don't leak the
		// previous batch's rows to the garbage collector or to the next flush
		spare := table.currentBatch[:1]
		assert.Nil(t, spare[0].Data)
	})
}
