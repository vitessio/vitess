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

package schema

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestResetSequencesLogLocksSequence checks that logging the sequence values
// does not race with concurrent ID allocation, which holds the sequence lock.
func TestResetSequencesLogLocksSequence(t *testing.T) {
	se := NewEngineForTests()
	table := NewTable("seq", Sequence)
	table.SequenceInfo = &SequenceInfo{}
	se.SetTableForTests(table)

	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 1000 {
			table.SequenceInfo.Lock()
			table.SequenceInfo.NextVal = int64(i)
			table.SequenceInfo.LastVal = int64(i)
			table.SequenceInfo.Unlock()
		}
	})
	for range 100 {
		require.NoError(t, se.ResetSequences([]string{"seq"}))
	}
	wg.Wait()
}
