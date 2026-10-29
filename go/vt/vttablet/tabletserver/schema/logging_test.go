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
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/sqlparser"
)

// TestGetTableForPosNotFoundLog checks that the current tables are logged by
// name, sorted, rather than as a map of table pointers.
func TestGetTableForPosNotFoundLog(t *testing.T) {
	se := NewEngineForTests()
	se.SetTableForTests(NewTable("b", NoType))
	se.SetTableForTests(NewTable("a", NoType))

	logs := logtest.Capture(t, slog.LevelInfo)
	_, err := se.GetTableForPos(t.Context(), sqlparser.NewIdentifierCS("missing"), "")
	require.EqualError(t, err, "table missing not found in vttablet schema")

	record := logs.Record(t, "table not found in vttablet schema")
	assert.Equal(t, "missing", record["table"])
	assert.Equal(t, "a,b", record["tables"])
}

// TestResetSequencesLog checks that the sequence cache is logged as its two
// values rather than the whole struct, mutex included.
func TestResetSequencesLog(t *testing.T) {
	se := NewEngineForTests()
	table := NewTable("seq", Sequence)
	table.SequenceInfo = &SequenceInfo{NextVal: 5, LastVal: 10}
	se.SetTableForTests(table)

	logs := logtest.Capture(t, slog.LevelInfo)
	require.NoError(t, se.ResetSequences([]string{"seq"}))

	record := logs.Record(t, "Resetting sequence info for table")
	assert.Equal(t, map[string]any{
		"time":       record["time"],
		"level":      "INFO",
		"msg":        "Resetting sequence info for table",
		"table":      "seq",
		"next_value": float64(5),
		"last_value": float64(10),
	}, record)
}

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
