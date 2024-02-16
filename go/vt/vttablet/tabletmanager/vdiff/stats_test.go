/*
Copyright 2024 The Vitess Authors.

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

package vdiff

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/stats"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func TestVDiffStats(t *testing.T) {
	testStats := &vdiffStats{
		ErrorCount:          stats.NewCounter("", ""),
		RestartedTableDiffs: stats.NewCountersWithSingleLabel("", "", "Table", ""),
		RowsDiffedCount:     stats.NewCounter("", ""),
	}
	id := int64(1)
	testStats.controllers = map[int64]*controller{
		id: {
			id:                    id,
			workflow:              "testwf",
			workflowType:          binlogdatapb.VReplicationWorkflowType_MoveTables,
			uuid:                  uuid.New().String(),
			Errors:                stats.NewCountersWithMultiLabels("", "", []string{"Error"}),
			TableDiffRowCounts:    stats.NewCountersWithMultiLabels("", "", []string{"Rows"}),
			TableDiffPhaseTimings: stats.NewTimings("", "", "", "TablePhase"),
		},
	}

	require.Equal(t, int64(1), testStats.numControllers())

	sleepTime := 1 * time.Millisecond
	record := func(phase string) {
		defer testStats.controllers[id].TableDiffPhaseTimings.Record(phase, time.Now())
		time.Sleep(sleepTime)
	}
	want := 10 * sleepTime // Allow 10x overhead for recording timing on flaky test hosts
	record(string(initializing))
	require.Greater(t, want, testStats.controllers[id].TableDiffPhaseTimings.Histograms()[string(initializing)].Total())
	record(string(pickingTablets))
	require.Greater(t, want, testStats.controllers[id].TableDiffPhaseTimings.Histograms()[string(pickingTablets)].Total())
	record(string(diffingTable))
	require.Greater(t, want, testStats.controllers[id].TableDiffPhaseTimings.Histograms()[string(diffingTable)].Total())

	testStats.ErrorCount.Set(11)
	require.Equal(t, int64(11), testStats.ErrorCount.Get())

	testStats.controllers[id].Errors.Add([]string{"test error"}, int64(12))
	require.Equal(t, int64(12), testStats.controllers[id].Errors.Counts()["test error"])

	testStats.RestartedTableDiffs.Add("t1", int64(5))
	require.Equal(t, int64(5), testStats.RestartedTableDiffs.Counts()["t1"])

	testStats.RowsDiffedCount.Add(512)
	require.Equal(t, int64(512), testStats.RowsDiffedCount.Get())
}
