/*
Copyright 2023 The Vitess Authors.

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
	"sync"
	"time"

	"vitess.io/vitess/go/stats"
)

var (
	globalStats = &vdiffStats{}
)

func init() {
	globalStats.Timings = stats.NewTimings("", "", "")
	globalStats.Rates = stats.NewRates("", globalStats.Timings, 15*60/5, 5*time.Second)
	globalStats.PhaseTimings = stats.NewTimings("", "", "Phase")
	globalStats.register()
}

// This is a singleton.
// vdiffStats exports the stats for Engine. It's a separate structure to
// prevent potential deadlocks with the mutex in Engine.
type vdiffStats struct {
	mu sync.Mutex

	Created             *stats.Counter
	CreatedByWorkflow   *stats.CountersWithSingleLabel
	Errors              *stats.Counter
	ErrorsByWorkflow    *stats.CountersWithSingleLabel
	RestartedTableDiffs *stats.CountersWithSingleLabel

	Timings      *stats.Timings // How long a VDiff run takes to complete.
	PhaseTimings *stats.Timings // How long we spend in phases such as table diff initialization.
	Rates        *stats.Rates

	RowsDiffedPerSecond *stats.CountersWithMultiLabels
}

func (st *vdiffStats) register() {
	globalStats.Created = stats.NewCounter("", "")
	globalStats.CreatedByWorkflow = stats.NewCountersWithSingleLabel("", "", "Workflow", "")
	globalStats.Errors = stats.NewCounter("", "")
	globalStats.ErrorsByWorkflow = stats.NewCountersWithSingleLabel("", "", "Workflow", "")
	globalStats.RestartedTableDiffs = stats.NewCountersWithSingleLabel("", "", "Table", "")
	globalStats.Timings = stats.NewTimings("", "", "")
	globalStats.Rates = stats.NewRates("", globalStats.Timings, 15*60/5, 5*time.Second) // Pers second avg with 15 second samples
	globalStats.PhaseTimings = stats.NewTimings("", "", "Phase")

	/*
		bps.BulkQueryCount = stats.NewCountersWithSingleLabel("", "", "Statement", "")
		bps.TrxQueryBatchCount = stats.NewCountersWithSingleLabel("", "", "Statement", "")
		bps.CopyRowCount = stats.NewCounter("", "")
		bps.CopyLoopCount = stats.NewCounter("", "")
		bps.ErrorCounts = stats.NewCountersWithMultiLabels("", "", []string{"type"})
		bps.NoopQueryCount = stats.NewCountersWithSingleLabel("", "", "Statement", "")
		bps.VReplicationLags = stats.NewTimings("", "", "")
		bps.VReplicationLagRates = stats.NewRates("", bps.VReplicationLags, 15*60/5, 5*time.Second)
		bps.TableCopyRowCounts = stats.NewCountersWithSingleLabel("", "", "Table", "")
		bps.TableCopyTimings = stats.NewTimings("", "", "Table")
		bps.PartialQueryCacheSize = stats.NewCountersWithMultiLabels("", "", []string{"type"})
		bps.PartialQueryCount = stats.NewCountersWithMultiLabels("", "", []string{"type"})
	*/

	stats.NewCounterFunc(
		"VDiffErrorsTotal",
		"number of errors encountered across all vdiffs",
		func() int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			return globalStats.Errors.Get()
		})

	stats.NewGaugesFuncWithMultiLabels(
		"VDiffErrorsByWorkflow",
		"number of errors encountered for vdiffs by vreplication workflow name",
		[]string{"table_name"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64)
			for label, count := range globalStats.ErrorsByWorkflow.Counts() {
				if label == "" {
					continue
				}
				result[label] = count
			}
			return result
		},
	)

	stats.NewGaugesFuncWithMultiLabels(
		"VDiffRestartedTableDiffsCount",
		"vdiff table diffs restarted due to max-diff-duration counts per table",
		[]string{"table_name"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64)
			for label, count := range globalStats.RestartedTableDiffs.Counts() {
				if label == "" {
					continue
				}
				result[label] = count
			}
			return result
		},
	)

	stats.NewRateFunc(
		"VDiffRowsComparedPerSecond",
		"number of rows diffed per second all vdiffs",
		func() map[string][]float64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			return globalStats.Rates.Get()
		})
}
