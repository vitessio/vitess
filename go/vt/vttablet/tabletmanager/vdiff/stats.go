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
	"fmt"
	"sync"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

var (
	globalStats = &vdiffStats{}
)

func init() {
	globalStats.register()
}

// This is a singleton.
// vdiffStats exports the stats for Engine. It's a separate structure to
// prevent potential deadlocks with the mutex in Engine.
type vdiffStats struct {
	mu          sync.Mutex
	controllers map[int64]*controller

	Count               *stats.Gauge
	ErrorCount          *stats.Counter
	RestartedTableDiffs *stats.CountersWithSingleLabel
	RowsDiffedCount     *stats.Counter
}

func (st *vdiffStats) register() {
	globalStats.Count = stats.NewGauge("", "")
	globalStats.ErrorCount = stats.NewCounter("", "")
	globalStats.RestartedTableDiffs = stats.NewCountersWithSingleLabel("", "", "Table", "")
	globalStats.RowsDiffedCount = stats.NewCounter("", "")

	stats.NewGaugeFunc("VDiffCount", "Number of current vdiffs", st.numControllers)

	stats.NewCounterFunc(
		"VDiffErrorCountTotal",
		"number of errors encountered across all vdiffs",
		func() int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			return globalStats.ErrorCount.Get()
		})

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

	stats.NewCounterFunc(
		"VDiffRowsComparedTotal",
		"number of rows compared across all vdiffs",
		func() int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			return globalStats.RowsDiffedCount.Get()
		})

	stats.NewGaugesFuncWithMultiLabels(
		"VDiffRowsCompared",
		"live number of rows compared per vdiff by table",
		[]string{"workflow", "uuid", "table"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64, len(st.controllers))
			for _, ct := range st.controllers {
				for key, val := range ct.TableDiffRowCounts.Counts() {
					result[fmt.Sprintf("%s.%s.%s", ct.workflow, ct.uuid, key)] = val
				}
			}
			return result
		},
	)

	stats.NewStringMapFuncWithMultiLabels(
		"VDiffStreamingTablets",
		"latest tablets used on the source and target for streaming table data",
		[]string{"workflow", "uuid", "target_shard"},
		"tablets",
		func() map[string]string {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]string, len(st.controllers))
			for _, ct := range st.controllers {
				tt := topoproto.TabletAliasString(ct.targetShardStreamer.tablet.Alias)
				for _, s := range ct.sources {
					result[fmt.Sprintf("%s.%s.%s", ct.workflow, ct.uuid, s.shard)] =
						fmt.Sprintf("source:%s,target:%s", topoproto.TabletAliasString(s.tablet.Alias), tt)
				}
			}
			return result
		})

	stats.NewCountersFuncWithMultiLabels(
		"VDiffErrors",
		"count of specific errors seen when performing a vdiff",
		[]string{"workflow", "uuid", "error"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64, len(st.controllers))
			for _, ct := range st.controllers {
				for key, val := range ct.ErrorCounts.Counts() {
					result[fmt.Sprintf("%s.%s.%s", ct.workflow, ct.uuid, key)] = val
				}
			}
			return result
		})

	stats.NewGaugesFuncWithMultiLabels(
		"VDiffPhaseTimings",
		"vdiff phase timings",
		[]string{"workflow", "uuid", "table", "phase"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64, len(st.controllers))
			for _, ct := range st.controllers {
				for tablePhase, h := range ct.TableDiffPhaseTimings.Histograms() {
					result[fmt.Sprintf("%s.%s.%s", ct.workflow, ct.uuid, tablePhase)] = h.Total()
				}
			}
			return result
		})
}

func (st *vdiffStats) numControllers() int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	return int64(len(st.controllers))
}
