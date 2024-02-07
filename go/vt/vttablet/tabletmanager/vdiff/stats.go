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
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

const (
// Keys for the rates/timings stats map.
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

	Count                *stats.Gauge
	Errors               *stats.Counter
	ErrorsByWorkflow     *stats.CountersWithSingleLabel
	RestartedTableDiffs  *stats.CountersWithSingleLabel
	RowsDiffed           *stats.Counter
	RowsDiffedByWorkflow *stats.CountersWithSingleLabel

	DiffTimings  *stats.Timings // How long a VDiff run takes to complete.
	DiffRates    *stats.Rates   // How many things we're doing per second.
	PhaseTimings *stats.Timings // How long we spend in phases such as table diff initialization.
}

func (st *vdiffStats) register() {
	globalStats.Count = stats.NewGauge("", "")
	globalStats.Errors = stats.NewCounter("", "")
	globalStats.ErrorsByWorkflow = stats.NewCountersWithSingleLabel("", "", "Workflow", "")
	globalStats.RestartedTableDiffs = stats.NewCountersWithSingleLabel("", "", "Table", "")
	globalStats.RowsDiffed = stats.NewCounter("", "")
	globalStats.RowsDiffedByWorkflow = stats.NewCountersWithSingleLabel("", "", "Workflow", "")
	globalStats.DiffTimings = stats.NewTimings("", "", "")
	globalStats.DiffRates = stats.NewRates("", globalStats.DiffTimings, 15*60/5, 5*time.Second) // Pers second avg with 15 second samples
	globalStats.PhaseTimings = stats.NewTimings("VDiffPhaseTimings", "vdiff per phase timings", "Phase")

	stats.NewGaugeFunc("VDiffCount", "Number of current vdiffs", st.numControllers)

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

	stats.NewCounterFunc(
		"VDiffRowsCompared",
		"number of rows compared across all vdiffs",
		func() int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			return globalStats.RowsDiffed.Get()
		})

	stats.NewGaugesFuncWithMultiLabels(
		"VDiffRowsComparedByWorkflow",
		"number of rows compared by vreplication workflow name",
		[]string{"table_name"},
		func() map[string]int64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			result := make(map[string]int64)
			for label, count := range globalStats.RowsDiffedByWorkflow.Counts() {
				if label == "" {
					continue
				}
				result[label] = count
			}
			return result
		},
	)

	stats.NewRateFunc(
		"VDiffActionsPerSecond",
		"number of actions per second across all vdiffs",
		func() map[string][]float64 {
			st.mu.Lock()
			defer st.mu.Unlock()
			return globalStats.DiffRates.Get()
		})

	stats.Publish("VDiffStreamingTablets", stats.StringMapFunc(func() map[string]string {
		st.mu.Lock()
		defer st.mu.Unlock()
		result := make(map[string]string, len(st.controllers))
		for _, ct := range st.controllers {
			tt := topoproto.TabletAliasString(ct.targetShardStreamer.tablet.Alias)
			for _, s := range ct.sources {
				result[fmt.Sprintf("%s.%s.%s", ct.workflow, ct.uuid, s.shard)] =
					fmt.Sprintf("source:%s;target:%s", topoproto.TabletAliasString(s.tablet.Alias), tt)
			}
		}
		return result
	}))
}

func (st *vdiffStats) numControllers() int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	return int64(len(st.controllers))
}
