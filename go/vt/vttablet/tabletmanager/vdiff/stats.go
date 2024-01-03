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

	"vitess.io/vitess/go/stats"
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
	mu sync.Mutex

	RestartedTableDiffs *stats.CountersWithSingleLabel
}

func (st *vdiffStats) register() {
	globalStats.RestartedTableDiffs = stats.NewCountersWithSingleLabel("", "", "Table", "")

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
}
