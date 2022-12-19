/*
Copyright 2022 The Vitess Authors.

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

package backupstats

import (
	"sync"
	"time"

	"vitess.io/vitess/go/stats"
	vtstats "vitess.io/vitess/go/stats"
)

// Stats is a reporting interface meant to be shared amount backup and restore
// components.
//
// This interface is meant to give those components a way to reports stats
// without having to register their own stats or to import globally registered
// stats. This way out-of-tree plugins can have a mechanism for reporting
// stats, while the policies for those stats (metric names and labels, stat
// sinks) remain in the control of in-tree Vitess code and Vitess users.
type Stats interface {
	// Scope creates a new Stats which inherits this Stats' scopes plus any
	// new provided scopes.
	//
	// Once a ScopeType has been set in a Stats, it cannot be changed by
	// further calls to Scope().
	//
	// This allows parent components to prepare properly scoped Stats and pass
	// them to child components in such a way that child components cannot
	// overwrite another components' metrics.
	Scope(...Scope) Stats
	// Increment count by 1 and increase duration.
	TimedIncrement(time.Duration)
}

type nopStats struct{}

type scopedStats struct {
	count       *vtstats.CountersWithMultiLabels
	durationNs  *vtstats.CountersWithMultiLabels
	labelValues []string
}

const unscoped = "-"

var (
	defaultNopStats *nopStats

	labels = []string{"component", "implementation", "operation"}

	registerBackupStats  sync.Once
	registerRestoreStats sync.Once

	backupCount       *stats.CountersWithMultiLabels
	backupDurationNs  *stats.CountersWithMultiLabels
	restoreCount      *stats.CountersWithMultiLabels
	restoreDurationNs *stats.CountersWithMultiLabels
)

// BackupStats creates a new Stats for backup operations.
//
// It registers two stats with the Vitess stats package.
//
//   - backup_count: number of times an operation has happened for given
//     component and implementation.
//   - backup_duration_nanoseconds: time spent on an operation for a given
//     component and implementation.
func BackupStats() Stats {
	registerBackupStats.Do(func() {
		backupCount = stats.NewCountersWithMultiLabels(
			"backup_count",
			"How many backup operations have happened.",
			labels,
		)
		backupDurationNs = stats.NewCountersWithMultiLabels(
			"backup_duration_nanoseconds",
			"How much time has been spent on backup operations (in nanoseconds).",
			labels,
		)
	})
	return newScopedStats(backupCount, backupDurationNs, nil)
}

// RestoreStats creates a new Stats for restore operations.
//
// It registers two stats with the Vitess stats package.
//
//   - restore_count: number of times an operation has happened for given
//     component and implementation.
//   - restore_duration_nanoseconds: time spent on an operation for a given
//     component and implementation.
func RestoreStats() Stats {
	registerRestoreStats.Do(func() {
		restoreCount = stats.NewCountersWithMultiLabels(
			"restore_count",
			"How many restore operations have happened.",
			labels,
		)
		restoreDurationNs = stats.NewCountersWithMultiLabels(
			"restore_duration_nanoseconds",
			"How much time has been spent on restore operations (in nanoseconds).",
			labels,
		)
	})
	return newScopedStats(restoreCount, restoreDurationNs, nil)
}

// NopStats returns a no-op Stats suitable for tests and for backwards
// compoatibility.
func NopStats() Stats {
	return defaultNopStats
}

func (ns *nopStats) Lock(...ScopeType) Stats        { return ns }
func (ns *nopStats) Scope(...Scope) Stats           { return ns }
func (ns *nopStats) TimedIncrement(d time.Duration) {}

func newScopedStats(
	count *stats.CountersWithMultiLabels,
	durationNs *stats.CountersWithMultiLabels,
	labelValues []string,
) Stats {
	if labelValues == nil {
		labelValues = make([]string, len(durationNs.Labels()))
		for i := 0; i < len(labelValues); i++ {
			labelValues[i] = unscoped
		}
	}

	return &scopedStats{count, durationNs, labelValues}
}

func (s *scopedStats) Scope(scopes ...Scope) Stats {
	copyOfLabelValues := make([]string, len(s.labelValues))
	copy(copyOfLabelValues, s.labelValues)
	for _, scope := range scopes {
		// Ignore this scope if the ScopeType is invalid.
		if scope.Type.Index() > len(copyOfLabelValues)-1 {
			continue
		}
		// Ignore this scope if it is already been set in this Stats' label values.
		if copyOfLabelValues[scope.Type.Index()] == unscoped {
			copyOfLabelValues[scope.Type.Index()] = scope.Value
		}
	}
	return newScopedStats(s.count, s.durationNs, copyOfLabelValues)
}

func (s *scopedStats) TimedIncrement(d time.Duration) {
	s.count.Add(s.labelValues, 1)
	s.durationNs.Add(s.labelValues, int64(d.Nanoseconds()))
}
