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

// Stats is a reporting interface meant to be shared among backup and restore
// components.
//
// This interface is meant to give those components a way to report stats
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
	// Increment bytes and increase duration.
	TimedIncrementBytes(int, time.Duration)
}

type noStats struct{}

type scopedStats struct {
	bytes       *vtstats.CountersWithMultiLabels
	count       *vtstats.CountersWithMultiLabels
	durationNs  *vtstats.CountersWithMultiLabels
	labelValues []string
}

const unscoped = "-"

var (
	defaultNoStats *noStats

	labels = []string{"component", "implementation", "operation"}

	registerBackupStats  sync.Once
	registerRestoreStats sync.Once

	backupBytes       *stats.CountersWithMultiLabels
	backupCount       *stats.CountersWithMultiLabels
	backupDurationNs  *stats.CountersWithMultiLabels
	restoreBytes      *stats.CountersWithMultiLabels
	restoreCount      *stats.CountersWithMultiLabels
	restoreDurationNs *stats.CountersWithMultiLabels
)

// BackupStats creates a new Stats for backup operations.
//
// It registers the following metrics with the Vitess stats package.
//
//   - BackupBytes: number of bytes processed by an an operation for given
//     component and implementation.
//   - BackupCount: number of times an operation has happened for given
//     component and implementation.
//   - BackupDurationNanoseconds: time spent on an operation for a given
//     component and implementation.
func BackupStats() Stats {
	registerBackupStats.Do(func() {
		backupBytes = stats.NewCountersWithMultiLabels(
			"BackupBytes",
			"How many backup bytes processed.",
			labels,
		)
		backupCount = stats.NewCountersWithMultiLabels(
			"BackupCount",
			"How many backup operations have happened.",
			labels,
		)
		backupDurationNs = stats.NewCountersWithMultiLabels(
			"BackupDurationNanoseconds",
			"How much time has been spent on backup operations (in nanoseconds).",
			labels,
		)
	})
	return newScopedStats(backupBytes, backupCount, backupDurationNs, nil)
}

// RestoreStats creates a new Stats for restore operations.
//
// It registers the following metrics with the Vitess stats package.
//
//   - RestoreBytes: number of bytes processed by an an operation for given
//     component and implementation.
//   - RestoreCount: number of times an operation has happened for given
//     component and implementation.
//   - RestoreDurationNanoseconds: time spent on an operation for a given
//     component and implementation.
func RestoreStats() Stats {
	registerRestoreStats.Do(func() {
		restoreBytes = stats.NewCountersWithMultiLabels(
			"RestoreBytes",
			"How many restore bytes processed.",
			labels,
		)
		restoreCount = stats.NewCountersWithMultiLabels(
			"RestoreCount",
			"How many restore operations have happened.",
			labels,
		)
		restoreDurationNs = stats.NewCountersWithMultiLabels(
			"RestoreDurationNanoseconds",
			"How much time has been spent on restore operations (in nanoseconds).",
			labels,
		)
	})
	return newScopedStats(restoreBytes, restoreCount, restoreDurationNs, nil)
}

// NoStats returns a no-op Stats suitable for tests and for backwards
// compoatibility.
func NoStats() Stats {
	return defaultNoStats
}

func (ns *noStats) Lock(...ScopeType) Stats                { return ns }
func (ns *noStats) Scope(...Scope) Stats                   { return ns }
func (ns *noStats) TimedIncrement(time.Duration)           {}
func (ns *noStats) TimedIncrementBytes(int, time.Duration) {}

func newScopedStats(
	bytes *stats.CountersWithMultiLabels,
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

	return &scopedStats{bytes, count, durationNs, labelValues}
}

// Scope returns a new Stats narrowed by the provided scopes. If a provided
// scope is already set in this Stats, the new Stats uses that scope, and the
// provided scope is ignored.
func (s *scopedStats) Scope(scopes ...Scope) Stats {
	copyOfLabelValues := make([]string, len(s.labelValues))
	copy(copyOfLabelValues, s.labelValues)
	for _, scope := range scopes {
		typeIdx := int(scope.Type)

		// Ignore this scope if the ScopeType is invalid.
		if typeIdx > len(copyOfLabelValues)-1 {
			continue
		}
		// Ignore this scope if it is already been set in this Stats' label values.
		if copyOfLabelValues[typeIdx] == unscoped {
			copyOfLabelValues[typeIdx] = scope.Value
		}
	}
	return newScopedStats(s.bytes, s.count, s.durationNs, copyOfLabelValues)
}

// TimedIncrement increments the count and duration of the current scope.
func (s *scopedStats) TimedIncrement(d time.Duration) {
	s.count.Add(s.labelValues, 1)
	s.durationNs.Add(s.labelValues, int64(d.Nanoseconds()))
}

// TimedIncrementBytes increments the byte-count and duration of the current scope.
func (s *scopedStats) TimedIncrementBytes(b int, d time.Duration) {
	s.bytes.Add(s.labelValues, int64(b))
	s.durationNs.Add(s.labelValues, int64(d.Nanoseconds()))
}
