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

import "vitess.io/vitess/go/stats"

var (
	// DeprecatedBackupDurationS is a deprecated statistic that will be removed
	// in the next release. Use backup_duration_nanoseconds instead.
	DeprecatedBackupDurationS = stats.NewGauge(
		"backup_duration_seconds",
		"[DEPRECATED] How long it took to complete the last backup operation (in seconds)",
	)

	// DeprecatedRestoreDurationS is a deprecated statistic that will be
	// removed in the next release. Use restore_duration_nanoseconds instead.
	DeprecatedRestoreDurationS = stats.NewGauge(
		"restore_duration_seconds",
		"[DEPRECATED] How long it took to complete the last restore operation (in seconds)",
	)
)
