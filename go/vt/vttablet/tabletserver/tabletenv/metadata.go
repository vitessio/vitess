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

package tabletenv

import (
	"os"
	"strconv"
	"time"
)

// TabletMetadata holds runtime metadata about a tablet that is not configuration.
// This is analogous to Stats (runtime state) vs Config (settings).
// The metadata is used for features like replica warming, where VTGate needs
// to know when a tablet started in order to route traffic appropriately.
type TabletMetadata struct {
	startTime time.Time
}

// NewTabletMetadata creates metadata with the current time as start time.
// Checks VTTEST_TABLET_START_TIME env var for test overrides.
func NewTabletMetadata() *TabletMetadata {
	startTime := time.Now()

	// Allow tests to override start time via environment variable.
	// This is used by e2e tests to simulate "old" and "new" tablets
	// without waiting for real time to pass.
	if startTimeStr := os.Getenv("VTTEST_TABLET_START_TIME"); startTimeStr != "" {
		if startTimeSec, err := strconv.ParseInt(startTimeStr, 10, 64); err == nil {
			startTime = time.Unix(startTimeSec, 0)
		}
	}

	return &TabletMetadata{startTime: startTime}
}

// StartTime returns when this tablet started.
func (tm *TabletMetadata) StartTime() time.Time {
	return tm.startTime
}

// StartTimeUnix returns the start time as Unix seconds (for proto serialization).
func (tm *TabletMetadata) StartTimeUnix() int64 {
	return tm.startTime.Unix()
}

// SetStartTimeForTest allows tests to override the start time.
// This should only be called from test code.
func (tm *TabletMetadata) SetStartTimeForTest(t time.Time) {
	tm.startTime = t
}
