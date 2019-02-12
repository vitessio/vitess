/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package throttler

import (
	"time"
)

// fakeRatesHistory simplifies faking the actual throttler rate for each
// 1 second interval. With the add() method up to a given time all missing
// intervals will be filled in with a given rate.
type fakeRatesHistory struct {
	*aggregatedIntervalHistory

	nextIntervalStart time.Time
	interval          time.Duration
}

func newFakeRatesHistory() *fakeRatesHistory {
	return &fakeRatesHistory{
		aggregatedIntervalHistory: newAggregatedIntervalHistory(1000, 1*time.Second, 1),
		interval:                  1 * time.Second,
	}
}

// add records the rate from the end of the last recorded interval up to
// (and including) the time "to".
// The first call to add() starts at the interval of 0s.
func (h *fakeRatesHistory) add(to time.Time, rate int64) {
	for h.nextIntervalStart.Before(to) {
		r := record{h.nextIntervalStart, rate}
		h.addPerThread(0 /* threadID */, r)
		h.nextIntervalStart = h.nextIntervalStart.Add(h.interval)
	}
}
