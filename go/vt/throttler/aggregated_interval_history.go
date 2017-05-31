/*
Copyright 2017 Google Inc.

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

package throttler

import "time"

// aggregatedIntervalHistory aggregates data across multiple "intervalHistory"
// instances. An instance should be mapped to a thread.
type aggregatedIntervalHistory struct {
	threadCount      int
	historyPerThread []*intervalHistory
}

func newAggregatedIntervalHistory(capacity int64, interval time.Duration, threadCount int) *aggregatedIntervalHistory {
	historyPerThread := make([]*intervalHistory, threadCount)
	for i := 0; i < threadCount; i++ {
		historyPerThread[i] = newIntervalHistory(capacity, interval)
	}
	return &aggregatedIntervalHistory{
		threadCount:      threadCount,
		historyPerThread: historyPerThread,
	}
}

// addPerThread calls add() on the thread's intervalHistory instance.
func (h *aggregatedIntervalHistory) addPerThread(threadID int, record record) {
	h.historyPerThread[threadID].add(record)
}

// average aggregates the average of all intervalHistory instances.
func (h *aggregatedIntervalHistory) average(from, to time.Time) float64 {
	sum := 0.0
	for i := 0; i < h.threadCount; i++ {
		sum += h.historyPerThread[i].average(from, to)
	}
	return sum
}
