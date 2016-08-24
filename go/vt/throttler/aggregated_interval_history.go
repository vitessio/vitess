// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
