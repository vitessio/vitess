// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package throttler

import (
	"fmt"
	"time"
)

// intervalHistory stores a value per interval over time.
// For example, thread_trottler.go stores the number of requests per 1 second
// interval in an intervalHistory instance.
// This data is used by the MaxReplicationLagModule to determine the historic
// average value between two arbitrary points in time e.g. to find out the
// average actual throttler rate between two replication lag measurements.
// In general, the history should reflect only a short period of time (on the
// order of minutes) and is therefore bounded.
type intervalHistory struct {
	records           []record
	interval          time.Duration
	nextIntervalStart time.Time
}

func newIntervalHistory(capacity int64, interval time.Duration) *intervalHistory {
	return &intervalHistory{
		records:  make([]record, 0, capacity),
		interval: interval,
	}
}

// add
// It is up to the programmer to ensure that two add() calls do not cover the
// same interval.
func (h *intervalHistory) add(record record) {
	if record.time.Before(h.nextIntervalStart) {
		panic(fmt.Sprintf("BUG: cannot add record because it is already covered by a previous entry. record: %v next expected interval start: %v", record, h.nextIntervalStart))
	}
	if !record.time.Truncate(h.interval).Equal(record.time) {
		panic(fmt.Sprintf("BUG: cannot add record because it does not start at the beginning of the interval. record: %v", record))
	}
	// TODO(mberlin): Bound the list.
	h.records = append(h.records, record)
	h.nextIntervalStart = record.time.Add(h.interval)
}

// average returns the average value across all observations which span
// the range [from, to).
// Partially included observations are accounted by their included fraction.
// Missing observations are assumed with the value zero.
func (h *intervalHistory) average(from, to time.Time) float64 {
	// Search only entries whose time of observation is in [start, end).
	// Example: [from, to) = [1.5s, 2.5s) => [start, end) = [1s, 2s)
	start := from.Truncate(h.interval)
	end := to.Truncate(h.interval)

	sum := 0.0
	count := 0.0
	var nextIntervalStart time.Time
	for i := len(h.records) - 1; i >= 0; i-- {
		t := h.records[i].time

		if t.After(end) {
			continue
		}
		if t.Before(start) {
			break
		}

		// Account for intervals which were not recorded.
		if !nextIntervalStart.IsZero() {
			uncoveredRange := nextIntervalStart.Sub(t)
			count += float64(uncoveredRange / h.interval)
		}

		// If an interval is only partially included, count only that fraction.
		durationAfterTo := t.Add(h.interval).Sub(to)
		if durationAfterTo < 0 {
			durationAfterTo = 0
		}
		durationBeforeFrom := from.Sub(t)
		if durationBeforeFrom < 0 {
			durationBeforeFrom = 0
		}
		weight := float64((h.interval - durationBeforeFrom - durationAfterTo).Nanoseconds()) / float64(h.interval.Nanoseconds())

		sum += weight * float64(h.records[i].value)
		count += weight
		nextIntervalStart = t.Add(-1 * h.interval)
	}

	return float64(sum) / count
}
