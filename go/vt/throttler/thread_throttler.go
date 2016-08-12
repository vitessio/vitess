// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package throttler

import (
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sync2"
)

// threadThrottler implements the core logic which decides if a Throttle() call
// should be throttled (and for how long) or not.
// It does so by splitting the time into 1 second intervals. For example,
// the current rate is based on the number of requests received within the
// current second.
type threadThrottler struct {
	threadID int
	// maxRate is the maximum allowed rate.
	// If it gets updated, we won't consider it until the next second starts
	// because our throttler operates at a 1s granularity and doesn't support
	// changes within the same second.
	// For example in case of a steep rate increase at the end of the second,
	// it would let too many requests through for the remainder of the second:
	//   old rate = 10   (1 request every 100 ms)
	//   new rate = 1000 (1 request every   1 ms)
	//   milliseconds left in the second = 100 ms (i.e. 900 ms elapsed)
	//   => old rate would allow   1 more request
	//   => new rate would allow 991 more requests
	maxRate sync2.AtomicInt64

	// Fields below are unguarded because they must not be modified concurrently.
	// initialized is false if we must update the fields below first.
	initialized   bool
	currentSecond time.Time
	// maxRateSecond is the allowed max rate since the second started.
	// It won't change until currentSecond changes.
	maxRateSecond int64
	// currentRate is the number of allowed requests since currentSecond started.
	currentRate int64
	// nextRequestInterval is the time when the next request will be allowed.
	// Tracking this ensures there won't be more than one request per interval.
	nextRequestInterval time.Time

	actualRateHistory *aggregatedIntervalHistory
}

func newThreadThrottler(threadID int, actualRateHistory *aggregatedIntervalHistory) *threadThrottler {
	return &threadThrottler{
		threadID:          threadID,
		actualRateHistory: actualRateHistory,
	}
}

func (t *threadThrottler) throttle(now time.Time) time.Duration {
	// Initialize or advance the current second interval when necessary.
	nowSecond := now.Truncate(time.Second)
	if !t.initialized {
		t.resetSecond(nowSecond)
		t.initialized = true
	}
	if !t.currentSecond.Equal(nowSecond) {
		t.resetSecond(nowSecond)
	}

	maxRate := t.maxRateSecond
	if maxRate == ZeroRateNoProgess {
		// Throughput is effectively paused. Do not let anything through until
		// the max rate changes.
		return t.currentSecond.Add(1 * time.Second).Sub(now)
	}
	// Check if we have already received too many requests within this second.
	if t.currentRate >= maxRate {
		return t.currentSecond.Add(1 * time.Second).Sub(now)
	}

	// Next request isn't expected earlier than nextRequestInterval.
	// With this check we ensure there's one request per request interval at most.
	if now.Before(t.nextRequestInterval) {
		return t.nextRequestInterval.Sub(now)
	}

	// Check if we have to pace the user.
	// NOTE: Pacing won't work if maxRate > 1e9 (since 1e9ns = 1s) and therefore
	//       the returned backoff will always be zero.
	// Minimum time between two requests.
	requestIntervalNs := (1 * time.Second).Nanoseconds() / maxRate
	// End of the previous request is the earliest allowed time of this request.
	earliestArrivalOffsetNs := t.currentRate * requestIntervalNs
	earliestArrival := t.currentSecond.Add(time.Duration(earliestArrivalOffsetNs) * time.Nanosecond)
	// TODO(mberlin): Most likely we overshoot here since we don't take into
	// account our and the user's processing time. Due to too long backoffs, they
	// might not be able to fully use their capacity/maximum rate.
	backoff := earliestArrival.Sub(now)
	if backoff > 0 {
		return backoff
	}

	// Calculate the earlist time the next request can pass.
	requestInterval := time.Duration(requestIntervalNs) * time.Nanosecond
	currentRequestInterval := now.Truncate(requestInterval)
	t.nextRequestInterval = currentRequestInterval.Add(requestInterval)
	// QPS rates >= 10k are prone to skipping their next request interval.
	// We have to be more relaxed in this case.
	if requestInterval <= 100*time.Microsecond {
		t.nextRequestInterval = t.nextRequestInterval.Add(-requestInterval)
	}

	t.currentRate++
	return NotThrottled
}

func (t *threadThrottler) resetSecond(nowSecond time.Time) {
	if nowSecond.Before(t.currentSecond) {
		log.Warningf("Time did not increase monotonously. Make sure your system operates properly. time observed before: %v now: %v", t.currentSecond, nowSecond)
	}
	// Track rate from the past second.
	if !t.currentSecond.IsZero() {
		t.actualRateHistory.addPerThread(t.threadID, record{t.currentSecond, t.currentRate})
	}
	t.currentSecond = nowSecond
	t.maxRateSecond = t.maxRate.Get()
	t.currentRate = 0
	t.nextRequestInterval = nowSecond
}

func (t *threadThrottler) setMaxRate(rate int64) {
	t.maxRate.Set(rate)
}
