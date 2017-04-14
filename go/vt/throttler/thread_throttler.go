// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package throttler

import (
	"fmt"
	"time"

	"github.com/youtube/vitess/go/sync2"

	"golang.org/x/time/rate"
)

// threadThrottler implements the core logic which decides if a Throttle() call
// should be throttled (and for how long) or not.
type threadThrottler struct {
	threadID int

	// Fields below are unguarded because they must not be modified concurrently.
	// currentSecond is the last time throttle() was called. The value is truncated to a
	// second granularity.
	currentSecond time.Time
	// currentRate is the number of allowed requests since 'currentSecond'.
	currentRate int64

	// maxRate holds the last rate set by setMaxRate. It will be set
	// in the limiter object in the next call to throttle().
	maxRate           sync2.AtomicInt64
	limiter           *rate.Limiter
	actualRateHistory *aggregatedIntervalHistory
}

func newThreadThrottler(threadID int, actualRateHistory *aggregatedIntervalHistory) *threadThrottler {
	// Initially, the set rate is 0 and the throttler should deny all requests. We'd like the first
	// throttle() call to be accepted after setMaxRate() has been called with a nonzero rate.
	// Unfortunately, if we initialize the limiter rate to 0, the internal token buffer will be
	// empty by the time the first throttle() call is executed and it will be denied.
	// Instead, we initialize the limiter rate to 1. This way the token buffer will be full (assuming
	// the 'now' parameter of the first throttle() call is at least 1 second) and the rate will
	// be reset to 0 if setMaxRate() has not been called with a nonzero rate.
	result := threadThrottler{
		threadID:          threadID,
		actualRateHistory: actualRateHistory,
		limiter:           rate.NewLimiter(1 /* limit */, 1 /* burst */),
	}
	return &result
}

var (
	oneSecond = time.Time{}.Add(1 * time.Second)
)

func (t *threadThrottler) throttle(now time.Time) time.Duration {
	if now.Before(oneSecond) {
		panic(fmt.Sprintf(
			"BUG: throttle() must not be called with a time of less than 1 second. now: %v",
			now))
	}

	// Pass the limit set by the last call to setMaxRate. Limiter.SetLimitAt
	// is idempotent, so we can call it with the same value more than once without
	// issues.
	t.limiter.SetLimitAt(now, rate.Limit(t.maxRate.Get()))

	// Initialize or advance the current second interval when necessary.
	nowSecond := now.Truncate(time.Second)
	if t.currentSecond != nowSecond {
		// Report the number of successful (not-throttled) requests from the "last" second if this is
		// not the first time 'throttle' is called.
		if !t.currentSecond.IsZero() {
			t.actualRateHistory.addPerThread(t.threadID, record{t.currentSecond, t.currentRate})
		}
		t.currentRate = 0
		t.currentSecond = nowSecond
	}

	if t.limiter.Limit() == 0 {
		// If the limit is 0 this request won't be let through. However, the caller
		// should poll again in the future in case the limit has changed.
		return 1 * time.Second
	}

	// Figure out how long to backoff: We use the limiter.ReserveN() method to reserve an event.
	// The returned reservation contains the backoff delay. We cancel the reservation if the
	// delay is greater than 0, since the caller is expected to call throttle() again at that time
	// rather than proceed.
	reservation := t.limiter.ReserveN(now, 1)
	if !reservation.OK() {
		panic(fmt.Sprintf("BUG: limiter was unable to reserve an event. "+
			"threadThrottler: %v, reservation:%v", *t, *reservation))
	}
	waitDuration := reservation.DelayFrom(now)
	if waitDuration <= 0 {
		t.currentRate++
		return NotThrottled
	}
	reservation.CancelAt(now)
	return waitDuration
}

// setMaxRate sets the maximum rate for the next time throttle() is called.
// setMaxRate() can be called concurrently with other methods of this object.
func (t *threadThrottler) setMaxRate(newRate int64) {
	t.maxRate.Set(newRate)
}

// maxRate returns the rate set by the last call to setMaxRate().
// If setMaxRate() was not called, this method returns 0.
func (t *threadThrottler) getMaxRate() int64 {
	return t.maxRate.Get()
}
