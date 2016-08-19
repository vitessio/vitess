// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package throttler

import (
	"testing"
	"time"
)

func TestThrottle_NoBurstDueToLateStart(t *testing.T) {
	// Within a request interval, there must be one request at most.
	// Unused request intervals e.g. due to a late start, cannot be used.
	tt := newThreadThrottler(0, newAggregatedIntervalHistory(1, 1*time.Second, 1))
	// Two request intervals (chunks) in this second: [0, 500 ms), [500 ms, 1 s)
	tt.setMaxRate(2)
	// First request interval is unused. 1 query out of 2 QPS is wasted.
	// One request in second request interval goes through.
	if gotBackoff := tt.throttle(sinceZero(500 * time.Millisecond)); gotBackoff != NotThrottled {
		t.Fatalf("throttler should not have throttled us: backoff = %v", gotBackoff)
	}
	// More requests are not allowed in the same interval.
	wantBackoff := 499 * time.Millisecond
	if gotBackoff := tt.throttle(sinceZero(501 * time.Millisecond)); gotBackoff != wantBackoff {
		t.Fatalf("throttler should have throttled us. got = %v, want = %v", gotBackoff, wantBackoff)
	}
}

func TestThrottle_BurstsForHighQPSAllowed(t *testing.T) {
	// For high QPS rates, we don't enforce the invariant of one request per
	// request interval at most. Test this with a 10k QPS rate.
	// The reason for that is that a time.Sleep() on the returned backoff
	// duration is too coarse-grained and might be too long. In consequence,
	// the user might miss many of their request intervals and won't be able
	// to achieve the configured QPS rate. For example, at 10k QPS, the user
	// must not miss any of the 100 us request intervals to not waste capacity.
	tt := newThreadThrottler(0, newAggregatedIntervalHistory(1, 1*time.Second, 1))
	qps := 10 * 1000
	tt.setMaxRate(int64(qps))
	requestInterval := 100 * time.Microsecond
	lastRequestInterval := time.Second - requestInterval
	// 10k requests are allowed to burst within a 100 us request interval.
	for i := 0; i < qps; i++ {
		if gotBackoff := tt.throttle(sinceZero(lastRequestInterval)); gotBackoff != NotThrottled {
			t.Fatalf("throttler should not have throttled us: index = %v, backoff = %v", i, gotBackoff)
		}
	}
	// More requests are not allowed in the same interval.
	if gotBackoff := tt.throttle(sinceZero(lastRequestInterval)); gotBackoff != requestInterval {
		t.Fatalf("throttler should have throttled us. got = %v, want = %v", gotBackoff, requestInterval)
	}
}

func TestThrottle_RateIncreaseDoesNotBurst(t *testing.T) {
	// If we're within a second, a rate increase won't apply retroactively.
	// This way we prevent a burst of requests at the end of the second.
	tt := newThreadThrottler(0, newAggregatedIntervalHistory(1, 1*time.Second, 1))
	tt.setMaxRate(2)
	// 1st request.
	if gotBackoff := tt.throttle(sinceZero(0 * time.Millisecond)); gotBackoff != NotThrottled {
		t.Fatalf("throttler should not have throttled us: backoff = %v", gotBackoff)
	}

	// 2nd request after rate increase (allowed based on old rate limit).
	tt.setMaxRate(1000)
	if gotBackoff := tt.throttle(sinceZero(500 * time.Millisecond)); gotBackoff != NotThrottled {
		t.Fatalf("throttler should not have throttled us: backoff = %v", gotBackoff)
	}

	// 3rd request not allowed since the old limit still applies.
	wantBackoff := 499 * time.Millisecond
	if gotBackoff := tt.throttle(sinceZero(501 * time.Millisecond)); gotBackoff != wantBackoff {
		t.Fatalf("throttler should have throttled us. got = %v, want = %v", gotBackoff, wantBackoff)
	}

	// More than two requests succeed starting with the next limit.
	if gotBackoff := tt.throttle(sinceZero(1000 * time.Millisecond)); gotBackoff != NotThrottled {
		t.Fatalf("throttler should not have throttled us: backoff = %v", gotBackoff)
	}
	if gotBackoff := tt.throttle(sinceZero(1001 * time.Millisecond)); gotBackoff != NotThrottled {
		t.Fatalf("throttler should not have throttled us: backoff = %v", gotBackoff)
	}
	if gotBackoff := tt.throttle(sinceZero(1002 * time.Millisecond)); gotBackoff != NotThrottled {
		t.Fatalf("throttler should not have throttled us: backoff = %v", gotBackoff)
	}
}
