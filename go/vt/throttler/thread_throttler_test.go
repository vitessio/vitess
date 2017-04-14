// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package throttler

import (
	"testing"
	"time"
)

func TestThrottle_NoBurst(t *testing.T) {
	tt := newThreadThrottler(0, newAggregatedIntervalHistory(1, 1*time.Second, 1))
	tt.setMaxRate(2)
	// We set the rate to 2 requests per second, and internally the throttler uses a burst value of
	// 1. This means that in any time interval of length t seconds, the throttler should
	// not allow more than floor(2*t+1) requests. For example, in the interval [1500ms, 1501ms], of
	// length 1ms, we shouldn't be able to send more than floor(2*10^-3+1)=1 requests.
	if gotBackoff := tt.throttle(sinceZero(1500 * time.Millisecond)); gotBackoff != NotThrottled {
		t.Fatalf("throttler should not have throttled us: backoff = %v", gotBackoff)
	}
	wantBackoff := 499 * time.Millisecond
	if gotBackoff := tt.throttle(sinceZero(1501 * time.Millisecond)); gotBackoff != wantBackoff {
		t.Fatalf("throttler should have throttled us. got = %v, want = %v", gotBackoff, wantBackoff)
	}
}
