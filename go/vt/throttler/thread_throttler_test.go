package throttler

import (
	"testing"
	"time"
)

func TestThrottle_RateIncreaseDoesNotBurst(t *testing.T) {
	// If we're within a second, a rate increase won't apply retroactively.
	// This way we prevent a burst of requests at the end of the second.
	tt := newThreadThrottler(0)
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
