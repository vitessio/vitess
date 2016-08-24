package throttler

import (
	"testing"
	"time"
)

func TestAggregatedIntervalHistory(t *testing.T) {
	h := newAggregatedIntervalHistory(10, 1*time.Second, 2)
	h.addPerThread(0, record{sinceZero(0 * time.Second), 1000})
	h.addPerThread(1, record{sinceZero(0 * time.Second), 2000})

	if got, want := h.average(sinceZero(250*time.Millisecond), sinceZero(750*time.Millisecond)), 3000.0; got != want {
		t.Errorf("average(0.25s, 0.75s) across both threads = %v, want = %v", got, want)
	}
}
