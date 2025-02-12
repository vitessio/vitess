package smartconnpool

import (
	"math"
	"sync/atomic"
	"time"
)

var monotonicRoot = time.Now()

// timestamp is a monotonic point in time, stored as a number of
// nanoseconds since the monotonic root. This type is only 8 bytes
// and hence can always be accessed atomically
type timestamp struct {
	nano atomic.Int64
}

// timestampExpired is a special value that means this timestamp is now past
// an arbitrary expiration point, and hence doesn't need to store
const timestampExpired = math.MaxInt64

// timestampBusy is a special value that means this timestamp no longer
// tracks an expiration point
const timestampBusy = math.MinInt64

// monotonicNow returns the current monotonic time as a time.Duration.
// This is a very efficient operation because time.Since performs direct
// substraction of monotonic times without considering the wall clock times.
func monotonicNow() time.Duration {
	return time.Since(monotonicRoot)
}

// monotonicFromTime converts a wall-clock time from time.Now into a
// monotonic timestamp.
// This is a very efficient operation because time.(*Time).Sub performs direct
// substraction of monotonic times without considering the wall clock times.
func monotonicFromTime(now time.Time) time.Duration {
	return now.Sub(monotonicRoot)
}

// set sets this timestamp to the given monotonic value
func (t *timestamp) set(mono time.Duration) {
	t.nano.Store(int64(mono))
}

// get returns the monotonic time of this timestamp as the number of nanoseconds
// since the monotonic root.
func (t *timestamp) get() time.Duration {
	return time.Duration(t.nano.Load())
}

// elapsed returns the number of nanoseconds that have passed since
// this timestamp was updated
func (t *timestamp) elapsed() time.Duration {
	return monotonicNow() - t.get()
}

// update sets this timestamp's value to the current monotonic time
func (t *timestamp) update() {
	t.nano.Store(int64(monotonicNow()))
}

// borrow attempts to borrow this timestamp atomically.
// It only succeeds if we can ensure that nobody else has marked
// this timestamp as expired. When succeeded, the timestamp
// is cleared as "busy" as it no longer tracks an expiration point.
func (t *timestamp) borrow() bool {
	stamp := t.nano.Load()
	switch stamp {
	case timestampExpired:
		return false
	case timestampBusy:
		panic("timestampBusy when borrowing a time")
	default:
		return t.nano.CompareAndSwap(stamp, timestampBusy)
	}
}

// expired attempts to atomically expire this timestamp.
// It only succeeds if we can ensure the timestamp hasn't been
// concurrently expired or borrowed.
func (t *timestamp) expired(now time.Duration, timeout time.Duration) bool {
	stamp := t.nano.Load()
	if stamp == timestampExpired {
		return false
	}
	if stamp == timestampBusy {
		return false
	}
	if now-time.Duration(stamp) > timeout {
		return t.nano.CompareAndSwap(stamp, timestampExpired)
	}
	return false
}
