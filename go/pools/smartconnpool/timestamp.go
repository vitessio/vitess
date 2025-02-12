package smartconnpool

import (
	"math"
	"sync/atomic"
	"time"
)

var monotonicRoot = time.Now()

type timestamp struct {
	nano atomic.Int64
}

const timestampExpired = math.MaxInt64
const timestampBusy = math.MinInt64

func (t *timestamp) update() {
	t.nano.Store(int64(monotonicNow()))
}

func monotonicNow() time.Duration {
	return time.Since(monotonicRoot)
}

func monotonicFromTime(now time.Time) time.Duration {
	return now.Sub(monotonicRoot)
}

func (t *timestamp) set(mono time.Duration) {
	t.nano.Store(int64(mono))
}

func (t *timestamp) get() time.Duration {
	return time.Duration(t.nano.Load())
}

func (t *timestamp) elapsed() time.Duration {
	return monotonicNow() - t.get()
}

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
