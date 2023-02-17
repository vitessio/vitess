package backupstats

import (
	"sync"
	"time"
)

// ThrottledStats is a Stats object that throttles and batches calls to
// TimedIncrement and TimedIncrementBytes when they occur more than once within
// a specified time interval. Unflushed calls are flushed when a subsequent call
// is made that is at least the specified time interval after the last flush
// time.
type ThrottledStats struct {
	timedCount *memoryStats
	timedBytes *memoryStats

	children      []*ThrottledStats
	lastFlushTime time.Time
	maxInterval   time.Duration
	mu            *sync.Mutex
	stats         Stats
}

// Throttle wraps the provided stats object so that calls to TimedIncrement and
// TimedIncrementBytes are passed to the provided stats object no more than
// once per the specified maxInterval.
func Throttle(stats Stats, maxInterval time.Duration) *ThrottledStats {
	return &ThrottledStats{
		timedCount: newMemoryStats(),
		timedBytes: newMemoryStats(),

		children:    make([]*ThrottledStats, 0),
		maxInterval: maxInterval,
		mu:          &sync.Mutex{},
		stats:       stats,
	}
}

// Flush flushes any unflushed TimedIncrement or TimedIncrementBytes calls to
// the wrapped Stats object. It also flushes any children created with Scope.
func (ts *ThrottledStats) Flush() {
	ts.flush()
	for _, c := range ts.children {
		c.Flush()
	}
}

// Scope scopes the wrapped Stats object with the provided scopes, and returns
// a new ThrottledStats object wrapping that new Stats object.
//
// Scope is safe for use across goroutines.
func (ts *ThrottledStats) Scope(scopes ...Scope) Stats {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	stats := ts.stats.Scope(scopes...)
	newTs := Throttle(stats, ts.maxInterval)
	ts.children = append(ts.children, newTs)
	return newTs
}

// Stats returns the wrapped Stats object.
func (ts *ThrottledStats) Stats() Stats {
	return ts.stats
}

// TimedIncrement batches the provided duration d with any previous, unflushed
// calls to TimedIncrement, and flushes all unflushed calls if enough time has
// elapsed since the last flush.
func (ts *ThrottledStats) TimedIncrement(d time.Duration) {
	ts.timedCount.TimedIncrement(d)
	ts.maybeFlush()
}

// TimedIncrementBytes batches the provided bytes b and duration d with any
// previous, unflushed calls to TimedIncrementBytes, and flushes all unflushed
// calls if enough time has elapsed since the last flush.
func (ts *ThrottledStats) TimedIncrementBytes(b int, d time.Duration) {
	ts.timedBytes.TimedIncrementBytes(b, d)
	ts.maybeFlush()
}

func (ts *ThrottledStats) flush() {
	if ts.timedCount.calls > 0 {
		ts.stats.TimedIncrement(ts.timedCount.duration)
		ts.timedCount.Reset()
	}
	if ts.timedBytes.calls > 0 {
		ts.stats.TimedIncrementBytes(ts.timedBytes.bytes, ts.timedBytes.duration)
		ts.timedBytes.Reset()
	}
}

func (ts *ThrottledStats) maybeFlush() {
	now := time.Now()
	emitWaitTime := ts.maxInterval - now.Sub(ts.lastFlushTime)
	if emitWaitTime < 0 {
		ts.flush()
		ts.lastFlushTime = now
	}
}
