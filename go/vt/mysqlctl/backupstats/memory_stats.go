package backupstats

import "time"

type memoryStats struct {
	// bytes stores the sum of bytes passed to TimedIncrementBytes since the last
	// call to Reset.
	bytes int
	// calls stores the number of calls to TimedIncrement or
	// TimedIncrementBytesCalls since the last call to Reset.
	calls int
	// count stores the number of times TimedIncrement was called since the
	// last call to Reset.
	count int64
	// duration stores the sum of durations passed to TimedIncrement and
	// TimedIncrementBytes since the the last call to Reset.
	duration time.Duration
}

func newMemoryStats() *memoryStats {
	return &memoryStats{}
}

// TimedIncrement increments Count by 1 and Duration by d.
func (ms *memoryStats) TimedIncrement(d time.Duration) {
	ms.calls++
	ms.count++
	ms.duration += d
}

// TimedIncrementBytes increments Bytes by b and Duration by d.
func (ms *memoryStats) TimedIncrementBytes(b int, d time.Duration) {
	ms.bytes += b
	ms.calls++
	ms.duration += d
}

// Reset sets Bytes, Count, and Duration to zero.
func (ms *memoryStats) Reset() {
	ms.bytes = 0
	ms.count = 0
	ms.duration = 0
	ms.calls = 0
}
