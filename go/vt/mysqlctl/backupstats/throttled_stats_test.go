package backupstats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestThrottledStatsFlush(t *testing.T) {
	fakeStats := NewFakeStats()
	throttled := Throttle(fakeStats, 5*time.Second)

	// First call to TimedIncrement is not throttled.
	throttled.TimedIncrement(5 * time.Minute)

	require.Len(t, fakeStats.TimedIncrementCalls, 1)
	require.Equal(t, 5*time.Minute, fakeStats.TimedIncrementCalls[0])

	// Subsequent calls are throttled, batched and combined...
	throttled.TimedIncrement(5 * time.Minute)
	throttled.TimedIncrement(5 * time.Minute)

	require.Len(t, fakeStats.TimedIncrementCalls, 1)

	// ...and are emitted when Flush() is called.
	throttled.Flush()

	require.Len(t, fakeStats.TimedIncrementCalls, 2)
	require.Equal(t, 10*time.Minute, fakeStats.TimedIncrementCalls[1])

	// The next call to Flush() is a no-op because there is nothing to flush.
	throttled.Flush()

	require.Len(t, fakeStats.TimedIncrementCalls, 2)
	require.Equal(t, 10*time.Minute, fakeStats.TimedIncrementCalls[1])

	// Children are also flushed.
	childThrottled1 := throttled.Scope(Component(BackupEngine)).(*ThrottledStats)
	childStats1 := childThrottled1.Stats().(*FakeStats)
	childThrottled2 := throttled.Scope(Component(BackupEngine)).(*ThrottledStats)
	childStats2 := childThrottled2.Stats().(*FakeStats)

	require.Len(t, childStats1.TimedIncrementCalls, 0)
	require.Len(t, childStats2.TimedIncrementCalls, 0)

	childThrottled1.TimedIncrement(5 * time.Minute)
	childThrottled2.TimedIncrement(5 * time.Minute)

	require.Len(t, childStats1.TimedIncrementCalls, 1)
	require.Len(t, childStats2.TimedIncrementCalls, 1)

	childThrottled1.TimedIncrement(5 * time.Minute)
	childThrottled2.TimedIncrement(5 * time.Minute)

	require.Len(t, childStats1.TimedIncrementCalls, 1)
	require.Len(t, childStats2.TimedIncrementCalls, 1)

	throttled.Flush()

	require.Len(t, childStats1.TimedIncrementCalls, 2)
	require.Len(t, childStats2.TimedIncrementCalls, 2)
}

func TestThrottledStatsScope(t *testing.T) {
	fakeStats := NewFakeStats()
	throttled := Throttle(fakeStats, 5*time.Second)

	// Get a scoped throttled stats, and the underlying scoped fake stats.
	scopedThrottled := throttled.Scope(Component(BackupEngine))
	require.Len(t, fakeStats.ScopeCalls, 1)
	require.Len(t, fakeStats.ScopeReturns, 1)

	scopedFakeStats := fakeStats.ScopeReturns[0].(*FakeStats)
	require.Equal(t, BackupEngine.String(), scopedFakeStats.ScopeV[ScopeComponent])

	// Validate that changes to the parent stats don't change the child, and
	// vice versa.
	throttled.TimedIncrement(5 * time.Second)
	require.Len(t, fakeStats.TimedIncrementCalls, 1)
	require.Len(t, scopedFakeStats.TimedIncrementCalls, 0)

	scopedThrottled.TimedIncrement(5 * time.Second)
	require.Len(t, fakeStats.TimedIncrementCalls, 1)
	require.Len(t, scopedFakeStats.TimedIncrementCalls, 1)
}

func TestThrottledStatsTimedIncrement(t *testing.T) {
	fakeStats := NewFakeStats()
	throttled := Throttle(fakeStats, 1*time.Second)

	throttled.TimedIncrement(5 * time.Minute)

	require.Len(t, fakeStats.TimedIncrementCalls, 1)
	require.Equal(t, 5*time.Minute, fakeStats.TimedIncrementCalls[0])

	throttled.TimedIncrement(5 * time.Minute)

	require.Len(t, fakeStats.TimedIncrementCalls, 1)

	time.Sleep(500 * time.Millisecond)

	require.Len(t, fakeStats.TimedIncrementCalls, 1)

	time.Sleep(501 * time.Millisecond)

	throttled.TimedIncrement(5 * time.Minute)

	require.Len(t, fakeStats.TimedIncrementCalls, 2)
	require.Equal(t, 10*time.Minute, fakeStats.TimedIncrementCalls[1])
}

func TestThrottledStatsTimedIncrementBytes(t *testing.T) {
	fakeStats := NewFakeStats()
	throttled := Throttle(fakeStats, 1*time.Second)

	throttled.TimedIncrementBytes(5, 5*time.Minute)

	require.Len(t, fakeStats.TimedIncrementBytesCalls, 1)
	require.Equal(t, 5, fakeStats.TimedIncrementBytesCalls[0].Bytes)
	require.Equal(t, 5*time.Minute, fakeStats.TimedIncrementBytesCalls[0].Duration)

	throttled.TimedIncrementBytes(5, 5*time.Minute)

	require.Len(t, fakeStats.TimedIncrementBytesCalls, 1)

	time.Sleep(500 * time.Millisecond)

	require.Len(t, fakeStats.TimedIncrementBytesCalls, 1)

	time.Sleep(501 * time.Millisecond)

	throttled.TimedIncrementBytes(5, 5*time.Minute)

	require.Len(t, fakeStats.TimedIncrementBytesCalls, 2)
	require.Equal(t, 10, fakeStats.TimedIncrementBytesCalls[1].Bytes)
	require.Equal(t, 10*time.Minute, fakeStats.TimedIncrementBytesCalls[1].Duration)
}
