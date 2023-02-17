package backupstats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewMemoryStats(t *testing.T) {
	ms := newMemoryStats()

	require.Equal(t, 0, ms.bytes)
	require.Equal(t, int64(0), ms.count)
	require.Equal(t, time.Duration(0), ms.duration)
}

func TestIncrement(t *testing.T) {
	ms := newMemoryStats()

	ms.TimedIncrement(5 * time.Minute)
	require.Equal(t, 0, ms.bytes)
	require.Equal(t, int64(1), ms.count)
	require.Equal(t, 5*time.Minute, ms.duration)

	ms.TimedIncrement(10 * time.Minute)
	require.Equal(t, 0, ms.bytes)
	require.Equal(t, int64(2), ms.count)
	require.Equal(t, 15*time.Minute, ms.duration)
}

func TestIncrementBytes(t *testing.T) {
	ms := newMemoryStats()

	ms.TimedIncrementBytes(5, 5*time.Minute)
	require.Equal(t, 5, ms.bytes)
	require.Equal(t, int64(0), ms.count)
	require.Equal(t, 5*time.Minute, ms.duration)

	ms.TimedIncrementBytes(10, 10*time.Minute)
	require.Equal(t, 15, ms.bytes)
	require.Equal(t, int64(0), ms.count)
	require.Equal(t, 15*time.Minute, ms.duration)
}

func TestReset(t *testing.T) {
	ms := newMemoryStats()

	ms.TimedIncrement(5 * time.Minute)
	ms.TimedIncrementBytes(5, 5*time.Minute)

	ms.Reset()

	require.Equal(t, 0, ms.bytes)
	require.Equal(t, int64(0), ms.count)
	require.Equal(t, time.Duration(0), ms.duration)
}
