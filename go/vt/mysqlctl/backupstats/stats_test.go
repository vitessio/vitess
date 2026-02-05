package backupstats

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/stats"
)

func TestBackupStats(t *testing.T) {
	require.Nil(t, backupBytes)
	require.Nil(t, backupCount)
	require.Nil(t, backupDurationNs)
	require.Nil(t, restoreCount)
	require.Nil(t, restoreDurationNs)

	BackupStats()
	defer resetStats()

	require.NotNil(t, backupBytes)
	require.NotNil(t, backupCount)
	require.NotNil(t, backupDurationNs)
	require.Nil(t, restoreBytes)
	require.Nil(t, restoreCount)
	require.Nil(t, restoreDurationNs)
}

func TestRestoreStats(t *testing.T) {
	require.Nil(t, backupBytes)
	require.Nil(t, backupCount)
	require.Nil(t, backupDurationNs)
	require.Nil(t, restoreCount)
	require.Nil(t, restoreDurationNs)

	RestoreStats()
	defer resetStats()

	require.Nil(t, backupBytes)
	require.Nil(t, backupCount)
	require.Nil(t, backupDurationNs)
	require.NotNil(t, restoreBytes)
	require.NotNil(t, restoreCount)
	require.NotNil(t, restoreDurationNs)
}

func TestScope(t *testing.T) {
	bytes := stats.NewCountersWithMultiLabels("TestScopeBytes", "", labels)
	count := stats.NewCountersWithMultiLabels("TestScopeCount", "", labels)
	durationNs := stats.NewCountersWithMultiLabels("TestScopeDurationNs", "", labels)

	duration := 10 * time.Second

	stats1 := newScopedStats(bytes, count, durationNs, nil)
	path1 := strings.Join([]string{unscoped, unscoped, unscoped}, ".")

	stats2 := stats1.Scope(Component(BackupEngine), Implementation("Test"))
	path2 := strings.Join([]string{BackupEngine.String(), "Test", unscoped}, ".")

	// New stats2 with new scope, let's test:
	// - TimedIncrement on new stats1 increments stats1 scope but not stats2.
	// - TimedIncrement on new stats2 increments stats2 scope but not stats1.
	stats1.TimedIncrement(duration)

	require.Equal(t, 1, len(count.Counts()))
	require.Equal(t, int64(1), count.Counts()[path1])
	require.Equal(t, 1, len(durationNs.Counts()))
	require.Equal(t, duration.Nanoseconds(), durationNs.Counts()[path1])

	stats2.TimedIncrement(duration)

	require.Equal(t, 2, len(count.Counts()))
	require.Equal(t, int64(1), count.Counts()[path1])
	require.Equal(t, int64(1), count.Counts()[path2])
	require.Equal(t, 2, len(durationNs.Counts()))
	require.Equal(t, duration.Nanoseconds(), durationNs.Counts()[path1])
	require.Equal(t, duration.Nanoseconds(), durationNs.Counts()[path2])

	// Next let's test that:
	// - We cannot rescope a ScopeType once it's been set.
	// - We can scope a ScopeType that is not yet set.
	stats3 := stats2.Scope(
		Component(BackupStorage),     /* not rescoped, because Component already set on stats2. */
		Implementation("TestChange"), /* not rescoped, because Implementation already set on stats2 */
		Operation("Test"),            /* scoped, because Operation not yet set on stats2 */
	)
	path3 := strings.Join([]string{BackupEngine.String(), "Test", "Test"}, ".")
	stats3.TimedIncrement(duration)

	require.Equal(t, 3, len(count.Counts()))
	require.Equal(t, int64(1), count.Counts()[path1])
	require.Equal(t, int64(1), count.Counts()[path2])
	require.Equal(t, int64(1), count.Counts()[path3])
	require.Equal(t, 3, len(durationNs.Counts()))
	require.Equal(t, duration.Nanoseconds(), durationNs.Counts()[path1])
	require.Equal(t, duration.Nanoseconds(), durationNs.Counts()[path2])
	require.Equal(t, duration.Nanoseconds(), durationNs.Counts()[path3])
}

func TestStatsAreNotInitializedByDefault(t *testing.T) {
	require.Nil(t, backupBytes)
	require.Nil(t, backupCount)
	require.Nil(t, backupDurationNs)
	require.Nil(t, restoreBytes)
	require.Nil(t, restoreCount)
	require.Nil(t, restoreDurationNs)
}

func TestTimedIncrement(t *testing.T) {
	bytes := stats.NewCountersWithMultiLabels(fmt.Sprintf("%s_test_timed_increment_bytes", t.Name()), "", labels)
	count := stats.NewCountersWithMultiLabels(fmt.Sprintf("%s_test_timed_increment_count", t.Name()), "", labels)
	durationNs := stats.NewCountersWithMultiLabels(fmt.Sprintf("%s_test_timed_increment_duration_ns", t.Name()), "", labels)

	stats := newScopedStats(bytes, count, durationNs, nil)

	duration := 10 * time.Second
	path := strings.Join([]string{unscoped, unscoped, unscoped}, ".")

	stats.TimedIncrement(duration)

	require.Equal(t, 0, len(bytes.Counts()))

	require.Equal(t, 1, len(count.Counts()))
	require.Equal(t, int64(1), count.Counts()[path])

	require.Equal(t, 1, len(durationNs.Counts()))
	require.Equal(t, duration.Nanoseconds(), durationNs.Counts()[path])

	stats.TimedIncrement(duration)

	require.Equal(t, 0, len(bytes.Counts()))

	require.Equal(t, 1, len(count.Counts()))
	require.Equal(t, int64(2), count.Counts()[path])

	require.Equal(t, 1, len(durationNs.Counts()))
	require.Equal(t, 2*duration.Nanoseconds(), durationNs.Counts()[path])
}

func TestTimedIncrementBytes(t *testing.T) {
	bytes := stats.NewCountersWithMultiLabels(fmt.Sprintf("%s_test_timed_increment_bytes", t.Name()), "", labels)
	count := stats.NewCountersWithMultiLabels(fmt.Sprintf("%s_test_timed_increment_count", t.Name()), "", labels)
	durationNs := stats.NewCountersWithMultiLabels(fmt.Sprintf("%s_test_timed_increment_duration_ns", t.Name()), "", labels)

	stats := newScopedStats(bytes, count, durationNs, nil)

	incBytes := 1024
	duration := 10 * time.Second
	path := strings.Join([]string{unscoped, unscoped, unscoped}, ".")

	stats.TimedIncrementBytes(incBytes, duration)

	require.Equal(t, 1, len(bytes.Counts()))
	require.Equal(t, int64(incBytes), bytes.Counts()[path])

	require.Equal(t, 0, len(count.Counts()))

	require.Equal(t, 1, len(durationNs.Counts()))
	require.Equal(t, duration.Nanoseconds(), durationNs.Counts()[path])

	stats.TimedIncrementBytes(incBytes, duration)

	require.Equal(t, 1, len(bytes.Counts()))
	require.Equal(t, int64(2*incBytes), bytes.Counts()[path])

	require.Equal(t, 0, len(count.Counts()))

	require.Equal(t, 1, len(durationNs.Counts()))
	require.Equal(t, 2*duration.Nanoseconds(), durationNs.Counts()[path])
}

func resetStats() {
	backupBytes = nil
	backupCount = nil
	backupDurationNs = nil
	restoreBytes = nil
	restoreCount = nil
	restoreDurationNs = nil
}
