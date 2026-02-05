package logic

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/process"
)

func TestWaitForLocksRelease(t *testing.T) {
	oldShutdownWaitTime := shutdownWaitTime
	// Restore initial values
	defer func() {
		shutdownWaitTime = oldShutdownWaitTime
	}()

	t.Run("No locks to wait for", func(t *testing.T) {
		// Initially when shardsLockCounter is zero, waitForLocksRelease should run immediately
		timeSpent := waitForLocksReleaseAndGetTimeWaitedFor()
		assert.Less(t, timeSpent, 1*time.Second, "waitForLocksRelease should run immediately if there are no locks to wait for")
	})

	t.Run("Timeout from shutdownWaitTime", func(t *testing.T) {
		// Increment shardsLockCounter to simulate locking of a shard
		atomic.AddInt64(&shardsLockCounter, +1)
		defer func() {
			// Restore the initial value
			atomic.StoreInt64(&shardsLockCounter, 0)
		}()
		shutdownWaitTime = 200 * time.Millisecond
		timeSpent := waitForLocksReleaseAndGetTimeWaitedFor()
		assert.Greater(t, timeSpent, 100*time.Millisecond, "waitForLocksRelease should timeout after 200 milliseconds and not before")
		assert.Less(t, timeSpent, 300*time.Millisecond, "waitForLocksRelease should timeout after 200 milliseconds and not take any longer")
	})

	t.Run("Successful wait for locks release", func(t *testing.T) {
		// Increment shardsLockCounter to simulate locking of a shard
		atomic.AddInt64(&shardsLockCounter, +1)
		shutdownWaitTime = 500 * time.Millisecond
		// Release the locks after 200 milliseconds
		go func() {
			time.Sleep(200 * time.Millisecond)
			atomic.StoreInt64(&shardsLockCounter, 0)
		}()
		timeSpent := waitForLocksReleaseAndGetTimeWaitedFor()
		assert.Greater(t, timeSpent, 100*time.Millisecond, "waitForLocksRelease should wait for the locks and not return early")
		assert.Less(t, timeSpent, 300*time.Millisecond, "waitForLocksRelease should be successful after 200 milliseconds as all the locks are released")
	})
}

func waitForLocksReleaseAndGetTimeWaitedFor() time.Duration {
	start := time.Now()
	waitForLocksRelease()
	return time.Since(start)
}

func TestRefreshAllInformation(t *testing.T) {
	// Store the old flags and restore on test completion
	oldTs := ts
	defer func() {
		ts = oldTs
	}()

	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		db.ClearVTOrcDatabase()
	}()

	// Verify in the beginning, we have the first DiscoveredOnce field false.
	_, discoveredOnce := process.HealthTest()
	require.False(t, discoveredOnce)

	// Create a memory topo-server and create the keyspace and shard records
	ts = memorytopo.NewServer(context.Background(), cell1)
	_, err := ts.GetOrCreateShard(context.Background(), keyspace, shard)
	require.NoError(t, err)

	// Test error
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel context to simulate timeout
	require.Error(t, refreshAllInformation(ctx))
	require.False(t, process.FirstDiscoveryCycleComplete.Load())
	_, discoveredOnce = process.HealthTest()
	require.False(t, discoveredOnce)

	// Test success
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	require.NoError(t, refreshAllInformation(ctx2))
	require.True(t, process.FirstDiscoveryCycleComplete.Load())
	_, discoveredOnce = process.HealthTest()
	require.True(t, discoveredOnce)
}
