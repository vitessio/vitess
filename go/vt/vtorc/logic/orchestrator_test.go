package logic

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
		atomic.AddInt32(&shardsLockCounter, +1)
		defer func() {
			// Restore the initial value
			atomic.StoreInt32(&shardsLockCounter, 0)
		}()
		shutdownWaitTime = 200 * time.Millisecond
		timeSpent := waitForLocksReleaseAndGetTimeWaitedFor()
		assert.Greater(t, timeSpent, 100*time.Millisecond, "waitForLocksRelease should timeout after 200 milliseconds and not before")
		assert.Less(t, timeSpent, 300*time.Millisecond, "waitForLocksRelease should timeout after 200 milliseconds and not take any longer")
	})

	t.Run("Successful wait for locks release", func(t *testing.T) {
		// Increment shardsLockCounter to simulate locking of a shard
		atomic.AddInt32(&shardsLockCounter, +1)
		shutdownWaitTime = 500 * time.Millisecond
		// Release the locks after 200 milliseconds
		go func() {
			time.Sleep(200 * time.Millisecond)
			atomic.StoreInt32(&shardsLockCounter, 0)
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
