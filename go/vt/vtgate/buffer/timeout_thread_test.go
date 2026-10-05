/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package buffer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTimeoutThreadNotificationBeforeWaitIsNotLost tests that a request which
// is buffered after the timeout thread found the queue empty, but before the
// thread waits for the queue to become non-empty, wakes up the thread.
// Otherwise, the request would stay buffered past its window.
func TestTimeoutThreadNotificationBeforeWaitIsNotLost(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.Enabled = true
	sb := New(cfg).getOrCreateBuffer(keyspace, shard)

	// The thread is not started. The test runs the steps of its loop in the
	// order of the race instead.
	tt := newTimeoutThread(sb, 10*time.Minute)
	t.Cleanup(func() {
		tt.maxDuration.Stop()
		// Unblock waitForNonEmptyQueue() if the notification was lost.
		close(tt.stopChan)
	})

	// The thread found the queue empty. Then a request is buffered and the
	// queue becomes non-empty.
	tt.notifyQueueNotEmpty()

	// Now the thread waits for the queue to become non-empty.
	stoppedCh := make(chan bool, 1)
	go func() {
		stoppedCh <- tt.waitForNonEmptyQueue()
	}()

	var stopped bool
	require.Eventually(t, func() bool {
		select {
		case stopped = <-stoppedCh:
			return true
		default:
			return false
		}
	}, 30*time.Second, 10*time.Millisecond, "timeout thread missed that the queue became non-empty")
	assert.False(t, stopped, "timeout thread should keep running")
}
