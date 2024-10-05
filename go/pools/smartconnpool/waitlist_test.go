/*
Copyright 2024 The Vitess Authors.

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

package smartconnpool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWaitlistExpireWithMultipleWaiters(t *testing.T) {
	wait := waitlist[*TestConn]{}
	wait.init()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	waiterCount := 2
	expireCount := atomic.Int32{}

	for i := 0; i < waiterCount; i++ {
		go func() {
			_, err := wait.waitForConn(ctx, nil)
			if err != nil {
				expireCount.Add(1)
			}
		}()
	}

	// Wait for the context to expire
	<-ctx.Done()

	// Expire the waiters
	wait.expire(false)

	// Wait for the notified goroutines to finish
	timeout := time.After(1 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for expireCount.Load() != int32(waiterCount) {
		select {
		case <-timeout:
			require.Failf(t, "Timed out waiting for all waiters to expire", "Wanted %d, got %d", waiterCount, expireCount.Load())
		case <-ticker.C:
			// try again
		}
	}

	assert.Equal(t, int32(waiterCount), expireCount.Load())
}
