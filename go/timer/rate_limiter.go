/*
Copyright 2022 The Vitess Authors.

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

package timer

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// RateLimiter runs given tasks, at no more than one per defined duration.
// For example, we can create a RateLimiter of 1second. Then, we can ask it, over time, to run many
// tasks. It will only ever run a single task in any 1 second time frame. The rest are ignored.
type RateLimiter struct {
	tickerValue int64
	lastDoValue int64

	mu     sync.Mutex
	cancel context.CancelFunc
}

// NewRateLimiter creates a new limiter with given duration. It is immediately ready to run tasks.
func NewRateLimiter(d time.Duration) *RateLimiter {
	r := &RateLimiter{tickerValue: 1}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	go func() {
		ticker := time.NewTicker(d)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				atomic.StoreInt64(&r.tickerValue, r.tickerValue+1)
			}
		}
	}()
	return r
}

// Do runs a given func assuming rate limiting allows. This function is thread safe.
// f may be nil, in which case it is not invoked.
func (r *RateLimiter) Do(f func() error) (err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.lastDoValue >= atomic.LoadInt64(&r.tickerValue) {
		return nil // rate limited. Skipped.
	}
	if f != nil {
		err = f()
	}
	r.lastDoValue = atomic.LoadInt64(&r.tickerValue)
	return err
}

// Stop terminates rate limiter's operation and will not allow any more Do() executions.
func (r *RateLimiter) Stop() {
	r.cancel()

	r.mu.Lock()
	defer r.mu.Unlock()

	r.lastDoValue = math.MaxInt64
}
