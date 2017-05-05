/*
Copyright 2017 Google Inc.

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

// Package ratelimiter implements rate limiting functionality.
package ratelimiter

import (
	"sync"
	"time"
)

// RateLimiter was inspired by https://github.com/golang/go/wiki/RateLimiting.
// However, the go example is not good for setting high qps limits because
// it will cause the ticker to fire too often. Also, the ticker will continue
// to fire when the system is idle. This new Ratelimiter achieves the same thing,
// but by using just counters with no tickers or channels.
type RateLimiter struct {
	maxCount int
	interval time.Duration

	mu       sync.Mutex
	curCount int
	lastTime time.Time
}

// NewRateLimiter creates a new RateLimiter. maxCount is the max burst allowed
// while interval specifies the duration for a burst. The effective rate limit is
// equal to maxCount/interval. For example, if you want to a max QPS of 5000,
// and want to limit bursts to no more than 500, you'd specify a maxCount of 500
// and an interval of 100*time.Millilsecond.
func NewRateLimiter(maxCount int, interval time.Duration) *RateLimiter {
	return &RateLimiter{
		maxCount: maxCount,
		interval: interval,
	}
}

// Allow returns true if a request is within the rate limit norms.
// Otherwise, it returns false.
func (rl *RateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if time.Now().Sub(rl.lastTime) < rl.interval {
		if rl.curCount > 0 {
			rl.curCount--
			return true
		}
		return false
	}
	rl.curCount = rl.maxCount - 1
	rl.lastTime = time.Now()
	return true
}
