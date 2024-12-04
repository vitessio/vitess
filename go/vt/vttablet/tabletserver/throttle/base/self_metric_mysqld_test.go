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

package base

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetMysqlMetricsRateLimiter(t *testing.T) {
	rateLimit := 10 * time.Millisecond
	for i := range 3 {
		testName := fmt.Sprintf("iteration %d", i)
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			{
				rateLimiter := mysqlHostMetricsRateLimiter.Load()
				assert.Nil(t, rateLimiter)
			}
			rateLimiter := getMysqlMetricsRateLimiter(ctx, rateLimit)
			assert.NotNil(t, rateLimiter)
			for range 5 {
				r := getMysqlMetricsRateLimiter(ctx, rateLimit)
				// Returning the same rate limiter
				assert.Equal(t, rateLimiter, r)
			}
			val := 0
			incr := func() error {
				val++
				return nil
			}
			for range 10 {
				rateLimiter.Do(incr)
				time.Sleep(2 * rateLimit)
			}
			assert.EqualValues(t, 10, val)
			cancel()
			// There can be a race condition where the rate limiter still emits one final tick after the context is cancelled.
			// So we wait enough time to ensure that tick is "wasted".
			time.Sleep(2 * rateLimit)
			// Now that the rate limited was stopped (we invoked `cancel()`), its `Do()` should not invoke the function anymore.
			for range 7 {
				rateLimiter.Do(incr)
				time.Sleep(time.Millisecond)
			}
			assert.EqualValues(t, 10, val) // Same "10" value as before.
			{
				rateLimiter := mysqlHostMetricsRateLimiter.Load()
				assert.Nil(t, rateLimiter)
			}
		})
	}
}
