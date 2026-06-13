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

package discovery

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestRetryInterval verifies that the retry interval stays within a bounded
// jitter window around the configured base delay. The jitter de-synchronizes
// reconnection attempts across vtgate instances so that a fleet recovering from
// a shared outage does not stampede a tablet at the same instant (#19894),
// while never growing the interval the way exponential backoff did.
func TestRetryInterval(t *testing.T) {
	const base = 5 * time.Second
	lower := time.Duration(float64(base) * 0.75)
	upper := time.Duration(float64(base) * 1.25)

	seen := make(map[time.Duration]struct{})
	for range 1000 {
		got := retryInterval(base)
		assert.GreaterOrEqual(t, got, lower, "interval must not drop below 75% of base")
		assert.LessOrEqual(t, got, upper, "interval must not exceed 125% of base")
		seen[got] = struct{}{}
	}

	// Jitter must actually vary; a single repeated value would re-synchronize
	// the fleet and defeat the purpose.
	assert.Greater(t, len(seen), 1, "retry interval should vary across calls")
}

// TestRetryIntervalNonPositive verifies that a zero or negative base delay is
// returned unchanged, so the jitter math never produces a panic or a negative
// sleep duration.
func TestRetryIntervalNonPositive(t *testing.T) {
	assert.Equal(t, time.Duration(0), retryInterval(0))
	assert.Equal(t, -time.Second, retryInterval(-time.Second))
}

// TestRetryIntervalTinyBase verifies that a positive base too small to jitter
// (base/2 truncates to zero) is returned unchanged rather than panicking inside
// rand.Int64N, which rejects a non-positive argument.
func TestRetryIntervalTinyBase(t *testing.T) {
	assert.Equal(t, time.Nanosecond, retryInterval(time.Nanosecond))
}
