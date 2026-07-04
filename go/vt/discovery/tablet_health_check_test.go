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

// TestNextHealthCheckRetryDelay verifies the reconnection back-off doubles while
// below the cap and never grows past maxHealthCheckRetryDelay. Capping the
// back-off well below healthCheckTimeout (default 1m) is what lets vtgate
// rediscover a recovered tablet promptly. See #19894.
func TestNextHealthCheckRetryDelay(t *testing.T) {
	// Doubles while comfortably below the cap.
	assert.Equal(t, 20*time.Millisecond, nextHealthCheckRetryDelay(10*time.Millisecond))
	assert.Equal(t, 2*time.Second, nextHealthCheckRetryDelay(time.Second))

	// Caps at maxHealthCheckRetryDelay once doubling would exceed it.
	assert.Equal(t, maxHealthCheckRetryDelay, nextHealthCheckRetryDelay(6*time.Second))
	assert.Equal(t, maxHealthCheckRetryDelay, nextHealthCheckRetryDelay(maxHealthCheckRetryDelay))

	// Never exceeds the cap, even from an already-huge delay.
	assert.Equal(t, maxHealthCheckRetryDelay, nextHealthCheckRetryDelay(time.Hour))

	// The cap stays well under the default health check timeout, so a recovered
	// tablet is not stranded behind a back-off grown to the silence timeout.
	assert.Less(t, maxHealthCheckRetryDelay, DefaultHealthCheckTimeout)
}
