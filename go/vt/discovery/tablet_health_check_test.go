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

// TestNextHealthCheckRetryDelay verifies the back-off doubles below the cap,
// caps at maxHealthCheckRetryDelay, and leaves an already-above-cap delay
// unchanged rather than reducing it. See #19894.
func TestNextHealthCheckRetryDelay(t *testing.T) {
	// Doubles while comfortably below the cap.
	assert.Equal(t, 20*time.Millisecond, nextHealthCheckRetryDelay(10*time.Millisecond))
	assert.Equal(t, 2*time.Second, nextHealthCheckRetryDelay(time.Second))

	// Caps at maxHealthCheckRetryDelay once doubling would exceed it.
	assert.Equal(t, maxHealthCheckRetryDelay, nextHealthCheckRetryDelay(6*time.Second))
	assert.Equal(t, maxHealthCheckRetryDelay, nextHealthCheckRetryDelay(maxHealthCheckRetryDelay))

	// A delay already above the cap is left unchanged, not reduced.
	assert.Equal(t, time.Hour, nextHealthCheckRetryDelay(time.Hour))

	// The cap stays well under the default health check timeout, so a recovered
	// tablet is not stranded behind a back-off grown to the silence timeout.
	assert.Less(t, maxHealthCheckRetryDelay, DefaultHealthCheckTimeout)
}
