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

package process

import (
	"testing"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestHealthTest(t *testing.T) {
	defer func() {
		FirstDiscoveryCycleComplete.Store(false)
		ThisNodeHealth = &NodeHealth{}
		ResetLastHealthCheckCache()
	}()

	require.Zero(t, ThisNodeHealth.LastReported)

	ThisNodeHealth = &NodeHealth{}
	health, err := HealthTest()
	require.NoError(t, err)
	require.False(t, health.Healthy)
	require.False(t, health.DiscoveredOnce)
	require.NotZero(t, ThisNodeHealth.LastReported)
	ResetLastHealthCheckCache()

	ThisNodeHealth = &NodeHealth{}
	FirstDiscoveryCycleComplete.Store(true)
	health, err = HealthTest()
	require.NoError(t, err)
	require.True(t, health.Healthy)
	require.True(t, health.DiscoveredOnce)
	require.NotZero(t, ThisNodeHealth.LastReported)
	ResetLastHealthCheckCache()
}
