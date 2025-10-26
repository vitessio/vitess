/*
Copyright 2025 The Vitess Authors.

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

package stats

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGauges(t *testing.T) {
	clearStats()

	// Create a Gauges with 3 samples, sampling every 1 second.
	g := NewGauges(3, 1*time.Second)
	defer g.Stop()

	// Set initial values.
	g.Set("metric1", 10)
	g.Set("metric2", 20)

	// Manually trigger snapshot instead of waiting.
	g.snapshot()

	g.Set("metric1", 15)
	g.Set("metric2", 25)

	// Trigger second snapshot.
	g.snapshot()

	// Change values again
	g.Set("metric1", 20)
	g.Set("metric2", 30)

	// Trigger third snapshot.
	g.snapshot()
	result := g.Get()

	// Verify we have both metrics.
	require.NotNil(t, result["metric1"])
	require.NotNil(t, result["metric2"])

	// Verify we have multiple samples.
	require.Greater(t, len(result["metric1"]), 2)
	require.Greater(t, len(result["metric2"]), 2)

	// Verify values are increasing.
	m1 := result["metric1"]
	if len(m1) >= 2 && m1[len(m1)-1] <= m1[0] {
		require.FailNow(t, fmt.Sprintf("Expected metric1 to increase, got %v", m1))
	}
	m2 := result["metric2"]
	if len(m2) >= 2 && m2[len(m2)-1] <= m2[0] {
		require.FailNow(t, fmt.Sprintf("Expected metric2 to increase, got %v", m1))
	}
}

func TestGaugesFunc(t *testing.T) {
	clearStats()

	callCount := 0
	f := NewGaugesFunc("test_gauges", "test gauges", func() map[string][]float64 {
		callCount++
		return map[string][]float64{
			"gauge1": {1.0, 2.0, 3.0},
			"gauge2": {4.0, 5.0, 6.0},
		}
	})

	// Verify String() calls the function.
	result := f.String()
	require.Greater(t, callCount, 0)
	require.NotEmpty(t, result)

	// Verify Help().
	require.Equal(t, f.Help(), "test gauges")
}

func TestGaugesNoSampling(t *testing.T) {
	// Test with interval = -1 (no background sampling).
	g := NewGauges(3, -1*time.Second)
	defer g.Stop()

	g.Set("metric1", 100)
	g.snapshot()

	result := g.Get()
	require.NotNil(t, result["metric1"])
	require.Len(t, result["metric1"], 1)
	require.Equal(t, result["metric1"][0], 100.0)
}
