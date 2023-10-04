//go:build !race

/*
Copyright 2023 The Vitess Authors.

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

package throttle

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProbesPostDisable runs the throttler for some time, and then investigates the internal throttler maps and values.
// While the therottle is disabled, it is technically safe to iterate those structures. However, `go test -race` disagrees,
// which is why this test is in this *exclude_race* file
func TestProbesPostDisable(t *testing.T) {
	throttler := newTestThrottler()
	runThrottler(t, throttler, 2*time.Second, nil)

	time.Sleep(time.Second) // throttler's Operate() quits asycnhronously. For sake of `go test -race` we allow a graceful wait.
	probes := throttler.mysqlInventory.ClustersProbes
	assert.NotEmpty(t, probes)

	selfProbes := probes[selfStoreName]
	t.Run("self", func(t *testing.T) {
		assert.NotEmpty(t, selfProbes)
		require.Equal(t, 1, len(selfProbes)) // should always be true once refreshMySQLInventory() runs
		probe, ok := selfProbes[""]
		assert.True(t, ok)
		assert.NotNil(t, probe)

		assert.Equal(t, "", probe.Alias)
		assert.Nil(t, probe.Tablet)
		assert.Equal(t, "select 1", probe.MetricQuery)
		assert.Equal(t, int64(0), probe.QueryInProgress)
	})

	shardProbes := probes[shardStoreName]
	t.Run("shard", func(t *testing.T) {
		assert.NotEmpty(t, shardProbes)
		assert.Equal(t, 2, len(shardProbes)) // see fake FindAllTabletAliasesInShard above
		for _, probe := range shardProbes {
			require.NotNil(t, probe)
			assert.NotEmpty(t, probe.Alias)
			assert.NotNil(t, probe.Tablet)
			assert.Equal(t, "select 1", probe.MetricQuery)
			assert.Equal(t, int64(0), probe.QueryInProgress)
		}
	})

	t.Run("metrics", func(t *testing.T) {
		assert.Equal(t, 3, len(throttler.mysqlInventory.TabletMetrics)) // 1 self tablet + 2 shard tablets
	})

	t.Run("aggregated", func(t *testing.T) {
		assert.Equal(t, 0, throttler.aggregatedMetrics.ItemCount()) // flushed upon Disable()
		aggr := throttler.aggregatedMetricsSnapshot()
		assert.Equal(t, 0, len(aggr))
	})
}
