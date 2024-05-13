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

package throttle

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
)

func TestSplitMetricTokens(t *testing.T) {
	tcases := []struct {
		aggregatedName string
		storeName      string
		metricName     base.MetricName
		expectErr      string
	}{
		{
			aggregatedName: "",
			expectErr:      base.ErrNoSuchMetric.Error(),
		},
		{
			aggregatedName: "self",
			storeName:      "self",
			metricName:     base.DefaultMetricName,
		},
		{
			aggregatedName: "self/default",
			storeName:      "self",
			metricName:     base.DefaultMetricName,
		},
		{
			aggregatedName: "self/lag",
			storeName:      "self",
			metricName:     base.LagMetricName,
		},
		{
			aggregatedName: "shard/lag",
			storeName:      "shard",
			metricName:     base.LagMetricName,
		},
		{
			aggregatedName: "shard/lag/zone1-01",
			expectErr:      base.ErrNoSuchMetric.Error(),
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.aggregatedName, func(t *testing.T) {
			store, metricName, err := splitMetricTokens(tcase.aggregatedName)
			if tcase.expectErr != "" {
				assert.ErrorContains(t, err, tcase.expectErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tcase.storeName, store)
			assert.Equal(t, tcase.metricName, metricName)

			if tcase.metricName != base.DefaultMetricName {
				// Run the reverse.
				// Remember that for backwards compatibility, we do not add "default" to the aggregated name,
				// and we therefore skip tests that would fail because of this.
				aggregatedName := getAggregatedMetricName(base.Store(store), metricName)
				assert.Equal(t, tcase.aggregatedName, aggregatedName)
			}
		})
	}
}
