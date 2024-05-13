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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAggregateName(t *testing.T) {
	tcases := []struct {
		aggregatedName string
		scope          Scope
		metricName     MetricName
		expectErr      string
	}{
		{
			aggregatedName: "",
			expectErr:      ErrNoSuchMetric.Error(),
		},
		{
			aggregatedName: "self",
			scope:          SelfScope,
			metricName:     DefaultMetricName,
		},
		{
			aggregatedName: "shard",
			scope:          ShardScope,
			metricName:     DefaultMetricName,
		},
		{
			aggregatedName: "self/default",
			expectErr:      ErrNoSuchMetric.Error(),
		},
		{
			aggregatedName: "lag",
			scope:          ShardScope,
			metricName:     LagMetricName,
		},
		{
			aggregatedName: "loadavg",
			scope:          SelfScope,
			metricName:     LoadAvgMetricName,
		},
		{
			aggregatedName: "lag2",
			expectErr:      ErrNoSuchMetric.Error(),
		},
		{
			aggregatedName: "self/lag",
			scope:          SelfScope,
			metricName:     LagMetricName,
		},
		{
			aggregatedName: "shard/lag",
			scope:          ShardScope,
			metricName:     LagMetricName,
		},
		{
			aggregatedName: "shard/lag3",
			expectErr:      ErrNoSuchMetric.Error(),
		},
		{
			aggregatedName: "shard/lag/zone1-01",
			expectErr:      ErrNoSuchMetric.Error(),
		},
		{
			aggregatedName: "shard/loadavg",
			scope:          ShardScope,
			metricName:     LoadAvgMetricName,
		},
	}
	assert.Equal(t, 3*len(KnownMetricNames), len(aggregatedMetricNames))
	fmt.Println(aggregatedMetricNames)
	for _, tcase := range tcases {
		t.Run(tcase.aggregatedName, func(t *testing.T) {
			scope, metricName, err := DisaggregateMetricName(tcase.aggregatedName)
			if tcase.expectErr != "" {
				assert.ErrorContains(t, err, tcase.expectErr)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tcase.scope, scope)
			assert.Equal(t, tcase.metricName, metricName)

			if strings.Contains(tcase.aggregatedName, "/") && tcase.metricName != DefaultMetricName {
				// Run the reverse.
				// Remember that for backwards compatibility, we do not add "default" to the aggregated name,
				// and we therefore skip tests that would fail because of this.
				aggregatedName := metricName.AggregatedName(scope)
				assert.Equal(t, tcase.aggregatedName, aggregatedName)
			}
		})
	}
}
