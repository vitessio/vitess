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
	for _, tcase := range tcases {
		t.Run(tcase.aggregatedName, func(t *testing.T) {
			scope, metricName, err := DisaggregateMetricName(tcase.aggregatedName)
			{
				scope2, metricName2, err2 := MetricName(tcase.aggregatedName).Disaggregated()
				assert.Equal(t, scope, scope2)
				assert.Equal(t, metricName, metricName2)
				assert.Equal(t, err, err2)
			}
			if tcase.expectErr != "" {
				assert.ErrorContains(t, err, tcase.expectErr)
				return
			}
			assert.NoError(t, err)
			assert.NotEqual(t, UndefinedScope, scope)
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

func TestScopeFromString(t *testing.T) {
	{
		scope, err := ScopeFromString("")
		assert.NoError(t, err)
		assert.Equal(t, UndefinedScope, scope)
	}
	{
		scope, err := ScopeFromString("self")
		assert.NoError(t, err)
		assert.Equal(t, SelfScope, scope)
	}
	{
		scope, err := ScopeFromString("shard")
		assert.NoError(t, err)
		assert.Equal(t, ShardScope, scope)
	}
	{
		_, err := ScopeFromString("something")
		assert.ErrorContains(t, err, "unknown scope")
	}
	{
		_, err := ScopeFromString("self/lag")
		assert.ErrorContains(t, err, "unknown scope")
	}
}

func TestUnique(t *testing.T) {
	tcases := []struct {
		names  MetricNames
		expect MetricNames
	}{
		{
			names:  nil,
			expect: nil,
		},
		{
			names:  MetricNames{},
			expect: MetricNames{},
		},
		{
			names:  MetricNames{LagMetricName},
			expect: MetricNames{LagMetricName},
		},
		{
			names:  MetricNames{LagMetricName, LagMetricName},
			expect: MetricNames{LagMetricName},
		},
		{
			names:  MetricNames{LagMetricName, ThreadsRunningMetricName, LagMetricName},
			expect: MetricNames{LagMetricName, ThreadsRunningMetricName},
		},
		{
			names:  MetricNames{LagMetricName, ThreadsRunningMetricName, LagMetricName, LoadAvgMetricName, LoadAvgMetricName, CustomMetricName},
			expect: MetricNames{LagMetricName, ThreadsRunningMetricName, LoadAvgMetricName, CustomMetricName},
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.names.String(), func(t *testing.T) {
			assert.Equal(t, tcase.expect, tcase.names.Unique())
		})
	}
}

func TestContains(t *testing.T) {
	tcases := []struct {
		names  MetricNames
		name   MetricName
		expect bool
	}{
		{
			names: nil,
			name:  LagMetricName,
		},
		{
			names: MetricNames{},
			name:  LagMetricName,
		},
		{
			names:  MetricNames{LagMetricName},
			name:   LagMetricName,
			expect: true,
		},
		{
			names:  MetricNames{LagMetricName},
			name:   ThreadsRunningMetricName,
			expect: false,
		},
		{
			names:  MetricNames{LagMetricName, LagMetricName},
			name:   LagMetricName,
			expect: true,
		},
		{
			names:  MetricNames{LagMetricName, ThreadsRunningMetricName, LagMetricName},
			name:   LagMetricName,
			expect: true,
		},
		{
			names:  MetricNames{LagMetricName, ThreadsRunningMetricName, LagMetricName, LoadAvgMetricName, LoadAvgMetricName, CustomMetricName},
			name:   LoadAvgMetricName,
			expect: true,
		},
		{
			names:  MetricNames{LagMetricName, ThreadsRunningMetricName, LagMetricName, LoadAvgMetricName, LoadAvgMetricName},
			name:   CustomMetricName,
			expect: false,
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.names.String(), func(t *testing.T) {
			assert.Equal(t, tcase.expect, tcase.names.Contains(tcase.name))
		})
	}
}

func TestKnownMetricNames(t *testing.T) {
	assert.NotEmpty(t, KnownMetricNames)
	assert.Contains(t, KnownMetricNames, LagMetricName)
	assert.Contains(t, KnownMetricNames, ThreadsRunningMetricName)
	assert.Contains(t, KnownMetricNames, LoadAvgMetricName)
	assert.Contains(t, KnownMetricNames, CustomMetricName)
	assert.Contains(t, KnownMetricNames, DefaultMetricName)
}
