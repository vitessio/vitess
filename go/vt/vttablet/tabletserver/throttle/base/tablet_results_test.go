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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	alias1 = "zone1-0001"
	alias2 = "zone1-0002"
	alias3 = "zone1-0003"
	alias4 = "zone1-0004"
	alias5 = "zone1-0005"
)

const (
	nonexistentMetricName MetricName = "nonexistent"
)

func newMetricResultMap(val float64) MetricResultMap {
	return MetricResultMap{
		DefaultMetricName: NewSimpleMetricResult(val),
		LagMetricName:     NewSimpleMetricResult(val),
		LoadAvgMetricName: NewSimpleMetricResult(3.14),
	}
}

func noSuchMetricMap() MetricResultMap {
	result := make(MetricResultMap)
	for _, metricName := range KnownMetricNames {
		result[metricName] = NoSuchMetric
	}
	return result
}

func TestAggregateTabletMetricResultsNoErrors(t *testing.T) {
	tabletResultsMap := TabletResultMap{
		alias1: newMetricResultMap(1.2),
		alias2: newMetricResultMap(1.7),
		alias3: newMetricResultMap(0.3),
		alias4: newMetricResultMap(0.6),
		alias5: newMetricResultMap(1.1),
	}

	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 0, false, 0, WorstValuePercentile)
		value, err := worstMetric.Get()
		require.NoError(t, err)
		assert.Equal(t, 1.7, value)
	}
	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 1, false, 0, WorstValuePercentile)
		value, err := worstMetric.Get()
		require.NoError(t, err)
		assert.Equal(t, 1.2, value)
	}
	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 2, false, 0, WorstValuePercentile)
		value, err := worstMetric.Get()
		require.NoError(t, err)
		assert.Equal(t, 1.1, value)
	}
	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 3, false, 0, WorstValuePercentile)
		value, err := worstMetric.Get()
		require.NoError(t, err)
		assert.Equal(t, 0.6, value)
	}
	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 4, false, 0, WorstValuePercentile)
		value, err := worstMetric.Get()
		require.NoError(t, err)
		assert.Equal(t, 0.3, value)
	}
	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 5, false, 0, WorstValuePercentile)
		value, err := worstMetric.Get()
		require.NoError(t, err)
		assert.Equal(t, 0.3, value)
	}
}

func TestAggregateTabletMetricResultsNoErrorsIgnoreHostsThreshold(t *testing.T) {
	tabletResultsMap := TabletResultMap{
		alias1: newMetricResultMap(1.2),
		alias2: newMetricResultMap(1.7),
		alias3: newMetricResultMap(0.3),
		alias4: newMetricResultMap(0.6),
		alias5: newMetricResultMap(1.1),
	}

	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 0, false, 1.0, WorstValuePercentile)
		value, err := worstMetric.Get()
		require.NoError(t, err)
		assert.Equal(t, 1.7, value)
	}
	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 1, false, 1.0, WorstValuePercentile)
		value, err := worstMetric.Get()
		require.NoError(t, err)
		assert.Equal(t, 1.2, value)
	}
	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 2, false, 1.0, WorstValuePercentile)
		value, err := worstMetric.Get()
		require.NoError(t, err)
		assert.Equal(t, 1.1, value)
	}
	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 3, false, 1.0, WorstValuePercentile)
		value, err := worstMetric.Get()
		require.NoError(t, err)
		assert.Equal(t, 0.6, value)
	}
	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 4, false, 1.0, WorstValuePercentile)
		value, err := worstMetric.Get()
		require.NoError(t, err)
		assert.Equal(t, 0.6, value)
	}
	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 5, false, 1.0, WorstValuePercentile)
		value, err := worstMetric.Get()
		require.NoError(t, err)
		assert.Equal(t, 0.6, value)
	}
}

func TestAggregateTabletMetricResultsWithErrors(t *testing.T) {
	tabletResultsMap := TabletResultMap{
		alias1: newMetricResultMap(1.2),
		alias2: newMetricResultMap(1.7),
		alias3: newMetricResultMap(0.3),
		alias4: noSuchMetricMap(),
		alias5: newMetricResultMap(1.1),
	}

	t.Run("nonexistent", func(t *testing.T) {
		worstMetric := AggregateTabletMetricResults(nonexistentMetricName, tabletResultsMap, 0, false, 0, WorstValuePercentile)
		_, err := worstMetric.Get()
		require.Error(t, err)
		assert.Equal(t, ErrNoSuchMetric, err)
	})
	t.Run("no ignore", func(t *testing.T) {
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 0, false, 0, WorstValuePercentile)
		_, err := worstMetric.Get()
		require.Error(t, err)
		assert.Equal(t, ErrNoSuchMetric, err)
	})
	t.Run("ignore 1", func(t *testing.T) {
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 1, false, 0, WorstValuePercentile)
		value, err := worstMetric.Get()
		require.NoError(t, err)
		assert.Equal(t, 1.7, value)
	})
	t.Run("ignore 2", func(t *testing.T) {
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 2, false, 0, WorstValuePercentile)
		value, err := worstMetric.Get()
		require.NoError(t, err)
		assert.Equal(t, 1.2, value)
	})

	tabletResultsMap[alias1][DefaultMetricName] = NoSuchMetric
	t.Run("no such metric", func(t *testing.T) {
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 0, false, 0, WorstValuePercentile)
		_, err := worstMetric.Get()
		require.Error(t, err)
		assert.Equal(t, ErrNoSuchMetric, err)
	})
	t.Run("no such metric, ignore 1", func(t *testing.T) {
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 1, false, 0, WorstValuePercentile)
		_, err := worstMetric.Get()
		require.Error(t, err)
		assert.Equal(t, ErrNoSuchMetric, err)
	})
	t.Run("metric found", func(t *testing.T) {
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 2, false, 0, WorstValuePercentile)
		value, err := worstMetric.Get()
		require.NoError(t, err)
		assert.Equal(t, 1.7, value)
	})
}

func TestAggregateTabletMetricResultsPercentile(t *testing.T) {
	// Each tablet's value is its 1-based rank, so the expected value is the selected rank.
	tabletsWithRanks := func(ranks ...float64) TabletResultMap {
		tablets := make(TabletResultMap, len(ranks))
		for _, rank := range ranks {
			tablets[fmt.Sprintf("zone1-%04d", int(rank))] = newMetricResultMap(rank)
		}
		return tablets
	}
	ranks1To5Unordered := tabletsWithRanks(4, 5, 1, 2, 3)
	ranks1To4 := tabletsWithRanks(1, 2, 3, 4)

	tcs := []struct {
		name             string
		tablets          TabletResultMap
		ignoreHostsCount int
		percentile       float64
		expect           float64
	}{
		{name: "zero is worst", tablets: ranks1To5Unordered, percentile: 0, expect: 5},
		{name: "negative is worst", tablets: ranks1To5Unordered, percentile: -5, expect: 5},
		{name: "NaN is worst", tablets: ranks1To4, percentile: math.NaN(), expect: 4},
		{name: "100 is worst", tablets: ranks1To5Unordered, percentile: 100, expect: 5},
		{name: "above 100 is worst", tablets: ranks1To5Unordered, percentile: 150, expect: 5},
		{name: "just below 100 is worst", tablets: ranks1To4, percentile: math.Nextafter(100, 0), expect: 4},
		{name: "smallest positive is best", tablets: ranks1To4, percentile: math.SmallestNonzeroFloat64, expect: 1},
		{name: "p1 of 5", tablets: ranks1To5Unordered, percentile: 1, expect: 1},
		{name: "p20 of 5", tablets: ranks1To5Unordered, percentile: 20, expect: 1},
		{name: "p21 of 5", tablets: ranks1To5Unordered, percentile: 21, expect: 2},
		{name: "p50 of 5", tablets: ranks1To5Unordered, percentile: 50, expect: 3},
		{name: "p80 of 5", tablets: ranks1To5Unordered, percentile: 80, expect: 4},
		{name: "p81 of 5", tablets: ranks1To5Unordered, percentile: 81, expect: 5},
		{name: "p50 of 4", tablets: ranks1To4, percentile: 50, expect: 2},
		{name: "p75 of 1", tablets: tabletsWithRanks(1), percentile: 75, expect: 1},
		{name: "p75 of 2", tablets: tabletsWithRanks(1, 2), percentile: 75, expect: 2},
		{name: "p75 of 3", tablets: tabletsWithRanks(1, 2, 3), percentile: 75, expect: 3},
		{name: "p75 of 4", tablets: ranks1To4, percentile: 75, expect: 3},
		{name: "p75 of 5", tablets: ranks1To5Unordered, percentile: 75, expect: 4},
		{name: "p75 of 8", tablets: tabletsWithRanks(1, 2, 3, 4, 5, 6, 7, 8), percentile: 75, expect: 6},
		{name: "p50 of 5 after ignoring the worst", tablets: ranks1To5Unordered, ignoreHostsCount: 1, percentile: 50, expect: 2},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			value, err := AggregateTabletMetricResults(LagMetricName, tc.tablets, tc.ignoreHostsCount, false, 0, tc.percentile).Get()
			require.NoError(t, err)
			assert.Equal(t, tc.expect, value)
		})
	}
}

func TestTabletResultMapWithout(t *testing.T) {
	tablets := TabletResultMap{
		alias1: newMetricResultMap(1),
		alias2: newMetricResultMap(2),
	}

	without := tablets.Without(alias1)

	assert.Equal(t, TabletResultMap{alias2: tablets[alias2]}, without)
	assert.Len(t, tablets, 2)
	assert.Equal(t, tablets, tablets.Without("zone1-9999"))
}

func TestFloorMetricResult(t *testing.T) {
	low := NewSimpleMetricResult(1)
	high := NewSimpleMetricResult(5)
	tcs := []struct {
		name   string
		floor  MetricResult
		result MetricResult
		expect MetricResult
	}{
		{name: "floor above the result", floor: high, result: low, expect: high},
		{name: "floor below the result", floor: low, result: high, expect: high},
		{name: "result without hosts", floor: low, result: NoHostsMetricResult, expect: low},
		{name: "floor without hosts", floor: NoHostsMetricResult, result: low, expect: low},
		{name: "floor error", floor: NoMetricResultYet, result: high, expect: NoMetricResultYet},
		{name: "result error", floor: high, result: NoSuchMetric, expect: NoSuchMetric},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			assert.Same(t, tc.expect, FloorMetricResult(tc.floor, tc.result))
		})
	}
}

func TestIsPercentileBelowWorst(t *testing.T) {
	assert.True(t, IsPercentileBelowWorst(75))
	assert.True(t, IsPercentileBelowWorst(math.Nextafter(100, 0)))
	assert.False(t, IsPercentileBelowWorst(0))
	assert.False(t, IsPercentileBelowWorst(-1))
	assert.False(t, IsPercentileBelowWorst(100))
	assert.False(t, IsPercentileBelowWorst(math.NaN()))
}
