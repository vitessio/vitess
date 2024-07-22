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
	"testing"

	"github.com/stretchr/testify/assert"
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
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 0, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.7)
	}
	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 1, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.2)
	}
	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 2, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.1)
	}
	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 3, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.6)
	}
	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 4, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.3)
	}
	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 5, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.3)
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
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 0, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.7)
	}
	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 1, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.2)
	}
	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 2, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.1)
	}
	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 3, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.6)
	}
	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 4, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.6)
	}
	{
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 5, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.6)
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
		worstMetric := AggregateTabletMetricResults(nonexistentMetricName, tabletResultsMap, 0, false, 0)
		_, err := worstMetric.Get()
		assert.Error(t, err)
		assert.Equal(t, ErrNoSuchMetric, err)
	})
	t.Run("no ignore", func(t *testing.T) {
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 0, false, 0)
		_, err := worstMetric.Get()
		assert.Error(t, err)
		assert.Equal(t, ErrNoSuchMetric, err)
	})
	t.Run("ignore 1", func(t *testing.T) {
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 1, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, 1.7, value)
	})
	t.Run("ignore 2", func(t *testing.T) {
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 2, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, 1.2, value)
	})

	tabletResultsMap[alias1][DefaultMetricName] = NoSuchMetric
	t.Run("no such metric", func(t *testing.T) {
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 0, false, 0)
		_, err := worstMetric.Get()
		assert.Error(t, err)
		assert.Equal(t, ErrNoSuchMetric, err)
	})
	t.Run("no such metric, ignore 1", func(t *testing.T) {
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 1, false, 0)
		_, err := worstMetric.Get()
		assert.Error(t, err)
		assert.Equal(t, ErrNoSuchMetric, err)
	})
	t.Run("metric found", func(t *testing.T) {
		worstMetric := AggregateTabletMetricResults(DefaultMetricName, tabletResultsMap, 2, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.7)
	})
}
