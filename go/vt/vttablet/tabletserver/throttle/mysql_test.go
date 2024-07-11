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

// This codebase originates from https://github.com/github/freno, See https://github.com/github/freno/blob/master/LICENSE
/*
	MIT License

	Copyright (c) 2017 GitHub

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
*/

package throttle

import (
	"testing"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"

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
	nonexistentMetricName base.MetricName = "nonexistent"
)

func newMetricResultMap(val float64) base.MetricResultMap {
	return base.MetricResultMap{
		base.DefaultMetricName: base.NewSimpleMetricResult(val),
		base.LagMetricName:     base.NewSimpleMetricResult(val),
		base.LoadAvgMetricName: base.NewSimpleMetricResult(3.14),
	}
}
func noSuchMetricMap() base.MetricResultMap {
	result := make(base.MetricResultMap)
	for _, metricName := range base.KnownMetricNames {
		result[metricName] = base.NoSuchMetric
	}
	return result
}

func TestAggregateMetricResultsNoErrors(t *testing.T) {
	tabletResultsMap := base.TabletResultMap{
		alias1: newMetricResultMap(1.2),
		alias2: newMetricResultMap(1.7),
		alias3: newMetricResultMap(0.3),
		alias4: newMetricResultMap(0.6),
		alias5: newMetricResultMap(1.1),
	}

	{
		worstMetric := aggregateMetricResults(base.DefaultMetricName, tabletResultsMap, 0, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.7)
	}
	{
		worstMetric := aggregateMetricResults(base.DefaultMetricName, tabletResultsMap, 1, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.2)
	}
	{
		worstMetric := aggregateMetricResults(base.DefaultMetricName, tabletResultsMap, 2, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.1)
	}
	{
		worstMetric := aggregateMetricResults(base.DefaultMetricName, tabletResultsMap, 3, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.6)
	}
	{
		worstMetric := aggregateMetricResults(base.DefaultMetricName, tabletResultsMap, 4, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.3)
	}
	{
		worstMetric := aggregateMetricResults(base.DefaultMetricName, tabletResultsMap, 5, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.3)
	}
}

func TestAggregateMetricResultsNoErrorsIgnoreHostsThreshold(t *testing.T) {
	tabletResultsMap := base.TabletResultMap{
		alias1: newMetricResultMap(1.2),
		alias2: newMetricResultMap(1.7),
		alias3: newMetricResultMap(0.3),
		alias4: newMetricResultMap(0.6),
		alias5: newMetricResultMap(1.1),
	}

	{
		worstMetric := aggregateMetricResults(base.DefaultMetricName, tabletResultsMap, 0, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.7)
	}
	{
		worstMetric := aggregateMetricResults(base.DefaultMetricName, tabletResultsMap, 1, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.2)
	}
	{
		worstMetric := aggregateMetricResults(base.DefaultMetricName, tabletResultsMap, 2, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.1)
	}
	{
		worstMetric := aggregateMetricResults(base.DefaultMetricName, tabletResultsMap, 3, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.6)
	}
	{
		worstMetric := aggregateMetricResults(base.DefaultMetricName, tabletResultsMap, 4, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.6)
	}
	{
		worstMetric := aggregateMetricResults(base.DefaultMetricName, tabletResultsMap, 5, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.6)
	}
}

func TestAggregateMetricResultsWithErrors(t *testing.T) {
	tabletResultsMap := base.TabletResultMap{
		alias1: newMetricResultMap(1.2),
		alias2: newMetricResultMap(1.7),
		alias3: newMetricResultMap(0.3),
		alias4: noSuchMetricMap(),
		alias5: newMetricResultMap(1.1),
	}

	t.Run("nonexistent", func(t *testing.T) {
		worstMetric := aggregateMetricResults(nonexistentMetricName, tabletResultsMap, 0, false, 0)
		_, err := worstMetric.Get()
		assert.Error(t, err)
		assert.Equal(t, base.ErrNoSuchMetric, err)
	})
	t.Run("no ignore", func(t *testing.T) {
		worstMetric := aggregateMetricResults(base.DefaultMetricName, tabletResultsMap, 0, false, 0)
		_, err := worstMetric.Get()
		assert.Error(t, err)
		assert.Equal(t, base.ErrNoSuchMetric, err)
	})
	t.Run("ignore 1", func(t *testing.T) {
		worstMetric := aggregateMetricResults(base.DefaultMetricName, tabletResultsMap, 1, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, 1.7, value)
	})
	t.Run("ignore 2", func(t *testing.T) {
		worstMetric := aggregateMetricResults(base.DefaultMetricName, tabletResultsMap, 2, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, 1.2, value)
	})

	tabletResultsMap[alias1][base.DefaultMetricName] = base.NoSuchMetric
	t.Run("no such metric", func(t *testing.T) {
		worstMetric := aggregateMetricResults(base.DefaultMetricName, tabletResultsMap, 0, false, 0)
		_, err := worstMetric.Get()
		assert.Error(t, err)
		assert.Equal(t, base.ErrNoSuchMetric, err)
	})
	t.Run("no such metric, ignore 1", func(t *testing.T) {
		worstMetric := aggregateMetricResults(base.DefaultMetricName, tabletResultsMap, 1, false, 0)
		_, err := worstMetric.Get()
		assert.Error(t, err)
		assert.Equal(t, base.ErrNoSuchMetric, err)
	})
	t.Run("metric found", func(t *testing.T) {
		worstMetric := aggregateMetricResults(base.DefaultMetricName, tabletResultsMap, 2, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.7)
	})
}
