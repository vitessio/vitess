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
	"context"
	"testing"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/mysql"

	"github.com/stretchr/testify/assert"
)

var (
	alias1 = "zone1-0001"
	alias2 = "zone1-0002"
	alias3 = "zone1-0003"
	alias4 = "zone1-0004"
	alias5 = "zone1-0005"
)

func TestAggregateMySQLProbesNoErrors(t *testing.T) {
	ctx := context.Background()
	clusterName := "c0"
	key1cluster := mysql.GetClusterTablet(clusterName, alias1)
	key2cluster := mysql.GetClusterTablet(clusterName, alias2)
	key3cluster := mysql.GetClusterTablet(clusterName, alias3)
	key4cluster := mysql.GetClusterTablet(clusterName, alias4)
	key5cluster := mysql.GetClusterTablet(clusterName, alias5)
	tabletResultsMap := mysql.TabletResultMap{
		key1cluster: base.NewSimpleMetricResult(1.2),
		key2cluster: base.NewSimpleMetricResult(1.7),
		key3cluster: base.NewSimpleMetricResult(0.3),
		key4cluster: base.NewSimpleMetricResult(0.6),
		key5cluster: base.NewSimpleMetricResult(1.1),
	}
	var probes mysql.Probes = map[string](*mysql.Probe){}
	for clusterKey := range tabletResultsMap {
		probes[clusterKey.Alias] = &mysql.Probe{Alias: clusterKey.Alias}
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, probes, clusterName, tabletResultsMap, 0, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.7)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, probes, clusterName, tabletResultsMap, 1, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.2)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, probes, clusterName, tabletResultsMap, 2, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.1)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, probes, clusterName, tabletResultsMap, 3, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.6)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, probes, clusterName, tabletResultsMap, 4, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.3)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, probes, clusterName, tabletResultsMap, 5, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.3)
	}
}

func TestAggregateMySQLProbesNoErrorsIgnoreHostsThreshold(t *testing.T) {
	ctx := context.Background()
	clusterName := "c0"
	key1cluster := mysql.GetClusterTablet(clusterName, alias1)
	key2cluster := mysql.GetClusterTablet(clusterName, alias2)
	key3cluster := mysql.GetClusterTablet(clusterName, alias3)
	key4cluster := mysql.GetClusterTablet(clusterName, alias4)
	key5cluster := mysql.GetClusterTablet(clusterName, alias5)
	tableteResultsMap := mysql.TabletResultMap{
		key1cluster: base.NewSimpleMetricResult(1.2),
		key2cluster: base.NewSimpleMetricResult(1.7),
		key3cluster: base.NewSimpleMetricResult(0.3),
		key4cluster: base.NewSimpleMetricResult(0.6),
		key5cluster: base.NewSimpleMetricResult(1.1),
	}
	var probes mysql.Probes = map[string](*mysql.Probe){}
	for clusterKey := range tableteResultsMap {
		probes[clusterKey.Alias] = &mysql.Probe{Alias: clusterKey.Alias}
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, probes, clusterName, tableteResultsMap, 0, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.7)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, probes, clusterName, tableteResultsMap, 1, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.2)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, probes, clusterName, tableteResultsMap, 2, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.1)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, probes, clusterName, tableteResultsMap, 3, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.6)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, probes, clusterName, tableteResultsMap, 4, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.6)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, probes, clusterName, tableteResultsMap, 5, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.6)
	}
}

func TestAggregateMySQLProbesWithErrors(t *testing.T) {
	ctx := context.Background()
	clusterName := "c0"
	key1cluster := mysql.GetClusterTablet(clusterName, alias1)
	key2cluster := mysql.GetClusterTablet(clusterName, alias2)
	key3cluster := mysql.GetClusterTablet(clusterName, alias3)
	key4cluster := mysql.GetClusterTablet(clusterName, alias4)
	key5cluster := mysql.GetClusterTablet(clusterName, alias5)
	tabletResultsMap := mysql.TabletResultMap{
		key1cluster: base.NewSimpleMetricResult(1.2),
		key2cluster: base.NewSimpleMetricResult(1.7),
		key3cluster: base.NewSimpleMetricResult(0.3),
		key4cluster: base.NoSuchMetric,
		key5cluster: base.NewSimpleMetricResult(1.1),
	}
	var probes mysql.Probes = map[string](*mysql.Probe){}
	for clusterKey := range tabletResultsMap {
		probes[clusterKey.Alias] = &mysql.Probe{Alias: clusterKey.Alias}
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, probes, clusterName, tabletResultsMap, 0, false, 0)
		_, err := worstMetric.Get()
		assert.Error(t, err)
		assert.Equal(t, err, base.ErrNoSuchMetric)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, probes, clusterName, tabletResultsMap, 1, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.7)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, probes, clusterName, tabletResultsMap, 2, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.2)
	}

	tabletResultsMap[key1cluster] = base.NoSuchMetric
	{
		worstMetric := aggregateMySQLProbes(ctx, probes, clusterName, tabletResultsMap, 0, false, 0)
		_, err := worstMetric.Get()
		assert.Error(t, err)
		assert.Equal(t, err, base.ErrNoSuchMetric)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, probes, clusterName, tabletResultsMap, 1, false, 0)
		_, err := worstMetric.Get()
		assert.Error(t, err)
		assert.Equal(t, err, base.ErrNoSuchMetric)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, probes, clusterName, tabletResultsMap, 2, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.7)
	}
}
