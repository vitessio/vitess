/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
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
	key1 = mysql.InstanceKey{Hostname: "10.0.0.1", Port: 3306}
	key2 = mysql.InstanceKey{Hostname: "10.0.0.2", Port: 3306}
	key3 = mysql.InstanceKey{Hostname: "10.0.0.3", Port: 3306}
	key4 = mysql.InstanceKey{Hostname: "10.0.0.4", Port: 3306}
	key5 = mysql.InstanceKey{Hostname: "10.0.0.5", Port: 3306}
)

func TestAggregateMySQLProbesNoErrors(t *testing.T) {
	ctx := context.Background()
	clusterName := "c0"
	key1cluster := mysql.GetClusterInstanceKey(clusterName, &key1)
	key2cluster := mysql.GetClusterInstanceKey(clusterName, &key2)
	key3cluster := mysql.GetClusterInstanceKey(clusterName, &key3)
	key4cluster := mysql.GetClusterInstanceKey(clusterName, &key4)
	key5cluster := mysql.GetClusterInstanceKey(clusterName, &key5)
	instanceResultsMap := mysql.InstanceMetricResultMap{
		key1cluster: base.NewSimpleMetricResult(1.2),
		key2cluster: base.NewSimpleMetricResult(1.7),
		key3cluster: base.NewSimpleMetricResult(0.3),
		key4cluster: base.NewSimpleMetricResult(0.6),
		key5cluster: base.NewSimpleMetricResult(1.1),
	}
	var probes mysql.Probes = map[mysql.InstanceKey](*mysql.Probe){}
	for clusterKey := range instanceResultsMap {
		probes[clusterKey.Key] = &mysql.Probe{Key: clusterKey.Key}
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, &probes, clusterName, instanceResultsMap, 0, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.7)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, &probes, clusterName, instanceResultsMap, 1, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.2)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, &probes, clusterName, instanceResultsMap, 2, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.1)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, &probes, clusterName, instanceResultsMap, 3, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.6)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, &probes, clusterName, instanceResultsMap, 4, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.3)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, &probes, clusterName, instanceResultsMap, 5, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.3)
	}
}

func TestAggregateMySQLProbesNoErrorsIgnoreHostsThreshold(t *testing.T) {
	ctx := context.Background()
	clusterName := "c0"
	key1cluster := mysql.GetClusterInstanceKey(clusterName, &key1)
	key2cluster := mysql.GetClusterInstanceKey(clusterName, &key2)
	key3cluster := mysql.GetClusterInstanceKey(clusterName, &key3)
	key4cluster := mysql.GetClusterInstanceKey(clusterName, &key4)
	key5cluster := mysql.GetClusterInstanceKey(clusterName, &key5)
	instanceResultsMap := mysql.InstanceMetricResultMap{
		key1cluster: base.NewSimpleMetricResult(1.2),
		key2cluster: base.NewSimpleMetricResult(1.7),
		key3cluster: base.NewSimpleMetricResult(0.3),
		key4cluster: base.NewSimpleMetricResult(0.6),
		key5cluster: base.NewSimpleMetricResult(1.1),
	}
	var probes mysql.Probes = map[mysql.InstanceKey](*mysql.Probe){}
	for clusterKey := range instanceResultsMap {
		probes[clusterKey.Key] = &mysql.Probe{Key: clusterKey.Key}
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, &probes, clusterName, instanceResultsMap, 0, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.7)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, &probes, clusterName, instanceResultsMap, 1, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.2)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, &probes, clusterName, instanceResultsMap, 2, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.1)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, &probes, clusterName, instanceResultsMap, 3, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.6)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, &probes, clusterName, instanceResultsMap, 4, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.6)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, &probes, clusterName, instanceResultsMap, 5, false, 1.0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 0.6)
	}
}

func TestAggregateMySQLProbesWithErrors(t *testing.T) {
	ctx := context.Background()
	clusterName := "c0"
	key1cluster := mysql.GetClusterInstanceKey(clusterName, &key1)
	key2cluster := mysql.GetClusterInstanceKey(clusterName, &key2)
	key3cluster := mysql.GetClusterInstanceKey(clusterName, &key3)
	key4cluster := mysql.GetClusterInstanceKey(clusterName, &key4)
	key5cluster := mysql.GetClusterInstanceKey(clusterName, &key5)
	instanceResultsMap := mysql.InstanceMetricResultMap{
		key1cluster: base.NewSimpleMetricResult(1.2),
		key2cluster: base.NewSimpleMetricResult(1.7),
		key3cluster: base.NewSimpleMetricResult(0.3),
		key4cluster: base.NoSuchMetric,
		key5cluster: base.NewSimpleMetricResult(1.1),
	}
	var probes mysql.Probes = map[mysql.InstanceKey](*mysql.Probe){}
	for clusterKey := range instanceResultsMap {
		probes[clusterKey.Key] = &mysql.Probe{Key: clusterKey.Key}
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, &probes, clusterName, instanceResultsMap, 0, false, 0)
		_, err := worstMetric.Get()
		assert.Error(t, err)
		assert.Equal(t, err, base.ErrNoSuchMetric)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, &probes, clusterName, instanceResultsMap, 1, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.7)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, &probes, clusterName, instanceResultsMap, 2, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.2)
	}

	instanceResultsMap[key1cluster] = base.NoSuchMetric
	{
		worstMetric := aggregateMySQLProbes(ctx, &probes, clusterName, instanceResultsMap, 0, false, 0)
		_, err := worstMetric.Get()
		assert.Error(t, err)
		assert.Equal(t, err, base.ErrNoSuchMetric)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, &probes, clusterName, instanceResultsMap, 1, false, 0)
		_, err := worstMetric.Get()
		assert.Error(t, err)
		assert.Equal(t, err, base.ErrNoSuchMetric)
	}
	{
		worstMetric := aggregateMySQLProbes(ctx, &probes, clusterName, instanceResultsMap, 2, false, 0)
		value, err := worstMetric.Get()
		assert.NoError(t, err)
		assert.Equal(t, value, 1.7)
	}
}
