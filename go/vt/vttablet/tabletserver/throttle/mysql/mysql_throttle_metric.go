/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package mysql

import (
	"fmt"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"
	metrics "github.com/rcrowley/go-metrics"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"
)

var mysqlMetricCache = cache.New(cache.NoExpiration, 10*time.Millisecond)

func getMySQLMetricCacheKey(probe *Probe) string {
	return fmt.Sprintf("%s:%s", probe.Key, probe.MetricQuery)
}

func cacheMySQLThrottleMetric(probe *Probe, mySQLThrottleMetric *MySQLThrottleMetric) *MySQLThrottleMetric {
	if mySQLThrottleMetric.Err != nil {
		return mySQLThrottleMetric
	}
	if probe.CacheMillis > 0 {
		mysqlMetricCache.Set(getMySQLMetricCacheKey(probe), mySQLThrottleMetric, time.Duration(probe.CacheMillis)*time.Millisecond)
	}
	return mySQLThrottleMetric
}

func getCachedMySQLThrottleMetric(probe *Probe) *MySQLThrottleMetric {
	if probe.CacheMillis == 0 {
		return nil
	}
	if metric, found := mysqlMetricCache.Get(getMySQLMetricCacheKey(probe)); found {
		mySQLThrottleMetric, _ := metric.(*MySQLThrottleMetric)
		return mySQLThrottleMetric
	}
	return nil
}

// MySQLThrottleMetric has the probed metric for a mysql instance
type MySQLThrottleMetric struct {
	ClusterName string
	Key         InstanceKey
	Value       float64
	Err         error
}

// NewMySQLThrottleMetric creates a new MySQLThrottleMetric
func NewMySQLThrottleMetric() *MySQLThrottleMetric {
	return &MySQLThrottleMetric{Value: 0}
}

// GetClusterInstanceKey returns the ClusterInstanceKey part of the metric
func (metric *MySQLThrottleMetric) GetClusterInstanceKey() ClusterInstanceKey {
	return GetClusterInstanceKey(metric.ClusterName, &metric.Key)
}

// Get implements MetricResult
func (metric *MySQLThrottleMetric) Get() (float64, error) {
	return metric.Value, metric.Err
}

// ReadThrottleMetric returns a metric for the given probe. Either by explicit query
// or via SHOW SLAVE STATUS
func ReadThrottleMetric(probe *Probe, clusterName string, overrideGetMetricFunc func() *MySQLThrottleMetric) (mySQLThrottleMetric *MySQLThrottleMetric) {
	if mySQLThrottleMetric := getCachedMySQLThrottleMetric(probe); mySQLThrottleMetric != nil {
		return mySQLThrottleMetric
		// On cached results we avoid taking latency metrics
	}

	started := time.Now()
	mySQLThrottleMetric = NewMySQLThrottleMetric()
	mySQLThrottleMetric.ClusterName = clusterName
	mySQLThrottleMetric.Key = probe.Key

	defer func(metric *MySQLThrottleMetric, started time.Time) {
		go func() {
			metrics.GetOrRegisterTimer("probes.latency", nil).Update(time.Since(started))
			metrics.GetOrRegisterCounter("probes.total", nil).Inc(1)
			if metric.Err != nil {
				metrics.GetOrRegisterCounter("probes.error", nil).Inc(1)
			}
		}()
	}(mySQLThrottleMetric, started)

	if overrideGetMetricFunc != nil {
		mySQLThrottleMetric = overrideGetMetricFunc()
		return cacheMySQLThrottleMetric(probe, mySQLThrottleMetric)
	}

	dbURI := probe.GetDBUri("information_schema")
	db, fromCache, err := sqlutils.GetDB(dbURI)

	if err != nil {
		mySQLThrottleMetric.Err = err
		return mySQLThrottleMetric
	}
	if !fromCache {
		db.SetMaxOpenConns(maxPoolConnections)
		db.SetMaxIdleConns(maxIdleConnections)
	}
	if strings.HasPrefix(strings.ToLower(probe.MetricQuery), "select") {
		mySQLThrottleMetric.Err = db.QueryRow(probe.MetricQuery).Scan(&mySQLThrottleMetric.Value)
		return cacheMySQLThrottleMetric(probe, mySQLThrottleMetric)
	}

	if strings.HasPrefix(strings.ToLower(probe.MetricQuery), "show global") {
		var variableName string // just a placeholder
		mySQLThrottleMetric.Err = db.QueryRow(probe.MetricQuery).Scan(&variableName, &mySQLThrottleMetric.Value)
		return cacheMySQLThrottleMetric(probe, mySQLThrottleMetric)
	}

	if probe.MetricQuery != "" {
		mySQLThrottleMetric.Err = fmt.Errorf("Unsupported metrics query type: %s", probe.MetricQuery)
		return mySQLThrottleMetric
	}

	// No metric query? By default we look at replication lag as output of SHOW SLAVE STATUS

	mySQLThrottleMetric.Err = sqlutils.QueryRowsMap(db, `show slave status`, func(m sqlutils.RowMap) error {
		slaveIORunning := m.GetString("Slave_IO_Running")
		slaveSQLRunning := m.GetString("Slave_SQL_Running")
		secondsBehindMaster := m.GetNullInt64("Seconds_Behind_Master")
		if !secondsBehindMaster.Valid {
			return fmt.Errorf("replication not running; Slave_IO_Running=%+v, Slave_SQL_Running=%+v", slaveIORunning, slaveSQLRunning)
		}
		mySQLThrottleMetric.Value = float64(secondsBehindMaster.Int64)
		return nil
	})
	return cacheMySQLThrottleMetric(probe, mySQLThrottleMetric)
}
