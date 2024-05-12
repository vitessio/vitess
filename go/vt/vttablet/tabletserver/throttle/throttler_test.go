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
	"context"
	"fmt"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/config"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/mysql"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	selfMetrics = mysql.MySQLThrottleMetrics{
		base.LagMetricName: &mysql.MySQLThrottleMetric{
			StoreName: selfStoreName,
			Alias:     "",
			Value:     0.3,
			Err:       nil,
		},
		base.ThreadsRunningMetricName: &mysql.MySQLThrottleMetric{
			StoreName: selfStoreName,
			Alias:     "",
			Value:     26,
			Err:       nil,
		},
		base.CustomMetricName: &mysql.MySQLThrottleMetric{
			StoreName: selfStoreName,
			Alias:     "",
			Value:     17,
			Err:       nil,
		},
		base.LoadAvgMetricName: &mysql.MySQLThrottleMetric{
			StoreName: selfStoreName,
			Alias:     "",
			Value:     2.718,
			Err:       nil,
		},
	}
	replicaMetrics = map[string]*MetricResult{
		base.LagMetricName.String(): {
			StatusCode: http.StatusOK,
			Value:      0.9,
		},
		base.ThreadsRunningMetricName.String(): {
			StatusCode: http.StatusOK,
			Value:      13,
		},
		base.CustomMetricName.String(): {
			StatusCode: http.StatusOK,
			Value:      14,
		},
		base.LoadAvgMetricName.String(): {
			StatusCode: http.StatusOK,
			Value:      0.8,
		},
	}
)

const (
	waitForProbesTimeout = 30 * time.Second
)

type fakeTMClient struct {
	tmclient.TabletManagerClient
	appNames []string
	v19      atomic.Bool // help validate v19 backwards compatibility

	mu sync.Mutex
}

func (c *fakeTMClient) Close() {
}

func (c *fakeTMClient) CheckThrottler(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.CheckThrottlerRequest) (*tabletmanagerdatapb.CheckThrottlerResponse, error) {
	resp := &tabletmanagerdatapb.CheckThrottlerResponse{
		StatusCode:      http.StatusOK,
		Value:           0.139,
		Threshold:       1,
		RecentlyChecked: false,
	}
	if !c.v19.Load() {
		resp.Metrics = make(map[string]*tabletmanagerdatapb.CheckThrottlerResponse_Metric)
		for name, metric := range replicaMetrics {
			resp.Metrics[name] = &tabletmanagerdatapb.CheckThrottlerResponse_Metric{
				Name:       name,
				StatusCode: int32(metric.StatusCode),
				Value:      metric.Value,
				Threshold:  metric.Threshold,
				Message:    metric.Message,
			}
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.appNames = append(c.appNames, request.AppName)
	return resp, nil
}

func (c *fakeTMClient) AppNames() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.appNames
}

type FakeTopoServer struct {
}

func (ts *FakeTopoServer) GetTablet(ctx context.Context, alias *topodatapb.TabletAlias) (*topo.TabletInfo, error) {
	tabletType := topodatapb.TabletType_PRIMARY
	if alias.Uid != 100 {
		tabletType = topodatapb.TabletType_REPLICA
	}
	tablet := &topo.TabletInfo{
		Tablet: &topodatapb.Tablet{
			Alias:         alias,
			Hostname:      "127.0.0.1",
			MysqlHostname: "127.0.0.1",
			MysqlPort:     3306,
			PortMap:       map[string]int32{"vt": 5000},
			Type:          tabletType,
		},
	}
	return tablet, nil
}

func (ts *FakeTopoServer) FindAllTabletAliasesInShard(ctx context.Context, keyspace, shard string) ([]*topodatapb.TabletAlias, error) {
	aliases := []*topodatapb.TabletAlias{
		{Cell: "fakezone0", Uid: 100},
		{Cell: "fakezone1", Uid: 101},
		{Cell: "fakezone2", Uid: 102},
	}
	return aliases, nil
}

func (ts *FakeTopoServer) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	ks := &topodatapb.SrvKeyspace{}
	return ks, nil
}

type FakeHeartbeatWriter struct {
	requests atomic.Int64
}

func (w *FakeHeartbeatWriter) RequestHeartbeats() {
	w.requests.Add(1)
}

func (w *FakeHeartbeatWriter) Requests() int64 {
	return w.requests.Load()
}

func init() {
	for metricName, metric := range selfMetrics {
		metric.Name = metricName
	}
}

func TestGetAggregatedMetricName(t *testing.T) {
	assert.Equal(t, "self", getAggregatedMetricName("self", base.DefaultMetricName))
	assert.Equal(t, "self/lag", getAggregatedMetricName("self", base.LagMetricName))
	assert.Equal(t, "shard/loadavg", getAggregatedMetricName("shard", base.LoadAvgMetricName))
}

func newTestThrottler() *Throttler {
	metricsQuery := "select 1"

	env := tabletenv.NewEnv(vtenv.NewTestEnv(), nil, "TabletServerTest")
	throttler := &Throttler{
		mysqlClusterProbesChan: make(chan *mysql.ClusterProbes),
		heartbeatWriter:        &FakeHeartbeatWriter{},
		ts:                     &FakeTopoServer{},
		mysqlInventory:         mysql.NewInventory(),
		pool:                   connpool.NewPool(env, "ThrottlerPool", tabletenv.ConnPoolConfig{}),
		tabletTypeFunc:         func() topodatapb.TabletType { return topodatapb.TabletType_PRIMARY },
		overrideTmClient:       &fakeTMClient{},
	}
	throttler.metricsQuery.Store(metricsQuery)
	throttler.MetricsThreshold.Store(math.Float64bits(0.75))
	throttler.configSettings = config.NewConfigurationSettings()
	throttler.initConfig()
	throttler.mysqlThrottleMetricChan = make(chan *mysql.MySQLThrottleMetric)
	throttler.mysqlClusterProbesChan = make(chan *mysql.ClusterProbes)
	throttler.throttlerConfigChan = make(chan *topodatapb.ThrottlerConfig)
	throttler.serialFuncChan = make(chan func())
	throttler.mysqlInventory = mysql.NewInventory()

	throttler.throttledApps = cache.New(cache.NoExpiration, 0)
	throttler.mysqlMetricThresholds = cache.New(cache.NoExpiration, 0)
	throttler.aggregatedMetrics = cache.New(10*aggregatedMetricsExpiration, 0)
	throttler.recentApps = cache.New(recentAppsExpiration, 0)
	throttler.metricsHealth = cache.New(cache.NoExpiration, 0)
	throttler.appCheckedMetrics = cache.New(cache.NoExpiration, 0)
	throttler.nonLowPriorityAppRequestsThrottled = cache.New(nonDeprioritizedAppMapExpiration, 0)
	throttler.initThrottleTabletTypes()
	throttler.check = NewThrottlerCheck(throttler)

	// High contention & racy intervals:
	throttler.leaderCheckInterval = 10 * time.Millisecond
	throttler.mysqlCollectInterval = 10 * time.Millisecond
	throttler.mysqlDormantCollectInterval = 10 * time.Millisecond
	throttler.mysqlRefreshInterval = 10 * time.Millisecond
	throttler.mysqlAggregateInterval = 10 * time.Millisecond
	throttler.throttledAppsSnapshotInterval = 10 * time.Millisecond
	throttler.dormantPeriod = 5 * time.Second
	throttler.recentCheckDormantDiff = int64(throttler.dormantPeriod / recentCheckRateLimiterInterval)

	throttler.readSelfThrottleMetrics = func(ctx context.Context) mysql.MySQLThrottleMetrics {
		for _, metric := range selfMetrics {
			go func() { throttler.mysqlThrottleMetricChan <- metric }()
		}
		return selfMetrics
	}
	throttler.ThrottleApp(throttlerapp.TestingAlwaysThrottlerName.String(), time.Now().Add(time.Hour*24*365*10), DefaultThrottleRatio, false)

	return throttler
}

// runSerialFunction runs the given function inside the throttler's serial and goroutine-safe main `select` loop.
// This function returns a channel that is populated when the input function is completed. Callers of this
// function should read from the channel if they want to block until the function is completed, or that could
// ignore the channel if they just want to fire-and-forget the function.
func runSerialFunction(t *testing.T, ctx context.Context, throttler *Throttler, f func(context.Context)) (done chan any) {
	done = make(chan any, 1)
	select {
	case throttler.serialFuncChan <- func() {
		f(ctx)
		done <- true
	}:
	case <-ctx.Done():
		assert.FailNow(t, ctx.Err().Error(), "waiting in runSerialFunction")
	}
	return done
}

func TestApplyThrottlerConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	throttler := newTestThrottler()
	throttlerConfig := &topodatapb.ThrottlerConfig{
		Enabled:   false,
		Threshold: 14,
		AppCheckedMetrics: map[string]*topodatapb.ThrottlerConfig_MetricNames{
			"app1":                              {Names: []string{"lag", "threads_running"}},
			throttlerapp.OnlineDDLName.String(): {Names: []string{"loadavg"}},
		},
	}
	assert.Equal(t, 0.75, throttler.GetMetricsThreshold())
	throttler.appCheckedMetrics.Set("app1", base.MetricNames{base.ThreadsRunningMetricName}, cache.DefaultExpiration)
	throttler.appCheckedMetrics.Set("app2", base.MetricNames{base.ThreadsRunningMetricName}, cache.DefaultExpiration)
	throttler.appCheckedMetrics.Set("app3", base.MetricNames{base.ThreadsRunningMetricName}, cache.DefaultExpiration)
	runThrottler(t, ctx, throttler, 10*time.Second, func(t *testing.T, ctx context.Context) {
		assert.True(t, throttler.IsEnabled())
		throttler.applyThrottlerConfig(ctx, throttlerConfig)
		cancel() // end test early
	})
	assert.False(t, throttler.IsEnabled())
	assert.Equal(t, float64(14), throttler.GetMetricsThreshold())
	assert.Equal(t, 2, throttler.appCheckedMetrics.ItemCount())
	{
		value, ok := throttler.appCheckedMetrics.Get("app1")
		assert.True(t, ok)
		names := value.(base.MetricNames)
		assert.Equal(t, base.MetricNames{base.LagMetricName, base.ThreadsRunningMetricName}, names)
	}
	{
		value, ok := throttler.appCheckedMetrics.Get(throttlerapp.OnlineDDLName.String())
		assert.True(t, ok)
		names := value.(base.MetricNames)
		assert.Equal(t, base.MetricNames{base.LoadAvgMetricName}, names)
	}
}

func TestIsAppThrottled(t *testing.T) {
	plusOneHour := time.Now().Add(time.Hour)
	throttler := Throttler{
		throttledApps:   cache.New(cache.NoExpiration, 0),
		heartbeatWriter: &FakeHeartbeatWriter{},
	}
	t.Run("initial", func(t *testing.T) {
		assert.False(t, throttler.IsAppThrottled("app1"))
		assert.False(t, throttler.IsAppThrottled("app2"))
		assert.False(t, throttler.IsAppThrottled("app3"))
		assert.False(t, throttler.IsAppThrottled("app4"))
	})
	//
	t.Run("set some rules", func(t *testing.T) {
		throttler.ThrottleApp("app1", plusOneHour, DefaultThrottleRatio, true)
		throttler.ThrottleApp("app2", time.Now(), DefaultThrottleRatio, false)
		throttler.ThrottleApp("app3", plusOneHour, DefaultThrottleRatio, false)
		throttler.ThrottleApp("app4", plusOneHour, 0, false)
		assert.False(t, throttler.IsAppThrottled("app1")) // exempted
		assert.False(t, throttler.IsAppThrottled("app2")) // expired
		assert.True(t, throttler.IsAppThrottled("app3"))
		assert.False(t, throttler.IsAppThrottled("app4"))      // ratio is zero
		assert.False(t, throttler.IsAppThrottled("app_other")) // not specified
	})
	t.Run("all", func(t *testing.T) {
		// throttle "all", see how it affects app
		throttler.ThrottleApp(throttlerapp.AllName.String(), plusOneHour, DefaultThrottleRatio, false)
		defer throttler.UnthrottleApp(throttlerapp.AllName.String())
		assert.True(t, throttler.IsAppThrottled("all"))   //
		assert.False(t, throttler.IsAppThrottled("app1")) // exempted
		assert.True(t, throttler.IsAppThrottled("app2"))  // expired, so falls under "all"
		assert.True(t, throttler.IsAppThrottled("app3"))
		assert.False(t, throttler.IsAppThrottled("app4"))     // ratio is zero, there is a specific instruction for this app, so it doesn't fall under "all"
		assert.True(t, throttler.IsAppThrottled("app_other")) // falls under "all"
	})
	//
	t.Run("unthrottle", func(t *testing.T) {
		throttler.UnthrottleApp("app1")
		throttler.UnthrottleApp("app2")
		throttler.UnthrottleApp("app3")
		throttler.UnthrottleApp("app4")
		assert.False(t, throttler.IsAppThrottled("app1"))
		assert.False(t, throttler.IsAppThrottled("app2"))
		assert.False(t, throttler.IsAppThrottled("app3"))
		assert.False(t, throttler.IsAppThrottled("app4"))
	})
	t.Run("all again", func(t *testing.T) {
		// throttle "all", see how it affects app
		throttler.ThrottleApp(throttlerapp.AllName.String(), plusOneHour, DefaultThrottleRatio, false)
		defer throttler.UnthrottleApp(throttlerapp.AllName.String())
		assert.True(t, throttler.IsAppThrottled("all"))
		assert.True(t, throttler.IsAppThrottled("app1"))
		assert.True(t, throttler.IsAppThrottled("app2"))
		assert.True(t, throttler.IsAppThrottled("app3"))
		assert.True(t, throttler.IsAppThrottled("app4"))
		assert.True(t, throttler.IsAppThrottled("app_other"))
	})
}

func TestIsAppExempted(t *testing.T) {

	throttler := Throttler{
		throttledApps:   cache.New(cache.NoExpiration, 0),
		heartbeatWriter: &FakeHeartbeatWriter{},
	}
	assert.False(t, throttler.IsAppExempted("app1"))
	assert.False(t, throttler.IsAppExempted("app2"))
	assert.False(t, throttler.IsAppExempted("app3"))
	//
	throttler.ThrottleApp("app1", time.Now().Add(time.Hour), DefaultThrottleRatio, true)
	throttler.ThrottleApp("app2", time.Now(), DefaultThrottleRatio, true) // instantly expire
	assert.True(t, throttler.IsAppExempted("app1"))
	assert.True(t, throttler.IsAppExempted("app1:other-tag"))
	assert.False(t, throttler.IsAppExempted("app2")) // expired
	assert.False(t, throttler.IsAppExempted("app3"))
	//
	throttler.UnthrottleApp("app1")
	throttler.ThrottleApp("app2", time.Now().Add(time.Hour), DefaultThrottleRatio, false)
	assert.False(t, throttler.IsAppExempted("app1"))
	assert.False(t, throttler.IsAppExempted("app2"))
	assert.False(t, throttler.IsAppExempted("app3"))
	//
	assert.True(t, throttler.IsAppExempted("schema-tracker"))
	throttler.UnthrottleApp("schema-tracker") // meaningless. App is statically exempted
	assert.True(t, throttler.IsAppExempted("schema-tracker"))
}

// TestRefreshMySQLInventory tests the behavior of the throttler's RefreshMySQLInventory() function, which
// is called periodically in actual throttler. For a given cluster name, it generates a list of probes
// the throttler will use to check metrics.
// On a replica tablet, that list is expect to probe the tablet itself.
// On the PRIMARY, the list includes all shard tablets, including the PRIMARY itself.
func TestRefreshMySQLInventory(t *testing.T) {
	metricsQuery := "select 1"
	configSettings := config.NewConfigurationSettings()

	throttler := &Throttler{
		mysqlClusterProbesChan: make(chan *mysql.ClusterProbes),
		mysqlMetricThresholds:  cache.New(cache.NoExpiration, 0),
		ts:                     &FakeTopoServer{},
		mysqlInventory:         mysql.NewInventory(),
	}
	throttler.metricsQuery.Store(metricsQuery)
	throttler.configSettings = configSettings
	throttler.initConfig()
	throttler.initThrottleTabletTypes()

	validateClusterProbes := func(t *testing.T, ctx context.Context) {
		testName := fmt.Sprintf("leader=%t", throttler.isLeader.Load())
		t.Run(testName, func(t *testing.T) {
			// validateProbesCount expects number of probes according to cluster name and throttler's leadership status
			validateProbesCount := func(t *testing.T, probes mysql.Probes) {
				if throttler.isLeader.Load() {
					assert.Equal(t, 3, len(probes))
				} else {
					assert.Equal(t, 1, len(probes))
				}
			}
			t.Run("waiting for probes", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(ctx, waitForProbesTimeout)
				defer cancel()
				for {
					select {
					case probes := <-throttler.mysqlClusterProbesChan:
						// Worth noting that in this unit test, the throttler is _closed_ and _disabled_. Its own Operate() function does
						// not run, and therefore there is none but us to both populate `mysqlClusterProbesChan` as well as
						// read from it. We do not compete here with any other goroutine.
						assert.NotNil(t, probes)
						throttler.updateMySQLClusterProbes(ctx, probes)
						validateProbesCount(t, probes.TabletProbes)
						// Achieved our goal
						return
					case <-ctx.Done():
						assert.FailNowf(t, ctx.Err().Error(), "waiting for cluster probes")
					}
				}
			})
			t.Run("validating probes", func(t *testing.T) {
				probes := throttler.mysqlInventory.ClustersProbes
				validateProbesCount(t, probes)
			})
		})
	}
	//
	ctx := context.Background()

	t.Run("initial, not leader", func(t *testing.T) {
		throttler.isLeader.Store(false)
		throttler.refreshMySQLInventory(ctx)
		validateClusterProbes(t, ctx)
	})

	t.Run("promote", func(t *testing.T) {
		throttler.isLeader.Store(true)
		throttler.refreshMySQLInventory(ctx)
		validateClusterProbes(t, ctx)
	})

	t.Run("demote, expect cleanup", func(t *testing.T) {
		throttler.isLeader.Store(false)
		throttler.refreshMySQLInventory(ctx)
		validateClusterProbes(t, ctx)
	})
}

// runThrottler opens and enables the throttler, thereby making it run the Operate() function, for a given amount of time.
// Optionally, running a given function halfway while the throttler is still open and running.
func runThrottler(t *testing.T, ctx context.Context, throttler *Throttler, timeout time.Duration, f func(*testing.T, context.Context)) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	assert.False(t, throttler.IsOpen())
	assert.False(t, throttler.IsEnabled())

	throttler.isOpen.Swap(true)
	defer throttler.isOpen.Swap(false)
	assert.True(t, throttler.IsOpen())
	assert.False(t, throttler.IsEnabled())

	wg := throttler.Enable()
	require.NotNil(t, wg)
	defer wg.Wait()
	defer throttler.Disable()
	assert.True(t, throttler.IsEnabled())

	// Enabling again does nothing:
	wg2 := throttler.Enable()
	assert.Nil(t, wg2)

	sleepTime := 3 * time.Second
	if timeout/2 < sleepTime {
		sleepTime = timeout / 2
	}
	if f != nil {
		select {
		case <-ctx.Done():
			return
		case <-time.After(sleepTime):
			f(t, ctx)
		}
	}

	<-ctx.Done()
	assert.Error(t, ctx.Err())

	throttler.Disable()
	assert.False(t, throttler.IsEnabled())
}

// TestRace merely lets the throttler run with aggressive intervals for a few seconds, so as to detect race conditions.
// This is relevant to `go test -race`
func TestRace(t *testing.T) {
	throttler := newTestThrottler()
	runThrottler(t, context.Background(), throttler, 5*time.Second, nil)
}

// TestProbes enables a throttler for a few seconds, and afterwards expects to find probes and metrics.
func TestProbesWhileOperating(t *testing.T) {
	throttler := newTestThrottler()

	tmClient, ok := throttler.overrideTmClient.(*fakeTMClient)
	require.True(t, ok)
	assert.Empty(t, tmClient.AppNames())

	t.Run("aggregated initial", func(t *testing.T) {
		assert.Equal(t, 0, throttler.aggregatedMetrics.ItemCount())
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runThrottler(t, ctx, throttler, time.Minute, func(t *testing.T, ctx context.Context) {
		t.Run("aggregated", func(t *testing.T) {
			assert.Equal(t, base.LagMetricName, throttler.metricNameUsedAsDefault())
			aggr := throttler.aggregatedMetricsSnapshot()
			assert.Equalf(t, 2*len(base.KnownMetricNames), len(aggr), "aggregated: %+v", aggr)     // "self" and "shard", per known metric
			assert.Equal(t, 2*len(base.KnownMetricNames), throttler.aggregatedMetrics.ItemCount()) // flushed upon Disable()
			for aggregatedMetricName, metricResult := range aggr {
				val, err := metricResult.Get()
				assert.NoErrorf(t, err, "aggregatedMetricName: %v", aggregatedMetricName)
				assert.NotEmpty(t, aggregatedMetricName)
				storeName, metricName, err := splitMetricTokens(aggregatedMetricName)
				assert.NotEmpty(t, metricName)
				assert.NoError(t, err)

				switch storeName {
				case selfStoreName:
					switch metricName {
					case base.DefaultMetricName:
						assert.Equalf(t, float64(0.3), val, "storeName=%v, metricName=%v", storeName, metricName) // same value as "lag"
					case base.LagMetricName:
						assert.Equalf(t, float64(0.3), val, "storeName=%v, metricName=%v", storeName, metricName)
					case base.ThreadsRunningMetricName:
						assert.Equalf(t, float64(26), val, "storeName=%v, metricName=%v", storeName, metricName)
					case base.CustomMetricName:
						assert.Equalf(t, float64(17), val, "storeName=%v, metricName=%v", storeName, metricName)
					case base.LoadAvgMetricName:
						assert.Equalf(t, float64(2.718), val, "storeName=%v, metricName=%v", storeName, metricName)
					}
				case shardStoreName:
					switch metricName {
					case base.DefaultMetricName:
						assert.Equalf(t, float64(0.9), val, "storeName=%v, metricName=%v", storeName, metricName) // same value as "lag"
					case base.LagMetricName:
						assert.Equalf(t, float64(0.9), val, "storeName=%v, metricName=%v", storeName, metricName)
					case base.ThreadsRunningMetricName:
						assert.Equalf(t, float64(13), val, "storeName=%v, metricName=%v", storeName, metricName)
					case base.CustomMetricName:
						assert.Equalf(t, float64(14), val, "storeName=%v, metricName=%v", storeName, metricName)
					case base.LoadAvgMetricName:
						assert.Equalf(t, float64(0.8), val, "storeName=%v, metricName=%v", storeName, metricName)
					}
				default:
					assert.Failf(t, "unknown storeName", "storeName=%v", storeName)
				}
			}
			assert.NotEmpty(t, tmClient.AppNames())
			// The throttler here emulates a PRIMARY tablet, and therefore should probe the replicas using
			// the "vitess" app name.
			uniqueNames := map[string]int{}
			for _, appName := range tmClient.AppNames() {
				uniqueNames[appName]++
			}
			// PRIMARY throttler probes replicas with empty app name, which is then
			// interpreted as "vitess" name.
			_, ok := uniqueNames[""]
			assert.Truef(t, ok, "%+v", uniqueNames)
			// And that's the only app we expect to see.
			assert.Equalf(t, 1, len(uniqueNames), "%+v", uniqueNames)

			t.Run("client, shard", func(t *testing.T) {
				client := NewProductionClient(throttler, "test", ThrottleCheckPrimaryWrite)
				t.Run("threshold exceeded", func(t *testing.T) {
					<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
						throttler.refreshMySQLInventory(ctx)
					})
					{
						checkOK := client.ThrottleCheckOK(ctx, "")
						assert.False(t, checkOK) // we expect threshold exceeded
					}
				})

				savedThreshold := throttler.MetricsThreshold.Load()
				t.Run("adjust threshold", func(t *testing.T) {
					throttler.MetricsThreshold.Store(math.Float64bits(0.95))
					<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
						throttler.refreshMySQLInventory(ctx)
					})
					{
						checkOK := client.ThrottleCheckOK(ctx, "")
						assert.True(t, checkOK)
					}
				})
				t.Run("restore threshold", func(t *testing.T) {
					throttler.MetricsThreshold.Store(savedThreshold)
					<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
						throttler.refreshMySQLInventory(ctx)
					})
					client.clearSuccessfulResultsCache() // ensure we don't read the successful result from the test above
					{
						checkOK := client.ThrottleCheckOK(ctx, "")
						assert.False(t, checkOK)
					}
				})
			})
		})

		t.Run("aggregated with custom query", func(t *testing.T) {
			// The query itself isn't important here, since we're emulating. What's important is that it's not empty.
			// Hence, the throttler will choose to set the "custom" metric results in the aggregated "default" metrics,
			// as opposed to choosing the "lag" metric results.
			throttler.customMetricsQuery.Store("select non_empty")
			<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
				throttler.aggregateMySQLMetrics(ctx)
			})
			assert.Equal(t, base.CustomMetricName, throttler.metricNameUsedAsDefault())
			// throttler.aggregateMySQLMetrics(ctx)
			aggr := throttler.aggregatedMetricsSnapshot()
			assert.Equalf(t, 2*len(base.KnownMetricNames), len(aggr), "aggregated: %+v", aggr)     // "self" and "shard", per known metric
			assert.Equal(t, 2*len(base.KnownMetricNames), throttler.aggregatedMetrics.ItemCount()) // flushed upon Disable()
			for aggregatedMetricName, metricResult := range aggr {
				val, err := metricResult.Get()
				assert.NoErrorf(t, err, "aggregatedMetricName: %v", aggregatedMetricName)
				assert.NotEmpty(t, aggregatedMetricName)
				storeName, metricName, err := splitMetricTokens(aggregatedMetricName)
				assert.NotEmpty(t, metricName)
				assert.NoError(t, err)

				switch storeName {
				case selfStoreName:
					switch metricName {
					case base.DefaultMetricName:
						assert.Equalf(t, float64(17), val, "storeName=%v, metricName=%v", storeName, metricName) // same value as "custom"
					case base.LagMetricName:
						assert.Equalf(t, float64(0.3), val, "storeName=%v, metricName=%v", storeName, metricName)
					case base.ThreadsRunningMetricName:
						assert.Equalf(t, float64(26), val, "storeName=%v, metricName=%v", storeName, metricName)
					case base.CustomMetricName:
						assert.Equalf(t, float64(17), val, "storeName=%v, metricName=%v", storeName, metricName)
					case base.LoadAvgMetricName:
						assert.Equalf(t, float64(2.718), val, "storeName=%v, metricName=%v", storeName, metricName)
					}
				case shardStoreName:
					switch metricName {
					case base.DefaultMetricName:
						assert.Equalf(t, float64(14), val, "storeName=%v, metricName=%v", storeName, metricName) // same value as "custom"
					case base.LagMetricName:
						assert.Equalf(t, float64(0.9), val, "storeName=%v, metricName=%v", storeName, metricName)
					case base.ThreadsRunningMetricName:
						assert.Equalf(t, float64(13), val, "storeName=%v, metricName=%v", storeName, metricName)
					case base.CustomMetricName:
						assert.Equalf(t, float64(14), val, "storeName=%v, metricName=%v", storeName, metricName)
					case base.LoadAvgMetricName:
						assert.Equalf(t, float64(0.8), val, "storeName=%v, metricName=%v", storeName, metricName)
					}
				default:
					assert.Failf(t, "unknown storeName", "storeName=%v", storeName)
				}
			}

			t.Run("client, shard", func(t *testing.T) {
				client := NewProductionClient(throttler, "test", ThrottleCheckPrimaryWrite)
				t.Run("threshold exceeded", func(t *testing.T) {
					<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
						throttler.refreshMySQLInventory(ctx)
					})
					{
						checkOK := client.ThrottleCheckOK(ctx, "")
						assert.False(t, checkOK) // we expect threshold exceeded
					}
				})

				savedThreshold := throttler.MetricsThreshold.Load()
				t.Run("adjust threshold, too low", func(t *testing.T) {
					throttler.MetricsThreshold.Store(math.Float64bits(0.95))
					<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
						throttler.refreshMySQLInventory(ctx)
					})
					{
						checkOK := client.ThrottleCheckOK(ctx, "")
						assert.False(t, checkOK) // 0.95 still too low for custom query
					}
				})
				t.Run("adjust threshold", func(t *testing.T) {
					throttler.MetricsThreshold.Store(math.Float64bits(15))
					<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
						throttler.refreshMySQLInventory(ctx)
					})
					{
						checkOK := client.ThrottleCheckOK(ctx, "")
						assert.True(t, checkOK)
					}
				})
				t.Run("restore threshold", func(t *testing.T) {
					throttler.MetricsThreshold.Store(savedThreshold)
					<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
						throttler.refreshMySQLInventory(ctx)
					})
					client.clearSuccessfulResultsCache() // ensure we don't read the successful result from the test above
					{
						checkOK := client.ThrottleCheckOK(ctx, "")
						assert.False(t, checkOK)
					}
				})
			})
		})
		cancel() // end test early
	})
}

// TestProbesWithV19Replicas is similar to TestProbesWhileOperating, but assumes a v19 replica, which does not report any of the named metrics.
func TestProbesWithV19Replicas(t *testing.T) {
	throttler := newTestThrottler()

	tmClient, ok := throttler.overrideTmClient.(*fakeTMClient)
	require.True(t, ok)
	assert.Empty(t, tmClient.AppNames())
	tmClient.v19.Store(true)

	t.Run("aggregated initial", func(t *testing.T) {
		assert.Equal(t, 0, throttler.aggregatedMetrics.ItemCount())
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runThrottler(t, ctx, throttler, time.Minute, func(t *testing.T, ctx context.Context) {
		t.Run("aggregated", func(t *testing.T) {
			aggr := throttler.aggregatedMetricsSnapshot()
			assert.Equalf(t, 2*len(base.KnownMetricNames), len(aggr), "aggregated: %+v", aggr)     // "self" and "shard", per known metric
			assert.Equal(t, 2*len(base.KnownMetricNames), throttler.aggregatedMetrics.ItemCount()) // flushed upon Disable()
			for aggregatedMetricName, metricResult := range aggr {
				assert.NotEmpty(t, aggregatedMetricName)
				storeName, metricName, err := splitMetricTokens(aggregatedMetricName)
				assert.NotEmpty(t, metricName)
				assert.NoError(t, err)

				val, metricResultErr := metricResult.Get()
				expectMetricNotCollectedYet := false
				switch storeName {
				case selfStoreName:
					switch metricName {
					case base.DefaultMetricName:
						assert.Equalf(t, float64(0.3), val, "storeName=%v, metricName=%v", storeName, metricName) // same value as "lag"
					case base.LagMetricName:
						assert.Equalf(t, float64(0.3), val, "storeName=%v, metricName=%v", storeName, metricName)
					case base.ThreadsRunningMetricName:
						assert.Equalf(t, float64(26), val, "storeName=%v, metricName=%v", storeName, metricName)
					case base.CustomMetricName:
						assert.Equalf(t, float64(17), val, "storeName=%v, metricName=%v", storeName, metricName)
					case base.LoadAvgMetricName:
						assert.Equalf(t, float64(2.718), val, "storeName=%v, metricName=%v", storeName, metricName)
					}
				case shardStoreName:
					// Replicas will nto report named metrics, since they now assume v19 behavior. They will only
					// produce the single v19 metric (which we call "default", though they don't advertise it under the name "base.DefaultMetricName")
					switch metricName {
					case base.DefaultMetricName:
						assert.Equalf(t, float64(0.139), val, "storeName=%v, metricName=%v", storeName, metricName) // same value as "lag"
					case base.LagMetricName:
						assert.Equalf(t, float64(0.139), val, "storeName=%v, metricName=%v", storeName, metricName) //
					default:
						assert.Zero(t, val, "storeName=%v, metricName=%v", storeName, metricName)
						expectMetricNotCollectedYet = true
					}
				default:
					assert.Failf(t, "unknown storeName", "storeName=%v", storeName)
				}
				if expectMetricNotCollectedYet {
					assert.ErrorIs(t, metricResultErr, base.ErrNoResultYet)
				} else {
					assert.NoErrorf(t, metricResultErr, "aggregatedMetricName: %v", aggregatedMetricName)
				}
			}
			assert.NotEmpty(t, tmClient.AppNames())
			// The throttler here emulates a PRIMARY tablet, and therefore should probe the replicas using
			// the "vitess" app name.
			uniqueNames := map[string]int{}
			for _, appName := range tmClient.AppNames() {
				uniqueNames[appName]++
			}
			// PRIMARY throttler probes replicas with empty app name, which is then
			// interpreted as "vitess" name.
			_, ok := uniqueNames[""]
			assert.Truef(t, ok, "%+v", uniqueNames)
			// And that's the only app we expect to see.
			assert.Equalf(t, 1, len(uniqueNames), "%+v", uniqueNames)
		})
		cancel() // end test early
	})
}

// TestProbesPostDisable runs the throttler for some time, and then investigates the internal throttler maps and values.
func TestProbesPostDisable(t *testing.T) {
	throttler := newTestThrottler()
	runThrottler(t, context.Background(), throttler, 2*time.Second, nil)

	probes := throttler.mysqlInventory.ClustersProbes

	<-time.After(1 * time.Second) // throttler's context was cancelled, but still some functionality needs to complete
	t.Run("probes", func(t *testing.T) {
		assert.Equal(t, 3, len(probes)) // see fake FindAllTabletAliasesInShard above
		localTabletFound := 0
		for _, probe := range probes {
			require.NotNil(t, probe)
			if probe.Alias == throttler.tabletAlias {
				localTabletFound++
			} else {
				assert.NotEmpty(t, probe.Alias)
				assert.NotNil(t, probe.Tablet)
			}
			assert.Zero(t, atomic.LoadInt64(&probe.QueryInProgress), "alias=%s", probe.Alias)
		}
		assert.Equal(t, 1, localTabletFound)
	})

	t.Run("metrics", func(t *testing.T) {
		assert.Equal(t, 3, len(throttler.mysqlInventory.TabletMetrics)) // 1 self tablet + 2 shard tablets
	})

	t.Run("aggregated", func(t *testing.T) {
		assert.Zero(t, throttler.aggregatedMetrics.ItemCount()) // flushed upon Disable()
		aggr := throttler.aggregatedMetricsSnapshot()
		assert.Empty(t, aggr)
	})
}

func TestDormant(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	throttler := newTestThrottler()

	heartbeatWriter, ok := throttler.heartbeatWriter.(*FakeHeartbeatWriter)
	assert.True(t, ok)
	assert.Zero(t, heartbeatWriter.Requests()) // once upon Enable()

	runThrottler(t, ctx, throttler, time.Minute, func(t *testing.T, ctx context.Context) {
		assert.True(t, throttler.isDormant())
		assert.EqualValues(t, 1, heartbeatWriter.Requests()) // once upon Enable()
		flags := &CheckFlags{
			CheckType: ThrottleCheckSelf,
		}
		throttler.Check(ctx, throttlerapp.VitessName.String(), nil, flags)
		go func() {
			select {
			case <-ctx.Done():
				require.FailNow(t, "context expired before testing completed")
			case <-time.After(time.Second):
				assert.True(t, throttler.isDormant())
				assert.EqualValues(t, 1, heartbeatWriter.Requests()) // "vitess" name does not cause heartbeat requests
			}
			throttler.Check(ctx, throttlerapp.ThrottlerStimulatorName.String(), nil, flags)
			select {
			case <-ctx.Done():
				require.FailNow(t, "context expired before testing completed")
			case <-time.After(time.Second):
				assert.False(t, throttler.isDormant())
				assert.Greater(t, heartbeatWriter.Requests(), int64(1))
			}
			throttler.Check(ctx, throttlerapp.OnlineDDLName.String(), nil, flags)
			select {
			case <-ctx.Done():
				require.FailNow(t, "context expired before testing completed")
			case <-time.After(time.Second):
				assert.False(t, throttler.isDormant())
				assert.Greater(t, heartbeatWriter.Requests(), int64(2))
			}

			// Dormant period
			select {
			case <-ctx.Done():
				require.FailNow(t, "context expired before testing completed")
			case <-time.After(throttler.dormantPeriod):
				assert.True(t, throttler.isDormant())
			}
			cancel() // end test early
		}()
	})
}

func TestReplica(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	throttler := newTestThrottler()
	throttler.dormantPeriod = time.Minute
	throttler.tabletTypeFunc = func() topodatapb.TabletType { return topodatapb.TabletType_REPLICA }

	tmClient, ok := throttler.overrideTmClient.(*fakeTMClient)
	require.True(t, ok)
	assert.Empty(t, tmClient.AppNames())

	runThrottler(t, ctx, throttler, time.Minute, func(t *testing.T, ctx context.Context) {
		assert.Empty(t, tmClient.AppNames())
		flags := &CheckFlags{
			CheckType: ThrottleCheckSelf,
		}
		checkResult := throttler.Check(ctx, throttlerapp.VitessName.String(), nil, flags)
		assert.NotNil(t, checkResult)
		go func() {
			t.Run("checks", func(t *testing.T) {
				select {
				case <-ctx.Done():
					require.FailNow(t, "context expired before testing completed")
				case <-time.After(time.Second):
					assert.Empty(t, tmClient.AppNames())
				}
				checkResult := throttler.Check(ctx, throttlerapp.OnlineDDLName.String(), nil, flags)
				assert.NotNil(t, checkResult)
				select {
				case <-ctx.Done():
					require.FailNow(t, "context expired before testing completed")
				case <-time.After(time.Second):
					appNames := tmClient.AppNames()
					// The replica reports to the primary that it had been checked, by issuing a CheckThrottler
					// on the primary using the ThrottlerStimulatorName app.
					assert.Equal(t, []string{throttlerapp.ThrottlerStimulatorName.String()}, appNames)
				}
				checkResult = throttler.Check(ctx, throttlerapp.OnlineDDLName.String(), nil, flags)
				assert.NotNil(t, checkResult)
				select {
				case <-ctx.Done():
					require.FailNow(t, "context expired before testing completed")
				case <-time.After(time.Second):
					// Due to stimulation rate limiting, we shouldn't see a 2nd CheckThrottler request.
					appNames := tmClient.AppNames()
					assert.Equal(t, []string{throttlerapp.ThrottlerStimulatorName.String()}, appNames)
				}
			})

			t.Run("metrics", func(t *testing.T) {
				// See which metrics are available
				checkResult = throttler.Check(ctx, throttlerapp.VitessName.String(), base.KnownMetricNames, flags)
				require.NotNil(t, checkResult)
				assert.Equal(t, len(base.KnownMetricNames), len(checkResult.Metrics))

				for metricName, metricResult := range checkResult.Metrics {
					val := metricResult.Value
					threshold := metricResult.Threshold
					storeName := selfStoreName
					switch base.MetricName(metricName) {
					case base.DefaultMetricName:
						assert.NoError(t, metricResult.Error, "metricName=%v, value=%v, threshold=%v", metricName, metricResult.Value, metricResult.Threshold)
						assert.Equalf(t, float64(0.3), val, "storeName=%v, metricName=%v", storeName, metricName) // same value as "lag"
						assert.Equalf(t, float64(0.75), threshold, "storeName=%v, metricName=%v", storeName, metricName)
					case base.LagMetricName:
						assert.NoError(t, metricResult.Error, "metricName=%v, value=%v, threshold=%v", metricName, metricResult.Value, metricResult.Threshold)
						assert.Equalf(t, float64(0.3), val, "storeName=%v, metricName=%v", storeName, metricName)
						assert.Equalf(t, float64(0.75), threshold, "storeName=%v, metricName=%v", storeName, metricName) // default threshold
					case base.ThreadsRunningMetricName:
						assert.NoError(t, metricResult.Error, "metricName=%v, value=%v, threshold=%v", metricName, metricResult.Value, metricResult.Threshold)
						assert.Equalf(t, float64(26), val, "storeName=%v, metricName=%v", storeName, metricName)
						assert.Equalf(t, float64(100), threshold, "storeName=%v, metricName=%v", storeName, metricName)
					case base.CustomMetricName:
						assert.ErrorIs(t, metricResult.Error, base.ErrThresholdExceeded)
						assert.Equalf(t, float64(0), threshold, "storeName=%v, metricName=%v", storeName, metricName)
					case base.LoadAvgMetricName:
						assert.ErrorIs(t, metricResult.Error, base.ErrThresholdExceeded)
						assert.Equalf(t, float64(1), threshold, "storeName=%v, metricName=%v", storeName, metricName)
					}
				}
			})
			t.Run("metrics not named", func(t *testing.T) {
				checkResult = throttler.Check(ctx, throttlerapp.VitessName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.Equal(t, 1, len(checkResult.Metrics))
				for metricName, metricResult := range checkResult.Metrics {
					assert.Equal(t, base.LagMetricName, throttler.metricNameUsedAsDefault())
					assert.Equal(t, base.LagMetricName.String(), metricName)
					val := metricResult.Value
					threshold := metricResult.Threshold
					storeName := selfStoreName

					assert.NoError(t, metricResult.Error, "metricName=%v, value=%v, threshold=%v", metricName, metricResult.Value, metricResult.Threshold)
					assert.Equalf(t, float64(0.3), val, "storeName=%v, metricName=%v", storeName, metricName)
					assert.Equalf(t, float64(0.75), threshold, "storeName=%v, metricName=%v", storeName, metricName) // default threshold
				}
			})
			t.Run("metrics names mapped", func(t *testing.T) {
				throttler.appCheckedMetrics.Set(throttlerapp.VitessName.String(), base.MetricNames{base.LoadAvgMetricName, base.LagMetricName, base.ThreadsRunningMetricName}, cache.DefaultExpiration)
				defer throttler.appCheckedMetrics.Delete(throttlerapp.VitessName.String())
				checkResult = throttler.Check(ctx, throttlerapp.VitessName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.Equal(t, 3, len(checkResult.Metrics))
			})
			t.Run("client, OK", func(t *testing.T) {
				client := NewProductionClient(throttler, throttlerapp.TestingName, ThrottleCheckSelf)
				checkOK := client.ThrottleCheckOK(ctx, "")
				assert.True(t, checkOK)
			})
			t.Run("client, metrics names mapped, OK", func(t *testing.T) {
				// Specified metrics do not exceed threshold, therefore overall result should be OK.
				throttler.appCheckedMetrics.Set(throttlerapp.TestingName.String(), base.MetricNames{base.LagMetricName, base.ThreadsRunningMetricName}, cache.DefaultExpiration)
				defer throttler.appCheckedMetrics.Delete(throttlerapp.TestingName.String())
				client := NewProductionClient(throttler, throttlerapp.TestingName, ThrottleCheckSelf)
				checkOK := client.ThrottleCheckOK(ctx, "")
				assert.True(t, checkOK)
			})
			t.Run("client, metrics names mapped, not OK", func(t *testing.T) {
				// LoadAvgMetricName metric exceeds threshold, therefore overall check should be in error.
				throttler.appCheckedMetrics.Set(throttlerapp.TestingName.String(), base.MetricNames{base.LagMetricName, base.LoadAvgMetricName, base.ThreadsRunningMetricName}, cache.DefaultExpiration)
				defer throttler.appCheckedMetrics.Delete(throttlerapp.TestingName.String())
				client := NewProductionClient(throttler, throttlerapp.TestingName, ThrottleCheckSelf)
				checkOK := client.ThrottleCheckOK(ctx, "")
				assert.False(t, checkOK)
			})

			t.Run("custom query, metrics", func(t *testing.T) {
				// For v19 backwards compatibility, we also report the standard metric/value in CheckResult:
				assert.NoError(t, checkResult.Error, "value=%v, threshold=%v", checkResult.Value, checkResult.Threshold)
				assert.Equal(t, float64(0.3), checkResult.Value)
				// Change custom threshold
				throttler.MetricsThreshold.Store(math.Float64bits(0.1))
				<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
					throttler.refreshMySQLInventory(ctx)
				})
				checkResult = throttler.Check(ctx, throttlerapp.VitessName.String(), base.KnownMetricNames, flags)
				require.NotNil(t, checkResult)
				assert.Equal(t, len(base.KnownMetricNames), len(checkResult.Metrics))

				assert.Equal(t, base.LagMetricName, throttler.metricNameUsedAsDefault())

				for metricName, metricResult := range checkResult.Metrics {
					switch base.MetricName(metricName) {
					case base.CustomMetricName,
						base.LagMetricName, // Lag metrics affected by the new low threshold
						base.LoadAvgMetricName,
						base.DefaultMetricName:
						assert.Error(t, metricResult.Error, "metricName=%v, value=%v, threshold=%v", metricName, metricResult.Value, metricResult.Threshold)
						assert.ErrorIs(t, metricResult.Error, base.ErrThresholdExceeded)
					case base.ThreadsRunningMetricName:
						assert.NoError(t, metricResult.Error, "metricName=%v, value=%v, threshold=%v", metricName, metricResult.Value, metricResult.Threshold)
					}
				}
			})
			t.Run("client, not OK", func(t *testing.T) {
				client := NewProductionClient(throttler, throttlerapp.TestingName, ThrottleCheckSelf)
				checkOK := client.ThrottleCheckOK(ctx, "")
				assert.False(t, checkOK)
			})

			// done
			cancel() // end test early
		}()
	})
}
