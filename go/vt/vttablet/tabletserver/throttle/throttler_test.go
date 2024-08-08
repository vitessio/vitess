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
	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/config"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	selfMetrics = base.ThrottleMetrics{
		base.LagMetricName: &base.ThrottleMetric{
			Scope: base.SelfScope,
			Alias: "",
			Value: 0.3,
			Err:   nil,
		},
		base.ThreadsRunningMetricName: &base.ThrottleMetric{
			Scope: base.SelfScope,
			Alias: "",
			Value: 26,
			Err:   nil,
		},
		base.CustomMetricName: &base.ThrottleMetric{
			Scope: base.SelfScope,
			Alias: "",
			Value: 17,
			Err:   nil,
		},
		base.LoadAvgMetricName: &base.ThrottleMetric{
			Scope: base.SelfScope,
			Alias: "",
			Value: 2.718,
			Err:   nil,
		},
	}
	replicaMetrics = map[string]*MetricResult{
		base.LagMetricName.String(): {
			StatusCode:   http.StatusOK,
			ResponseCode: tabletmanagerdatapb.CheckThrottlerResponseCode_OK,
			Value:        0.9,
		},
		base.ThreadsRunningMetricName.String(): {
			StatusCode:   http.StatusOK,
			ResponseCode: tabletmanagerdatapb.CheckThrottlerResponseCode_OK,
			Value:        13,
		},
		base.CustomMetricName.String(): {
			StatusCode:   http.StatusOK,
			ResponseCode: tabletmanagerdatapb.CheckThrottlerResponseCode_OK,
			Value:        14,
		},
		base.LoadAvgMetricName.String(): {
			StatusCode:   http.StatusOK,
			ResponseCode: tabletmanagerdatapb.CheckThrottlerResponseCode_OK,
			Value:        5.1,
		},
	}
	nonPrimaryTabletType atomic.Int32
)

const (
	waitForProbesTimeout = 30 * time.Second
	testAppName          = throttlerapp.TestingName
)

type fakeTMClient struct {
	tmclient.TabletManagerClient
	appNames []string
	v20      atomic.Bool // help validate v20 backwards compatibility

	mu sync.Mutex
}

func (c *fakeTMClient) Close() {
}

func (c *fakeTMClient) CheckThrottler(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.CheckThrottlerRequest) (*tabletmanagerdatapb.CheckThrottlerResponse, error) {
	resp := &tabletmanagerdatapb.CheckThrottlerResponse{
		StatusCode:      http.StatusOK,
		Value:           0.339,
		Threshold:       1,
		RecentlyChecked: false,
	}
	if !c.v20.Load() {
		resp.ResponseCode = tabletmanagerdatapb.CheckThrottlerResponseCode_OK
		resp.Metrics = make(map[string]*tabletmanagerdatapb.CheckThrottlerResponse_Metric)
		for name, metric := range replicaMetrics {
			resp.Metrics[name] = &tabletmanagerdatapb.CheckThrottlerResponse_Metric{
				Name:         name,
				StatusCode:   int32(metric.StatusCode),
				ResponseCode: metric.ResponseCode,
				Value:        metric.Value,
				Threshold:    metric.Threshold,
				Message:      metric.Message,
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
		val := topodatapb.TabletType(nonPrimaryTabletType.Load())
		if val == topodatapb.TabletType_UNKNOWN {
			val = topodatapb.TabletType_REPLICA
		}
		tabletType = val
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

func waitForMetricsToBeCollected(t *testing.T, ctx context.Context, throttler *Throttler) {
	ticker := time.NewTicker(100 * time.Millisecond)
	for {
		foundAll := true
		aggr := throttler.aggregatedMetricsSnapshot()
		for _, metric := range base.KnownMetricNames {
			if _, ok := aggr[metric.AggregatedName(base.SelfScope)]; !ok {
				foundAll = false
				break
			}
			if _, ok := aggr[metric.AggregatedName(base.ShardScope)]; !ok {
				foundAll = false
				break
			}
		}
		if foundAll {
			return
		}
		select {
		case <-ctx.Done():
			assert.Fail(t, "timed out waiting for metrics to be collected")
			return
		case <-ticker.C:
		}
	}
}
func sleepTillThresholdApplies() {
	time.Sleep(time.Second)
}

func TestGetAggregatedMetricName(t *testing.T) {
	assert.Equal(t, "self", base.DefaultMetricName.AggregatedName(base.SelfScope))
	assert.Equal(t, "self/lag", base.LagMetricName.AggregatedName(base.SelfScope))
	assert.Equal(t, "shard/loadavg", base.LoadAvgMetricName.AggregatedName(base.ShardScope))
}

func newTestThrottler() *Throttler {
	metricsQuery := "select 1"

	env := tabletenv.NewEnv(vtenv.NewTestEnv(), nil, "TabletServerTest")
	throttler := &Throttler{
		clusterProbesChan: make(chan *base.ClusterProbes),
		heartbeatWriter:   &FakeHeartbeatWriter{},
		ts:                &FakeTopoServer{},
		inventory:         base.NewInventory(),
		pool:              connpool.NewPool(env, "ThrottlerPool", tabletenv.ConnPoolConfig{}),
		tabletTypeFunc:    func() topodatapb.TabletType { return topodatapb.TabletType_PRIMARY },
		overrideTmClient:  &fakeTMClient{},
	}
	lagSelfMetric := base.RegisteredSelfMetrics[base.LagMetricName].(*base.LagSelfMetric)
	lagSelfMetric.SetQuery(metricsQuery)
	throttler.MetricsThreshold.Store(math.Float64bits(0.75))
	throttler.configSettings = config.NewConfigurationSettings()
	throttler.initConfig()
	throttler.throttleMetricChan = make(chan *base.ThrottleMetric)
	throttler.clusterProbesChan = make(chan *base.ClusterProbes)
	throttler.throttlerConfigChan = make(chan *topodatapb.ThrottlerConfig)
	throttler.serialFuncChan = make(chan func())
	throttler.inventory = base.NewInventory()

	throttler.throttledApps = cache.New(cache.NoExpiration, 0)
	throttler.metricThresholds = cache.New(cache.NoExpiration, 0)
	throttler.aggregatedMetrics = cache.New(10*aggregatedMetricsExpiration, 0)
	throttler.recentApps = cache.New(recentAppsExpiration, 0)
	throttler.metricsHealth = cache.New(cache.NoExpiration, 0)
	throttler.appCheckedMetrics = cache.New(cache.NoExpiration, 0)
	throttler.initThrottleTabletTypes()
	throttler.check = NewThrottlerCheck(throttler)

	// High contention & racy intervals:
	throttler.leaderCheckInterval = 10 * time.Millisecond
	throttler.activeCollectInterval = 10 * time.Millisecond
	throttler.dormantCollectInterval = 10 * time.Millisecond
	throttler.inventoryRefreshInterval = 10 * time.Millisecond
	throttler.metricsAggregateInterval = 10 * time.Millisecond
	throttler.throttledAppsSnapshotInterval = 10 * time.Millisecond
	throttler.dormantPeriod = 5 * time.Second
	throttler.recentCheckDormantDiff = int64(throttler.dormantPeriod / recentCheckRateLimiterInterval)
	throttler.recentCheckDiff = int64(3 * time.Second / recentCheckRateLimiterInterval)

	throttler.readSelfThrottleMetrics = func(ctx context.Context) base.ThrottleMetrics {
		for _, metric := range selfMetrics {
			go func() {
				select {
				case <-ctx.Done():
				case throttler.throttleMetricChan <- metric:
				}
			}()
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

func TestInitThrottler(t *testing.T) {
	throttler := newTestThrottler()
	assert.Equal(t, 5*time.Second, throttler.dormantPeriod)
	assert.EqualValues(t, 5, throttler.recentCheckDormantDiff)
	assert.EqualValues(t, 3, throttler.recentCheckDiff)
}

func TestApplyThrottlerConfig(t *testing.T) {
	ctx := context.Background() // for development, replace with	ctx := utils.LeakCheckContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	timeNow := time.Now()
	throttler := newTestThrottler()
	throttlerConfig := &topodatapb.ThrottlerConfig{
		Enabled:   false,
		Threshold: 14,
		ThrottledApps: map[string]*topodatapb.ThrottledAppRule{
			throttlerapp.OnlineDDLName.String(): {
				Name:      throttlerapp.OnlineDDLName.String(),
				Ratio:     0.5,
				ExpiresAt: protoutil.TimeToProto(timeNow.Add(time.Hour)),
				Exempt:    false,
			},
			throttlerapp.TableGCName.String(): {
				Name:      throttlerapp.TableGCName.String(),
				ExpiresAt: protoutil.TimeToProto(timeNow.Add(time.Hour)),
				Exempt:    true,
			},
			throttlerapp.VPlayerName.String(): {
				Name:      throttlerapp.VPlayerName.String(),
				Ratio:     DefaultThrottleRatio,
				ExpiresAt: protoutil.TimeToProto(timeNow), // instantly expires
				Exempt:    false,
			},
		},
		AppCheckedMetrics: map[string]*topodatapb.ThrottlerConfig_MetricNames{
			"app1":                              {Names: []string{"lag", "threads_running"}},
			throttlerapp.OnlineDDLName.String(): {Names: []string{"loadavg"}},
		},
		MetricThresholds: map[string]float64{
			"threads_running": 3.0,
		},
	}
	assert.Equal(t, 0.75, throttler.GetMetricsThreshold())
	throttler.appCheckedMetrics.Set("app1", base.MetricNames{base.ThreadsRunningMetricName}, cache.DefaultExpiration)
	throttler.appCheckedMetrics.Set("app2", base.MetricNames{base.ThreadsRunningMetricName}, cache.DefaultExpiration)
	throttler.appCheckedMetrics.Set("app3", base.MetricNames{base.ThreadsRunningMetricName}, cache.DefaultExpiration)
	runThrottler(t, ctx, throttler, 10*time.Second, func(t *testing.T, ctx context.Context) {
		defer cancel() // early termination
		assert.True(t, throttler.IsEnabled())
		assert.Equal(t, 1, throttler.throttledApps.ItemCount(), "expecting always-throttled-app: %v", maps.Keys(throttler.throttledApps.Items()))
		throttler.applyThrottlerConfig(ctx, throttlerConfig)
	})

	sleepTillThresholdApplies()
	assert.Equal(t, 3, throttler.throttledApps.ItemCount(), "expecting online-ddl, tablegc, and always-throttled-app: %v", maps.Keys(throttler.throttledApps.Items()))
	assert.False(t, throttler.IsEnabled())
	assert.Equal(t, float64(14), throttler.GetMetricsThreshold())
	assert.Equal(t, 2, throttler.appCheckedMetrics.ItemCount())
	t.Run("checked metrics", func(t *testing.T) {
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
	})
	t.Run("metric thresholds", func(t *testing.T) {
		{
			val, ok := throttler.metricThresholds.Get("lag")
			require.True(t, ok)
			assert.Equal(t, float64(0.75), val)
		}
		{
			val, ok := throttler.metricThresholds.Get("threads_running")
			require.True(t, ok)
			assert.Equal(t, float64(3.0), val)
		}
		{
			val, ok := throttler.metricThresholds.Get("loadavg")
			require.True(t, ok)
			assert.Equal(t, float64(1.0), val)
		}
	})
}

// TestApplyThrottlerConfigMetricThresholds applies a specific 'lag' metric threshold,
// and validates that it overrides the default threshold.
func TestApplyThrottlerConfigMetricThresholds(t *testing.T) {
	ctx := context.Background() // for development, replace with	ctx := utils.LeakCheckContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	throttler := newTestThrottler()
	runThrottler(t, ctx, throttler, 10*time.Second, func(t *testing.T, ctx context.Context) {
		defer cancel() // early termination
		assert.True(t, throttler.IsEnabled())

		flags := &CheckFlags{
			Scope:                 base.SelfScope,
			SkipRequestHeartbeats: true,
			MultiMetricsEnabled:   true,
		}
		t.Run("check before apply", func(t *testing.T) {
			checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
			require.NotNil(t, checkResult)
			assert.EqualValues(t, 0.3, checkResult.Value) // self lag value
			assert.EqualValues(t, http.StatusOK, checkResult.StatusCode)
			assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode)
			assert.Len(t, checkResult.Metrics, 1)
			assert.Contains(t, checkResult.Summary(), testAppName.String()+" is granted access")
		})
		t.Run("apply low threshold", func(t *testing.T) {
			assert.Equal(t, 0.75, throttler.GetMetricsThreshold())
			throttlerConfig := &topodatapb.ThrottlerConfig{
				Enabled:   true,
				Threshold: 0.0033,
			}
			throttler.applyThrottlerConfig(ctx, throttlerConfig)
			assert.Equal(t, 0.0033, throttler.GetMetricsThreshold())
		})
		t.Run("check low threshold", func(t *testing.T) {
			sleepTillThresholdApplies()
			{
				_, ok := throttler.metricThresholds.Get("config/lag")
				assert.False(t, ok)
			}
			assert.Equal(t, float64(0.0033), throttler.GetMetricsThreshold())
			checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
			require.NotNil(t, checkResult)
			assert.EqualValues(t, 0.3, checkResult.Value, "unexpected result: %+v", checkResult) // self lag value
			assert.NotEqualValues(t, http.StatusOK, checkResult.StatusCode, "unexpected result: %+v", checkResult)
			assert.NotEqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode, "unexpected result: %+v", checkResult)
			assert.Len(t, checkResult.Metrics, 1)
			assert.Contains(t, checkResult.Summary(), testAppName.String()+" is denied access due to self/lag metric value")
		})
		t.Run("apply low threshold but high 'lag' override", func(t *testing.T) {
			throttlerConfig := &topodatapb.ThrottlerConfig{
				Enabled:   true,
				Threshold: 0.0033,
				MetricThresholds: map[string]float64{
					"lag": 4444.0,
				},
			}
			throttler.applyThrottlerConfig(ctx, throttlerConfig)
		})
		t.Run("check with high 'lag' threshold", func(t *testing.T) {
			sleepTillThresholdApplies()
			{
				val, ok := throttler.metricThresholds.Get("config/lag")
				require.True(t, ok)
				assert.Equal(t, float64(4444), val)
			}
			checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
			require.NotNil(t, checkResult)
			assert.EqualValues(t, 0.3, checkResult.Value, "unexpected result: %+v", checkResult) // self lag value
			assert.EqualValues(t, http.StatusOK, checkResult.StatusCode, "unexpected result: %+v", checkResult)
			assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode, "unexpected result: %+v", checkResult)
			assert.Len(t, checkResult.Metrics, 1)
			assert.Contains(t, checkResult.Summary(), testAppName.String()+" is granted access")
		})
	})

	assert.False(t, throttler.IsEnabled())
	assert.Equal(t, float64(0.0033), throttler.GetMetricsThreshold())
	t.Run("metric thresholds", func(t *testing.T) {
		{
			val, ok := throttler.metricThresholds.Get("config/lag")
			require.True(t, ok)
			assert.Equal(t, float64(4444), val)
		}
		{
			val, ok := throttler.metricThresholds.Get("inventory/lag")
			require.True(t, ok)
			assert.Equal(t, float64(0.0033), val)
		}
		{
			val, ok := throttler.metricThresholds.Get("lag")
			require.True(t, ok)
			assert.Equal(t, float64(4444), val)
		}
	})
}

// TestApplyThrottlerConfigAppCheckedMetrics applies different metrics to the "test" app and checks the result
func TestApplyThrottlerConfigAppCheckedMetrics(t *testing.T) {
	ctx := context.Background() // for development, replace with	ctx := utils.LeakCheckContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	throttler := newTestThrottler()
	runThrottler(t, ctx, throttler, 10*time.Second, func(t *testing.T, ctx context.Context) {
		defer cancel() // early termination

		assert.True(t, throttler.IsEnabled())
		aggr := throttler.aggregatedMetricsSnapshot()
		assert.Equalf(t, 2*len(base.KnownMetricNames), len(aggr), "aggregated: %+v", aggr)     // "self" and "shard", per known metric
		assert.Equal(t, 2*len(base.KnownMetricNames), throttler.aggregatedMetrics.ItemCount()) // flushed upon Disable()
		for _, metric := range []string{"self", "shard", "self/lag", "shard/lag", "self/loadavg", "shard/loadavg"} {
			_, ok := aggr[metric]
			assert.True(t, ok, "missing metric: %s", metric)
		}
		flags := &CheckFlags{
			SkipRequestHeartbeats: true,
			MultiMetricsEnabled:   true,
		}
		throttlerConfig := &topodatapb.ThrottlerConfig{
			Enabled:           true,
			MetricThresholds:  map[string]float64{},
			AppCheckedMetrics: map[string]*topodatapb.ThrottlerConfig_MetricNames{},
		}

		t.Run("check before apply", func(t *testing.T) {
			checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
			require.NotNil(t, checkResult)
			assert.EqualValues(t, 0.9, checkResult.Value) // shard lag value
			assert.NotEqualValues(t, http.StatusOK, checkResult.StatusCode)
			assert.NotEqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode)
			assert.Len(t, checkResult.Metrics, 1)
			assert.Contains(t, checkResult.Summary(), testAppName.String()+" is denied access due to shard/lag metric value")
		})
		t.Run("apply high lag threshold", func(t *testing.T) {
			throttlerConfig.Threshold = 4444.0
			throttlerConfig.MetricThresholds["lag"] = 4444.0
			throttler.applyThrottlerConfig(ctx, throttlerConfig)

			t.Run("check after apply, no impact", func(t *testing.T) {
				sleepTillThresholdApplies()
				// "test" not supposed to check "loadavg"
				checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 0.9, checkResult.Value) // self lag value
				assert.EqualValues(t, http.StatusOK, checkResult.StatusCode)
				assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode)
				assert.Len(t, checkResult.Metrics, 1)
				assert.Contains(t, checkResult.Summary(), testAppName.String()+" is granted access")
			})
		})
		t.Run("apply low 'loadavg' threshold", func(t *testing.T) {
			throttlerConfig.MetricThresholds["loadavg"] = 0.0077
			throttler.applyThrottlerConfig(ctx, throttlerConfig)

			t.Run("check after apply, no impact", func(t *testing.T) {
				sleepTillThresholdApplies()
				// "test" not supposed to check "loadavg"
				checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 0.9, checkResult.Value) // shard lag value
				assert.EqualValues(t, http.StatusOK, checkResult.StatusCode)
				assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode)
				assert.Len(t, checkResult.Metrics, 1)
				assert.Contains(t, checkResult.Summary(), testAppName.String()+" is granted access")
			})
		})
		t.Run("assign 'loadavg' to test app", func(t *testing.T) {
			throttlerConfig.AppCheckedMetrics[testAppName.String()] = &topodatapb.ThrottlerConfig_MetricNames{Names: []string{"loadavg"}}
			throttler.applyThrottlerConfig(ctx, throttlerConfig)

			t.Run("check after assignment", func(t *testing.T) {
				// "test" now checks "loadavg"
				appCheckedMetrics := throttler.appCheckedMetricsSnapshot()
				metrics, ok := appCheckedMetrics[testAppName.String()]
				require.True(t, ok)
				assert.Equal(t, "loadavg", metrics)

				checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 2.718, checkResult.Value) // self loadavg value
				assert.NotEqualValues(t, http.StatusOK, checkResult.StatusCode, "unexpected result: %+v", checkResult)
				assert.NotEqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode, "unexpected result: %+v", checkResult)
				assert.Len(t, checkResult.Metrics, 1)
				assert.Contains(t, checkResult.Summary(), testAppName.String()+" is denied access due to self/loadavg metric value")
			})
		})
		t.Run("assign 'shard/loadavg' to test app", func(t *testing.T) {
			throttlerConfig.AppCheckedMetrics[testAppName.String()] = &topodatapb.ThrottlerConfig_MetricNames{Names: []string{"shard/loadavg"}}
			throttler.applyThrottlerConfig(ctx, throttlerConfig)

			t.Run("check after assignment", func(t *testing.T) {
				// "test" now checks "loadavg"
				appCheckedMetrics := throttler.appCheckedMetricsSnapshot()
				metrics, ok := appCheckedMetrics[testAppName.String()]
				require.True(t, ok)
				assert.Equal(t, "shard/loadavg", metrics)

				checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 5.1, checkResult.Value) // shard loadavg value
				assert.NotEqualValues(t, http.StatusOK, checkResult.StatusCode, "unexpected result: %+v", checkResult)
				assert.NotEqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode, "unexpected result: %+v", checkResult)
				assert.Len(t, checkResult.Metrics, 1)
				assert.Contains(t, checkResult.Summary(), testAppName.String()+" is denied access due to shard/loadavg metric value")
			})
		})
		t.Run("assign 'lag,loadavg' to test app", func(t *testing.T) {
			throttlerConfig.AppCheckedMetrics[testAppName.String()] = &topodatapb.ThrottlerConfig_MetricNames{Names: []string{"lag", "loadavg"}}
			throttler.applyThrottlerConfig(ctx, throttlerConfig)
			t.Run("check after assignment", func(t *testing.T) {
				// "test" now checks both lag and loadavg
				appCheckedMetrics := throttler.appCheckedMetricsSnapshot()
				metrics, ok := appCheckedMetrics[testAppName.String()]
				require.True(t, ok)
				assert.Equal(t, "lag,loadavg", metrics)

				checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 2.718, checkResult.Value) // self loadavg value
				assert.NotEqualValues(t, http.StatusOK, checkResult.StatusCode, "unexpected result: %+v", checkResult)
				assert.NotEqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode, "unexpected result: %+v", checkResult)
				assert.Equal(t, 2, len(checkResult.Metrics))
				assert.Contains(t, checkResult.Summary(), testAppName.String()+" is denied access due to self/loadavg metric value")
			})
		})
		t.Run("assign 'lag,shard/loadavg' to test app", func(t *testing.T) {
			throttlerConfig.AppCheckedMetrics[testAppName.String()] = &topodatapb.ThrottlerConfig_MetricNames{Names: []string{"lag", "shard/loadavg"}}
			throttler.applyThrottlerConfig(ctx, throttlerConfig)
			t.Run("check after assignment", func(t *testing.T) {
				// "test" now checks both lag and loadavg
				appCheckedMetrics := throttler.appCheckedMetricsSnapshot()
				metrics, ok := appCheckedMetrics[testAppName.String()]
				require.True(t, ok)
				assert.Equal(t, "lag,shard/loadavg", metrics)

				checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 5.1, checkResult.Value) // shard loadavg value
				assert.NotEqualValues(t, http.StatusOK, checkResult.StatusCode, "unexpected result: %+v", checkResult)
				assert.NotEqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode, "unexpected result: %+v", checkResult)
				assert.Equal(t, 2, len(checkResult.Metrics))
				assert.Contains(t, checkResult.Summary(), testAppName.String()+" is denied access due to shard/loadavg metric value")
			})
		})
		t.Run("clear 'loadavg' threshold", func(t *testing.T) {
			throttlerConfig.AppCheckedMetrics[testAppName.String()] = &topodatapb.ThrottlerConfig_MetricNames{Names: []string{"lag"}}
			throttler.applyThrottlerConfig(ctx, throttlerConfig)
			t.Run("check after apply, clear", func(t *testing.T) {
				sleepTillThresholdApplies()

				checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 0.9, checkResult.Value) // shard lag value
				assert.EqualValues(t, http.StatusOK, checkResult.StatusCode, "unexpected result: %+v", checkResult)
				assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode, "unexpected result: %+v", checkResult)
				assert.Equal(t, 1, len(checkResult.Metrics), "unexpected metrics: %+v", checkResult.Metrics)
				assert.Contains(t, checkResult.Summary(), testAppName.String()+" is granted access")
			})
		})
		t.Run("assign 'lag,threads_running' to test app", func(t *testing.T) {
			throttlerConfig.AppCheckedMetrics[testAppName.String()] = &topodatapb.ThrottlerConfig_MetricNames{Names: []string{"lag", "threads_running"}}
			throttler.applyThrottlerConfig(ctx, throttlerConfig)
			t.Run("check after assignment", func(t *testing.T) {
				// "test" now checks both lag and loadavg
				appCheckedMetrics := throttler.appCheckedMetricsSnapshot()
				metrics, ok := appCheckedMetrics[testAppName.String()]
				require.True(t, ok)
				assert.Equal(t, "lag,threads_running", metrics)

				checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 0.9, checkResult.Value) // shard lag value
				assert.EqualValues(t, http.StatusOK, checkResult.StatusCode, "unexpected result: %+v", checkResult)
				assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode, "unexpected result: %+v", checkResult)
				assert.Equal(t, 2, len(checkResult.Metrics))
				assert.Contains(t, checkResult.Summary(), testAppName.String()+" is granted access")
			})
		})
		t.Run("assign 'custom,loadavg' to 'all' app", func(t *testing.T) {
			throttlerConfig.AppCheckedMetrics[testAppName.String()] = &topodatapb.ThrottlerConfig_MetricNames{Names: []string{"lag", "threads_running"}}
			throttlerConfig.AppCheckedMetrics[throttlerapp.AllName.String()] = &topodatapb.ThrottlerConfig_MetricNames{Names: []string{"custom", "loadavg"}}
			throttler.applyThrottlerConfig(ctx, throttlerConfig)
			t.Run("check 'all' after assignment", func(t *testing.T) {
				appCheckedMetrics := throttler.appCheckedMetricsSnapshot()
				metrics, ok := appCheckedMetrics[throttlerapp.AllName.String()]
				require.True(t, ok)
				assert.Equal(t, "custom,loadavg", metrics)

				checkResult := throttler.Check(ctx, throttlerapp.AllName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 2.718, checkResult.Value) // loadavg self value exceeds threshold
				assert.NotEqualValues(t, http.StatusOK, checkResult.StatusCode, "unexpected result: %+v", checkResult)
				assert.NotEqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode, "unexpected result: %+v", checkResult)
				assert.Equal(t, 2, len(checkResult.Metrics))
				assert.Contains(t, checkResult.Summary(), throttlerapp.AllName.String()+" is denied access due to self/loadavg metric value")
			})
			t.Run("check 'test' after assignment", func(t *testing.T) {
				// "test" app unaffected by 'all' assignment, because it has
				// explicit metrics assignment.
				appCheckedMetrics := throttler.appCheckedMetricsSnapshot()
				metrics, ok := appCheckedMetrics[testAppName.String()]
				require.True(t, ok)
				assert.Equal(t, "lag,threads_running", metrics)

				checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 0.9, checkResult.Value) // shard lag value
				assert.EqualValues(t, http.StatusOK, checkResult.StatusCode, "unexpected result: %+v", checkResult)
				assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode, "unexpected result: %+v", checkResult)
				assert.Equal(t, 2, len(checkResult.Metrics))
				assert.Contains(t, checkResult.Summary(), testAppName.String()+" is granted access")
			})
			t.Run("'online-ddl' app affected by 'all'", func(t *testing.T) {
				// "online-ddl" app is affected by 'all' assignment, because it has
				// no explicit metrics assignment.
				appCheckedMetrics := throttler.appCheckedMetricsSnapshot()
				_, ok := appCheckedMetrics[throttlerapp.OnlineDDLName.String()]
				require.False(t, ok)

				checkResult := throttler.Check(ctx, throttlerapp.OnlineDDLName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 2.718, checkResult.Value) // loadavg self value exceeds threshold
				assert.NotEqualValues(t, http.StatusOK, checkResult.StatusCode, "unexpected result: %+v", checkResult)
				assert.NotEqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode, "unexpected result: %+v", checkResult)
				assert.Equal(t, 2, len(checkResult.Metrics))
				assert.Contains(t, checkResult.Summary(), throttlerapp.AllName.String()+" is denied access due to self/loadavg metric value")
			})
		})
		t.Run("'vreplication:online-ddl:12345' app affected by 'all'", func(t *testing.T) {
			// "vreplication:online-ddl:12345" app is affected by 'all' assignment, because it has
			// no explicit metrics assignment.
			checkResult := throttler.Check(ctx, "vreplication:online-ddl:12345", nil, flags)
			require.NotNil(t, checkResult)
			assert.EqualValues(t, 2.718, checkResult.Value) // loadavg self value exceeds threshold
			assert.NotEqualValues(t, http.StatusOK, checkResult.StatusCode, "unexpected result: %+v", checkResult)
			assert.NotEqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode, "unexpected result: %+v", checkResult)
			assert.Equal(t, 2, len(checkResult.Metrics))
			assert.Contains(t, checkResult.Summary(), throttlerapp.AllName.String()+" is denied access due to self/loadavg metric value")
		})
		t.Run("'vreplication:online-ddl:test' app affected by 'test' and not by 'all'", func(t *testing.T) {
			// "vreplication:online-ddl:test" app is affected by 'test' assignment, because it has
			// the split name "test" has explicit metrics assignment.
			checkResult := throttler.Check(ctx, "vreplication:online-ddl:test", nil, flags)
			require.NotNil(t, checkResult)
			assert.EqualValues(t, 0.9, checkResult.Value) // shard lag value
			assert.EqualValues(t, http.StatusOK, checkResult.StatusCode, "unexpected result: %+v", checkResult)
			assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode, "unexpected result: %+v", checkResult)
			assert.Equal(t, 2, len(checkResult.Metrics))
			assert.Contains(t, checkResult.Summary(), testAppName.String()+" is granted access")
		})
		t.Run("deassign metrics from 'all' app", func(t *testing.T) {
			delete(throttlerConfig.AppCheckedMetrics, throttlerapp.AllName.String())
			throttler.applyThrottlerConfig(ctx, throttlerConfig)
			t.Run("check 'all' after assignment", func(t *testing.T) {
				appCheckedMetrics := throttler.appCheckedMetricsSnapshot()
				_, ok := appCheckedMetrics[throttlerapp.AllName.String()]
				require.False(t, ok)

				checkResult := throttler.Check(ctx, throttlerapp.AllName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 0.9, checkResult.Value) // shard lag value
				assert.EqualValues(t, http.StatusOK, checkResult.StatusCode, "unexpected result: %+v", checkResult)
				assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode, "unexpected result: %+v", checkResult)
				assert.Len(t, checkResult.Metrics, 1)
				assert.Contains(t, checkResult.Summary(), throttlerapp.AllName.String()+" is granted access")
			})
			t.Run("check 'test' after assignment", func(t *testing.T) {
				// "test" app unaffected by the entire 'all' assignment, because it has
				// explicit metrics assignment.
				appCheckedMetrics := throttler.appCheckedMetricsSnapshot()
				metrics, ok := appCheckedMetrics[testAppName.String()]
				require.True(t, ok)
				assert.Equal(t, "lag,threads_running", metrics)

				checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 0.9, checkResult.Value) // shard lag value
				assert.EqualValues(t, http.StatusOK, checkResult.StatusCode, "unexpected result: %+v", checkResult)
				assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode, "unexpected result: %+v", checkResult)
				assert.Equal(t, 2, len(checkResult.Metrics))
				assert.Contains(t, checkResult.Summary(), testAppName.String()+" is granted access")
			})
			t.Run("'online-ddl' no longer has 'all' impact", func(t *testing.T) {
				// "online-ddl" app is affected by 'all' assignment, because it has
				// no explicit metrics assignment.
				appCheckedMetrics := throttler.appCheckedMetricsSnapshot()
				_, ok := appCheckedMetrics[throttlerapp.OnlineDDLName.String()]
				require.False(t, ok)

				checkResult := throttler.Check(ctx, throttlerapp.OnlineDDLName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 0.9, checkResult.Value) // shard lag value
				assert.EqualValues(t, http.StatusOK, checkResult.StatusCode, "unexpected result: %+v", checkResult)
				assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode, "unexpected result: %+v", checkResult)
				assert.Len(t, checkResult.Metrics, 1)
				assert.Contains(t, checkResult.Summary(), throttlerapp.OnlineDDLName.String()+" is granted access")
			})
		})

		t.Run("deassign metrics from test app", func(t *testing.T) {
			delete(throttlerConfig.AppCheckedMetrics, testAppName.String())
			throttler.applyThrottlerConfig(ctx, throttlerConfig)
			t.Run("check after deassign, clear", func(t *testing.T) {
				appCheckedMetrics := throttler.appCheckedMetricsSnapshot()
				_, ok := appCheckedMetrics[testAppName.String()]
				require.False(t, ok)

				checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 0.9, checkResult.Value) // shard lag value
				assert.EqualValues(t, http.StatusOK, checkResult.StatusCode)
				assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode)
				assert.Len(t, checkResult.Metrics, 1)
				assert.Contains(t, checkResult.Summary(), testAppName.String()+" is granted access")
			})
		})

		t.Run("Disable", func(t *testing.T) {
			throttlerConfig := &topodatapb.ThrottlerConfig{
				Enabled:           false,
				MetricThresholds:  map[string]float64{},
				AppCheckedMetrics: map[string]*topodatapb.ThrottlerConfig_MetricNames{},
			}
			throttler.applyThrottlerConfig(ctx, throttlerConfig)
			sleepTillThresholdApplies()
		})
	})
}

func TestIsAppThrottled(t *testing.T) {
	plusOneHour := time.Now().Add(time.Hour)
	throttler := Throttler{
		throttledApps:   cache.New(cache.NoExpiration, 0),
		heartbeatWriter: &FakeHeartbeatWriter{},
	}
	t.Run("initial", func(t *testing.T) {
		{
			throttled, app := throttler.IsAppThrottled("app1")
			assert.False(t, throttled)
			assert.Equal(t, "app1", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app2")
			assert.False(t, throttled)
			assert.Equal(t, "app2", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app3")
			assert.False(t, throttled)
			assert.Equal(t, "app3", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app4")
			assert.False(t, throttled)
			assert.Equal(t, "app4", app)
		}

		assert.Equal(t, 0, throttler.throttledApps.ItemCount())
	})
	//
	t.Run("set some rules", func(t *testing.T) {
		throttler.ThrottleApp("app1", plusOneHour, DefaultThrottleRatio, true)
		throttler.ThrottleApp("app2", time.Now(), DefaultThrottleRatio, false) // instantly expire
		throttler.ThrottleApp("app3", plusOneHour, DefaultThrottleRatio, false)
		throttler.ThrottleApp("app4", plusOneHour, 0, false)
		{
			throttled, app := throttler.IsAppThrottled("app1")
			assert.False(t, throttled) // exempted
			assert.Equal(t, "app1", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app2")
			assert.False(t, throttled) // expired
			assert.Equal(t, "app2", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app3")
			assert.True(t, throttled)
			assert.Equal(t, "app3", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app4")
			assert.False(t, throttled) // ratio is zero
			assert.Equal(t, "app4", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app_other")
			assert.False(t, throttled) // not specified
			assert.Equal(t, "app_other", app)
		}

		assert.Equal(t, 3, throttler.throttledApps.ItemCount())
	})
	t.Run("all", func(t *testing.T) {
		// throttle "all", see how it affects app
		throttler.ThrottleApp(throttlerapp.AllName.String(), plusOneHour, DefaultThrottleRatio, false)
		defer throttler.UnthrottleApp(throttlerapp.AllName.String())
		{
			throttled, app := throttler.IsAppThrottled("all")
			assert.True(t, throttled) // explicitly throttled
			assert.Equal(t, "all", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app1")
			assert.False(t, throttled) // exempted
			assert.Equal(t, "app1", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app2")
			assert.True(t, throttled) // expired, so falls under "all"
			assert.Equal(t, "all", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app3")
			assert.True(t, throttled)
			assert.Equal(t, "app3", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app4")
			assert.False(t, throttled) // ratio is zero, there is a specific instruction for this app, so it doesn't fall under "all"
			assert.Equal(t, "app4", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app_other")
			assert.True(t, throttled) // falls under "all"
			assert.Equal(t, "all", app)
		}

		// continuing previous test, we had 3 throttled apps. "all" is a new app being throttled.
		assert.Equal(t, 4, throttler.throttledApps.ItemCount())
	})
	//
	t.Run("unthrottle", func(t *testing.T) {
		throttler.UnthrottleApp("app1")
		throttler.UnthrottleApp("app2")
		throttler.UnthrottleApp("app3")
		throttler.UnthrottleApp("app4")

		{
			throttled, app := throttler.IsAppThrottled("app1")
			assert.False(t, throttled)
			assert.Equal(t, "app1", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app2")
			assert.False(t, throttled)
			assert.Equal(t, "app2", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app3")
			assert.False(t, throttled)
			assert.Equal(t, "app3", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app4")
			assert.False(t, throttled)
			assert.Equal(t, "app4", app)
		}

		// we've manually unthrottled everything
		assert.Equal(t, 0, throttler.throttledApps.ItemCount())
	})
	t.Run("all again", func(t *testing.T) {
		// throttle "all", see how it affects app
		throttler.ThrottleApp(throttlerapp.AllName.String(), plusOneHour, DefaultThrottleRatio, false)
		defer throttler.UnthrottleApp(throttlerapp.AllName.String())

		{
			throttled, app := throttler.IsAppThrottled("all")
			assert.True(t, throttled) // explicitly throttled
			assert.Equal(t, "all", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app1")
			assert.True(t, throttled)
			assert.Equal(t, "all", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app2")
			assert.True(t, throttled)
			assert.Equal(t, "all", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app3")
			assert.True(t, throttled)
			assert.Equal(t, "all", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app4")
			assert.True(t, throttled)
			assert.Equal(t, "all", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app_other")
			assert.True(t, throttled)
			assert.Equal(t, "all", app)
		}

		// one rule, for "all" app
		assert.Equal(t, 1, throttler.throttledApps.ItemCount())
	})
	t.Run("exempt all", func(t *testing.T) {
		// throttle "all", see how it affects app
		throttler.ThrottleApp("app3", plusOneHour, DefaultThrottleRatio, false)
		throttler.ThrottleApp(throttlerapp.AllName.String(), plusOneHour, DefaultThrottleRatio, true)
		defer throttler.UnthrottleApp(throttlerapp.AllName.String())

		{
			throttled, app := throttler.IsAppThrottled("all")
			assert.False(t, throttled) // explicitly throttled
			assert.Equal(t, "all", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app1")
			assert.False(t, throttled)
			assert.Equal(t, "app1", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app2")
			assert.False(t, throttled)
			assert.Equal(t, "app2", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app3")
			assert.True(t, throttled) // explicitly throttled
			assert.Equal(t, "app3", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app4")
			assert.False(t, throttled)
			assert.Equal(t, "app4", app)
		}
		{
			throttled, app := throttler.IsAppThrottled("app_other")
			assert.False(t, throttled)
			assert.Equal(t, "app_other", app)
		}
		assert.Equal(t, 2, throttler.throttledApps.ItemCount())
	})
}

func TestIsAppExempted(t *testing.T) {
	plusOneHour := time.Now().Add(time.Hour)
	throttler := Throttler{
		throttledApps:   cache.New(cache.NoExpiration, 0),
		heartbeatWriter: &FakeHeartbeatWriter{},
	}
	t.Run("initial", func(t *testing.T) {
		{
			exempted, app := throttler.IsAppExempted("app1")
			assert.False(t, exempted)
			assert.Equal(t, "app1", app)
		}
		{
			exempted, app := throttler.IsAppExempted("app2")
			assert.False(t, exempted)
			assert.Equal(t, "app2", app)
		}
		{
			exempted, app := throttler.IsAppExempted("app3")
			assert.False(t, exempted)
			assert.Equal(t, "app3", app)
		}
	})
	t.Run("exempt", func(t *testing.T) {
		throttler.ThrottleApp("app1", time.Now().Add(time.Hour), DefaultThrottleRatio, true)
		throttler.ThrottleApp("app2", time.Now(), DefaultThrottleRatio, true) // instantly expire
		{
			exempted, app := throttler.IsAppExempted("app1")
			assert.True(t, exempted)
			assert.Equal(t, "app1", app)
		}
		{
			exempted, app := throttler.IsAppExempted("app1:other-tag")
			assert.True(t, exempted)
			assert.Equal(t, "app1", app)
		}
		{
			exempted, app := throttler.IsAppExempted("app2")
			assert.False(t, exempted)
			assert.Equal(t, "app2", app)
		}
		{
			exempted, app := throttler.IsAppExempted("app3")
			assert.False(t, exempted)
			assert.Equal(t, "app3", app)
		}
	})
	t.Run("throttle", func(t *testing.T) {
		throttler.UnthrottleApp("app1")
		throttler.ThrottleApp("app2", time.Now().Add(time.Hour), DefaultThrottleRatio, false)
		{
			exempted, app := throttler.IsAppExempted("app1")
			assert.False(t, exempted)
			assert.Equal(t, "app1", app)
		}
		{
			exempted, app := throttler.IsAppExempted("app2")
			assert.False(t, exempted)
			assert.Equal(t, "app2", app)
		}
		{
			exempted, app := throttler.IsAppExempted("app3")
			assert.False(t, exempted)
			assert.Equal(t, "app3", app)
		}
	})
	t.Run("special", func(t *testing.T) {
		{
			exempted, app := throttler.IsAppExempted("schema-tracker")
			assert.True(t, exempted)
			assert.Equal(t, "schema-tracker", app)
		}
		throttler.ThrottleApp("schema-tracker", plusOneHour, 1.0, false) // meaningless. App is statically exempted
		{
			exempted, app := throttler.IsAppExempted("schema-tracker")
			assert.True(t, exempted)
			assert.Equal(t, "schema-tracker", app)
		}
	})
}

// TestRefreshInventory tests the behavior of the throttler's RefreshInventory() function, which
// is called periodically in actual throttler. For a given cluster name, it generates a list of probes
// the throttler will use to check metrics.
// On a replica tablet, that list is expect to probe the tablet itself.
// On the PRIMARY, the list includes all shard tablets, including the PRIMARY itself.
func TestRefreshInventory(t *testing.T) {
	ctx := context.Background() // for development, replace with	ctx := utils.LeakCheckContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	metricsQuery := "select 1"
	configSettings := config.NewConfigurationSettings()

	throttler := &Throttler{
		clusterProbesChan: make(chan *base.ClusterProbes),
		metricThresholds:  cache.New(cache.NoExpiration, 0),
		ts:                &FakeTopoServer{},
		inventory:         base.NewInventory(),
	}
	lagSelfMetric := base.RegisteredSelfMetrics[base.LagMetricName].(*base.LagSelfMetric)
	lagSelfMetric.SetQuery(metricsQuery)
	throttler.configSettings = configSettings
	throttler.initConfig()
	throttler.initThrottleTabletTypes()

	validateClusterProbes := func(t *testing.T, ctx context.Context) {
		testName := fmt.Sprintf("leader=%t", throttler.isLeader.Load())
		t.Run(testName, func(t *testing.T) {
			// validateProbesCount expects number of probes according to cluster name and throttler's leadership status
			validateProbesCount := func(t *testing.T, probes base.Probes) {
				if throttler.isLeader.Load() {
					assert.Len(t, probes, 3)
				} else {
					assert.Len(t, probes, 1)
				}
			}
			t.Run("waiting for probes", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(ctx, waitForProbesTimeout)
				defer cancel()
				for {
					select {
					case probes := <-throttler.clusterProbesChan:
						// Worth noting that in this unit test, the throttler is _closed_ and _disabled_. Its own Operate() function does
						// not run, and therefore there is none but us to both populate `clusterProbesChan` as well as
						// read from it. We do not compete here with any other goroutine.
						assert.NotNil(t, probes)
						throttler.updateClusterProbes(probes)
						validateProbesCount(t, probes.TabletProbes)
						// Achieved our goal
						return
					case <-ctx.Done():
						assert.FailNowf(t, ctx.Err().Error(), "waiting for cluster probes")
					}
				}
			})
			t.Run("validating probes", func(t *testing.T) {
				probes := throttler.inventory.ClustersProbes
				validateProbesCount(t, probes)
			})
		})
	}

	t.Run("initial, not leader", func(t *testing.T) {
		throttler.isLeader.Store(false)
		throttler.refreshInventory(ctx)
		validateClusterProbes(t, ctx)
	})

	t.Run("promote", func(t *testing.T) {
		throttler.isLeader.Store(true)
		throttler.refreshInventory(ctx)
		validateClusterProbes(t, ctx)
	})

	t.Run("demote, expect cleanup", func(t *testing.T) {
		throttler.isLeader.Store(false)
		throttler.refreshInventory(ctx)
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
			waitForMetricsToBeCollected(t, ctx, throttler)
			f(t, ctx)
		}
	}

	<-ctx.Done()
	assert.Error(t, ctx.Err())

	throttler.Disable()
	wg.Wait()
	assert.False(t, throttler.IsEnabled())
}

// TestRace merely lets the throttler run with aggressive intervals for a few seconds, so as to detect race conditions.
// This is relevant to `go test -race`
func TestRace(t *testing.T) {
	ctx := context.Background() // for development, replace with	ctx := utils.LeakCheckContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	throttler := newTestThrottler()
	runThrottler(t, ctx, throttler, 5*time.Second, nil)
}

// TestProbes enables a throttler for a few seconds, and afterwards expects to find probes and metrics.
func TestProbesWhileOperating(t *testing.T) {
	ctx := context.Background() // for development, replace with	ctx := utils.LeakCheckContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	throttler := newTestThrottler()

	tmClient, ok := throttler.overrideTmClient.(*fakeTMClient)
	require.True(t, ok)
	assert.Empty(t, tmClient.AppNames())

	t.Run("aggregated initial", func(t *testing.T) {
		assert.Equal(t, 0, throttler.aggregatedMetrics.ItemCount())
	})
	runThrottler(t, ctx, throttler, time.Minute, func(t *testing.T, ctx context.Context) {
		defer cancel() // early termination
		t.Run("aggregated", func(t *testing.T) {
			assert.Equal(t, base.LagMetricName, throttler.metricNameUsedAsDefault())
			aggr := throttler.aggregatedMetricsSnapshot()
			assert.Equalf(t, 2*len(base.KnownMetricNames), len(aggr), "aggregated: %+v", aggr)     // "self" and "shard", per known metric
			assert.Equal(t, 2*len(base.KnownMetricNames), throttler.aggregatedMetrics.ItemCount()) // flushed upon Disable()
			for aggregatedMetricName, metricResult := range aggr {
				val, err := metricResult.Get()
				assert.NoErrorf(t, err, "aggregatedMetricName: %v", aggregatedMetricName)
				assert.NotEmpty(t, aggregatedMetricName)
				scope, metricName, err := base.DisaggregateMetricName(aggregatedMetricName)
				assert.NotEmpty(t, metricName)
				require.NoError(t, err)

				switch scope {
				case base.UndefinedScope, base.SelfScope:
					switch metricName {
					case base.DefaultMetricName:
						assert.Equalf(t, float64(0.3), val, "scope=%v, metricName=%v", scope, metricName) // same value as "lag"
					case base.LagMetricName:
						assert.Equalf(t, float64(0.3), val, "scope=%v, metricName=%v", scope, metricName)
					case base.ThreadsRunningMetricName:
						assert.Equalf(t, float64(26), val, "scope=%v, metricName=%v", scope, metricName)
					case base.CustomMetricName:
						assert.Equalf(t, float64(17), val, "scope=%v, metricName=%v", scope, metricName)
					case base.LoadAvgMetricName:
						assert.Equalf(t, float64(2.718), val, "scope=%v, metricName=%v", scope, metricName)
					}
				case base.ShardScope:
					switch metricName {
					case base.DefaultMetricName:
						assert.Equalf(t, float64(0.9), val, "scope=%v, metricName=%v", scope, metricName) // same value as "lag"
					case base.LagMetricName:
						assert.Equalf(t, float64(0.9), val, "scope=%v, metricName=%v", scope, metricName)
					case base.ThreadsRunningMetricName:
						assert.Equalf(t, float64(26), val, "scope=%v, metricName=%v", scope, metricName)
					case base.CustomMetricName:
						assert.Equalf(t, float64(17), val, "scope=%v, metricName=%v", scope, metricName)
					case base.LoadAvgMetricName:
						assert.Equalf(t, float64(5.1), val, "scope=%v, metricName=%v", scope, metricName)
					}
				default:
					assert.Failf(t, "unknown scope", "scope=%v", scope)
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
				client := NewBackgroundClient(throttler, testAppName, base.UndefinedScope)
				t.Run("threshold exceeded", func(t *testing.T) {
					<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
						throttler.refreshInventory(ctx)
					})
					{
						checkResult, checkOK := client.ThrottleCheckOK(ctx, "")
						assert.False(t, checkOK) // we expect threshold exceeded
						assert.NotNil(t, checkResult)
						assert.Contains(t, checkResult.Summary(), testAppName.String()+" is denied access due to")
					}
				})

				savedThreshold := throttler.MetricsThreshold.Load()
				t.Run("adjust threshold", func(t *testing.T) {
					throttler.MetricsThreshold.Store(math.Float64bits(0.95))
					<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
						throttler.refreshInventory(ctx)
					})
					{
						_, checkOK := client.ThrottleCheckOK(ctx, "")
						assert.True(t, checkOK)
					}
				})
				t.Run("restore threshold", func(t *testing.T) {
					throttler.MetricsThreshold.Store(savedThreshold)
					<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
						throttler.refreshInventory(ctx)
					})
					client.clearSuccessfulResultsCache() // ensure we don't read the successful result from the test above
					{
						checkResult, checkOK := client.ThrottleCheckOK(ctx, "")
						assert.False(t, checkOK)
						assert.NotNil(t, checkResult)
						assert.Contains(t, checkResult.Summary(), testAppName.String()+" is denied access due to")
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
				throttler.aggregateMetrics()
			})
			assert.Equal(t, base.CustomMetricName, throttler.metricNameUsedAsDefault())
			aggr := throttler.aggregatedMetricsSnapshot()
			assert.Equalf(t, 2*len(base.KnownMetricNames), len(aggr), "aggregated: %+v", aggr)     // "self" and "shard", per known metric
			assert.Equal(t, 2*len(base.KnownMetricNames), throttler.aggregatedMetrics.ItemCount()) // flushed upon Disable()
			for aggregatedMetricName, metricResult := range aggr {
				val, err := metricResult.Get()
				assert.NoErrorf(t, err, "aggregatedMetricName: %v", aggregatedMetricName)
				assert.NotEmpty(t, aggregatedMetricName)
				scope, metricName, err := base.DisaggregateMetricName(aggregatedMetricName)
				assert.NotEmpty(t, metricName)
				require.NoError(t, err)

				switch scope {
				case base.UndefinedScope, base.SelfScope:
					switch metricName {
					case base.DefaultMetricName:
						assert.Equalf(t, float64(17), val, "scope=%v, metricName=%v", scope, metricName) // same value as "custom"
					case base.LagMetricName:
						assert.Equalf(t, float64(0.3), val, "scope=%v, metricName=%v", scope, metricName)
					case base.ThreadsRunningMetricName:
						assert.Equalf(t, float64(26), val, "scope=%v, metricName=%v", scope, metricName)
					case base.CustomMetricName:
						assert.Equalf(t, float64(17), val, "scope=%v, metricName=%v", scope, metricName)
					case base.LoadAvgMetricName:
						assert.Equalf(t, float64(2.718), val, "scope=%v, metricName=%v", scope, metricName)
					}
				case base.ShardScope:
					switch metricName {
					case base.DefaultMetricName:
						assert.Equalf(t, float64(17), val, "scope=%v, metricName=%v", scope, metricName) // same value as "custom"
					case base.LagMetricName:
						assert.Equalf(t, float64(0.9), val, "scope=%v, metricName=%v", scope, metricName)
					case base.ThreadsRunningMetricName:
						assert.Equalf(t, float64(26), val, "scope=%v, metricName=%v", scope, metricName)
					case base.CustomMetricName:
						assert.Equalf(t, float64(17), val, "scope=%v, metricName=%v", scope, metricName)
					case base.LoadAvgMetricName:
						assert.Equalf(t, float64(5.1), val, "scope=%v, metricName=%v", scope, metricName)
					}
				default:
					assert.Failf(t, "unknown scope", "scope=%v", scope)
				}
			}

			t.Run("client, shard", func(t *testing.T) {
				client := NewBackgroundClient(throttler, testAppName, base.UndefinedScope)
				t.Run("threshold exceeded", func(t *testing.T) {
					<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
						throttler.refreshInventory(ctx)
					})
					{
						checkResult, checkOK := client.ThrottleCheckOK(ctx, "")
						assert.False(t, checkOK) // we expect threshold exceeded
						assert.NotNil(t, checkResult)
					}
				})

				savedThreshold := throttler.MetricsThreshold.Load()
				t.Run("adjust threshold, too low", func(t *testing.T) {
					throttler.MetricsThreshold.Store(math.Float64bits(0.95))
					<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
						throttler.refreshInventory(ctx)
					})
					{
						_, checkOK := client.ThrottleCheckOK(ctx, "")
						assert.False(t, checkOK) // 0.95 still too low for custom query
					}
				})
				t.Run("adjust threshold, still too low", func(t *testing.T) {
					throttler.MetricsThreshold.Store(math.Float64bits(15))
					<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
						throttler.refreshInventory(ctx)
					})
					{
						_, checkOK := client.ThrottleCheckOK(ctx, "")
						assert.False(t, checkOK) // 15 still too low for custom query because primary has 17
					}
				})
				t.Run("adjust threshold", func(t *testing.T) {
					throttler.MetricsThreshold.Store(math.Float64bits(18))
					<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
						throttler.refreshInventory(ctx)
					})
					{
						_, checkOK := client.ThrottleCheckOK(ctx, "")
						assert.True(t, checkOK)
					}
				})
				t.Run("restore threshold", func(t *testing.T) {
					throttler.MetricsThreshold.Store(savedThreshold)
					<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
						throttler.refreshInventory(ctx)
					})
					client.clearSuccessfulResultsCache() // ensure we don't read the successful result from the test above
					{
						_, checkOK := client.ThrottleCheckOK(ctx, "")
						assert.False(t, checkOK)
					}
				})
			})
		})

		t.Run("metrics", func(t *testing.T) {
			var results base.TabletResultMap
			<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
				results = maps.Clone(throttler.inventory.TabletMetrics)
			})
			assert.Len(t, results, 3)                                      // 1 self tablet + 2 shard tablets
			assert.Contains(t, results, "", "TabletMetrics: %+v", results) // primary self identifies with empty alias
			assert.Contains(t, results, "fakezone1-0000000101", "TabletMetrics: %+v", results)
			assert.Contains(t, results, "fakezone2-0000000102", "TabletMetrics: %+v", results)
		})

		t.Run("no REPLICA probes", func(t *testing.T) {
			nonPrimaryTabletType.Store(int32(topodatapb.TabletType_RDONLY))
			defer nonPrimaryTabletType.Store(int32(topodatapb.TabletType_REPLICA))

			t.Run("waiting for inventory metrics", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(ctx, waitForProbesTimeout)
				defer cancel()
				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()
				for {
					var results base.TabletResultMap
					<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
						results = maps.Clone(throttler.inventory.TabletMetrics)
					})
					if len(results) == 1 {
						// That's what we were waiting for. Good.
						assert.Contains(t, results, "", "TabletMetrics: %+v", results) // primary self identifies with empty alias
						return
					}

					select {
					case <-ticker.C:
					case <-ctx.Done():
						assert.FailNowf(t, ctx.Err().Error(), "waiting for inventory metrics")
					}
				}
			})
		})
		t.Run("again with probes", func(t *testing.T) {
			t.Run("waiting for inventory metrics", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(ctx, waitForProbesTimeout)
				defer cancel()
				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()
				for {
					var results base.TabletResultMap
					<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
						results = maps.Clone(throttler.inventory.TabletMetrics)
					})
					if len(results) == 3 {
						// That's what we were waiting for. Good.
						return
					}

					select {
					case <-ticker.C:
					case <-ctx.Done():
						assert.FailNowf(t, ctx.Err().Error(), "waiting for inventory metrics")
					}
				}
			})
		})
	})
}

// TestProbesWithV20Replicas is similar to TestProbesWhileOperating, but assumes a v20 replica, which does not report any of the named metrics.
func TestProbesWithV20Replicas(t *testing.T) {
	ctx := context.Background() // for development, replace with	ctx := utils.LeakCheckContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	throttler := newTestThrottler()

	tmClient, ok := throttler.overrideTmClient.(*fakeTMClient)
	require.True(t, ok)
	assert.Empty(t, tmClient.AppNames())
	tmClient.v20.Store(true)

	t.Run("aggregated initial", func(t *testing.T) {
		assert.Equal(t, 0, throttler.aggregatedMetrics.ItemCount())
	})

	runThrottler(t, ctx, throttler, time.Minute, func(t *testing.T, ctx context.Context) {
		defer cancel() // early termination
		t.Run("aggregated", func(t *testing.T) {
			aggr := throttler.aggregatedMetricsSnapshot()
			assert.Equalf(t, 2*len(base.KnownMetricNames), len(aggr), "aggregated: %+v", aggr)     // "self" and "shard", per known metric
			assert.Equal(t, 2*len(base.KnownMetricNames), throttler.aggregatedMetrics.ItemCount()) // flushed upon Disable()
			for aggregatedMetricName, metricResult := range aggr {
				assert.NotEmpty(t, aggregatedMetricName)
				scope, metricName, err := base.DisaggregateMetricName(aggregatedMetricName)
				assert.NotEmpty(t, metricName)
				require.NoError(t, err)

				val, metricResultErr := metricResult.Get()
				expectMetricNotCollectedYet := false
				switch base.Scope(scope) {
				case base.SelfScope:
					switch metricName {
					case base.DefaultMetricName:
						assert.Equalf(t, float64(0.3), val, "scope=%v, metricName=%v", scope, metricName) // same value as "lag"
					case base.LagMetricName:
						assert.Equalf(t, float64(0.3), val, "scope=%v, metricName=%v", scope, metricName)
					case base.ThreadsRunningMetricName:
						assert.Equalf(t, float64(26), val, "scope=%v, metricName=%v", scope, metricName)
					case base.CustomMetricName:
						assert.Equalf(t, float64(17), val, "scope=%v, metricName=%v", scope, metricName)
					case base.LoadAvgMetricName:
						assert.Equalf(t, float64(2.718), val, "scope=%v, metricName=%v", scope, metricName)
					}
				case base.ShardScope:
					// Replicas will nto report named metrics, since they now assume v20 behavior. They will only
					// produce the single v20 metric (which we call "default", though they don't advertise it under the name "base.DefaultMetricName")
					switch metricName {
					case base.DefaultMetricName:
						assert.Equalf(t, float64(0.339), val, "scope=%v, metricName=%v", scope, metricName) // same value as "lag"
					case base.LagMetricName:
						assert.Equalf(t, float64(0.339), val, "scope=%v, metricName=%v", scope, metricName) //
					default:
						assert.Zero(t, val, "scope=%v, metricName=%v", scope, metricName)
						expectMetricNotCollectedYet = true
					}
				default:
					assert.Failf(t, "unknown scope", "scope=%v", scope)
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
	})
}

// TestProbesPostDisable runs the throttler for some time, and then investigates the internal throttler maps and values.
func TestProbesPostDisable(t *testing.T) {
	ctx := context.Background() // for development, replace with	ctx := utils.LeakCheckContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	throttler := newTestThrottler()
	runThrottler(t, ctx, throttler, 2*time.Second, nil)

	probes := throttler.inventory.ClustersProbes

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
		assert.Empty(t, throttler.inventory.TabletMetrics) // map has been cleared
	})

	t.Run("aggregated", func(t *testing.T) {
		assert.Zero(t, throttler.aggregatedMetrics.ItemCount()) // flushed upon Disable()
		aggr := throttler.aggregatedMetricsSnapshot()
		assert.Empty(t, aggr)
	})
}

func TestDormant(t *testing.T) {
	ctx := context.Background() // for development, replace with	ctx := utils.LeakCheckContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	throttler := newTestThrottler()

	heartbeatWriter, ok := throttler.heartbeatWriter.(*FakeHeartbeatWriter)
	assert.True(t, ok)
	assert.Zero(t, heartbeatWriter.Requests()) // once upon Enable()

	runThrottler(t, ctx, throttler, time.Minute, func(t *testing.T, ctx context.Context) {
		assert.True(t, throttler.isDormant())
		assert.EqualValues(t, 1, heartbeatWriter.Requests()) // once upon Enable()
		flags := &CheckFlags{
			Scope:               base.SelfScope,
			MultiMetricsEnabled: true,
		}
		throttler.Check(ctx, throttlerapp.VitessName.String(), nil, flags)
		go func() {
			defer cancel() // early termination

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
		}()
	})
}

func TestChecks(t *testing.T) {
	ctx := context.Background() // for development, replace with	ctx := utils.LeakCheckContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	throttler := newTestThrottler()
	throttler.dormantPeriod = time.Minute

	tmClient, ok := throttler.overrideTmClient.(*fakeTMClient)
	require.True(t, ok)
	assert.Empty(t, tmClient.AppNames())

	validateAppNames := func(t *testing.T) {
		t.Run("app names", func(t *testing.T) {
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
			assert.Equalf(t, 1, len(uniqueNames), "%+v", uniqueNames)
		})
	}

	runThrottler(t, ctx, throttler, time.Minute, func(t *testing.T, ctx context.Context) {
		defer cancel()
		throttlerConfig := &topodatapb.ThrottlerConfig{
			Enabled:           true,
			MetricThresholds:  map[string]float64{},
			AppCheckedMetrics: map[string]*topodatapb.ThrottlerConfig_MetricNames{},
		}

		t.Run("apply high thresholds", func(t *testing.T) {
			// We apply high thresholds because if a value exceeds a threshold, as is the case
			// designed in the original values (load average 2.718 > 1) then the check result is
			// an error and indicates the errored value. Which is something we already test in
			// TestApplyThrottlerConfigAppCheckedMetrics.
			// In this test, we specifically look for how "lag" is used as the default metric.
			// We this mute other metrics by setting their thresholds to be high.
			throttlerConfig.MetricThresholds["loadavg"] = 7777
			throttlerConfig.MetricThresholds["custom"] = 7778
			throttler.applyThrottlerConfig(ctx, throttlerConfig)
		})
		sleepTillThresholdApplies()

		assert.Equal(t, base.LagMetricName, throttler.metricNameUsedAsDefault())
		aggr := throttler.aggregatedMetricsSnapshot()
		assert.Equalf(t, 2*len(base.KnownMetricNames), len(aggr), "aggregated: %+v", aggr)     // "self" and "shard", per known metric
		assert.Equal(t, 2*len(base.KnownMetricNames), throttler.aggregatedMetrics.ItemCount()) // flushed upon Disable()

		validateAppNames(t)
		t.Run("checks, self scope", func(t *testing.T) {
			flags := &CheckFlags{
				Scope:               base.SelfScope,
				MultiMetricsEnabled: true,
			}
			t.Run("implicit names", func(t *testing.T) {
				checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 0.3, checkResult.Value) // self lag value
				assert.EqualValues(t, http.StatusOK, checkResult.StatusCode)
				assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode)
				assert.Equal(t, testAppName.String(), checkResult.AppName)
				assert.Len(t, checkResult.Metrics, 1)
			})
			t.Run("explicit names", func(t *testing.T) {
				checkResult := throttler.Check(ctx, testAppName.String(), base.KnownMetricNames, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 0.3, checkResult.Value, "unexpected result: %+v", checkResult) // self lag value
				if !assert.EqualValues(t, http.StatusOK, checkResult.StatusCode, "unexpected result: %+v", checkResult) {
					for k, v := range checkResult.Metrics {
						t.Logf("%s: %+v", k, v)
					}
				}
				if !assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode, "unexpected result: %+v", checkResult) {
					for k, v := range checkResult.Metrics {
						t.Logf("%s: %+v", k, v)
					}
				}

				assert.Equal(t, testAppName.String(), checkResult.AppName)
				assert.Equal(t, len(base.KnownMetricNames), len(checkResult.Metrics))

				assert.EqualValues(t, 0.3, checkResult.Metrics[base.LagMetricName.String()].Value)           // self lag value, because flags.Scope is set
				assert.EqualValues(t, 26, checkResult.Metrics[base.ThreadsRunningMetricName.String()].Value) // self value, because flags.Scope is set
				assert.EqualValues(t, 17, checkResult.Metrics[base.CustomMetricName.String()].Value)         // self value, because flags.Scope is set
				assert.EqualValues(t, 2.718, checkResult.Metrics[base.LoadAvgMetricName.String()].Value)     // self value, because flags.Scope is set
				for _, metric := range checkResult.Metrics {
					assert.EqualValues(t, base.SelfScope.String(), metric.Scope)
				}
			})
		})
		t.Run("checks, self scope, vitess app", func(t *testing.T) {
			// "vitess" app always checks all known metrics.
			flags := &CheckFlags{
				// scope not important for this test
				MultiMetricsEnabled: true,
			}
			t.Run("implicit names, always all known", func(t *testing.T) {
				checkResult := throttler.Check(ctx, throttlerapp.VitessName.String(), nil, flags)
				// "vitess" app always checks all known metrics:
				assert.Equal(t, throttlerapp.VitessName.String(), checkResult.AppName)
				assert.Equal(t, len(base.KnownMetricNames), len(checkResult.Metrics))
			})
			t.Run("explicit names, irrelevant, always all known", func(t *testing.T) {
				metricNames := base.MetricNames{
					base.MetricName("self/threads_running"),
					base.MetricName("custom"),
				}

				checkResult := throttler.Check(ctx, throttlerapp.VitessName.String(), metricNames, flags)
				require.NotNil(t, checkResult)
				assert.Equal(t, throttlerapp.VitessName.String(), checkResult.AppName)
				assert.Equal(t, len(base.KnownMetricNames), len(checkResult.Metrics))
			})
		})

		t.Run("checks, shard scope", func(t *testing.T) {
			flags := &CheckFlags{
				Scope:               base.ShardScope,
				MultiMetricsEnabled: true,
			}
			t.Run("implicit names", func(t *testing.T) {
				checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 0.9, checkResult.Value) // shard lag value
				assert.NotEqualValues(t, http.StatusOK, checkResult.StatusCode)
				assert.NotEqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode)
				assert.ErrorIs(t, checkResult.Error, base.ErrThresholdExceeded)
				assert.Equal(t, testAppName.String(), checkResult.AppName)
				assert.Len(t, checkResult.Metrics, 1)
			})
			t.Run("explicit names", func(t *testing.T) {
				checkResult := throttler.Check(ctx, testAppName.String(), base.KnownMetricNames, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 0.9, checkResult.Value) // shard lag value
				assert.NotEqualValues(t, http.StatusOK, checkResult.StatusCode)
				assert.NotEqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode)
				assert.ErrorIs(t, checkResult.Error, base.ErrThresholdExceeded)
				assert.Equal(t, testAppName.String(), checkResult.AppName)
				assert.Equal(t, len(base.KnownMetricNames), len(checkResult.Metrics))

				assert.EqualValues(t, 0.9, checkResult.Metrics[base.LagMetricName.String()].Value)           // shard lag value, because flags.Scope is set
				assert.EqualValues(t, 26, checkResult.Metrics[base.ThreadsRunningMetricName.String()].Value) // shard value, because flags.Scope is set
				assert.EqualValues(t, 17, checkResult.Metrics[base.CustomMetricName.String()].Value)         // shard value, because flags.Scope is set
				assert.EqualValues(t, 5.1, checkResult.Metrics[base.LoadAvgMetricName.String()].Value)       // shard value, because flags.Scope is set
				for _, metric := range checkResult.Metrics {
					assert.EqualValues(t, base.ShardScope.String(), metric.Scope)
				}
			})
		})
		t.Run("checks, undefined scope", func(t *testing.T) {
			flags := &CheckFlags{
				// Leaving scope undefined, so that each metrics picks its own scope
				MultiMetricsEnabled: true,
			}
			t.Run("implicit names", func(t *testing.T) {
				checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 0.9, checkResult.Value) // shard lag value
				assert.NotEqualValues(t, http.StatusOK, checkResult.StatusCode)
				assert.NotEqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode)
				assert.ErrorIs(t, checkResult.Error, base.ErrThresholdExceeded)
				assert.Len(t, checkResult.Metrics, 1)
			})
			t.Run("explicit names", func(t *testing.T) {
				checkResult := throttler.Check(ctx, testAppName.String(), base.KnownMetricNames, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 0.9, checkResult.Value) // shard lag value
				assert.NotEqualValues(t, http.StatusOK, checkResult.StatusCode)
				assert.NotEqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode)
				assert.ErrorIs(t, checkResult.Error, base.ErrThresholdExceeded)
				assert.Equal(t, len(base.KnownMetricNames), len(checkResult.Metrics))

				assert.EqualValues(t, 0.9, checkResult.Metrics[base.LagMetricName.String()].Value)           // shard lag value, because "shard" is the default scope for lag
				assert.EqualValues(t, 26, checkResult.Metrics[base.ThreadsRunningMetricName.String()].Value) // self value, because "self" is the default scope for threads_running
				assert.EqualValues(t, 17, checkResult.Metrics[base.CustomMetricName.String()].Value)         // self value, because "self" is the default scope for custom
				assert.EqualValues(t, 2.718, checkResult.Metrics[base.LoadAvgMetricName.String()].Value)     // self value, because "self" is the default scope for loadavg
				assert.EqualValues(t, base.ShardScope.String(), checkResult.Metrics[base.LagMetricName.String()].Scope)
				assert.EqualValues(t, base.SelfScope.String(), checkResult.Metrics[base.ThreadsRunningMetricName.String()].Scope)
				assert.EqualValues(t, base.SelfScope.String(), checkResult.Metrics[base.CustomMetricName.String()].Scope)
				assert.EqualValues(t, base.SelfScope.String(), checkResult.Metrics[base.LoadAvgMetricName.String()].Scope)
			})
		})
		t.Run("checks, defined scope masks explicit scope metrics", func(t *testing.T) {
			flags := &CheckFlags{
				Scope:               base.ShardScope,
				MultiMetricsEnabled: true,
			}
			t.Run("explicit names", func(t *testing.T) {
				metricNames := base.MetricNames{
					base.MetricName("self/lag"),
					base.MetricName("self/threads_running"),
					base.MetricName("custom"),
					base.MetricName("shard/loadavg"),
					base.MetricName("default"),
				}
				checkResult := throttler.Check(ctx, testAppName.String(), metricNames, flags)

				require.NotNil(t, checkResult)
				assert.EqualValues(t, 0.9, checkResult.Value) // shard lag value
				assert.NotEqualValues(t, http.StatusOK, checkResult.StatusCode)
				assert.NotEqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode)
				assert.ErrorIs(t, checkResult.Error, base.ErrThresholdExceeded)
				assert.Equal(t, len(metricNames), len(checkResult.Metrics))

				assert.EqualValues(t, 0.9, checkResult.Metrics[base.LagMetricName.String()].Value)           // shard lag value, even though scope name is in metric name
				assert.EqualValues(t, 26, checkResult.Metrics[base.ThreadsRunningMetricName.String()].Value) // shard value, even though scope name is in metric name
				assert.EqualValues(t, 17, checkResult.Metrics[base.CustomMetricName.String()].Value)         // shard value because flags.Scope is set
				assert.EqualValues(t, 5.1, checkResult.Metrics[base.LoadAvgMetricName.String()].Value)       // shard value, not because scope name is in metric name but because flags.Scope is set
				for _, metric := range checkResult.Metrics {
					assert.EqualValues(t, base.ShardScope.String(), metric.Scope)
				}
			})
		})
		t.Run("checks, undefined scope and explicit scope metrics", func(t *testing.T) {
			flags := &CheckFlags{
				// Leaving scope undefined
				MultiMetricsEnabled: true,
			}
			t.Run("explicit names", func(t *testing.T) {
				metricNames := base.MetricNames{
					base.MetricName("self/lag"),
					base.MetricName("self/threads_running"),
					base.MetricName("custom"),
					base.MetricName("shard/loadavg"),
				}
				checkResult := throttler.Check(ctx, testAppName.String(), metricNames, flags)
				require.NotNil(t, checkResult)
				assert.EqualValues(t, 0.3, checkResult.Value) // explicitly set self lag value
				assert.EqualValues(t, http.StatusOK, checkResult.StatusCode)
				assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode)
				assert.Equal(t, len(metricNames), len(checkResult.Metrics))

				assert.EqualValues(t, 0.3, checkResult.Metrics[base.LagMetricName.String()].Value)           // self lag value, because scope name is in metric name
				assert.EqualValues(t, 26, checkResult.Metrics[base.ThreadsRunningMetricName.String()].Value) // self value, because scope name is in metric name
				assert.EqualValues(t, 17, checkResult.Metrics[base.CustomMetricName.String()].Value)         // self value, because that's the default...
				assert.EqualValues(t, 5.1, checkResult.Metrics[base.LoadAvgMetricName.String()].Value)       // shard value, because scope name is in metric name
				assert.EqualValues(t, base.SelfScope.String(), checkResult.Metrics[base.LagMetricName.String()].Scope)
				assert.EqualValues(t, base.SelfScope.String(), checkResult.Metrics[base.ThreadsRunningMetricName.String()].Scope)
				assert.EqualValues(t, base.SelfScope.String(), checkResult.Metrics[base.CustomMetricName.String()].Scope)
				assert.EqualValues(t, base.ShardScope.String(), checkResult.Metrics[base.LoadAvgMetricName.String()].Scope)
			})
		})
		// done
	})
}

func TestReplica(t *testing.T) {
	ctx := context.Background() // for development, replace with	ctx := utils.LeakCheckContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	throttler := newTestThrottler()
	throttler.dormantPeriod = time.Minute
	throttler.tabletTypeFunc = func() topodatapb.TabletType { return topodatapb.TabletType_REPLICA }

	tmClient, ok := throttler.overrideTmClient.(*fakeTMClient)
	require.True(t, ok)
	assert.Empty(t, tmClient.AppNames())
	{
		_, ok := throttler.recentApps.Get(throttlerapp.VitessName.String())
		assert.False(t, ok)
	}
	runThrottler(t, ctx, throttler, time.Minute, func(t *testing.T, ctx context.Context) {
		assert.Empty(t, tmClient.AppNames())
		flags := &CheckFlags{
			Scope:               base.SelfScope,
			MultiMetricsEnabled: true,
		}
		{
			checkResult := throttler.Check(ctx, throttlerapp.VitessName.String(), nil, flags)
			assert.NotNil(t, checkResult)
			assert.False(t, checkResult.RecentlyChecked) // "vitess" app does not mark the throttler as recently checked
			assert.False(t, throttler.recentlyChecked()) // "vitess" app does not mark the throttler as recently checked
			{
				_, ok := throttler.recentApps.Get(throttlerapp.VitessName.String())
				assert.True(t, ok)
			}
		}
		go func() {
			defer cancel() // early termination
			t.Run("checks", func(t *testing.T) {
				select {
				case <-ctx.Done():
					require.FailNow(t, "context expired before testing completed")
				case <-time.After(time.Second):
					assert.Empty(t, tmClient.AppNames())
				}
				t.Run("validate stimulator", func(t *testing.T) {
					checkResult := throttler.Check(ctx, throttlerapp.OnlineDDLName.String(), nil, flags)
					require.NotNil(t, checkResult)
					assert.EqualValues(t, 0.3, checkResult.Value) // self lag value
					assert.EqualValues(t, http.StatusOK, checkResult.StatusCode)
					assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode)
					assert.Len(t, checkResult.Metrics, 1)
					select {
					case <-ctx.Done():
						require.FailNow(t, "context expired before testing completed")
					case <-time.After(time.Second):
						appNames := tmClient.AppNames()
						// The replica reports to the primary that it had been checked, by issuing a CheckThrottler
						// on the primary using the ThrottlerStimulatorName app.
						assert.Equal(t, []string{throttlerapp.ThrottlerStimulatorName.String()}, appNames)
					}
				})
				t.Run("validate stimulator", func(t *testing.T) {
					{
						checkResult := throttler.Check(ctx, throttlerapp.OnlineDDLName.String(), nil, flags)
						require.NotNil(t, checkResult)
						assert.EqualValues(t, 0.3, checkResult.Value) // self lag value
						assert.EqualValues(t, http.StatusOK, checkResult.StatusCode)
						assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode)
						assert.Len(t, checkResult.Metrics, 1)
						assert.True(t, checkResult.RecentlyChecked)
						assert.True(t, throttler.recentlyChecked())
						{
							recentApp, ok := throttler.recentAppsSnapshot()[throttlerapp.OnlineDDLName.String()]
							require.True(t, ok)
							assert.EqualValues(t, http.StatusOK, recentApp.StatusCode)
							assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, recentApp.ResponseCode)
						}
					}
					{
						{
							_, ok := throttler.recentApps.Get(throttlerapp.VitessName.String())
							assert.True(t, ok)
						}
						checkResult := throttler.Check(ctx, throttlerapp.VitessName.String(), nil, flags)
						assert.True(t, checkResult.RecentlyChecked) // due to previous "online-ddl" check
						assert.True(t, throttler.recentlyChecked()) // due to previous "online-ddl" check
						{
							_, ok := throttler.recentAppsSnapshot()[throttlerapp.VitessName.String()]
							assert.True(t, ok)
						}
					}
					select {
					case <-ctx.Done():
						require.FailNow(t, "context expired before testing completed")
					case <-time.After(time.Second):
						// Due to stimulation rate limiting, we shouldn't see a 2nd CheckThrottler request.
						appNames := tmClient.AppNames()
						assert.Equal(t, []string{throttlerapp.ThrottlerStimulatorName.String()}, appNames)
					}
				})
				t.Run("validate multi-metric results", func(t *testing.T) {
					checkResult := throttler.Check(ctx, throttlerapp.VitessName.String(), nil, flags)
					require.NotNil(t, checkResult)
					// loadavg value exceeds threshold. This will show up in the check result as an error.
					assert.EqualValues(t, 2.718, checkResult.Value, "unexpected result: %+v", checkResult) // self lag value
					assert.NotEqualValues(t, http.StatusOK, checkResult.StatusCode, "unexpected result: %+v", checkResult)
					assert.NotEqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode, "unexpected result: %+v", checkResult)
					assert.Equal(t, len(base.KnownMetricNames), len(checkResult.Metrics))
				})
				t.Run("validate v20 non-multi-metric results", func(t *testing.T) {
					flags := &CheckFlags{
						Scope:               base.SelfScope,
						MultiMetricsEnabled: false,
					}
					checkResult := throttler.Check(ctx, throttlerapp.VitessName.String(), nil, flags)
					require.NotNil(t, checkResult)
					// loadavg value exceeds threshold. But since "MultiMetricsEnabled: false", the
					// throttler, acting as a replica, assumes it's being probed by a v20 primary, and
					// therefore does not report any of the multi-metric errors back. It only ever
					// reports the default metric.
					assert.EqualValues(t, 0.3, checkResult.Value) // self lag value
					assert.EqualValues(t, http.StatusOK, checkResult.StatusCode)
					assert.EqualValues(t, tabletmanagerdatapb.CheckThrottlerResponseCode_OK, checkResult.ResponseCode)
					assert.EqualValues(t, 0.75, checkResult.Threshold)
					// The replica will still report the multi-metrics, and that's fine. As long
					// as it does not reflect any of their values in the checkResult.Value/StatusCode/Threshold/Error/Message.
					assert.Equal(t, len(base.KnownMetricNames), len(checkResult.Metrics))
				})
			})

			t.Run("metrics", func(t *testing.T) {
				// See which metrics are available
				checkResult := throttler.Check(ctx, throttlerapp.VitessName.String(), base.KnownMetricNames, flags)
				require.NotNil(t, checkResult)
				assert.Equal(t, len(base.KnownMetricNames), len(checkResult.Metrics))

				for metricName, metricResult := range checkResult.Metrics {
					val := metricResult.Value
					threshold := metricResult.Threshold
					scope := base.SelfScope
					switch base.MetricName(metricName) {
					case base.DefaultMetricName:
						assert.NoError(t, metricResult.Error, "metricName=%v, value=%v, threshold=%v", metricName, metricResult.Value, metricResult.Threshold)
						assert.Equalf(t, float64(0.3), val, "scope=%v, metricName=%v", scope, metricName) // same value as "lag"
						assert.Equalf(t, float64(0.75), threshold, "scope=%v, metricName=%v", scope, metricName)
					case base.LagMetricName:
						assert.NoError(t, metricResult.Error, "metricName=%v, value=%v, threshold=%v", metricName, metricResult.Value, metricResult.Threshold)
						assert.Equalf(t, float64(0.3), val, "scope=%v, metricName=%v", scope, metricName)
						assert.Equalf(t, float64(0.75), threshold, "scope=%v, metricName=%v", scope, metricName) // default threshold
					case base.ThreadsRunningMetricName:
						assert.NoError(t, metricResult.Error, "metricName=%v, value=%v, threshold=%v", metricName, metricResult.Value, metricResult.Threshold)
						assert.Equalf(t, float64(26), val, "scope=%v, metricName=%v", scope, metricName)
						assert.Equalf(t, float64(100), threshold, "scope=%v, metricName=%v", scope, metricName)
					case base.CustomMetricName:
						assert.ErrorIs(t, metricResult.Error, base.ErrThresholdExceeded)
						assert.Equalf(t, float64(0), threshold, "scope=%v, metricName=%v", scope, metricName)
					case base.LoadAvgMetricName:
						assert.ErrorIs(t, metricResult.Error, base.ErrThresholdExceeded)
						assert.Equalf(t, float64(1), threshold, "scope=%v, metricName=%v", scope, metricName)
					}
				}
			})
			t.Run("metrics not named", func(t *testing.T) {
				checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.Len(t, checkResult.Metrics, 1)
				for metricName, metricResult := range checkResult.Metrics {
					assert.Equal(t, base.LagMetricName, throttler.metricNameUsedAsDefault())
					assert.Equal(t, base.LagMetricName.String(), metricName)
					val := metricResult.Value
					threshold := metricResult.Threshold
					scope := base.SelfScope

					assert.NoError(t, metricResult.Error, "metricName=%v, value=%v, threshold=%v", metricName, metricResult.Value, metricResult.Threshold)
					assert.Equalf(t, float64(0.3), val, "scope=%v, metricName=%v", scope, metricName)
					assert.Equalf(t, float64(0.75), threshold, "scope=%v, metricName=%v", scope, metricName) // default threshold
				}
			})
			t.Run("metrics names mapped", func(t *testing.T) {
				throttler.appCheckedMetrics.Set(testAppName.String(), base.MetricNames{base.LoadAvgMetricName, base.LagMetricName, base.ThreadsRunningMetricName}, cache.DefaultExpiration)
				defer throttler.appCheckedMetrics.Delete(testAppName.String())
				checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
				require.NotNil(t, checkResult)
				assert.Len(t, checkResult.Metrics, 3)
			})
			t.Run("client, OK", func(t *testing.T) {
				client := NewBackgroundClient(throttler, throttlerapp.TestingName, base.UndefinedScope)
				_, checkOK := client.ThrottleCheckOK(ctx, "")
				assert.True(t, checkOK)
			})
			t.Run("client, metrics names mapped, OK", func(t *testing.T) {
				// Specified metrics do not exceed threshold, therefore overall result should be OK.
				throttler.appCheckedMetrics.Set(throttlerapp.TestingName.String(), base.MetricNames{base.LagMetricName, base.ThreadsRunningMetricName}, cache.DefaultExpiration)
				defer throttler.appCheckedMetrics.Delete(throttlerapp.TestingName.String())
				client := NewBackgroundClient(throttler, throttlerapp.TestingName, base.UndefinedScope)
				_, checkOK := client.ThrottleCheckOK(ctx, "")
				assert.True(t, checkOK)
			})
			t.Run("client, metrics names mapped, not OK", func(t *testing.T) {
				// LoadAvgMetricName metric exceeds threshold, therefore overall check should be in error.
				throttler.appCheckedMetrics.Set(throttlerapp.TestingName.String(), base.MetricNames{base.LagMetricName, base.LoadAvgMetricName, base.ThreadsRunningMetricName}, cache.DefaultExpiration)
				defer throttler.appCheckedMetrics.Delete(throttlerapp.TestingName.String())
				client := NewBackgroundClient(throttler, throttlerapp.TestingName, base.UndefinedScope)
				checkResult, checkOK := client.ThrottleCheckOK(ctx, "")
				assert.False(t, checkOK)
				assert.NotNil(t, checkResult)
				assert.Contains(t, checkResult.Summary(), testAppName.String()+" is denied access due to")
			})

			t.Run("custom query, metrics", func(t *testing.T) {
				// For v20 backwards compatibility, we also report the standard metric/value in CheckResult:
				checkResult := throttler.Check(ctx, testAppName.String(), nil, flags)
				assert.NoError(t, checkResult.Error, "value=%v, threshold=%v", checkResult.Value, checkResult.Threshold)
				assert.Equal(t, float64(0.3), checkResult.Value)
				// Change custom threshold
				throttler.MetricsThreshold.Store(math.Float64bits(0.1))
				<-runSerialFunction(t, ctx, throttler, func(ctx context.Context) {
					throttler.refreshInventory(ctx)
				})
				checkResult = throttler.Check(ctx, testAppName.String(), base.KnownMetricNames, flags)
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
				client := NewBackgroundClient(throttler, throttlerapp.TestingName, base.SelfScope)
				checkResult, checkOK := client.ThrottleCheckOK(ctx, "")
				assert.False(t, checkOK)
				assert.NotNil(t, checkResult)
				assert.Contains(t, checkResult.Summary(), testAppName.String()+" is denied access due to")
			})
		}()
	})
}
