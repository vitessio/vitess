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
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/config"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/mysql"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	waitForProbesTimeout = 30 * time.Second
)

type fakeTMClient struct {
	tmclient.TabletManagerClient
	appNames []string

	mu sync.Mutex
}

func (c *fakeTMClient) Close() {
}

func (c *fakeTMClient) CheckThrottler(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.CheckThrottlerRequest) (*tabletmanagerdatapb.CheckThrottlerResponse, error) {
	resp := &tabletmanagerdatapb.CheckThrottlerResponse{
		StatusCode:      http.StatusOK,
		Value:           0,
		Threshold:       1,
		RecentlyChecked: false,
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
		{Cell: "fakezone1", Uid: 100},
		{Cell: "fakezone2", Uid: 101},
		{Cell: "fakezone3", Uid: 103},
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

func newTestThrottler() *Throttler {
	metricsQuery := "select 1"
	configSettings := config.NewConfigurationSettings()
	configSettings.Stores.MySQL.Clusters = map[string]*config.MySQLClusterConfigurationSettings{
		selfStoreName:  {},
		shardStoreName: {},
	}
	for _, s := range configSettings.Stores.MySQL.Clusters {
		s.MetricQuery = metricsQuery
		s.ThrottleThreshold = &atomic.Uint64{}
		s.ThrottleThreshold.Store(1)
	}
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), nil, "TabletServerTest")
	throttler := &Throttler{
		mysqlClusterProbesChan: make(chan *mysql.ClusterProbes),
		mysqlClusterThresholds: cache.New(cache.NoExpiration, 0),
		heartbeatWriter:        &FakeHeartbeatWriter{},
		ts:                     &FakeTopoServer{},
		mysqlInventory:         mysql.NewInventory(),
		pool:                   connpool.NewPool(env, "ThrottlerPool", tabletenv.ConnPoolConfig{}),
		tabletTypeFunc:         func() topodatapb.TabletType { return topodatapb.TabletType_PRIMARY },
		overrideTmClient:       &fakeTMClient{},
	}
	throttler.configSettings = configSettings
	throttler.mysqlThrottleMetricChan = make(chan *mysql.MySQLThrottleMetric)
	throttler.mysqlInventoryChan = make(chan *mysql.Inventory, 1)
	throttler.mysqlClusterProbesChan = make(chan *mysql.ClusterProbes)
	throttler.throttlerConfigChan = make(chan *topodatapb.ThrottlerConfig)
	throttler.mysqlInventory = mysql.NewInventory()

	throttler.throttledApps = cache.New(cache.NoExpiration, 0)
	throttler.mysqlClusterThresholds = cache.New(cache.NoExpiration, 0)
	throttler.aggregatedMetrics = cache.New(10*aggregatedMetricsExpiration, 0)
	throttler.recentApps = cache.New(recentAppsExpiration, 0)
	throttler.metricsHealth = cache.New(cache.NoExpiration, 0)
	throttler.nonLowPriorityAppRequestsThrottled = cache.New(nonDeprioritizedAppMapExpiration, 0)
	throttler.metricsQuery.Store(metricsQuery)
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

	throttler.readSelfThrottleMetric = func(ctx context.Context, p *mysql.Probe) *mysql.MySQLThrottleMetric {
		return &mysql.MySQLThrottleMetric{
			ClusterName: selfStoreName,
			Alias:       "",
			Value:       1,
			Err:         nil,
		}
	}

	return throttler
}

func TestIsAppThrottled(t *testing.T) {
	throttler := Throttler{
		throttledApps:   cache.New(cache.NoExpiration, 0),
		heartbeatWriter: &FakeHeartbeatWriter{},
	}
	assert.False(t, throttler.IsAppThrottled("app1"))
	assert.False(t, throttler.IsAppThrottled("app2"))
	assert.False(t, throttler.IsAppThrottled("app3"))
	assert.False(t, throttler.IsAppThrottled("app4"))
	//
	throttler.ThrottleApp("app1", time.Now().Add(time.Hour), DefaultThrottleRatio, true)
	throttler.ThrottleApp("app2", time.Now(), DefaultThrottleRatio, false)
	throttler.ThrottleApp("app3", time.Now().Add(time.Hour), DefaultThrottleRatio, false)
	throttler.ThrottleApp("app4", time.Now().Add(time.Hour), 0, false)
	assert.False(t, throttler.IsAppThrottled("app1")) // exempted
	assert.False(t, throttler.IsAppThrottled("app2")) // expired
	assert.True(t, throttler.IsAppThrottled("app3"))
	assert.False(t, throttler.IsAppThrottled("app4")) // ratio is zero
	//
	throttler.UnthrottleApp("app1")
	throttler.UnthrottleApp("app2")
	throttler.UnthrottleApp("app3")
	throttler.UnthrottleApp("app4")
	assert.False(t, throttler.IsAppThrottled("app1"))
	assert.False(t, throttler.IsAppThrottled("app2"))
	assert.False(t, throttler.IsAppThrottled("app3"))
	assert.False(t, throttler.IsAppThrottled("app4"))
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
// On a "self" cluster, that list is expect to probe the tablet itself.
// On any other cluster, the list is expected to be empty if non-leader (only leader throttler, on a
// `PRIMARY` tablet, probes other tablets). On the leader, the list is expected to be non-empty.
func TestRefreshMySQLInventory(t *testing.T) {
	metricsQuery := "select 1"
	configSettings := config.NewConfigurationSettings()
	clusters := map[string]*config.MySQLClusterConfigurationSettings{
		selfStoreName: {},
		"ks1":         {},
		"ks2":         {},
	}
	for _, s := range clusters {
		s.MetricQuery = metricsQuery
		s.ThrottleThreshold = &atomic.Uint64{}
		s.ThrottleThreshold.Store(1)
	}
	configSettings.Stores.MySQL.Clusters = clusters

	throttler := &Throttler{
		mysqlClusterProbesChan: make(chan *mysql.ClusterProbes),
		mysqlClusterThresholds: cache.New(cache.NoExpiration, 0),
		ts:                     &FakeTopoServer{},
		mysqlInventory:         mysql.NewInventory(),
	}
	throttler.configSettings = configSettings
	throttler.metricsQuery.Store(metricsQuery)
	throttler.initThrottleTabletTypes()

	validateClusterProbes := func(t *testing.T, ctx context.Context) {
		testName := fmt.Sprintf("leader=%t", throttler.isLeader.Load())
		t.Run(testName, func(t *testing.T) {
			// validateProbesCount expects number of probes according to cluster name and throttler's leadership status
			validateProbesCount := func(t *testing.T, clusterName string, probes mysql.Probes) {
				if clusterName == selfStoreName {
					assert.Equal(t, 1, len(probes))
				} else if throttler.isLeader.Load() {
					assert.NotZero(t, len(probes))
				} else {
					assert.Empty(t, probes)
				}
			}
			t.Run("waiting for probes", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(ctx, waitForProbesTimeout)
				defer cancel()
				numClusterProbesResults := 0
				for {
					select {
					case probes := <-throttler.mysqlClusterProbesChan:
						// Worth noting that in this unit test, the throttler is _closed_ and _disabled_. Its own Operate() function does
						// not run, and therefore there is none but us to both populate `mysqlClusterProbesChan` as well as
						// read from it. We do not compete here with any other goroutine.
						assert.NotNil(t, probes)

						throttler.updateMySQLClusterProbes(ctx, probes)

						numClusterProbesResults++
						validateProbesCount(t, probes.ClusterName, probes.TabletProbes)

						if numClusterProbesResults == len(clusters) {
							// Achieved our goal
							return
						}
					case <-ctx.Done():
						assert.FailNowf(t, ctx.Err().Error(), "waiting for %d cluster probes", len(clusters))
					}
				}
			})
			t.Run("validating probes", func(t *testing.T) {
				for clusterName := range clusters {
					probes, ok := throttler.mysqlInventory.ClustersProbes[clusterName]
					require.True(t, ok)
					validateProbesCount(t, clusterName, probes)
				}
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

	t.Run("aggregated", func(t *testing.T) {
		assert.Equal(t, 0, throttler.aggregatedMetrics.ItemCount())
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runThrottler(t, ctx, throttler, time.Minute, func(t *testing.T, ctx context.Context) {
		t.Run("aggregated", func(t *testing.T) {
			assert.Equal(t, 2, throttler.aggregatedMetrics.ItemCount()) // flushed upon Disable()
			aggr := throttler.aggregatedMetricsSnapshot()
			assert.Equal(t, 2, len(aggr)) // "self" and "shard" clusters
			for clusterName, metricResult := range aggr {
				val, err := metricResult.Get()
				assert.NoError(t, err)
				switch clusterName {
				case "mysql/self":
					assert.Equal(t, float64(1), val)
				case "mysql/shard":
					assert.Equal(t, float64(0), val)
				default:
					assert.Failf(t, "unknown clusterName", "%v", clusterName)
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

			cancel() // end test early
		})
	})
}

// TestProbesPostDisable runs the throttler for some time, and then investigates the internal throttler maps and values.
func TestProbesPostDisable(t *testing.T) {
	throttler := newTestThrottler()
	runThrottler(t, context.Background(), throttler, 2*time.Second, nil)

	probes := throttler.mysqlInventory.ClustersProbes
	assert.NotEmpty(t, probes)

	selfProbes := probes[selfStoreName]
	t.Run("self", func(t *testing.T) {
		assert.NotEmpty(t, selfProbes)
		require.Equal(t, 1, len(selfProbes)) // should always be true once refreshMySQLInventory() runs
		probe, ok := selfProbes[""]
		assert.True(t, ok)
		assert.NotNil(t, probe)

		assert.Equal(t, "", probe.Alias)
		assert.Nil(t, probe.Tablet)
		assert.Equal(t, "select 1", probe.MetricQuery)
		assert.Zero(t, atomic.LoadInt64(&probe.QueryInProgress))
	})

	shardProbes := probes[shardStoreName]
	t.Run("shard", func(t *testing.T) {
		assert.NotEmpty(t, shardProbes)
		assert.Equal(t, 2, len(shardProbes)) // see fake FindAllTabletAliasesInShard above
		for _, probe := range shardProbes {
			require.NotNil(t, probe)
			assert.NotEmpty(t, probe.Alias)
			assert.NotNil(t, probe.Tablet)
			assert.Equal(t, "select 1", probe.MetricQuery)
			assert.Zero(t, atomic.LoadInt64(&probe.QueryInProgress))
		}
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
		flags := &CheckFlags{}
		throttler.CheckByType(ctx, throttlerapp.VitessName.String(), "", flags, ThrottleCheckSelf)
		go func() {
			select {
			case <-ctx.Done():
				require.FailNow(t, "context expired before testing completed")
			case <-time.After(time.Second):
				assert.True(t, throttler.isDormant())
				assert.EqualValues(t, 1, heartbeatWriter.Requests()) // "vitess" name does not cause heartbeat requests
			}
			throttler.CheckByType(ctx, throttlerapp.ThrottlerStimulatorName.String(), "", flags, ThrottleCheckSelf)
			select {
			case <-ctx.Done():
				require.FailNow(t, "context expired before testing completed")
			case <-time.After(time.Second):
				assert.False(t, throttler.isDormant())
				assert.Greater(t, heartbeatWriter.Requests(), int64(1))
			}
			throttler.CheckByType(ctx, throttlerapp.OnlineDDLName.String(), "", flags, ThrottleCheckSelf)
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
		flags := &CheckFlags{}
		throttler.CheckByType(ctx, throttlerapp.VitessName.String(), "", flags, ThrottleCheckSelf)
		go func() {
			select {
			case <-ctx.Done():
				require.FailNow(t, "context expired before testing completed")
			case <-time.After(time.Second):
				assert.Empty(t, tmClient.AppNames())
			}
			throttler.CheckByType(ctx, throttlerapp.OnlineDDLName.String(), "", flags, ThrottleCheckSelf)
			select {
			case <-ctx.Done():
				require.FailNow(t, "context expired before testing completed")
			case <-time.After(time.Second):
				appNames := tmClient.AppNames()
				assert.NotEmpty(t, appNames)
				assert.Containsf(t, appNames, throttlerapp.ThrottlerStimulatorName.String(), "%+v", appNames)
				assert.Equalf(t, 1, len(appNames), "%+v", appNames)
			}
			throttler.CheckByType(ctx, throttlerapp.OnlineDDLName.String(), "", flags, ThrottleCheckSelf)
			select {
			case <-ctx.Done():
				require.FailNow(t, "context expired before testing completed")
			case <-time.After(time.Second):
				// Due to stimulation rate limiting, we shouldn't see a 2nd CheckThrottler request.
				appNames := tmClient.AppNames()
				assert.Equalf(t, 1, len(appNames), "%+v", appNames)
			}
			cancel() // end test early
		}()
	})
}
