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
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/config"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/mysql"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	waitForProbesTimeout = 30 * time.Second
)

type fakeTMClient struct {
	tmclient.TabletManagerClient
}

func (c *fakeTMClient) Close() {
}

func (c *fakeTMClient) CheckThrottler(ctx context.Context, tablet *topodatapb.Tablet, request *tabletmanagerdatapb.CheckThrottlerRequest) (*tabletmanagerdatapb.CheckThrottlerResponse, error) {
	resp := &tabletmanagerdatapb.CheckThrottlerResponse{
		StatusCode:      http.StatusOK,
		Value:           0,
		Threshold:       1,
		RecentlyChecked: true,
	}
	return resp, nil
}

type FakeTopoServer struct {
}

func (ts *FakeTopoServer) GetTablet(ctx context.Context, alias *topodatapb.TabletAlias) (*topo.TabletInfo, error) {
	tablet := &topo.TabletInfo{
		Tablet: &topodatapb.Tablet{
			Alias:         alias,
			Hostname:      "127.0.0.1",
			MysqlHostname: "127.0.0.1",
			MysqlPort:     3306,
			PortMap:       map[string]int32{"vt": 5000},
			Type:          topodatapb.TabletType_REPLICA,
		},
	}
	return tablet, nil
}

func (ts *FakeTopoServer) FindAllTabletAliasesInShard(ctx context.Context, keyspace, shard string) ([]*topodatapb.TabletAlias, error) {
	aliases := []*topodatapb.TabletAlias{
		{Cell: "zone1", Uid: 100},
		{Cell: "zone2", Uid: 101},
	}
	return aliases, nil
}

func (ts *FakeTopoServer) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	ks := &topodatapb.SrvKeyspace{}
	return ks, nil
}

type FakeHeartbeatWriter struct {
}

func (w FakeHeartbeatWriter) RequestHeartbeats() {
}

func newTestThrottler() *Throttler {
	metricsQuery := "select 1"
	config.Settings().Stores.MySQL.Clusters = map[string]*config.MySQLClusterConfigurationSettings{
		selfStoreName:  {},
		shardStoreName: {},
	}
	clusters := config.Settings().Stores.MySQL.Clusters
	for _, s := range clusters {
		s.MetricQuery = metricsQuery
		s.ThrottleThreshold = &atomic.Uint64{}
		s.ThrottleThreshold.Store(1)
	}
	env := tabletenv.NewEnv(nil, "TabletServerTest", collations.MySQL8(), sqlparser.NewTestParser())
	throttler := &Throttler{
		mysqlClusterProbesChan: make(chan *mysql.ClusterProbes),
		mysqlClusterThresholds: cache.New(cache.NoExpiration, 0),
		heartbeatWriter:        FakeHeartbeatWriter{},
		ts:                     &FakeTopoServer{},
		mysqlInventory:         mysql.NewInventory(),
		pool:                   connpool.NewPool(env, "ThrottlerPool", tabletenv.ConnPoolConfig{}),
		tabletTypeFunc:         func() topodatapb.TabletType { return topodatapb.TabletType_PRIMARY },
		overrideTmClient:       &fakeTMClient{},
	}
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

	// High contention & racy itnervals:
	throttler.leaderCheckInterval = 10 * time.Millisecond
	throttler.mysqlCollectInterval = 10 * time.Millisecond
	throttler.mysqlDormantCollectInterval = 10 * time.Millisecond
	throttler.mysqlRefreshInterval = 10 * time.Millisecond
	throttler.mysqlAggregateInterval = 10 * time.Millisecond
	throttler.throttledAppsSnapshotInterval = 10 * time.Millisecond

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
		heartbeatWriter: FakeHeartbeatWriter{},
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
		heartbeatWriter: FakeHeartbeatWriter{},
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
	config.Settings().Stores.MySQL.Clusters = map[string]*config.MySQLClusterConfigurationSettings{
		selfStoreName: {},
		"ks1":         {},
		"ks2":         {},
	}
	clusters := config.Settings().Stores.MySQL.Clusters
	for _, s := range clusters {
		s.MetricQuery = metricsQuery
		s.ThrottleThreshold = &atomic.Uint64{}
		s.ThrottleThreshold.Store(1)
	}

	throttler := &Throttler{
		mysqlClusterProbesChan: make(chan *mysql.ClusterProbes),
		mysqlClusterThresholds: cache.New(cache.NoExpiration, 0),
		ts:                     &FakeTopoServer{},
		mysqlInventory:         mysql.NewInventory(),
	}
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

// runThrottler opens and enables the throttler, therby making it run the Operate() function, for a given amount of time.
// Optionally, running a given function halfway while the throttler is still open and running.
func runThrottler(t *testing.T, throttler *Throttler, timeout time.Duration, f func(*testing.T)) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	assert.False(t, throttler.IsOpen())
	assert.False(t, throttler.IsEnabled())

	throttler.isOpen.Swap(true)
	defer throttler.isOpen.Swap(false)
	assert.True(t, throttler.IsOpen())
	assert.False(t, throttler.IsEnabled())

	ok := throttler.Enable()
	defer throttler.Disable()
	assert.True(t, ok)
	assert.True(t, throttler.IsEnabled())

	if f != nil {
		time.Sleep(timeout / 2)
		f(t)
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
	runThrottler(t, throttler, 5*time.Second, nil)
}

// TestProbes enables a throttler for a few seocnds, and afterwards expects to find probes and metrics.
func TestProbesWhileOperating(t *testing.T) {
	throttler := newTestThrottler()

	t.Run("aggregated", func(t *testing.T) {
		assert.Equal(t, 0, throttler.aggregatedMetrics.ItemCount())
	})
	runThrottler(t, throttler, 5*time.Second, func(t *testing.T) {
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
		})
	})
}
