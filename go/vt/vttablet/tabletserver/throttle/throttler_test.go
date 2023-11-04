/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package throttle

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/config"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/mysql"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	waitForProbesTimeout = 30 * time.Second
)

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
			// validateProbesCount expectes number of probes according to cluster name and throttler's leadership status
			validateProbesCount := func(t *testing.T, clusterName string, probes *mysql.Probes) {
				if clusterName == selfStoreName {
					assert.Equal(t, 1, len(*probes))
				} else if throttler.isLeader.Load() {
					assert.NotZero(t, len(*probes))
				} else {
					assert.Empty(t, *probes)
				}
			}
			t.Run("waiting for probes", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(ctx, waitForProbesTimeout)
				defer cancel()
				numClusterProbesResults := 0
				for {
					select {
					case probes := <-throttler.mysqlClusterProbesChan:
						// Worth noting that in this unit test, the throttler is _closed_. Its own Operate() function does
						// not run, and therefore there is none but us to both populate `mysqlClusterProbesChan` as well as
						// read from it. We do not compete here with any other goroutine.
						assert.NotNil(t, probes)

						throttler.updateMySQLClusterProbes(ctx, probes)

						numClusterProbesResults++
						validateProbesCount(t, probes.ClusterName, probes.InstanceProbes)

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
