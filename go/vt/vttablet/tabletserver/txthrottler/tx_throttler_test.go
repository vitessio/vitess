/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package txthrottler

// Commands to generate the mocks for this test.
//go:generate mockgen -destination mock_healthcheck_test.go -package txthrottler -mock_names "HealthCheck=MockHealthCheck" vitess.io/vitess/go/vt/discovery HealthCheck
//go:generate mockgen -destination mock_throttler_test.go -package txthrottler vitess.io/vitess/go/vt/vttablet/tabletserver/txthrottler ThrottlerInterface
//go:generate mockgen -destination mock_topology_watcher_test.go -package txthrottler vitess.io/vitess/go/vt/vttablet/tabletserver/txthrottler TopologyWatcherInterface

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/throttler"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	throttlerdatapb "vitess.io/vitess/go/vt/proto/throttlerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestDisabledThrottler(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	config.EnableTxThrottler = false
	env := tabletenv.NewEnv(config, t.Name())
	throttler := NewTxThrottler(env, nil)
	throttler.InitDBConfig(&querypb.Target{
		Keyspace: "keyspace",
		Shard:    "shard",
	})
	assert.Nil(t, throttler.Open())
	assert.False(t, throttler.Throttle(0))
	throttlerImpl, _ := throttler.(*txThrottler)
	assert.Zero(t, throttlerImpl.throttlerRunning.Get())
	throttler.Close()
}

func TestEnabledThrottler(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	defer resetTxThrottlerFactories()
	ts := memorytopo.NewServer("cell1", "cell2")

	mockHealthCheck := NewMockHealthCheck(mockCtrl)
	hcCall1 := mockHealthCheck.EXPECT().Subscribe()
	hcCall1.Do(func() {})
	hcCall2 := mockHealthCheck.EXPECT().Close()
	hcCall2.After(hcCall1)
	healthCheckFactory = func(topoServer *topo.Server, cell string, cellsToWatch []string) discovery.HealthCheck {
		return mockHealthCheck
	}

	topologyWatcherFactory = func(topoServer *topo.Server, hc discovery.HealthCheck, cell, keyspace, shard string, refreshInterval time.Duration, topoReadConcurrency int) TopologyWatcherInterface {
		assert.Equal(t, ts, topoServer)
		assert.Contains(t, []string{"cell1", "cell2"}, cell)
		assert.Equal(t, "keyspace", keyspace)
		assert.Equal(t, "shard", shard)
		result := NewMockTopologyWatcherInterface(mockCtrl)
		result.EXPECT().Stop()
		return result
	}

	mockThrottler := NewMockThrottlerInterface(mockCtrl)
	throttlerFactory = func(name, unit string, threadCount int, maxRate int64, maxReplicationLagConfig throttler.MaxReplicationLagModuleConfig) (ThrottlerInterface, error) {
		assert.Equal(t, 1, threadCount)
		return mockThrottler, nil
	}

	var calls []*gomock.Call

	call := mockThrottler.EXPECT().UpdateConfiguration(gomock.Any(), true /* copyZeroValues */)
	calls = append(calls, call)

	// 1
	call = mockThrottler.EXPECT().Throttle(0)
	call.Return(0 * time.Second)
	calls = append(calls, call)

	tabletStats := &discovery.TabletHealth{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_REPLICA,
		},
	}

	call = mockThrottler.EXPECT().RecordReplicationLag(gomock.Any(), tabletStats)
	calls = append(calls, call)

	// 2
	call = mockThrottler.EXPECT().Throttle(0)
	call.Return(1 * time.Second)
	calls = append(calls, call)

	// 3
	// Nothing gets mocked here because the order of evaluation in txThrottler.Throttle() evaluates first
	// whether the priority allows for throttling or not, so no need to mock calls in mockThrottler.Throttle()

	// 4
	// Nothing gets mocked here because the order of evaluation in txThrottlerStateImpl.Throttle() evaluates first
	// whether there is lag or not, so no call to the underlying mockThrottler is issued.

	call = mockThrottler.EXPECT().Close()
	calls = append(calls, call)

	for i := 1; i < len(calls); i++ {
		calls[i].After(calls[i-1])
	}

	config := tabletenv.NewDefaultConfig()
	config.EnableTxThrottler = true
	config.TxThrottlerHealthCheckCells = []string{"cell1", "cell2"}
	config.TxThrottlerTabletTypes = &topoproto.TabletTypeListFlag{topodatapb.TabletType_REPLICA}

	env := tabletenv.NewEnv(config, t.Name())
	throttlerImpl, err := tryCreateTxThrottler(env, ts)
	assert.Nil(t, err)
	throttlerImpl.InitDBConfig(&querypb.Target{
		Keyspace: "keyspace",
		Shard:    "shard",
	})
	assert.Nil(t, throttlerImpl.Open())
	assert.Equal(t, int64(1), throttlerImpl.throttlerRunning.Get())

	// Stop the go routine that keeps updating the cached  shard's max lag to prevent it from changing the value in a
	// way that will interfere with how we manipulate that value in our tests to evaluate different cases:
	throttlerImpl.state.done <- true

	// 1 should not throttle due to return value of underlying Throttle(), despite high lag
	atomic.StoreInt64(&throttlerImpl.state.maxLag, 20)
	assert.False(t, throttlerImpl.Throttle(100))
	assert.Equal(t, int64(1), throttlerImpl.requestsTotal.Get())
	assert.Zero(t, throttlerImpl.requestsThrottled.Get())

	throttlerImpl.state.StatsUpdate(tabletStats) // This calls replication lag thing
	rdonlyTabletStats := &discovery.TabletHealth{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_RDONLY,
		},
	}
	// This call should not be forwarded to the go/vt/throttler.Throttler object.
	throttlerImpl.state.StatsUpdate(rdonlyTabletStats)

	// 2 should throttle due to return value of underlying Throttle(), high lag & priority = 100
	assert.True(t, throttlerImpl.Throttle(100))
	assert.Equal(t, int64(2), throttlerImpl.requestsTotal.Get())
	assert.Equal(t, int64(1), throttlerImpl.requestsThrottled.Get())

	// 3 should not throttle despite return value of underlying Throttle() and high lag, due to priority = 0
	assert.False(t, throttlerImpl.Throttle(0))
	assert.Equal(t, int64(3), throttlerImpl.requestsTotal.Get())
	assert.Equal(t, int64(1), throttlerImpl.requestsThrottled.Get())

	// 4 should not throttle despite return value of underlying Throttle() and priority = 100, due to low lag
	atomic.StoreInt64(&throttlerImpl.state.maxLag, 1)
	assert.False(t, throttlerImpl.Throttle(100))
	assert.Equal(t, int64(4), throttlerImpl.requestsTotal.Get())
	assert.Equal(t, int64(1), throttlerImpl.requestsThrottled.Get())

	throttlerImpl.Close()
	assert.Zero(t, throttlerImpl.throttlerRunning.Get())
}

func TestNewTxThrottler(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	env := tabletenv.NewEnv(config, t.Name())

	{
		// disabled config
		throttler, err := newTxThrottler(env, nil, &txThrottlerConfig{enabled: false})
		assert.Nil(t, err)
		assert.NotNil(t, throttler)
	}
	{
		// enabled with invalid throttler config
		throttler, err := newTxThrottler(env, nil, &txThrottlerConfig{
			enabled:         true,
			throttlerConfig: &throttlerdatapb.Configuration{},
		})
		assert.NotNil(t, err)
		assert.Nil(t, throttler)
	}
	{
		// enabled
		throttler, err := newTxThrottler(env, nil, &txThrottlerConfig{
			enabled:          true,
			healthCheckCells: []string{"cell1"},
			throttlerConfig:  throttler.DefaultMaxReplicationLagModuleConfig().Configuration,
		})
		assert.Nil(t, err)
		assert.NotNil(t, throttler)
	}
}
