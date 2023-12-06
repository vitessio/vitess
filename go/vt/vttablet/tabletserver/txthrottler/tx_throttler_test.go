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
//go:generate mockgen -destination mock_healthcheck_test.go -package txthrottler -mock_names "LegacyHealthCheck=MockHealthCheck" vitess.io/vitess/go/vt/discovery LegacyHealthCheck
//go:generate mockgen -destination mock_throttler_test.go -package txthrottler vitess.io/vitess/go/vt/vttablet/tabletserver/txthrottler ThrottlerInterface
//go:generate mockgen -destination mock_topology_watcher_test.go -package txthrottler vitess.io/vitess/go/vt/vttablet/tabletserver/txthrottler TopologyWatcherInterface

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/throttler"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
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
	assert.False(t, throttler.Throttle(0, "some-workload"))
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
	var hcListener discovery.LegacyHealthCheckStatsListener
	hcCall1 := mockHealthCheck.EXPECT().SetListener(gomock.Any(), false /* sendDownEvents */)
	hcCall1.Do(func(listener discovery.LegacyHealthCheckStatsListener, sendDownEvents bool) {
		// Record the listener we're given.
		hcListener = listener
	})
	hcCall2 := mockHealthCheck.EXPECT().Close()
	hcCall2.After(hcCall1)
	healthCheckFactory = func() discovery.LegacyHealthCheck { return mockHealthCheck }

	topologyWatcherFactory = func(topoServer *topo.Server, tr discovery.LegacyTabletRecorder, cell, keyspace, shard string, refreshInterval time.Duration, topoReadConcurrency int) TopologyWatcherInterface {
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

	call0 := mockThrottler.EXPECT().UpdateConfiguration(gomock.Any(), true /* copyZeroValues */)
	// Call 1 cannot throttle because the return value of call to underlying Throttle is zero.
	call1 := mockThrottler.EXPECT().Throttle(0)
	call1.Return(0 * time.Second)
	tabletStatsReplicaLagged := &discovery.LegacyTabletStats{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_REPLICA,
		},
		Stats: &querypb.RealtimeStats{ReplicationLagSeconds: 20}, // Lag high enough for throttling
		Name:  "replica-name",
	}
	// call2 Adds a lag record above threshold for replica tablets
	call2 := mockThrottler.EXPECT().RecordReplicationLag(gomock.Any(), tabletStatsReplicaLagged)

	// call3 can throttle because the return value of call to Throttle is > zero - provided other conditions are met
	call3 := mockThrottler.EXPECT().Throttle(0)
	call3.Return(1 * time.Second)
	// call4 can throttle because the return value of call to Throttle is > zero - provided other conditions are met
	call4 := mockThrottler.EXPECT().Throttle(0)
	call4.Return(1 * time.Second)

	tabletStatsReplicaNotLagged := &discovery.LegacyTabletStats{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_REPLICA,
		},
		Stats: &querypb.RealtimeStats{ReplicationLagSeconds: 1}, // Lag high enough for throttling
		Name:  "replica-name",
	}

	// call5 Adds a lag record below threshold for the right tablet type
	call5 := mockThrottler.EXPECT().RecordReplicationLag(gomock.Any(), tabletStatsReplicaNotLagged)
	// call6 can throttle because the return value of call to Throttle is > zero - provided other conditions are met
	call6 := mockThrottler.EXPECT().Throttle(0)
	call6.Return(1 * time.Second)
	// call7 can throttle because the return value of call to Throttle is > zero - provided other conditions are met
	call7 := mockThrottler.EXPECT().Throttle(0)
	call7.Return(1 * time.Second)

	calllast := mockThrottler.EXPECT().Close()

	call1.After(call0)
	call2.After(call1)
	call3.After(call2)
	call4.After(call3)
	call5.After(call4)
	call6.After(call5)
	call7.After(call6)
	calllast.After(call7)

	config := tabletenv.NewDefaultConfig()
	config.EnableTxThrottler = true
	config.TxThrottlerHealthCheckCells = []string{"cell1", "cell2"}
	config.TxThrottlerTabletTypes = []topodatapb.TabletType{topodatapb.TabletType_REPLICA}

	env := tabletenv.NewEnv(config, t.Name())
	throttler, err := tryCreateTxThrottler(env, ts)
	assert.Nil(t, err)
	throttler.InitDBConfig(&querypb.Target{
		Keyspace: "keyspace",
		Shard:    "shard",
	})
	assert.Nil(t, throttler.Open())
	assert.Equal(t, int64(1), throttler.throttlerRunning.Get())

	// call1 can't throttle.
	assert.False(t, throttler.Throttle(100, "some-workload"))
	assert.Equal(t, int64(1), throttler.requestsTotal.Counts()["some-workload"])
	assert.Zero(t, throttler.requestsThrottled.Counts()["some-workload"])
	// call2 records lag above threshold for REPLICA tablet
	throttler.state.StatsUpdate(tabletStatsReplicaLagged)

	// This call should not be forwarded to the go/vt/throttler.Throttler object. Ignore RDONLY replicas due to config
	rdonlyTabletStats := &discovery.LegacyTabletStats{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_RDONLY,
		},
		Stats: &querypb.RealtimeStats{ReplicationLagSeconds: 20}, // Lag high enough for throttling
		Name:  "rdonly-name",
	}
	hcListener.StatsUpdate(rdonlyTabletStats)

	// call3 throttles due to priority & enough replication lag besides return value for underlying Throttle call
	assert.True(t, throttler.Throttle(100, "some-workload"))
	assert.Equal(t, int64(2), throttler.requestsTotal.Counts()["some-workload"])
	assert.Equal(t, int64(1), throttler.requestsThrottled.Counts()["some-workload"])

	// call4 does not throttle due to priority, despite enough replication lag besides return value for underlying
	// Throttle call
	assert.False(t, throttler.Throttle(0, "some-workload"))
	assert.Equal(t, int64(3), throttler.requestsTotal.Counts()["some-workload"])
	assert.Equal(t, int64(1), throttler.requestsThrottled.Counts()["some-workload"])

	// call5 records lag below threshold for REPLICA tablet
	throttler.state.StatsUpdate(tabletStatsReplicaNotLagged)

	// call6 does not throttle despite priority, because lag is below threshold
	assert.False(t, throttler.Throttle(100, "some-workload"))
	assert.Equal(t, int64(4), throttler.requestsTotal.Counts()["some-workload"])
	assert.Equal(t, int64(1), throttler.requestsThrottled.Counts()["some-workload"])
	// call7 does not throttle due to priority and replication lag below threshold
	assert.False(t, throttler.Throttle(0, "some-workload"))
	assert.Equal(t, int64(5), throttler.requestsTotal.Counts()["some-workload"])
	assert.Equal(t, int64(1), throttler.requestsThrottled.Counts()["some-workload"])

	throttler.Close()
	assert.Zero(t, throttler.throttlerRunning.Get())
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

func TestDryRunThrottler(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	env := tabletenv.NewEnv(config, t.Name())

	testCases := []struct {
		Name                           string
		txThrottlerStateShouldThrottle bool
		throttlerDryRun                bool
		expectedResult                 bool
	}{
		{Name: "Real run throttles when txThrottlerStateImpl says it should", txThrottlerStateShouldThrottle: true, throttlerDryRun: false, expectedResult: true},
		{Name: "Real run does not throttle when txThrottlerStateImpl says it should not", txThrottlerStateShouldThrottle: false, throttlerDryRun: false, expectedResult: false},
		{Name: "Dry run does not throttle when txThrottlerStateImpl says it should", txThrottlerStateShouldThrottle: true, throttlerDryRun: true, expectedResult: false},
		{Name: "Dry run does not throttle when txThrottlerStateImpl says it should not", txThrottlerStateShouldThrottle: false, throttlerDryRun: true, expectedResult: false},
	}

	for _, aTestCase := range testCases {
		theTestCase := aTestCase

		t.Run(theTestCase.Name, func(t *testing.T) {
			aTxThrottler := &txThrottler{
				config: &txThrottlerConfig{
					enabled: true,
					dryRun:  theTestCase.throttlerDryRun,
				},
				state:             &mockTxThrottlerState{shouldThrottle: theTestCase.txThrottlerStateShouldThrottle},
				throttlerRunning:  env.Exporter().NewGauge("TransactionThrottlerRunning", "transaction throttler running state"),
				requestsTotal:     env.Exporter().NewCountersWithSingleLabel("TransactionThrottlerRequests", "transaction throttler requests", "workload"),
				requestsThrottled: env.Exporter().NewCountersWithSingleLabel("TransactionThrottlerThrottled", "transaction throttler requests throttled", "workload"),
			}

			assert.Equal(t, theTestCase.expectedResult, aTxThrottler.Throttle(100, "some-workload"))
		})
	}
}

type mockTxThrottlerState struct {
	shouldThrottle bool
}

func (t *mockTxThrottlerState) deallocateResources() {

}
func (t *mockTxThrottlerState) StatsUpdate(*discovery.LegacyTabletStats) {

}

func (t *mockTxThrottlerState) throttle() bool {
	return t.shouldThrottle
}
