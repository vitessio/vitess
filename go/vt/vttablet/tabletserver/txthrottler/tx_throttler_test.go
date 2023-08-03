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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/throttler"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestDisabledThrottler(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	config.EnableTxThrottler = false
	env := tabletenv.NewEnv(config, t.Name())
	mockEngine := NewMockTabletserverEngine()
	throttler := NewTxThrottler(env, nil, mockEngine, mockEngine)
	throttler.InitDBConfig(&querypb.Target{
		Keyspace: "keyspace",
		Shard:    "shard",
	})
	assert.Nil(t, throttler.Open())
	assert.Nil(t, throttler.Throttle(
		&planbuilder.Plan{PlanID: planbuilder.PlanInsert},
		&querypb.ExecuteOptions{Priority: "0", WorkloadName: "some_workload"},
	))
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

	call0 := mockThrottler.EXPECT().UpdateConfiguration(gomock.Any(), true /* copyZeroValues */)
	call1 := mockThrottler.EXPECT().Throttle(0)
	call1.Return(0 * time.Second)
	tabletStats := &discovery.TabletHealth{
		Target: &querypb.Target{
			Cell:       "cell1",
			TabletType: topodatapb.TabletType_REPLICA,
		},
	}
	call2 := mockThrottler.EXPECT().RecordReplicationLag(gomock.Any(), tabletStats)
	call3 := mockThrottler.EXPECT().Throttle(0)
	call3.Return(1 * time.Second)
	call4 := mockThrottler.EXPECT().Throttle(0)
	call4.Return(0 * time.Second)

	calllast := mockThrottler.EXPECT().Close()

	call1.After(call0)
	call2.After(call1)
	call3.After(call2)
	call4.After(call3)
	calllast.After(call4)

	config := tabletenv.NewDefaultConfig()
	config.EnableTxThrottler = true
	config.TxThrottlerTabletTypes = &topoproto.TabletTypeListFlag{topodatapb.TabletType_REPLICA}
	config.TxThrottlerQueryPoolThresholds = &flagutil.LowHighFloat64Values{Low: 66.66, High: 80}
	config.TxThrottlerTxPoolThresholds = &flagutil.LowHighFloat64Values{Low: 66.66, High: 80}

	env := tabletenv.NewEnv(config, t.Name())
	mockQueryEngine := NewMockTabletserverEngine()
	mockTxEngine := NewMockTabletserverEngine()
	throttler := NewTxThrottler(env, ts, mockQueryEngine, mockTxEngine)
	throttlerImpl, _ := throttler.(*txThrottler)
	assert.NotNil(t, throttlerImpl)
	throttler.InitDBConfig(&querypb.Target{
		Cell:     "cell1",
		Keyspace: "keyspace",
		Shard:    "shard",
	})

	//TODO(timvaillancourt): move the test cases below to parallel t.Run(...) tests

	assert.Nil(t, throttlerImpl.Open())
	assert.Equal(t, int64(1), throttlerImpl.throttlerRunning.Get())
	assert.Equal(t, map[string]int64{"cell1": 1, "cell2": 1}, throttlerImpl.topoWatchers.Counts())

	// call1 - returns 0
	assert.Nil(t, throttlerImpl.Throttle(
		&planbuilder.Plan{PlanID: planbuilder.PlanBegin},
		&querypb.ExecuteOptions{Priority: "100"},
	))
	assert.Equal(t, map[string]int64{
		planbuilder.PlanBegin.String() + ".": 1,
	}, throttlerImpl.requestsTotal.Counts())
	assert.Len(t, throttlerImpl.requestsThrottled.Counts(), 0)

	// call2 - should record lag
	throttlerImpl.state.StatsUpdate(tabletStats) // This calls replication lag thing
	assert.Equal(t, map[string]int64{"cell1.REPLICA": 1}, throttlerImpl.healthChecksReadTotal.Counts())
	assert.Equal(t, map[string]int64{"cell1.REPLICA": 1}, throttlerImpl.healthChecksRecordedTotal.Counts())
	rdonlyTabletStats := &discovery.TabletHealth{
		Target: &querypb.Target{
			Cell:       "cell2",
			TabletType: topodatapb.TabletType_RDONLY,
		},
	}
	// This call should not be forwarded to the go/vt/throttlerImpl.Throttler object.
	throttlerImpl.state.StatsUpdate(rdonlyTabletStats)
	assert.Equal(t, map[string]int64{"cell1.REPLICA": 1, "cell2.RDONLY": 1}, throttlerImpl.healthChecksReadTotal.Counts())
	assert.Equal(t, map[string]int64{"cell1.REPLICA": 1}, throttlerImpl.healthChecksRecordedTotal.Counts())

	// call3 - returns 1 (throttle due to replication lag)
	// The second throttle call should reject.
	assert.ErrorIs(t, ErrThrottledReplicationLag, throttlerImpl.Throttle(
		&planbuilder.Plan{PlanID: planbuilder.PlanInsert},
		&querypb.ExecuteOptions{Priority: "100", WorkloadName: "some_workload"},
	))
	assert.Equal(t, map[string]int64{
		planbuilder.PlanBegin.String() + ".":               1,
		planbuilder.PlanInsert.String() + ".some_workload": 1,
	}, throttlerImpl.requestsTotal.Counts())
	assert.Equal(t, map[string]int64{
		planbuilder.PlanInsert.String() + "." + ErrThrottledReplicationLag.Error() + ".some_workload": 1,
	}, throttlerImpl.requestsThrottled.Counts())

	// This call should not throttle due to priority. Check that's the case and counters agree.
	assert.Nil(t, throttlerImpl.Throttle(
		&planbuilder.Plan{PlanID: planbuilder.PlanInsert},
		&querypb.ExecuteOptions{Priority: "0", WorkloadName: "some_workload"},
	))
	assert.Equal(t, map[string]int64{
		planbuilder.PlanBegin.String() + ".":               1,
		planbuilder.PlanInsert.String() + ".some_workload": 2,
	}, throttlerImpl.requestsTotal.Counts())
	assert.Equal(t, map[string]int64{
		planbuilder.PlanInsert.String() + "." + ErrThrottledReplicationLag.Error() + ".some_workload": 1,
	}, throttlerImpl.requestsThrottled.Counts())

	// Test select + query conn pool signal, which is below threshold. This call should not throttle.
	mockQueryEngine.setPoolUsagePercent(12.345)
	assert.Nil(t, throttlerImpl.Throttle(
		&planbuilder.Plan{PlanID: planbuilder.PlanSelect},
		&querypb.ExecuteOptions{Priority: "100", WorkloadName: "some_workload"},
	))
	assert.Equal(t, map[string]int64{
		planbuilder.PlanBegin.String() + ".":               1,
		planbuilder.PlanInsert.String() + ".some_workload": 2,
		planbuilder.PlanSelect.String() + ".some_workload": 1,
	}, throttlerImpl.requestsTotal.Counts())
	assert.Equal(t, map[string]int64{
		planbuilder.PlanInsert.String() + "." + ErrThrottledReplicationLag.Error() + ".some_workload": 1,
	}, throttlerImpl.requestsThrottled.Counts())

	// Test select + query conn pool signal, which is above the "soft" threshold. This call should throttle.
	mockQueryEngine.setPoolUsagePercent(75)
	assert.ErrorIs(t, ErrThrottledConnPoolUsageSoft, throttlerImpl.Throttle(
		&planbuilder.Plan{PlanID: planbuilder.PlanSelect},
		&querypb.ExecuteOptions{Priority: "100", WorkloadName: "some_workload"},
	))
	assert.Equal(t, map[string]int64{
		planbuilder.PlanBegin.String() + ".":               1,
		planbuilder.PlanInsert.String() + ".some_workload": 2,
		planbuilder.PlanSelect.String() + ".some_workload": 2,
	}, throttlerImpl.requestsTotal.Counts())
	assert.Equal(t, map[string]int64{
		planbuilder.PlanInsert.String() + "." + ErrThrottledReplicationLag.Error() + ".some_workload":    1,
		planbuilder.PlanSelect.String() + "." + ErrThrottledConnPoolUsageSoft.Error() + ".some_workload": 1,
	}, throttlerImpl.requestsThrottled.Counts())

	// Test select + query conn pool signal, which is above the "high" threshold. This call should throttle.
	mockQueryEngine.setPoolUsagePercent(99.999)
	assert.ErrorIs(t, ErrThrottledConnPoolUsageHard, throttlerImpl.Throttle(
		&planbuilder.Plan{PlanID: planbuilder.PlanSelect},
		&querypb.ExecuteOptions{Priority: "1", WorkloadName: "some_workload"},
	))
	assert.Equal(t, map[string]int64{
		planbuilder.PlanBegin.String() + ".":               1,
		planbuilder.PlanInsert.String() + ".some_workload": 2,
		planbuilder.PlanSelect.String() + ".some_workload": 3,
	}, throttlerImpl.requestsTotal.Counts())
	assert.Equal(t, map[string]int64{
		planbuilder.PlanInsert.String() + "." + ErrThrottledReplicationLag.Error() + ".some_workload":    1,
		planbuilder.PlanSelect.String() + "." + ErrThrottledConnPoolUsageSoft.Error() + ".some_workload": 1,
		planbuilder.PlanSelect.String() + "." + ErrThrottledConnPoolUsageHard.Error() + ".some_workload": 1,
	}, throttlerImpl.requestsThrottled.Counts())

	// call4 - returns 0 (no throttling)
	// Test insert + tx pool signal, which is below threshold. This call should not throttle.
	mockTxEngine.setPoolUsagePercent(12.345)
	assert.Nil(t, throttlerImpl.Throttle(
		&planbuilder.Plan{PlanID: planbuilder.PlanInsert},
		&querypb.ExecuteOptions{Priority: "100", WorkloadName: "some_workload"},
	))
	assert.Equal(t, map[string]int64{
		planbuilder.PlanBegin.String() + ".":               1,
		planbuilder.PlanInsert.String() + ".some_workload": 3,
		planbuilder.PlanSelect.String() + ".some_workload": 3,
	}, throttlerImpl.requestsTotal.Counts())
	assert.Equal(t, map[string]int64{
		planbuilder.PlanInsert.String() + "." + ErrThrottledReplicationLag.Error() + ".some_workload":    1,
		planbuilder.PlanSelect.String() + "." + ErrThrottledConnPoolUsageSoft.Error() + ".some_workload": 1,
		planbuilder.PlanSelect.String() + "." + ErrThrottledConnPoolUsageHard.Error() + ".some_workload": 1,
	}, throttlerImpl.requestsThrottled.Counts())

	// Test insert + tx pool signal, which is above the "soft" threshold. This call should throttle.
	mockTxEngine.setPoolUsagePercent(75)
	assert.ErrorIs(t, ErrThrottledTxPoolUsageSoft, throttlerImpl.Throttle(
		&planbuilder.Plan{PlanID: planbuilder.PlanInsert},
		&querypb.ExecuteOptions{Priority: "100", WorkloadName: "some_workload"},
	))
	assert.Equal(t, map[string]int64{
		planbuilder.PlanBegin.String() + ".":               1,
		planbuilder.PlanInsert.String() + ".some_workload": 4,
		planbuilder.PlanSelect.String() + ".some_workload": 3,
	}, throttlerImpl.requestsTotal.Counts())
	assert.Equal(t, map[string]int64{
		planbuilder.PlanInsert.String() + "." + ErrThrottledReplicationLag.Error() + ".some_workload":    1,
		planbuilder.PlanSelect.String() + "." + ErrThrottledConnPoolUsageSoft.Error() + ".some_workload": 1,
		planbuilder.PlanSelect.String() + "." + ErrThrottledConnPoolUsageHard.Error() + ".some_workload": 1,
		planbuilder.PlanInsert.String() + "." + ErrThrottledTxPoolUsageSoft.Error() + ".some_workload":   1,
	}, throttlerImpl.requestsThrottled.Counts())

	// Test insert + tx pool signal, which is above the "high" threshold. This call should throttle.
	mockTxEngine.setPoolUsagePercent(99.999)
	assert.ErrorIs(t, ErrThrottledTxPoolUsageHard, throttlerImpl.Throttle(
		&planbuilder.Plan{PlanID: planbuilder.PlanInsert},
		&querypb.ExecuteOptions{Priority: "1", WorkloadName: "some_workload"},
	))
	assert.Equal(t, map[string]int64{
		planbuilder.PlanBegin.String() + ".":               1,
		planbuilder.PlanInsert.String() + ".some_workload": 5,
		planbuilder.PlanSelect.String() + ".some_workload": 3,
	}, throttlerImpl.requestsTotal.Counts())
	assert.Equal(t, map[string]int64{
		planbuilder.PlanInsert.String() + "." + ErrThrottledReplicationLag.Error() + ".some_workload":    1,
		planbuilder.PlanSelect.String() + "." + ErrThrottledConnPoolUsageSoft.Error() + ".some_workload": 1,
		planbuilder.PlanSelect.String() + "." + ErrThrottledConnPoolUsageHard.Error() + ".some_workload": 1,
		planbuilder.PlanInsert.String() + "." + ErrThrottledTxPoolUsageSoft.Error() + ".some_workload":   1,
		planbuilder.PlanInsert.String() + "." + ErrThrottledTxPoolUsageHard.Error() + ".some_workload":   1,
	}, throttlerImpl.requestsThrottled.Counts())

	// Close throttler.
	throttlerImpl.Close()
	assert.Zero(t, throttlerImpl.throttlerRunning.Get())
	assert.Equal(t, map[string]int64{"cell1": 0, "cell2": 0}, throttlerImpl.topoWatchers.Counts())
}

func TestFetchKnownCells(t *testing.T) {
	{
		ts := memorytopo.NewServer("cell1", "cell2")
		cells := fetchKnownCells(context.Background(), ts, &querypb.Target{Cell: "cell1"})
		assert.Equal(t, []string{"cell1", "cell2"}, cells)
	}
	{
		ts, factory := memorytopo.NewServerAndFactory("shouldfail")
		factory.SetError(errors.New("mock topo error"))
		cells := fetchKnownCells(context.Background(), ts, &querypb.Target{Cell: "cell1"})
		assert.Equal(t, []string{"cell1"}, cells)
	}
}

func TestNewTxThrottler(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	env := tabletenv.NewEnv(config, t.Name())
	mockEngine := NewMockTabletserverEngine()

	{
		// disabled
		config.EnableTxThrottler = false
		throttler := NewTxThrottler(env, nil, mockEngine, mockEngine)
		throttlerImpl, _ := throttler.(*txThrottler)
		assert.NotNil(t, throttlerImpl)
		assert.NotNil(t, throttlerImpl.config)
		assert.False(t, throttlerImpl.config.enabled)
	}
	{
		// enabled
		config.EnableTxThrottler = true
		config.TxThrottlerHealthCheckCells = []string{"cell1", "cell2"}
		config.TxThrottlerTabletTypes = &topoproto.TabletTypeListFlag{topodatapb.TabletType_REPLICA}
		throttler := NewTxThrottler(env, nil, mockEngine, mockEngine)
		throttlerImpl, _ := throttler.(*txThrottler)
		assert.NotNil(t, throttlerImpl)
		assert.NotNil(t, throttlerImpl.config)
		assert.True(t, throttlerImpl.config.enabled)
		assert.Equal(t, []string{"cell1", "cell2"}, throttlerImpl.config.healthCheckCells)
	}
}

func TestDryRunThrottler(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	env := tabletenv.NewEnv(config, t.Name())

	testCases := []struct {
		Name                        string
		txThrottlerStateThrottleErr error
		throttlerDryRun             bool
		expectedResult              error
	}{
		{Name: "Real run throttles when txThrottlerStateImpl says it should", txThrottlerStateThrottleErr: ErrThrottledReplicationLag, throttlerDryRun: false, expectedResult: ErrThrottledReplicationLag},
		{Name: "Real run does not throttle when txThrottlerStateImpl says it should not", txThrottlerStateThrottleErr: nil, throttlerDryRun: false, expectedResult: nil},
		{Name: "Dry run does not throttle when txThrottlerStateImpl says it should", txThrottlerStateThrottleErr: ErrThrottledReplicationLag, throttlerDryRun: true, expectedResult: nil},
		{Name: "Dry run does not throttle when txThrottlerStateImpl says it should not", txThrottlerStateThrottleErr: nil, throttlerDryRun: true, expectedResult: nil},
	}

	for _, aTestCase := range testCases {
		theTestCase := aTestCase

		t.Run(theTestCase.Name, func(t *testing.T) {
			aTxThrottler := &txThrottler{
				config: &txThrottlerConfig{
					enabled: true,
					dryRun:  theTestCase.throttlerDryRun,
				},
				state:            &mockTxThrottlerState{throttleErr: theTestCase.txThrottlerStateThrottleErr},
				throttlerRunning: env.Exporter().NewGauge("TransactionThrottlerRunning", "transaction throttler running state"),
				requestsTotal: env.Exporter().NewCountersWithMultiLabels("TransactionThrottlerRequests", "transaction throttler requests",
					[]string{"plan", "workload"}),
				requestsThrottled: env.Exporter().NewCountersWithMultiLabels("TransactionThrottlerThrottled", "transaction throttler requests throttled",
					[]string{"plan", "cause", "workload"}),
			}
			assert.ErrorIs(t, theTestCase.expectedResult, aTxThrottler.Throttle(
				&planbuilder.Plan{PlanID: planbuilder.PlanInsert},
				&querypb.ExecuteOptions{Priority: "100", WorkloadName: "some_workload"},
			))
		})
	}
}

type mockTxThrottlerState struct {
	throttleErr error
}

func (t *mockTxThrottlerState) deallocateResources() {

}
func (t *mockTxThrottlerState) StatsUpdate(tabletStats *discovery.TabletHealth) {

}

func (t *mockTxThrottlerState) throttle(plan *planbuilder.Plan) error {
	return t.throttleErr
}
