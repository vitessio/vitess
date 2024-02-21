/*
Copyright 2019 The Vitess Authors.

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

package txthrottler

// Commands to generate the mocks for this test.
//go:generate mockgen -destination mock_healthcheck_test.go -package txthrottler -mock_names "HealthCheck=MockHealthCheck" vitess.io/vitess/go/vt/discovery HealthCheck
//go:generate mockgen -destination mock_throttler_test.go -package txthrottler vitess.io/vitess/go/vt/vttablet/tabletserver/txthrottler ThrottlerInterface

import (
	"context"
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
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestDisabledThrottler(t *testing.T) {
	cfg := tabletenv.NewDefaultConfig()
	cfg.EnableTxThrottler = false
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, t.Name())
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	defer resetTxThrottlerFactories()
	ts := memorytopo.NewServer(ctx, "cell1", "cell2")

	mockHealthCheck := NewMockHealthCheck(mockCtrl)
	hcCall1 := mockHealthCheck.EXPECT().Subscribe()
	hcCall1.Do(func() {})
	hcCall2 := mockHealthCheck.EXPECT().Close()
	hcCall2.After(hcCall1)
	healthCheckFactory = func(topoServer *topo.Server, cell string, cellsToWatch []string) discovery.HealthCheck {
		return mockHealthCheck
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
			Cell:       "cell1",
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

	cfg := tabletenv.NewDefaultConfig()
	cfg.EnableTxThrottler = true
	cfg.TxThrottlerTabletTypes = &topoproto.TabletTypeListFlag{topodatapb.TabletType_REPLICA}

	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, t.Name())
	throttler := NewTxThrottler(env, ts)
	throttlerImpl, _ := throttler.(*txThrottler)
	assert.NotNil(t, throttlerImpl)
	throttler.InitDBConfig(&querypb.Target{
		Cell:     "cell1",
		Keyspace: "keyspace",
		Shard:    "shard",
	})

	assert.Nil(t, throttlerImpl.Open())
	throttlerStateImpl, ok := throttlerImpl.state.(*txThrottlerStateImpl)
	assert.True(t, ok)
	assert.Equal(t, map[topodatapb.TabletType]bool{topodatapb.TabletType_REPLICA: true}, throttlerStateImpl.tabletTypes)
	assert.Equal(t, int64(1), throttlerImpl.throttlerRunning.Get())

	// Stop the go routine that keeps updating the cached  shard's max lag to prevent it from changing the value in a
	// way that will interfere with how we manipulate that value in our tests to evaluate different cases:
	throttlerStateImpl.done <- true

	// 1 should not throttle due to return value of underlying Throttle(), despite high lag
	atomic.StoreInt64(&throttlerStateImpl.maxLag, 20)
	assert.False(t, throttlerImpl.Throttle(100, "some-workload"))
	assert.Equal(t, int64(1), throttlerImpl.requestsTotal.Counts()["some-workload"])
	assert.Zero(t, throttlerImpl.requestsThrottled.Counts()["some-workload"])

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

	// 2 should throttle due to return value of underlying Throttle(), high lag & priority = 100
	assert.True(t, throttlerImpl.Throttle(100, "some-workload"))
	assert.Equal(t, int64(2), throttlerImpl.requestsTotal.Counts()["some-workload"])
	assert.Equal(t, int64(1), throttlerImpl.requestsThrottled.Counts()["some-workload"])

	// 3 should not throttle despite return value of underlying Throttle() and high lag, due to priority = 0
	assert.False(t, throttlerImpl.Throttle(0, "some-workload"))
	assert.Equal(t, int64(3), throttlerImpl.requestsTotal.Counts()["some-workload"])
	assert.Equal(t, int64(1), throttlerImpl.requestsThrottled.Counts()["some-workload"])

	// 4 should not throttle despite return value of underlying Throttle() and priority = 100, due to low lag
	atomic.StoreInt64(&throttlerStateImpl.maxLag, 1)
	assert.False(t, throttler.Throttle(100, "some-workload"))
	assert.Equal(t, int64(4), throttlerImpl.requestsTotal.Counts()["some-workload"])
	assert.Equal(t, int64(1), throttlerImpl.requestsThrottled.Counts()["some-workload"])

	throttler.Close()
	assert.Zero(t, throttlerImpl.throttlerRunning.Get())
}

func TestFetchKnownCells(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	{
		ts := memorytopo.NewServer(ctx, "cell1", "cell2")
		cells := fetchKnownCells(context.Background(), ts, &querypb.Target{Cell: "cell1"})
		assert.Equal(t, []string{"cell1", "cell2"}, cells)
	}
	{
		ts := memorytopo.NewServer(ctx)
		cells := fetchKnownCells(context.Background(), ts, &querypb.Target{Cell: "cell1"})
		assert.Equal(t, []string{"cell1"}, cells)
	}
}

func TestDryRunThrottler(t *testing.T) {
	cfg := tabletenv.NewDefaultConfig()
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, t.Name())

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
				config: &tabletenv.TabletConfig{
					EnableTxThrottler: true,
					TxThrottlerDryRun: theTestCase.throttlerDryRun,
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
func (t *mockTxThrottlerState) StatsUpdate(tabletStats *discovery.TabletHealth) {

}

func (t *mockTxThrottlerState) throttle() bool {
	return t.shouldThrottle
}
