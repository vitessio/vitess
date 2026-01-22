package tabletthrottler

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/srvtopo/srvtopotest"
	"vitess.io/vitess/go/vt/topo"

	querypb "vitess.io/vitess/go/vt/proto/query"
	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	"vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/querythrottler/registry"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

// createTestTabletConfig creates a minimal tablet config for testing
func createTestTabletConfig() *tabletenv.TabletConfig {
	return &tabletenv.TabletConfig{
		TabletThrottlerCacheUpdateInterval: 10 * time.Millisecond,
	}
}

// createTestEnv creates a minimal env for testing
func createTestEnv() tabletenv.Env {
	return tabletenv.NewEnv(vtenv.NewTestEnv(), createTestTabletConfig(), "TestTabletThrottlerStrategy")
}

// toQueryAttributesForTest converts ExecuteOptions to QueryAttributes for testing.
func toQueryAttributesForTest(options *querypb.ExecuteOptions) registry.QueryAttributes {
	if options == nil {
		return registry.QueryAttributes{WorkloadName: "unknown", Priority: 100}
	}

	workloadName := "default"
	if options.WorkloadName != "" {
		workloadName = options.WorkloadName
	}

	priority := 100
	if options.Priority != "" {
		if p, err := strconv.Atoi(options.Priority); err == nil && p >= 0 && p <= 100 {
			priority = p
		}
	}

	return registry.QueryAttributes{WorkloadName: workloadName, Priority: priority}
}

// makeThresholds creates a slice of ThrottleThreshold pointers from above/throttle pairs.
func makeThresholds(pairs ...float64) []*querythrottlerpb.ThrottleThreshold {
	if len(pairs)%2 != 0 {
		panic("makeThresholds requires pairs of (above, throttle)")
	}
	thresholds := make([]*querythrottlerpb.ThrottleThreshold, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		thresholds = append(thresholds, &querythrottlerpb.ThrottleThreshold{
			Above:    pairs[i],
			Throttle: int32(pairs[i+1]),
		})
	}
	return thresholds
}

// makeTabletStrategyConfig creates a TabletStrategyConfig for testing.
// tabletType -> stmtType -> metricName -> thresholds
func makeTabletStrategyConfig(tabletType, stmtType, metricName string, thresholds []*querythrottlerpb.ThrottleThreshold) *querythrottlerpb.TabletStrategyConfig {
	return &querythrottlerpb.TabletStrategyConfig{
		TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
			tabletType: {
				StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
					stmtType: {
						MetricRules: map[string]*querythrottlerpb.MetricRule{
							metricName: {
								Thresholds: thresholds,
							},
						},
					},
				},
			},
		},
	}
}

// testRandomFuncs holds injectable random functions for testing.
type testRandomFuncs struct {
	randFloat64 func() float64
	randIntN    func(n int) int
}

// newTestStrategyWithRandom creates a TabletThrottlerStrategy with injected random functions for deterministic testing.
func newTestStrategyWithRandom(client ThrottleClientWrapper, cfg *querythrottlerpb.TabletStrategyConfig, randFuncs testRandomFuncs) *TabletThrottlerStrategy {
	strategy := NewTabletThrottlerStrategy(client, cfg, createTestTabletConfig(), createTestEnv(), "test-keyspace", "test-cell", nil)
	if randFuncs.randFloat64 != nil {
		strategy.randFloat64 = randFuncs.randFloat64
	}
	if randFuncs.randIntN != nil {
		strategy.randIntN = randFuncs.randIntN
	}
	return strategy
}

// createTestSrvKeyspaceWithConfig creates a test SrvKeyspace with the given TabletStrategyConfig.
func createTestSrvKeyspaceWithConfig(cfg *querythrottlerpb.TabletStrategyConfig) *topodatapb.SrvKeyspace {
	return &topodatapb.SrvKeyspace{
		QueryThrottlerConfig: &querythrottlerpb.Config{
			Enabled:              true,
			Strategy:             querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
			DryRun:               false,
			TabletStrategyConfig: cfg,
		},
	}
}

// TestTabletThrottlerStrategy_Evaluate_NilParsedQuery tests that Evaluate() handles nil parsedQuery correctly.
func TestTabletThrottlerStrategy_Evaluate_NilParsedQuery(t *testing.T) {
	mockClient := NewFakeThrottleClientWrapper(&throttle.CheckResult{}, false)

	cfg := makeTabletStrategyConfig(
		topodatapb.TabletType_PRIMARY.String(),
		"SELECT",
		"lag",
		makeThresholds(10, 100),
	)

	env := createTestEnv()
	strategy := NewTabletThrottlerStrategy(mockClient, cfg, createTestTabletConfig(), env, "test_keyspace", "test_cell", nil)

	decision := strategy.Evaluate(
		context.Background(),
		topodatapb.TabletType_PRIMARY,
		nil,
		12345,
		registry.QueryAttributes{
			WorkloadName: "test-workload",
			Priority:     50,
		},
	)

	require.False(t, decision.Throttle, "Expected Throttle to be false when parsedQuery is nil")
	require.Equal(t, "No query to throttle", decision.Message)
	require.Empty(t, decision.MetricName)
	require.Equal(t, 0.0, decision.MetricValue)
	require.Equal(t, 0.0, decision.Threshold)
	require.Equal(t, 0.0, decision.ThrottlePercentage)
}

// TestTabletThrottlerStrategy_ThrottleIfNeeded_Legacy tests the legacy ThrottleIfNeeded-style behavior via Evaluate.
func TestTabletThrottlerStrategy_ThrottleIfNeeded_Legacy(t *testing.T) {
	tests := []struct {
		name                  string
		giveCheckResult       *throttle.CheckResult
		giveThrottleCheckOK   bool
		giveTabletType        topodatapb.TabletType
		giveSQL               string
		giveTxnID             int64
		giveOptions           *querypb.ExecuteOptions
		giveCfg               *querythrottlerpb.TabletStrategyConfig
		giveRandValue         float64
		givePriorityRandValue int
		wantErr               string
	}{
		{
			name: "No-op if tablet type in config is not the current tablet type",
			giveCfg: makeTabletStrategyConfig(
				topodatapb.TabletType_PRIMARY.String(),
				"SELECT",
				"lag",
				makeThresholds(10, 10, 25, 25, 50, 50),
			),
			giveSQL:        "SELECT * from A where X=1",
			giveTxnID:      1,
			giveTabletType: topodatapb.TabletType_REPLICA,
		},
		{
			name:           "No-op if tablet type in config is the current tablet type but throttle check is ok",
			giveTabletType: topodatapb.TabletType_PRIMARY,
			giveCfg: makeTabletStrategyConfig(
				topodatapb.TabletType_PRIMARY.String(),
				"SELECT",
				"lag",
				makeThresholds(10, 10, 25, 25, 50, 50),
			),
			giveSQL:             "SELECT * from A where X=1",
			giveTxnID:           1,
			giveCheckResult:     &throttle.CheckResult{},
			giveThrottleCheckOK: true,
		},
		{
			name:           "throttle if tablet type in config is the current tablet type but throttle check is not ok",
			giveTabletType: topodatapb.TabletType_PRIMARY,
			giveCfg: makeTabletStrategyConfig(
				topodatapb.TabletType_PRIMARY.String(),
				"SELECT",
				"lag",
				makeThresholds(10, 10, 25, 25, 50, 50),
			),
			giveSQL:   "SELECT * from A where X=1",
			giveTxnID: 1,
			giveCheckResult: &throttle.CheckResult{
				Metrics: map[string]*throttle.MetricResult{
					"lag": {
						ResponseCode: tabletmanagerdata.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED,
						Value:        20,
					},
				},
			},
			giveThrottleCheckOK:   false,
			giveRandValue:         0.09,
			givePriorityRandValue: 99,
			wantErr:               "[VTTabletThrottler] Query=\"SELECT * from A where X=1\" throttled: workload=unknown priority=100 metric=lag value=20.00 breached threshold=10.00 throttle=10%",
		},
		{
			name:           "priority 0 query is NEVER throttled (edge case)",
			giveTabletType: topodatapb.TabletType_PRIMARY,
			giveOptions: &querypb.ExecuteOptions{
				WorkloadName: "critical-system-query",
				Priority:     "0",
			},
			giveCfg: makeTabletStrategyConfig(
				topodatapb.TabletType_PRIMARY.String(),
				"SELECT",
				"lag",
				makeThresholds(1, 100),
			),
			giveSQL:   "SELECT * from critical_table",
			giveTxnID: 1,
			giveCheckResult: &throttle.CheckResult{
				Metrics: map[string]*throttle.MetricResult{
					"lag": {
						ResponseCode: tabletmanagerdata.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED,
						Value:        1000,
					},
				},
			},
			giveThrottleCheckOK:   false,
			giveRandValue:         0.01,
			givePriorityRandValue: 99,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ftcw := NewFakeThrottleClientWrapper(tt.giveCheckResult, tt.giveThrottleCheckOK)
			tts := newTestStrategyWithRandom(ftcw, tt.giveCfg, testRandomFuncs{
				randFloat64: func() float64 { return tt.giveRandValue },
				randIntN:    func(n int) int { return tt.givePriorityRandValue },
			})

			decision := tts.Evaluate(context.Background(), tt.giveTabletType, &sqlparser.ParsedQuery{Query: tt.giveSQL}, tt.giveTxnID, toQueryAttributesForTest(tt.giveOptions))
			if tt.wantErr != "" {
				require.True(t, decision.Throttle, "Expected throttling decision")
				require.Equal(t, tt.wantErr, decision.Message, "Throttle message should match expected error")
				return
			}

			require.False(t, decision.Throttle, "Expected no throttling")
		})
	}
}

// TestTabletThrottlerStrategy_CachingLifecycle tests the Start/Stop lifecycle methods.
func TestTabletThrottlerStrategy_CachingLifecycle(t *testing.T) {
	ftcw := NewFakeThrottleClientWrapper(&throttle.CheckResult{}, true)
	cfg := &querythrottlerpb.TabletStrategyConfig{}
	strategy := NewTabletThrottlerStrategy(ftcw, cfg, createTestTabletConfig(), createTestEnv(), "test-keyspace", "test-cell", nil)

	require.False(t, strategy.running.Load())

	strategy.Start()
	require.True(t, strategy.running.Load())

	strategy.Start()
	require.True(t, strategy.running.Load())

	strategy.Stop()
	require.False(t, strategy.running.Load())

	strategy.Stop()
	require.False(t, strategy.running.Load())
}

// TestTabletThrottlerStrategy_CachingBehavior tests that caching reduces calls to ThrottleCheckOK.
func TestTabletThrottlerStrategy_CachingBehavior(t *testing.T) {
	checkResult := &throttle.CheckResult{
		Metrics: map[string]*throttle.MetricResult{
			"lag": {
				ResponseCode: tabletmanagerdata.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED,
				Value:        20,
			},
		},
	}

	ftcw := NewFakeThrottleClientWrapper(checkResult, false)

	cfg := makeTabletStrategyConfig(
		topodatapb.TabletType_PRIMARY.String(),
		"SELECT",
		"lag",
		makeThresholds(10, 100),
	)

	strategy := newTestStrategyWithRandom(ftcw, cfg, testRandomFuncs{
		randFloat64: func() float64 { return 0.5 },
		randIntN:    func(n int) int { return 99 },
	})

	options := &querypb.ExecuteOptions{Priority: "100"}

	_ = strategy.Evaluate(context.Background(), topodatapb.TabletType_PRIMARY, &sqlparser.ParsedQuery{Query: "SELECT * FROM table"}, 1, toQueryAttributesForTest(options))
	require.Equal(t, 1, ftcw.GetCallCount(), "Expected 1 call without cache")

	_ = strategy.Evaluate(context.Background(), topodatapb.TabletType_PRIMARY, &sqlparser.ParsedQuery{Query: "SELECT * FROM table"}, 1, toQueryAttributesForTest(options))
	require.Equal(t, 2, ftcw.GetCallCount(), "Expected 2 calls without cache")

	ftcw.ResetCallCount()
	strategy.Start()
	defer strategy.Stop()

	require.Eventually(t, func() bool {
		return ftcw.GetCallCount() >= 1 && strategy.running.Load()
	}, 1*time.Second, 10*time.Millisecond, "Cache should be primed with at least one background call")

	ftcw.ResetCallCount()

	_ = strategy.Evaluate(context.Background(), topodatapb.TabletType_PRIMARY, &sqlparser.ParsedQuery{Query: "SELECT * FROM table"}, 1, toQueryAttributesForTest(options))
	_ = strategy.Evaluate(context.Background(), topodatapb.TabletType_PRIMARY, &sqlparser.ParsedQuery{Query: "SELECT * FROM table"}, 1, toQueryAttributesForTest(options))
	_ = strategy.Evaluate(context.Background(), topodatapb.TabletType_PRIMARY, &sqlparser.ParsedQuery{Query: "SELECT * FROM table"}, 1, toQueryAttributesForTest(options))

	callCount := ftcw.GetCallCount()
	require.LessOrEqual(t, callCount, 2, "Cache should significantly reduce calls to ThrottleCheckOK")
}

// TestTabletThrottlerStrategy_BinarySearchThrottleDecision tests the binary search implementation
func TestTabletThrottlerStrategy_BinarySearchThrottleDecision(t *testing.T) {
	tests := []struct {
		name             string
		metricValue      float64
		thresholds       []*querythrottlerpb.ThrottleThreshold
		expectedThrottle float64
		expectedBreached float64
	}{
		{
			name:             "empty_thresholds",
			metricValue:      50.0,
			thresholds:       []*querythrottlerpb.ThrottleThreshold{},
			expectedThrottle: 0,
			expectedBreached: 0,
		},
		{
			name:             "single_threshold_not_breached",
			metricValue:      5.0,
			thresholds:       makeThresholds(10, 50),
			expectedThrottle: 0,
			expectedBreached: 0,
		},
		{
			name:             "single_threshold_breached",
			metricValue:      15.0,
			thresholds:       makeThresholds(10, 50),
			expectedThrottle: 0.5,
			expectedBreached: 10,
		},
		{
			name:             "two_thresholds_first_breached",
			metricValue:      15.0,
			thresholds:       makeThresholds(10, 25, 20, 75),
			expectedThrottle: 0.25,
			expectedBreached: 10,
		},
		{
			name:             "two_thresholds_both_breached",
			metricValue:      25.0,
			thresholds:       makeThresholds(10, 25, 20, 75),
			expectedThrottle: 0.75,
			expectedBreached: 20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			throttlePercent, breachedThreshold := GetThrottleDecision(tt.metricValue, tt.thresholds)
			require.Equal(t, tt.expectedThrottle, throttlePercent, "throttle percent mismatch")
			require.Equal(t, tt.expectedBreached, breachedThreshold, "breached threshold mismatch")
		})
	}
}

// TestTabletThrottlerStrategy_HandleConfigUpdate_ErrorHandling tests error handling in HandleConfigUpdate.
func TestTabletThrottlerStrategy_HandleConfigUpdate_ErrorHandling(t *testing.T) {
	tests := []struct {
		name           string
		inputErr       error
		expectedResult bool
	}{
		{
			name:           "ContextCanceledError",
			inputErr:       context.Canceled,
			expectedResult: false,
		},
		{
			name:           "NoNodeError",
			inputErr:       topo.NewError(topo.NoNode, "keyspace/test_keyspace"),
			expectedResult: false,
		},
		{
			name:           "InterruptedError",
			inputErr:       topo.NewError(topo.Interrupted, "interrupted"),
			expectedResult: false,
		},
		{
			name:           "TransientNetworkError",
			inputErr:       errors.New("temporary network error"),
			expectedResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := NewTabletThrottlerStrategy(nil, nil, createTestTabletConfig(), createTestEnv(), "test-keyspace", "test-cell", nil)

			srvks := &topodatapb.SrvKeyspace{}
			result := strategy.HandleConfigUpdate(srvks, tt.inputErr)

			require.Equal(t, tt.expectedResult, result)
		})
	}
}

// TestTabletThrottlerStrategy_HandleConfigUpdate_SuccessfulUpdate verifies that config updates are applied correctly.
func TestTabletThrottlerStrategy_HandleConfigUpdate_SuccessfulUpdate(t *testing.T) {
	ftcw := NewFakeThrottleClientWrapper(&throttle.CheckResult{}, true)

	initialCfg := makeTabletStrategyConfig("PRIMARY", "INSERT", "lag", makeThresholds(10, 50))
	strategy := NewTabletThrottlerStrategy(ftcw, initialCfg, createTestTabletConfig(), createTestEnv(), "test-keyspace", "test-cell", nil)

	newCfg := makeTabletStrategyConfig("PRIMARY", "SELECT", "lag", makeThresholds(20, 75))
	newSrvks := createTestSrvKeyspaceWithConfig(newCfg)

	result := strategy.HandleConfigUpdate(newSrvks, nil)
	require.True(t, result, "callback should return true")

	actualCfg := strategy.config.Load()
	require.NotNil(t, actualCfg)
	require.NotNil(t, actualCfg.TabletRules["PRIMARY"])
}

// TestTabletThrottlerStrategy_StartSrvKeyspaceWatch tests the startSrvKeyspaceWatch method
func TestTabletThrottlerStrategy_StartSrvKeyspaceWatch(t *testing.T) {
	tests := []struct {
		name               string
		srvTopoServer      srvtopo.Server
		keyspace           string
		cell               string
		expectWatchStarted bool
	}{
		{
			name:               "NilServer_EmptyKeyspace_EmptyCell",
			srvTopoServer:      nil,
			keyspace:           "",
			cell:               "",
			expectWatchStarted: false,
		},
		{
			name:               "ValidServer_ValidKeyspace_ValidCell",
			srvTopoServer:      srvtopotest.NewPassthroughSrvTopoServer(),
			keyspace:           "test_keyspace",
			cell:               "test_cell",
			expectWatchStarted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ftcw := NewFakeThrottleClientWrapper(&throttle.CheckResult{}, true)
			strategy := NewTabletThrottlerStrategy(ftcw, nil, createTestTabletConfig(), createTestEnv(), tt.keyspace, tt.cell, tt.srvTopoServer)

			if tt.expectWatchStarted {
				require.Eventually(t, func() bool {
					return strategy.watchStarted.Load()
				}, 1*time.Second, 10*time.Millisecond)
				return
			}

			require.Never(t, func() bool {
				return strategy.watchStarted.Load()
			}, 100*time.Millisecond, 10*time.Millisecond)
		})
	}
}

// slowThrottleClientWrapper simulates a slow/timeout-prone client
type slowThrottleClientWrapper struct {
	mu              sync.RWMutex
	checkResult     *throttle.CheckResult
	throttleCheckOK bool
	callCount       atomic.Int64
	delay           time.Duration
}

func (f *slowThrottleClientWrapper) ThrottleCheckOK(ctx context.Context, overrideAppName throttlerapp.Name) (*throttle.CheckResult, bool) {
	f.callCount.Add(1)
	select {
	case <-ctx.Done():
		return nil, false
	case <-time.After(f.delay):
		f.mu.RLock()
		defer f.mu.RUnlock()
		return f.checkResult, f.throttleCheckOK
	}
}

func (f *slowThrottleClientWrapper) GetCallCount() int {
	return int(f.callCount.Load())
}

// TestTabletThrottlerStrategy_CacheTimeoutScenarios tests various timeout scenarios.
func TestTabletThrottlerStrategy_CacheTimeoutScenarios(t *testing.T) {
	timeoutClient := &slowThrottleClientWrapper{
		checkResult: &throttle.CheckResult{
			Metrics: map[string]*throttle.MetricResult{
				"lag": {ResponseCode: tabletmanagerdata.CheckThrottlerResponseCode_OK, Value: 5},
			},
		},
		throttleCheckOK: true,
		delay:           100 * time.Millisecond,
	}

	strategy := NewTabletThrottlerStrategy(timeoutClient, &querythrottlerpb.TabletStrategyConfig{}, createTestTabletConfig(), createTestEnv(), "test-keyspace", "test-cell", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	result, ok := strategy.getCachedThrottleResult(ctx)

	require.False(t, ok)
	require.Nil(t, result)
}

// TestFakeThrottleClientWrapper_ThreadSafety tests that the FakeThrottleClientWrapper is thread-safe
func TestFakeThrottleClientWrapper_ThreadSafety(t *testing.T) {
	initialResult := &throttle.CheckResult{
		Metrics: map[string]*throttle.MetricResult{
			"lag": {ResponseCode: tabletmanagerdata.CheckThrottlerResponseCode_OK, Value: 5},
		},
	}

	client := NewFakeThrottleClientWrapper(initialResult, true)

	const numGoroutines = 10
	const numOperations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				if j%2 == 0 {
					_, _ = client.ThrottleCheckOK(context.Background(), throttlerapp.QueryThrottlerName)
					_, _ = client.GetCheckResult()
					_ = client.GetCallCount()
				} else {
					newResult := &throttle.CheckResult{
						Metrics: map[string]*throttle.MetricResult{
							"lag": {ResponseCode: tabletmanagerdata.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED, Value: 25},
						},
					}
					client.SetCheckResult(newResult, j%2 == 1)
				}
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond, "All goroutines should complete within timeout")

	finalResult, finalOk := client.GetCheckResult()
	require.NotNil(t, finalResult)
	require.NotNil(t, finalResult.Metrics)

	checkResult, checkOk := client.ThrottleCheckOK(context.Background(), throttlerapp.QueryThrottlerName)
	require.NotNil(t, checkResult)
	require.Equal(t, finalOk, checkOk)
}
