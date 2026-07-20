/*
Copyright 2026 The Vitess Authors.

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

package tabletthrottler

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"

	querypb "vitess.io/vitess/go/vt/proto/query"
	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	"vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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

// toQueryAttributesForTest converts ExecuteOptions to QueryAttributes for testing.
func toQueryAttributesForTest(options *querypb.ExecuteOptions) registry.QueryAttributes {
	if options == nil {
		return registry.QueryAttributes{WorkloadName: "unknown", Priority: 100}
	}

	workloadName := "unknown"
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
func newTestStrategyWithRandom(client ThrottlerClientWrapper, cfg *querythrottlerpb.TabletStrategyConfig, randFuncs testRandomFuncs) *TabletThrottlerStrategy {
	strategy := NewTabletThrottlerStrategy(client, cfg, createTestTabletConfig())
	if randFuncs.randFloat64 != nil {
		strategy.randFloat64 = randFuncs.randFloat64
	}
	if randFuncs.randIntN != nil {
		strategy.randIntN = randFuncs.randIntN
	}
	return strategy
}

// createTestQueryThrottlerConfig creates a Config (the type accepted by UpdateConfig)
// wrapping the given TabletStrategyConfig.
func createTestQueryThrottlerConfig(cfg *querythrottlerpb.TabletStrategyConfig) *querythrottlerpb.Config {
	return &querythrottlerpb.Config{
		Enabled:              true,
		Strategy:             querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		DryRun:               false,
		TabletStrategyConfig: cfg,
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

	tabletCfg := createTestTabletConfig()
	strategy := NewTabletThrottlerStrategy(mockClient, cfg, tabletCfg)

	decision := strategy.Evaluate(
		context.Background(),
		topodatapb.TabletType_PRIMARY,
		nil,
		sqlparser.StmtSelect,
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
		giveStmtType          sqlparser.StatementType
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
			giveStmtType:   sqlparser.StmtSelect,
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
			giveStmtType:        sqlparser.StmtSelect,
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
			giveSQL:      "SELECT * from A where X=1",
			giveStmtType: sqlparser.StmtSelect,
			giveTxnID:    1,
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
			wantErr:               "[VTTabletThrottler] query throttled: stmtType=SELECT workload=unknown priority=100 metric=lag value=20.00 breached threshold=10.00 throttle=10%",
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
			giveSQL:      "SELECT * from critical_table",
			giveStmtType: sqlparser.StmtSelect,
			giveTxnID:    1,
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
		{
			// Regression: CTE SELECT (WITH ... SELECT) must match the SELECT rule.
			// sqlparser.Preview classifies "WITH ..." as UNKNOWN, which would fail open;
			// the caller now supplies the AST-resolved type (sqlparser.StmtSelect).
			name:           "CTE SELECT matches SELECT rule and is throttled",
			giveTabletType: topodatapb.TabletType_PRIMARY,
			giveCfg: makeTabletStrategyConfig(
				topodatapb.TabletType_PRIMARY.String(),
				"SELECT",
				"lag",
				makeThresholds(10, 100),
			),
			giveSQL:      "WITH cte AS (SELECT id FROM A) SELECT * FROM cte",
			giveStmtType: sqlparser.StmtSelect,
			giveTxnID:    1,
			giveCheckResult: &throttle.CheckResult{
				Metrics: map[string]*throttle.MetricResult{
					"lag": {
						ResponseCode: tabletmanagerdata.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED,
						Value:        20,
					},
				},
			},
			giveThrottleCheckOK:   false,
			giveRandValue:         0.5,
			givePriorityRandValue: 99,
			wantErr:               "[VTTabletThrottler] query throttled: stmtType=SELECT workload=unknown priority=100 metric=lag value=20.00 breached threshold=10.00 throttle=100%",
		},
		{
			// Regression: CTE DML (WITH ... DELETE) must match the DELETE rule.
			name:           "CTE DELETE matches DELETE rule and is throttled",
			giveTabletType: topodatapb.TabletType_PRIMARY,
			giveCfg: makeTabletStrategyConfig(
				topodatapb.TabletType_PRIMARY.String(),
				"DELETE",
				"lag",
				makeThresholds(10, 100),
			),
			giveSQL:      "WITH cte AS (SELECT id FROM A) DELETE FROM A WHERE id IN (SELECT id FROM cte)",
			giveStmtType: sqlparser.StmtDelete,
			giveTxnID:    1,
			giveCheckResult: &throttle.CheckResult{
				Metrics: map[string]*throttle.MetricResult{
					"lag": {
						ResponseCode: tabletmanagerdata.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED,
						Value:        20,
					},
				},
			},
			giveThrottleCheckOK:   false,
			giveRandValue:         0.5,
			givePriorityRandValue: 99,
			wantErr:               "[VTTabletThrottler] query throttled: stmtType=DELETE workload=unknown priority=100 metric=lag value=20.00 breached threshold=10.00 throttle=100%",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ftcw := NewFakeThrottleClientWrapper(tt.giveCheckResult, tt.giveThrottleCheckOK)
			tts := newTestStrategyWithRandom(ftcw, tt.giveCfg, testRandomFuncs{
				randFloat64: func() float64 { return tt.giveRandValue },
				randIntN:    func(n int) int { return tt.givePriorityRandValue },
			})
			// Start() now primes the cache asynchronously in the updater goroutine.
			// Prime synchronously here so Evaluate() deterministically observes the
			// configured throttle result instead of racing the background refresh
			// (or failing open while the cache is still cold).
			tts.Start()
			defer tts.Stop()
			tts.refreshCache()

			decision := tts.Evaluate(context.Background(), tt.giveTabletType, &sqlparser.ParsedQuery{Query: tt.giveSQL}, tt.giveStmtType, tt.giveTxnID, toQueryAttributesForTest(tt.giveOptions))
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
	strategy := NewTabletThrottlerStrategy(ftcw, cfg, createTestTabletConfig())

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

	// Before Start(): the strategy is not running, so Evaluate() must not make
	// synchronous ThrottleCheckOK calls. getCachedThrottleResult fails open
	// (returns checkOk=true) to keep the hot path bounded in latency.
	_ = strategy.Evaluate(context.Background(), topodatapb.TabletType_PRIMARY, &sqlparser.ParsedQuery{Query: "SELECT * FROM table"}, sqlparser.StmtSelect, 1, toQueryAttributesForTest(options))
	_ = strategy.Evaluate(context.Background(), topodatapb.TabletType_PRIMARY, &sqlparser.ParsedQuery{Query: "SELECT * FROM table"}, sqlparser.StmtSelect, 1, toQueryAttributesForTest(options))
	require.Equal(t, 0, ftcw.GetCallCount(), "Expected no synchronous throttler calls before Start()")

	strategy.Start()
	defer strategy.Stop()

	require.Eventually(t, func() bool {
		return ftcw.GetCallCount() >= 1 && strategy.running.Load()
	}, 1*time.Second, 10*time.Millisecond, "Cache should be primed with at least one background call")

	ftcw.ResetCallCount()

	_ = strategy.Evaluate(context.Background(), topodatapb.TabletType_PRIMARY, &sqlparser.ParsedQuery{Query: "SELECT * FROM table"}, sqlparser.StmtSelect, 1, toQueryAttributesForTest(options))
	_ = strategy.Evaluate(context.Background(), topodatapb.TabletType_PRIMARY, &sqlparser.ParsedQuery{Query: "SELECT * FROM table"}, sqlparser.StmtSelect, 1, toQueryAttributesForTest(options))
	_ = strategy.Evaluate(context.Background(), topodatapb.TabletType_PRIMARY, &sqlparser.ParsedQuery{Query: "SELECT * FROM table"}, sqlparser.StmtSelect, 1, toQueryAttributesForTest(options))

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

// TestTabletThrottlerStrategy_UpdateConfig_AppliesNewRules verifies that UpdateConfig
// stores the new nested config so subsequent Evaluate calls observe it. UpdateConfig is
// invoked synchronously by QueryThrottler.HandleConfigUpdate before the snapshot swap,
// so this test exercises the same code path that eliminates the dual-watch race.
func TestTabletThrottlerStrategy_UpdateConfig_AppliesNewRules(t *testing.T) {
	ftcw := NewFakeThrottleClientWrapper(&throttle.CheckResult{}, true)

	initialCfg := makeTabletStrategyConfig("PRIMARY", "INSERT", "lag", makeThresholds(10, 50))
	strategy := NewTabletThrottlerStrategy(ftcw, initialCfg, createTestTabletConfig())

	newCfg := makeTabletStrategyConfig("PRIMARY", "SELECT", "lag", makeThresholds(20, 75))
	strategy.UpdateConfig(createTestQueryThrottlerConfig(newCfg))

	actualCfg := strategy.config.Load()
	require.NotNil(t, actualCfg)
	require.NotNil(t, actualCfg.TabletRules["PRIMARY"])
	require.Contains(t, actualCfg.TabletRules["PRIMARY"].StatementRules, "SELECT", "new rule should be visible after UpdateConfig")
	require.NotContains(t, actualCfg.TabletRules["PRIMARY"].StatementRules, "INSERT", "old rule should be replaced")
}

// TestTabletThrottlerStrategy_UpdateConfig_NilNestedNormalizesToEmpty verifies that
// UpdateConfig handles a Config whose TabletStrategyConfig is nil by normalizing to
// an empty config — preventing Evaluate from panicking on a nil proto field access.
func TestTabletThrottlerStrategy_UpdateConfig_NilNestedNormalizesToEmpty(t *testing.T) {
	ftcw := NewFakeThrottleClientWrapper(&throttle.CheckResult{}, true)

	initialCfg := makeTabletStrategyConfig("PRIMARY", "INSERT", "lag", makeThresholds(10, 50))
	strategy := NewTabletThrottlerStrategy(ftcw, initialCfg, createTestTabletConfig())

	strategy.UpdateConfig(&querythrottlerpb.Config{
		Enabled:              true,
		Strategy:             querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		TabletStrategyConfig: nil,
	})

	actualCfg := strategy.config.Load()
	require.NotNil(t, actualCfg, "UpdateConfig with nil nested config must store an empty config, not nil")
	require.Empty(t, actualCfg.TabletRules)
}

// TestTabletThrottlerStrategy_UpdateConfig_NoOpWhenUnchanged verifies that UpdateConfig
// skips the store when the new config is deep-equal to the current one. This mirrors the
// behavior of the (now-removed) in-strategy SrvKeyspace watch callback.
func TestTabletThrottlerStrategy_UpdateConfig_NoOpWhenUnchanged(t *testing.T) {
	ftcw := NewFakeThrottleClientWrapper(&throttle.CheckResult{}, true)

	cfg := makeTabletStrategyConfig("PRIMARY", "SELECT", "lag", makeThresholds(20, 75))
	strategy := NewTabletThrottlerStrategy(ftcw, cfg, createTestTabletConfig())
	before := strategy.config.Load()

	// Same logical config (deep-equal but distinct pointer) must not swap the pointer.
	dup := makeTabletStrategyConfig("PRIMARY", "SELECT", "lag", makeThresholds(20, 75))
	strategy.UpdateConfig(createTestQueryThrottlerConfig(dup))

	require.Same(t, before, strategy.config.Load(), "UpdateConfig must not swap when the new config is unchanged")
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

// TestTabletThrottlerStrategy_FailOpenWhenCacheNotPrimed verifies that when the
// strategy is not running (or the cache has not been primed yet),
// getCachedThrottleResult fails open without making any synchronous throttler
// call. This protects the query hot path from latency spikes when the throttler
// is slow or stuck during the brief cache-priming window after Start().
func TestTabletThrottlerStrategy_FailOpenWhenCacheNotPrimed(t *testing.T) {
	slowClient := &slowThrottleClientWrapper{
		checkResult: &throttle.CheckResult{
			Metrics: map[string]*throttle.MetricResult{
				"lag": {ResponseCode: tabletmanagerdata.CheckThrottlerResponseCode_OK, Value: 5},
			},
		},
		throttleCheckOK: true,
		delay:           5 * time.Second,
	}

	strategy := NewTabletThrottlerStrategy(slowClient, &querythrottlerpb.TabletStrategyConfig{}, createTestTabletConfig())

	start := time.Now()
	result, ok := strategy.getCachedThrottleResult()
	elapsed := time.Since(start)

	require.True(t, ok, "Expected fail-open (checkOk=true) when cache is not primed")
	require.Nil(t, result, "Expected nil checkResult when cache is not primed")
	require.Less(t, elapsed, 100*time.Millisecond, "Expected fail-open path to return immediately without calling the throttler")
	require.Equal(t, 0, slowClient.GetCallCount(), "Expected no synchronous ThrottleCheckOK calls from the hot path")
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

	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()
			for j := range numOperations {
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

// TestTabletThrottlerStrategy_StopWithoutStartCancelsContext verifies that Stop() is safe
// to call on a strategy that was never Started. The strategy context must be cancelled
// so any background work tied to it would exit immediately, even if Start() is never called.
func TestTabletThrottlerStrategy_StopWithoutStartCancelsContext(t *testing.T) {
	ftcw := NewFakeThrottleClientWrapper(&throttle.CheckResult{}, true)

	strategy := NewTabletThrottlerStrategy(ftcw, nil, createTestTabletConfig())

	strategy.Stop()
	require.Error(t, strategy.ctx.Err(), "Stop() must cancel the strategy context even when Start() was never called")
}

// nilResultThrottleClient is the canonical "ctx was canceled" return signature from a
// real ThrottleCheckOK caller: a nil CheckResult paired with checkOk=false. Used to
// reproduce the Stop-races-refresh race in refreshCache.
type nilResultThrottleClient struct{}

func (nilResultThrottleClient) ThrottleCheckOK(ctx context.Context, _ throttlerapp.Name) (*throttle.CheckResult, bool) {
	return nil, false
}

// TestTabletThrottlerStrategy_RefreshCache_CanceledContextPreservesPriorState verifies
// that when Stop() races an in-flight refresh and the underlying ThrottleCheckOK returns
// (nil, false) due to context cancellation, refreshCache does NOT overwrite the prior
// good cached state. Before the fix, refreshCache only treated DeadlineExceeded as a
// failure — Canceled fell through to the store path and poisoned the cache with a nil
// result, which would later panic in Evaluate's metric loop.
func TestTabletThrottlerStrategy_RefreshCache_CanceledContextPreservesPriorState(t *testing.T) {
	strategy := NewTabletThrottlerStrategy(nilResultThrottleClient{}, &querythrottlerpb.TabletStrategyConfig{}, createTestTabletConfig())

	// Prime with a known-good state — what a successful prior refresh would have produced.
	primed := &cacheState{
		ok: true,
		result: &throttle.CheckResult{
			Metrics: map[string]*throttle.MetricResult{
				"lag": {ResponseCode: tabletmanagerdata.CheckThrottlerResponseCode_OK, Value: 1},
			},
		},
		refreshedAt: time.Now(),
	}
	strategy.cachedState.Store(primed)

	// Cancel the strategy ctx — simulates Stop() running concurrently with a refresh
	// that has not yet returned. The derived ctx in refreshCache inherits this cancellation.
	strategy.cancel()

	// Run a refresh against the canceled ctx. The client returns (nil, false), the
	// ctx error is context.Canceled (not DeadlineExceeded), and the prior primed
	// state must be preserved rather than overwritten.
	strategy.refreshCache()

	require.Same(t, primed, strategy.cachedState.Load(),
		"canceled refresh must preserve the prior good cache state, not overwrite it with (nil, false)")
}

// TestTabletThrottlerStrategy_Evaluate_NilCachedResultFailsOpen verifies the
// defense-in-depth guard: even if some future change causes refreshCache to store a
// cacheState with result=nil, Evaluate must fail open rather than panic on the
// `for ... range checkResult.Metrics` loop.
func TestTabletThrottlerStrategy_Evaluate_NilCachedResultFailsOpen(t *testing.T) {
	cfg := makeTabletStrategyConfig(
		topodatapb.TabletType_PRIMARY.String(),
		"SELECT",
		"lag",
		makeThresholds(10, 100),
	)
	strategy := newTestStrategyWithRandom(
		NewFakeThrottleClientWrapper(nil, false),
		cfg,
		testRandomFuncs{
			randFloat64: func() float64 { return 0.5 },
			randIntN:    func(n int) int { return 99 },
		},
	)

	// Inject a cacheState{ok:false, result:nil} directly — the exact shape refreshCache
	// would have stored on a Canceled ctx before the fix. running=true so the hot-path
	// fail-open (returns checkOk=true when !running) does NOT mask the bug.
	strategy.cachedState.Store(&cacheState{ok: false, result: nil, refreshedAt: time.Now()})
	strategy.running.Store(true)

	options := &querypb.ExecuteOptions{Priority: "100"}
	require.NotPanics(t, func() {
		decision := strategy.Evaluate(
			context.Background(),
			topodatapb.TabletType_PRIMARY,
			&sqlparser.ParsedQuery{Query: "SELECT * FROM t"},
			sqlparser.StmtSelect,
			1,
			toQueryAttributesForTest(options),
		)
		require.False(t, decision.Throttle, "nil cached result must fail open, not throttle")
	}, "Evaluate must not panic when cached result is nil")
}
