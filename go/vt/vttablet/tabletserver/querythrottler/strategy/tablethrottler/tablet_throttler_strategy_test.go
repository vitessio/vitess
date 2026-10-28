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
	"fmt"
	"strconv"
	"strings"
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
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

// createTestTabletConfig creates a minimal tablet config for testing
func createTestTabletConfig() *tabletenv.TabletConfig {
	return &tabletenv.TabletConfig{
		QueryThrottlerCacheUpdateInterval: 10 * time.Millisecond,
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
func newTestStrategyWithRandom(client ThrottlerClientWrapper, randFuncs testRandomFuncs) *TabletThrottlerStrategy {
	strategy := NewTabletThrottlerStrategy(client, createTestTabletConfig())
	if randFuncs.randFloat64 != nil {
		strategy.randFloat64 = randFuncs.randFloat64
	}
	if randFuncs.randIntN != nil {
		strategy.randIntN = randFuncs.randIntN
	}
	return strategy
}

// createTestQueryThrottlerConfig creates a Config (the type Evaluate takes) wrapping the
// given TabletStrategyConfig.
func createTestQueryThrottlerConfig(cfg *querythrottlerpb.TabletStrategyConfig) *querythrottlerpb.Config {
	return &querythrottlerpb.Config{
		Enabled:              true,
		Strategy:             querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		DryRun:               false,
		TabletStrategyConfig: cfg,
	}
}

// A matching rule must throttle even when parsedQuery is nil. Build() leaves plan.FullQuery
// nil for partially parsed DDL, and the statement type alone decides which rule applies,
// so DDL rules must still be enforced.
func TestTabletThrottlerStrategy_Evaluate_NilParsedQuery(t *testing.T) {
	overloaded := &throttle.CheckResult{
		Metrics: map[string]*throttle.MetricResult{
			"lag": {ResponseCode: tabletmanagerdata.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED, Value: 20},
		},
	}
	cfg := createTestQueryThrottlerConfig(makeTabletStrategyConfig(
		topodatapb.TabletType_PRIMARY.String(),
		sqlparser.StmtDDL.String(),
		"lag",
		makeThresholds(10, 100),
	))
	strategy := newTestStrategyWithRandom(
		NewFakeThrottleClientWrapper(overloaded, false),
		testRandomFuncs{
			randFloat64: func() float64 { return 0.5 },
			randIntN:    func(n int) int { return 99 },
		},
	)
	strategy.running.Store(true)
	strategy.cachedState.Store(&cacheState{ok: false, result: overloaded, refreshedAt: time.Now()})

	decision := strategy.Evaluate(
		t.Context(),
		cfg,
		topodatapb.TabletType_PRIMARY,
		nil,
		sqlparser.StmtDDL,
		0,
		toQueryAttributesForTest(&querypb.ExecuteOptions{Priority: "100"}),
	)

	require.True(t, decision.Throttle, "a matching DDL rule must throttle when parsedQuery is nil")
	require.Equal(t, "lag", decision.MetricName)
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
		giveOptions           *querypb.ExecuteOptions
		giveCfg               *querythrottlerpb.TabletStrategyConfig
		giveRandValue         float64
		givePriorityRandValue int
		wantErr               string
	}{
		{
			// lag is breached, so only the tablet type mismatch can explain the allow.
			name: "No-op if tablet type in config is not the current tablet type",
			giveCfg: makeTabletStrategyConfig(
				topodatapb.TabletType_PRIMARY.String(),
				"SELECT",
				"lag",
				makeThresholds(10, 10, 25, 25, 50, 50),
			),
			giveSQL:        "SELECT * from A where X=1",
			giveStmtType:   sqlparser.StmtSelect,
			giveTabletType: topodatapb.TabletType_REPLICA,
			giveCheckResult: &throttle.CheckResult{
				Metrics: map[string]*throttle.MetricResult{
					"lag": {
						ResponseCode: tabletmanagerdata.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED,
						Value:        60,
						Scope:        base.ShardScope.String(),
					},
				},
			},
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
			wantErr:               "query throttled: stmtType=SELECT workload=unknown priority=100 metric=lag value=20.00 breached threshold=10.00 throttle=10%",
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
			wantErr:               "query throttled: stmtType=SELECT workload=unknown priority=100 metric=lag value=20.00 breached threshold=10.00 throttle=100%",
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
			wantErr:               "query throttled: stmtType=DELETE workload=unknown priority=100 metric=lag value=20.00 breached threshold=10.00 throttle=100%",
		},
		{
			// Regression: a scoped config key ("shard/lag") must match a result the
			// throttler keyed by the bare name ("lag"). The rule used to fail open.
			name:           "scoped metric rule (shard/lag) matches result keyed by bare name",
			giveTabletType: topodatapb.TabletType_PRIMARY,
			giveCfg: makeTabletStrategyConfig(
				topodatapb.TabletType_PRIMARY.String(),
				"SELECT",
				"shard/lag",
				makeThresholds(10, 100),
			),
			giveSQL:      "SELECT * from A where X=1",
			giveStmtType: sqlparser.StmtSelect,
			giveCheckResult: &throttle.CheckResult{
				Metrics: map[string]*throttle.MetricResult{
					"lag": {
						ResponseCode: tabletmanagerdata.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED,
						Value:        20,
						Scope:        base.ShardScope.String(),
					},
				},
			},
			giveThrottleCheckOK:   false,
			giveRandValue:         0.5,
			givePriorityRandValue: 99,
			wantErr:               "query throttled: stmtType=SELECT workload=unknown priority=100 metric=lag value=20.00 breached threshold=10.00 throttle=100%",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ftcw := NewFakeThrottleClientWrapper(tt.giveCheckResult, tt.giveThrottleCheckOK)
			tts := newTestStrategyWithRandom(ftcw, testRandomFuncs{
				randFloat64: func() float64 { return tt.giveRandValue },
				randIntN:    func(n int) int { return tt.givePriorityRandValue },
			})
			// Start() primes asynchronously, so prime again synchronously here rather
			// than race the background refresh or read a cold cache.
			tts.Start()
			t.Cleanup(tts.Stop)
			tts.refreshCache()

			decision := tts.Evaluate(t.Context(), createTestQueryThrottlerConfig(tt.giveCfg), tt.giveTabletType, &sqlparser.ParsedQuery{Query: tt.giveSQL}, tt.giveStmtType, 0, toQueryAttributesForTest(tt.giveOptions))
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
	strategy := NewTabletThrottlerStrategy(ftcw, createTestTabletConfig())

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

	strategy := newTestStrategyWithRandom(ftcw, testRandomFuncs{
		randFloat64: func() float64 { return 0.5 },
		randIntN:    func(n int) int { return 99 },
	})

	options := &querypb.ExecuteOptions{Priority: "100"}

	// Before Start() the strategy is not running, so Evaluate() must fail open rather
	// than call ThrottleCheckOK synchronously and add latency to the hot path.
	_ = strategy.Evaluate(t.Context(), createTestQueryThrottlerConfig(cfg), topodatapb.TabletType_PRIMARY, &sqlparser.ParsedQuery{Query: "SELECT * FROM table"}, sqlparser.StmtSelect, 0, toQueryAttributesForTest(options))
	_ = strategy.Evaluate(t.Context(), createTestQueryThrottlerConfig(cfg), topodatapb.TabletType_PRIMARY, &sqlparser.ParsedQuery{Query: "SELECT * FROM table"}, sqlparser.StmtSelect, 0, toQueryAttributesForTest(options))
	require.Equal(t, 0, ftcw.GetCallCount(), "Expected no synchronous throttler calls before Start()")

	strategy.Start()
	t.Cleanup(strategy.Stop)

	require.Eventually(t, func() bool {
		return ftcw.GetCallCount() >= 1 && strategy.running.Load()
	}, 30*time.Second, 10*time.Millisecond, "Cache should be primed with at least one background call")

	ftcw.ResetCallCount()

	_ = strategy.Evaluate(t.Context(), createTestQueryThrottlerConfig(cfg), topodatapb.TabletType_PRIMARY, &sqlparser.ParsedQuery{Query: "SELECT * FROM table"}, sqlparser.StmtSelect, 0, toQueryAttributesForTest(options))
	_ = strategy.Evaluate(t.Context(), createTestQueryThrottlerConfig(cfg), topodatapb.TabletType_PRIMARY, &sqlparser.ParsedQuery{Query: "SELECT * FROM table"}, sqlparser.StmtSelect, 0, toQueryAttributesForTest(options))
	_ = strategy.Evaluate(t.Context(), createTestQueryThrottlerConfig(cfg), topodatapb.TabletType_PRIMARY, &sqlparser.ParsedQuery{Query: "SELECT * FROM table"}, sqlparser.StmtSelect, 0, toQueryAttributesForTest(options))

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
		// A threshold is breached only when the value goes above it, not when it equals it.
		{
			name:             "value_equal_to_threshold_not_breached",
			metricValue:      10.0,
			thresholds:       makeThresholds(10, 50),
			expectedThrottle: 0,
			expectedBreached: 0,
		},
		{
			name:             "value_equal_to_second_threshold_uses_first",
			metricValue:      20.0,
			thresholds:       makeThresholds(10, 25, 20, 75),
			expectedThrottle: 0.25,
			expectedBreached: 10,
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

// Evaluate decides by the config it is given, not by the one the strategy was built with:
// the strategy holds no rules, so a config update needs only QueryThrottler's snapshot swap.
func TestTabletThrottlerStrategy_Evaluate_UsesGivenConfig(t *testing.T) {
	overloaded := &throttle.CheckResult{
		Metrics: map[string]*throttle.MetricResult{
			"lag": {ResponseCode: tabletmanagerdata.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED, Value: 20},
		},
	}
	strategy := newTestStrategyWithRandom(NewFakeThrottleClientWrapper(overloaded, false), testRandomFuncs{
		randFloat64: func() float64 { return 0.5 },
		randIntN:    func(n int) int { return 99 },
	})
	strategy.running.Store(true)
	strategy.cachedState.Store(&cacheState{ok: false, result: overloaded, refreshedAt: time.Now()})

	evaluate := func(cfg *querythrottlerpb.Config) registry.ThrottleDecision {
		return strategy.Evaluate(t.Context(), cfg, topodatapb.TabletType_PRIMARY, &sqlparser.ParsedQuery{Query: "SELECT 1"},
			sqlparser.StmtSelect, 0, toQueryAttributesForTest(&querypb.ExecuteOptions{Priority: "100"}))
	}

	selectRule := createTestQueryThrottlerConfig(makeTabletStrategyConfig("PRIMARY", "SELECT", "lag", makeThresholds(10, 100)))
	insertRule := createTestQueryThrottlerConfig(makeTabletStrategyConfig("PRIMARY", "INSERT", "lag", makeThresholds(10, 100)))
	require.True(t, evaluate(selectRule).Throttle, "a SELECT rule must throttle a SELECT")
	require.False(t, evaluate(insertRule).Throttle, "the same strategy given only an INSERT rule must not throttle a SELECT")

	// A config without rules, or no config at all, matches nothing and must not panic.
	require.False(t, evaluate(&querythrottlerpb.Config{Enabled: true}).Throttle)
	require.False(t, evaluate(nil).Throttle)
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

// While the strategy is not running, or the cache is not primed yet,
// getCachedThrottleResult must fail open rather than call the throttler synchronously —
// that call could be slow or stuck, and this is the query hot path.
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

	strategy := NewTabletThrottlerStrategy(slowClient, createTestTabletConfig())

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
					_, _ = client.ThrottleCheckOK(t.Context(), throttlerapp.QueryThrottlerName)
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
	}, 30*time.Second, 10*time.Millisecond, "All goroutines should complete within timeout")

	finalResult, finalOk := client.GetCheckResult()
	require.NotNil(t, finalResult)
	require.NotNil(t, finalResult.Metrics)

	checkResult, checkOk := client.ThrottleCheckOK(t.Context(), throttlerapp.QueryThrottlerName)
	require.NotNil(t, checkResult)
	require.Equal(t, finalOk, checkOk)
}

// Stop() must be safe on a strategy that was never Started, and must still cancel the
// context so any work tied to it exits.
func TestTabletThrottlerStrategy_StopWithoutStartCancelsContext(t *testing.T) {
	ftcw := NewFakeThrottleClientWrapper(&throttle.CheckResult{}, true)

	strategy := NewTabletThrottlerStrategy(ftcw, createTestTabletConfig())

	strategy.Stop()
	require.Error(t, strategy.ctx.Err(), "Stop() must cancel the strategy context even when Start() was never called")
}

// Start after Stop must not run a second updater: the context is canceled and the done
// channel is already closed, so a restarted updater would close it again and panic.
func TestTabletThrottlerStrategy_StartAfterStopIsNoOp(t *testing.T) {
	ftcw := NewFakeThrottleClientWrapper(&throttle.CheckResult{}, true)

	strategy := NewTabletThrottlerStrategy(ftcw, createTestTabletConfig())
	t.Cleanup(strategy.Stop)

	strategy.Start()
	require.True(t, strategy.running.Load())
	strategy.Stop()
	require.False(t, strategy.running.Load())

	strategy.Start()
	require.False(t, strategy.running.Load(), "Start after Stop must not start the updater")
	strategy.Stop()
}

// Start and Stop may run concurrently, for example when a config update replaces a strategy
// that is still starting. Run with -race: Stop must not read state that Start is still setting up.
func TestTabletThrottlerStrategy_ConcurrentStartStop(t *testing.T) {
	ftcw := NewFakeThrottleClientWrapper(&throttle.CheckResult{}, true)
	for range 100 {
		strategy := NewTabletThrottlerStrategy(ftcw, createTestTabletConfig())
		var wg sync.WaitGroup
		wg.Go(strategy.Start)
		wg.Go(strategy.Stop)
		wg.Wait()
		strategy.Stop()
		require.False(t, strategy.running.Load())
	}
}

// nilResultThrottleClient returns what a real ThrottleCheckOK returns on a canceled ctx:
// a nil CheckResult with checkOk=false.
type nilResultThrottleClient struct{}

func (nilResultThrottleClient) ThrottleCheckOK(ctx context.Context, _ throttlerapp.Name) (*throttle.CheckResult, bool) {
	return nil, false
}

// primeGoodCacheState stores the state a successful prior refresh would have produced.
func primeGoodCacheState(strategy *TabletThrottlerStrategy) *cacheState {
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
	return primed
}

// When Stop() races an in-flight refresh, refreshCache must keep the prior good state, even
// if the client still returns a result. Treating only DeadlineExceeded as failure would let
// Canceled reach the store path.
func TestTabletThrottlerStrategy_RefreshCache_CanceledContextPreservesPriorState(t *testing.T) {
	overloaded := &throttle.CheckResult{
		Metrics: map[string]*throttle.MetricResult{
			"lag": {ResponseCode: tabletmanagerdata.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED, Value: 20},
		},
	}
	strategy := NewTabletThrottlerStrategy(NewFakeThrottleClientWrapper(overloaded, false), createTestTabletConfig())
	primed := primeGoodCacheState(strategy)

	// Stands in for Stop() running while a refresh is still in flight; refreshCache's
	// derived ctx inherits the cancellation.
	strategy.cancel()
	strategy.refreshCache()

	require.Same(t, primed, strategy.cachedState.Load(),
		"canceled refresh must preserve the prior good cache state")
}

// A refresh that returns no result must keep the prior good state rather than store a nil
// result that later panics in Evaluate's metric loop, and must count as a failure.
func TestTabletThrottlerStrategy_RefreshCache_NilResultPreservesPriorState(t *testing.T) {
	strategy := NewTabletThrottlerStrategy(nilResultThrottleClient{}, createTestTabletConfig())
	primed := primeGoodCacheState(strategy)
	failures := cacheRefreshFailures.Get()

	strategy.refreshCache()

	require.NoError(t, strategy.ctx.Err(), "the refresh context must not be canceled, so only the nil result is under test")
	require.Same(t, primed, strategy.cachedState.Load(),
		"a refresh with a nil result must preserve the prior good cache state")
	require.Equal(t, failures+1, cacheRefreshFailures.Get(), "a nil result must count as a refresh failure")
}

// Defense-in-depth: should a future change let refreshCache store a nil result, Evaluate
// must fail open rather than panic ranging over checkResult.Metrics.
func TestTabletThrottlerStrategy_Evaluate_NilCachedResultFailsOpen(t *testing.T) {
	cfg := makeTabletStrategyConfig(
		topodatapb.TabletType_PRIMARY.String(),
		"SELECT",
		"lag",
		makeThresholds(10, 100),
	)
	strategy := newTestStrategyWithRandom(
		NewFakeThrottleClientWrapper(nil, false),
		testRandomFuncs{
			randFloat64: func() float64 { return 0.5 },
			randIntN:    func(n int) int { return 99 },
		},
	)

	// The exact shape refreshCache stored on a Canceled ctx before the fix. running=true
	// so the !running fail-open does not mask the bug.
	strategy.cachedState.Store(&cacheState{ok: false, result: nil, refreshedAt: time.Now()})
	strategy.running.Store(true)

	options := &querypb.ExecuteOptions{Priority: "100"}
	require.NotPanics(t, func() {
		decision := strategy.Evaluate(
			t.Context(),
			createTestQueryThrottlerConfig(cfg),
			topodatapb.TabletType_PRIMARY,
			&sqlparser.ParsedQuery{Query: "SELECT * FROM t"},
			sqlparser.StmtSelect,
			0,
			toQueryAttributesForTest(options),
		)
		require.False(t, decision.Throttle, "nil cached result must fail open, not throttle")
	}, "Evaluate must not panic when cached result is nil")
}

// A REPLACE rule must throttle a REPLACE statement, and an INSERT rule must not stand in
// for one. The tablet plan builder classifies REPLACE as StmtReplace; that is covered by
// TestBuildStatementType_ReplaceRegression in the planbuilder package.
func TestTabletThrottlerStrategy_Evaluate_ReplaceRuleMatches(t *testing.T) {
	const replaceSQL = "replace into t(a) values (1)"
	stmtType := sqlparser.StmtReplace

	overloaded := &throttle.CheckResult{
		Metrics: map[string]*throttle.MetricResult{
			"lag": {ResponseCode: tabletmanagerdata.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED, Value: 20},
		},
	}

	// evaluateWithRule runs the REPLACE statement against a config keyed by ruleStmtType.
	evaluateWithRule := func(ruleStmtType string) registry.ThrottleDecision {
		cfg := createTestQueryThrottlerConfig(makeTabletStrategyConfig(
			topodatapb.TabletType_PRIMARY.String(),
			ruleStmtType,
			"lag",
			makeThresholds(10, 100),
		))
		strategy := newTestStrategyWithRandom(
			NewFakeThrottleClientWrapper(overloaded, false),
			testRandomFuncs{
				randFloat64: func() float64 { return 0.5 },
				randIntN:    func(n int) int { return 99 },
			},
		)
		strategy.running.Store(true)
		strategy.cachedState.Store(&cacheState{ok: false, result: overloaded, refreshedAt: time.Now()})

		return strategy.Evaluate(
			t.Context(),
			cfg,
			topodatapb.TabletType_PRIMARY,
			&sqlparser.ParsedQuery{Query: replaceSQL},
			stmtType,
			0,
			toQueryAttributesForTest(&querypb.ExecuteOptions{Priority: "100"}),
		)
	}

	require.True(t, evaluateWithRule("REPLACE").Throttle,
		"a configured REPLACE rule must throttle a REPLACE statement")
	require.False(t, evaluateWithRule("INSERT").Throttle,
		"an INSERT rule must not match a REPLACE statement")
}

// An overloaded (ok=false) state must be discarded once it ages past the staleness
// threshold. refreshCache keeps the last state when refreshes fail, so a metrics outage
// freezes the cache at ok=false and would otherwise throttle every query forever.
func TestTabletThrottlerStrategy_Evaluate_StaleOverloadedStateFailsOpen(t *testing.T) {
	cfg := makeTabletStrategyConfig(
		topodatapb.TabletType_PRIMARY.String(),
		"SELECT",
		"lag",
		makeThresholds(10, 100),
	)
	overloaded := &throttle.CheckResult{
		Metrics: map[string]*throttle.MetricResult{
			"lag": {ResponseCode: tabletmanagerdata.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED, Value: 20},
		},
	}
	strategy := newTestStrategyWithRandom(
		NewFakeThrottleClientWrapper(overloaded, false),
		testRandomFuncs{
			randFloat64: func() float64 { return 0.5 },
			randIntN:    func(n int) int { return 99 },
		},
	)
	// running=true so the !running fail-open does not mask the behavior under test.
	strategy.running.Store(true)

	evaluate := func() registry.ThrottleDecision {
		return strategy.Evaluate(
			t.Context(),
			createTestQueryThrottlerConfig(cfg),
			topodatapb.TabletType_PRIMARY,
			&sqlparser.ParsedQuery{Query: "SELECT * FROM t"},
			sqlparser.StmtSelect,
			0,
			toQueryAttributesForTest(&querypb.ExecuteOptions{Priority: "100"}),
		)
	}

	// A fresh overloaded state must throttle, so the fail-open below is down to staleness
	// and not a discard so aggressive it disables throttling outright.
	strategy.cachedState.Store(&cacheState{ok: false, result: overloaded, refreshedAt: time.Now()})
	require.True(t, evaluate().Throttle, "fresh overloaded state must throttle")

	// Same overloaded state, now aged past the staleness threshold: it must be discarded
	// and the query allowed. This is what a prolonged refresh outage looks like.
	before := cacheStaleConsumption.Get()
	stale := time.Now().Add(-2 * strategy.cacheStalenessThreshold)
	strategy.cachedState.Store(&cacheState{ok: false, result: overloaded, refreshedAt: stale})
	require.False(t, evaluate().Throttle, "stale overloaded state must fail open, not throttle indefinitely")
	require.Greater(t, cacheStaleConsumption.Get(), before, "discarding stale state must increment CacheStaleConsumption")
}

// evaluateOverloaded evaluates a SELECT on PRIMARY against a SELECT rule throttling 100%
// above lag 10, with lag at 20, and the given priority and random draws.
func evaluateOverloaded(t *testing.T, priority string, ratioPercent float64, randIntN int, randFloat64 float64) registry.ThrottleDecision {
	t.Helper()
	overloaded := &throttle.CheckResult{
		Metrics: map[string]*throttle.MetricResult{
			"lag": {ResponseCode: tabletmanagerdata.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED, Value: 20},
		},
	}
	strategy := newTestStrategyWithRandom(NewFakeThrottleClientWrapper(overloaded, false), testRandomFuncs{
		randFloat64: func() float64 { return randFloat64 },
		randIntN:    func(n int) int { return randIntN },
	})
	strategy.running.Store(true)
	strategy.cachedState.Store(&cacheState{ok: false, result: overloaded, refreshedAt: time.Now()})
	cfg := createTestQueryThrottlerConfig(makeTabletStrategyConfig("PRIMARY", "SELECT", "lag", makeThresholds(10, ratioPercent)))
	return strategy.Evaluate(t.Context(), cfg, topodatapb.TabletType_PRIMARY, &sqlparser.ParsedQuery{Query: "SELECT 1"},
		sqlparser.StmtSelect, 0, toQueryAttributesForTest(&querypb.ExecuteOptions{Priority: priority}))
}

// A query is a throttling candidate when a draw from [0, 100) is below its priority, so
// priority 0 is never throttled, whatever the draw, and priority 100 always is a candidate.
func TestTabletThrottlerStrategy_Evaluate_PriorityBoundaries(t *testing.T) {
	tests := []struct {
		priority     string
		randIntN     int
		wantThrottle bool
	}{
		{priority: "0", randIntN: 0, wantThrottle: false},
		{priority: "0", randIntN: 99, wantThrottle: false},
		{priority: "1", randIntN: 0, wantThrottle: true},
		{priority: "1", randIntN: 1, wantThrottle: false},
		{priority: "100", randIntN: 99, wantThrottle: true},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("priority=%s/rand=%d", tt.priority, tt.randIntN), func(t *testing.T) {
			decision := evaluateOverloaded(t, tt.priority, 100, tt.randIntN, 0)
			require.Equal(t, tt.wantThrottle, decision.Throttle, decision.Message)
		})
	}
}

// A breached threshold throttles a query when a draw from [0, 1) is below its ratio, and
// otherwise reaches the final allow.
func TestTabletThrottlerStrategy_Evaluate_ThrottleRatioBoundaries(t *testing.T) {
	tests := []struct {
		name         string
		ratioPercent float64
		randFloat64  float64
		wantThrottle bool
	}{
		{name: "draw below ratio", ratioPercent: 50, randFloat64: 0.49, wantThrottle: true},
		{name: "draw equal to ratio", ratioPercent: 50, randFloat64: 0.5, wantThrottle: false},
		{name: "full ratio", ratioPercent: 100, randFloat64: 0.999, wantThrottle: true},
		{name: "zero ratio", ratioPercent: 0, randFloat64: 0, wantThrottle: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision := evaluateOverloaded(t, "100", tt.ratioPercent, 0, tt.randFloat64)
			require.Equal(t, tt.wantThrottle, decision.Throttle, decision.Message)
		})
	}
}

// A breach whose probabilistic draw misses must allow the query and count it as allowed.
func TestTabletThrottlerStrategy_Evaluate_FinalAllow(t *testing.T) {
	allowed := []string{"PRIMARY", "SELECT", decisionPathFull, decisionOutcomeAllowed, decisionReasonQueryAllowed}
	key := strings.Join(allowed, ".")
	before := decisionCount.Counts()[key]

	decision := evaluateOverloaded(t, "100", 10, 0, 0.99)

	require.False(t, decision.Throttle)
	require.Equal(t, "No throttling conditions met", decision.Message)
	require.Equal(t, before+1, decisionCount.Counts()[key], "the final allow must be counted as query_allowed")
}

// breachedMetric is a metric result above its threshold, with the given value.
func breachedMetric(value float64) *throttle.MetricResult {
	return &throttle.MetricResult{ResponseCode: tabletmanagerdata.CheckThrottlerResponseCode_THRESHOLD_EXCEEDED, Value: value}
}

// newOverloadedStrategy returns a running strategy whose cache holds result as a failed
// check, with the given random draws.
func newOverloadedStrategy(result *throttle.CheckResult, randFloat64 float64, randIntN int) *TabletThrottlerStrategy {
	strategy := newTestStrategyWithRandom(NewFakeThrottleClientWrapper(result, false), testRandomFuncs{
		randFloat64: func() float64 { return randFloat64 },
		randIntN:    func(int) int { return randIntN },
	})
	strategy.running.Store(true)
	strategy.cachedState.Store(&cacheState{ok: false, result: result, refreshedAt: time.Now()})
	return strategy
}

// Each tablet type is held to its own rules, and a tablet type without rules is not throttled.
func TestTabletThrottlerStrategy_Evaluate_RulesPerTabletType(t *testing.T) {
	overloaded := &throttle.CheckResult{Metrics: map[string]*throttle.MetricResult{"lag": breachedMetric(20)}}
	selectRule := func(ratioPercent float64) *querythrottlerpb.StatementRuleSet {
		return &querythrottlerpb.StatementRuleSet{StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
			"SELECT": {MetricRules: map[string]*querythrottlerpb.MetricRule{"lag": {Thresholds: makeThresholds(10, ratioPercent)}}},
		}}
	}
	cfg := createTestQueryThrottlerConfig(&querythrottlerpb.TabletStrategyConfig{TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
		"PRIMARY": selectRule(50),
		"REPLICA": selectRule(100),
	}})
	strategy := newOverloadedStrategy(overloaded, 0, 99)

	evaluate := func(tabletType topodatapb.TabletType) registry.ThrottleDecision {
		return strategy.Evaluate(t.Context(), cfg, tabletType, &sqlparser.ParsedQuery{Query: "SELECT 1"},
			sqlparser.StmtSelect, 0, toQueryAttributesForTest(&querypb.ExecuteOptions{Priority: "100"}))
	}

	primary := evaluate(topodatapb.TabletType_PRIMARY)
	require.True(t, primary.Throttle)
	require.Equal(t, 0.5, primary.ThrottlePercentage, "PRIMARY must use the PRIMARY rule")

	replica := evaluate(topodatapb.TabletType_REPLICA)
	require.True(t, replica.Throttle)
	require.Equal(t, 1.0, replica.ThrottlePercentage, "REPLICA must use the REPLICA rule")

	rdonly := evaluate(topodatapb.TabletType_RDONLY)
	require.False(t, rdonly.Throttle)
	require.Equal(t, "No throttling rules for tablet type: RDONLY", rdonly.Message)
}

// With several metrics breached, the one with the highest throttle ratio decides, whatever
// order the check result's map yields them in.
func TestTabletThrottlerStrategy_Evaluate_HighestRatioWins(t *testing.T) {
	overloaded := &throttle.CheckResult{Metrics: map[string]*throttle.MetricResult{
		"lag":             breachedMetric(20),
		"loadavg":         breachedMetric(3),
		"threads_running": breachedMetric(200),
	}}
	cfg := createTestQueryThrottlerConfig(&querythrottlerpb.TabletStrategyConfig{TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
		"PRIMARY": {StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
			"SELECT": {MetricRules: map[string]*querythrottlerpb.MetricRule{
				"lag":             {Thresholds: makeThresholds(10, 25)},
				"loadavg":         {Thresholds: makeThresholds(1, 25, 2, 100)},
				"threads_running": {Thresholds: makeThresholds(100, 50)},
			}},
		}},
	}})
	strategy := newOverloadedStrategy(overloaded, 0, 99)

	// Go randomizes map iteration order, so repeat to cover the orders.
	for range 20 {
		decision := strategy.Evaluate(t.Context(), cfg, topodatapb.TabletType_PRIMARY, &sqlparser.ParsedQuery{Query: "SELECT 1"},
			sqlparser.StmtSelect, 0, toQueryAttributesForTest(&querypb.ExecuteOptions{Priority: "100"}))
		require.True(t, decision.Throttle)
		require.Equal(t, "loadavg", decision.MetricName)
		require.Equal(t, 3.0, decision.MetricValue)
		require.Equal(t, 2.0, decision.Threshold)
		require.Equal(t, 1.0, decision.ThrottlePercentage)
		require.Equal(t, "query throttled: stmtType=SELECT workload=unknown priority=100 metric=loadavg value=3.00 breached threshold=2.00 throttle=100%", decision.Message)
	}
}

// Only metrics that the throttler reports as THRESHOLD_EXCEEDED count. A metric that is within the
// tablet throttler's own threshold must not throttle, even when its value is above a query throttler
// rule, because the check failed on another metric.
func TestTabletThrottlerStrategy_Evaluate_IgnoresMetricsWithinThrottlerThreshold(t *testing.T) {
	overloaded := &throttle.CheckResult{Metrics: map[string]*throttle.MetricResult{
		"lag":     breachedMetric(20),
		"loadavg": {ResponseCode: tabletmanagerdata.CheckThrottlerResponseCode_OK, Value: 3},
	}}
	cfg := createTestQueryThrottlerConfig(&querythrottlerpb.TabletStrategyConfig{TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
		"PRIMARY": {StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
			"SELECT": {MetricRules: map[string]*querythrottlerpb.MetricRule{
				"lag":     {Thresholds: makeThresholds(10, 25)},
				"loadavg": {Thresholds: makeThresholds(1, 100)},
			}},
		}},
	}})
	strategy := newOverloadedStrategy(overloaded, 0, 99)

	decision := strategy.Evaluate(t.Context(), cfg, topodatapb.TabletType_PRIMARY, &sqlparser.ParsedQuery{Query: "SELECT 1"},
		sqlparser.StmtSelect, 0, toQueryAttributesForTest(&querypb.ExecuteOptions{Priority: "100"}))
	require.True(t, decision.Throttle)
	require.Equal(t, "lag", decision.MetricName)
	require.Equal(t, 0.25, decision.ThrottlePercentage)
}

// A cached state goes stale cacheStalenessMultiplier refresh intervals after it was
// refreshed, and a non-positive interval falls back to the default.
func TestTabletThrottlerStrategy_CacheStaleness(t *testing.T) {
	require.Equal(t, defaultCacheUpdateInterval, resolveCacheUpdateInterval(nil))
	require.Equal(t, defaultCacheUpdateInterval, resolveCacheUpdateInterval(&tabletenv.TabletConfig{}))
	require.Equal(t, defaultCacheUpdateInterval, resolveCacheUpdateInterval(&tabletenv.TabletConfig{QueryThrottlerCacheUpdateInterval: -time.Second}))

	strategy := NewTabletThrottlerStrategy(NewFakeThrottleClientWrapper(nil, true), &tabletenv.TabletConfig{QueryThrottlerCacheUpdateInterval: 10 * time.Second})
	t.Cleanup(strategy.Stop)
	require.Equal(t, 60*time.Second, strategy.cacheStalenessThreshold)

	require.False(t, strategy.isStale(nil))
	require.False(t, strategy.isStale(&cacheState{}), "a state that was never refreshed is a miss, not stale")
	require.False(t, strategy.isStale(&cacheState{refreshedAt: time.Now().Add(-59 * time.Second)}))
	require.True(t, strategy.isStale(&cacheState{refreshedAt: time.Now().Add(-61 * time.Second)}))
}

// A failed refresh over a state that has already gone stale counts in CacheStaleRefreshes,
// since queries are failing open against it. One over a fresh state does not.
func TestTabletThrottlerStrategy_RefreshCache_CountsStaleRefreshes(t *testing.T) {
	strategy := NewTabletThrottlerStrategy(nilResultThrottleClient{}, &tabletenv.TabletConfig{QueryThrottlerCacheUpdateInterval: 10 * time.Second})
	t.Cleanup(strategy.Stop)
	staleRefreshes := cacheStaleRefreshes.Get()

	strategy.cachedState.Store(&cacheState{ok: true, result: &throttle.CheckResult{}, refreshedAt: time.Now()})
	strategy.refreshCache()
	require.Equal(t, staleRefreshes, cacheStaleRefreshes.Get(), "a failed refresh over a fresh state must not count as stale")

	strategy.cachedState.Store(&cacheState{ok: true, result: &throttle.CheckResult{}, refreshedAt: time.Now().Add(-2 * strategy.cacheStalenessThreshold)})
	strategy.refreshCache()
	require.Equal(t, staleRefreshes+1, cacheStaleRefreshes.Get(), "a failed refresh over a stale state must count as stale")
}

// Each decision counts once in DecisionCount, under its tablet type, statement type, path,
// outcome and reason, and its cache read counts as a hit or a miss.
func TestTabletThrottlerStrategy_Evaluate_RecordsDecisions(t *testing.T) {
	overloaded := &throttle.CheckResult{Metrics: map[string]*throttle.MetricResult{"lag": breachedMetric(20)}}
	overloadedState := &cacheState{ok: false, result: overloaded, refreshedAt: time.Now()}
	rules := makeTabletStrategyConfig("PRIMARY", "SELECT", "lag", makeThresholds(10, 50))

	tests := []struct {
		name        string
		running     bool
		state       *cacheState
		tabletType  topodatapb.TabletType
		stmtType    sqlparser.StatementType
		priority    string
		randFloat64 float64
		dryRun      bool
		wantLabels  []string
		wantHits    int64
		wantMisses  int64
	}{
		{
			name:       "fast path",
			running:    true,
			state:      &cacheState{ok: true, result: &throttle.CheckResult{}, refreshedAt: time.Now()},
			tabletType: topodatapb.TabletType_PRIMARY,
			stmtType:   sqlparser.StmtSelect,
			priority:   "100",
			wantLabels: []string{"PRIMARY", stmtTypeNotAvailable, decisionPathFast, decisionOutcomeAllowed, decisionReasonBypassFast},
		},
		{
			name:       "priority bypass",
			running:    true,
			state:      overloadedState,
			tabletType: topodatapb.TabletType_PRIMARY,
			stmtType:   sqlparser.StmtSelect,
			priority:   "0",
			wantLabels: []string{"PRIMARY", "SELECT", decisionPathFull, decisionOutcomeAllowed, decisionReasonBypassPrior},
		},
		{
			name:       "no rule for tablet type",
			running:    true,
			state:      overloadedState,
			tabletType: topodatapb.TabletType_REPLICA,
			stmtType:   sqlparser.StmtSelect,
			priority:   "100",
			wantLabels: []string{"REPLICA", "SELECT", decisionPathFull, decisionOutcomeAllowed, decisionReasonNoRuleForTabletType},
		},
		{
			name:       "no rule for statement type",
			running:    true,
			state:      overloadedState,
			tabletType: topodatapb.TabletType_PRIMARY,
			stmtType:   sqlparser.StmtInsert,
			priority:   "100",
			wantLabels: []string{"PRIMARY", "INSERT", decisionPathFull, decisionOutcomeAllowed, decisionReasonNoRuleForStmtType},
		},
		{
			name:       "cache miss while not running",
			running:    false,
			state:      overloadedState,
			tabletType: topodatapb.TabletType_PRIMARY,
			stmtType:   sqlparser.StmtSelect,
			priority:   "100",
			wantLabels: []string{"PRIMARY", "SELECT", decisionPathFull, decisionOutcomeAllowed, decisionReasonNoMetricBreach},
			wantMisses: 1,
		},
		{
			name:        "breach throttled",
			running:     true,
			state:       overloadedState,
			tabletType:  topodatapb.TabletType_PRIMARY,
			stmtType:    sqlparser.StmtSelect,
			priority:    "100",
			randFloat64: 0.4,
			wantLabels:  []string{"PRIMARY", "SELECT", decisionPathFull, decisionOutcomeThrottled, decisionReasonMetricBreach},
			wantHits:    1,
		},
		{
			// The query still runs in dry_run, so the decision must not count as throttled.
			name:        "breach in dry_run",
			running:     true,
			state:       overloadedState,
			tabletType:  topodatapb.TabletType_PRIMARY,
			stmtType:    sqlparser.StmtSelect,
			priority:    "100",
			randFloat64: 0.4,
			dryRun:      true,
			wantLabels:  []string{"PRIMARY", "SELECT", decisionPathFull, decisionOutcomeWouldThrottle, decisionReasonMetricBreach},
			wantHits:    1,
		},
		{
			name:        "breach allowed",
			running:     true,
			state:       overloadedState,
			tabletType:  topodatapb.TabletType_PRIMARY,
			stmtType:    sqlparser.StmtSelect,
			priority:    "100",
			randFloat64: 0.6,
			wantLabels:  []string{"PRIMARY", "SELECT", decisionPathFull, decisionOutcomeAllowed, decisionReasonQueryAllowed},
			wantHits:    1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := newOverloadedStrategy(overloaded, tt.randFloat64, 99)
			strategy.running.Store(tt.running)
			strategy.cachedState.Store(tt.state)

			cfg := createTestQueryThrottlerConfig(rules)
			cfg.DryRun = tt.dryRun

			key := strings.Join(tt.wantLabels, ".")
			decisions, hits, misses := decisionCount.Counts()[key], cacheHits.Get(), cacheMisses.Get()
			strategy.Evaluate(t.Context(), cfg, tt.tabletType, &sqlparser.ParsedQuery{Query: "SELECT 1"},
				tt.stmtType, 0, toQueryAttributesForTest(&querypb.ExecuteOptions{Priority: tt.priority}))

			require.Equal(t, decisions+1, decisionCount.Counts()[key], "DecisionCount for %s", key)
			require.Equal(t, hits+tt.wantHits, cacheHits.Get(), "CacheHits")
			require.Equal(t, misses+tt.wantMisses, cacheMisses.Get(), "CacheMisses")
		})
	}
}
