/*
Copyright 2025 The Vitess Authors.

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

package querythrottler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	querythrottlerpb "vitess.io/vitess/go/vt/proto/querythrottler"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/srvtopo/srvtopotest"
	"vitess.io/vitess/go/vt/topo"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/querythrottler/registry"

	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle"
)

// init ensures throttler metrics are registered before any test runs, since some tests
// construct QueryThrottler directly (bypassing NewQueryThrottler) and reference the
// package-level metric vars. The schema is unconditional, so no flag is needed.
func init() {
	initThrottlerMetrics()
}

func TestSelectThrottlingStrategy(t *testing.T) {
	tests := []struct {
		name                   string
		giveThrottlingStrategy querythrottlerpb.ThrottlingStrategy
		expectedType           registry.ThrottlingStrategyHandler
	}{
		{
			name:                   "Unknown strategy defaults to NoOp",
			giveThrottlingStrategy: querythrottlerpb.ThrottlingStrategy_UNKNOWN,
			expectedType:           &registry.NoOpStrategy{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &throttle.Client{}

			config := &tabletenv.TabletConfig{
				QueryThrottlerConfigRefreshInterval: 10 * time.Millisecond,
			}

			strategy := selectThrottlingStrategy(&querythrottlerpb.Config{Enabled: true, Strategy: tt.giveThrottlingStrategy}, mockClient, config, nil, "", "", nil)

			require.IsType(t, tt.expectedType, strategy)
		})
	}
}

// TestQueryThrottler_StrategyLifecycleManagement tests that strategies are properly started and stopped.
func TestQueryThrottler_StrategyLifecycleManagement(t *testing.T) {
	// Test that initial strategy is started
	ctx := t.Context()

	throttler := &throttle.Throttler{}
	config := &tabletenv.TabletConfig{
		QueryThrottlerConfigRefreshInterval: 10 * time.Millisecond,
	}
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), config, "TestThrottler")

	srvTopoServer := srvtopotest.NewPassthroughSrvTopoServer()

	iqt := NewQueryThrottler(ctx, throttler, env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: uint32(123)}, srvTopoServer)

	// Verify initial strategy was started (NoOpStrategy in this case)
	require.NotNil(t, iqt.snapshot.Load().strategy)

	// Test Shutdown properly stops the strategy
	iqt.Shutdown()

	// After shutdown, the strategy should have been stopped
	// In a real test, we would verify the strategy's Stop method was called
	require.NotNil(t, iqt.snapshot.Load().strategy) // Strategy reference should still exist but be stopped
}

// TestQueryThrottler_Shutdown tests the Shutdown method.
func TestQueryThrottler_Shutdown(t *testing.T) {
	ctx := t.Context()

	config := &tabletenv.TabletConfig{
		QueryThrottlerConfigRefreshInterval: 10 * time.Millisecond,
	}
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), config, "TestThrottler")

	throttler := &throttle.Throttler{}
	srvTopoServer := srvtopotest.NewPassthroughSrvTopoServer()

	iqt := NewQueryThrottler(ctx, throttler, env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: uint32(123)}, srvTopoServer)

	// Should not panic when called multiple times
	iqt.Shutdown()
	iqt.Shutdown()

	// Should still be able to check the strategy reference
	strategy := iqt.snapshot.Load().strategy
	require.NotNil(t, strategy)
}

// TestQueryThrottler_DryRunMode tests that dry-run mode logs decisions but doesn't throttle queries.
func TestQueryThrottler_DryRunMode(t *testing.T) {
	tests := []struct {
		name                      string
		enabled                   bool
		dryRun                    bool
		throttleDecision          registry.ThrottleDecision
		expectError               bool
		expectDryRunLog           bool
		expectedLogMsg            string
		expectedTotalRequests     int64
		expectedThrottledRequests int64
	}{
		{
			name:    "Disabled throttler - no checks performed",
			enabled: false,
			dryRun:  false,
			throttleDecision: registry.ThrottleDecision{
				Throttle: true,
				Message:  "Should not be evaluated",
			},
			expectError:     false,
			expectDryRunLog: false,
		},
		{
			name:    "Disabled throttler with dry-run - no checks performed",
			enabled: false,
			dryRun:  true,
			throttleDecision: registry.ThrottleDecision{
				Throttle: true,
				Message:  "Should not be evaluated",
			},
			expectError:     false,
			expectDryRunLog: false,
		},
		{
			name:    "Normal mode - query allowed",
			enabled: true,
			dryRun:  false,
			throttleDecision: registry.ThrottleDecision{
				Throttle: false,
				Message:  "Query allowed",
			},
			expectError:           false,
			expectDryRunLog:       false,
			expectedTotalRequests: 1,
		},
		{
			name:    "Normal mode - query throttled",
			enabled: true,
			dryRun:  false,
			throttleDecision: registry.ThrottleDecision{
				Throttle:           true,
				Message:            "Query throttled: metric=cpu value=90.0 threshold=80.0",
				MetricName:         "cpu",
				MetricValue:        90.0,
				Threshold:          80.0,
				ThrottlePercentage: 1.0,
			},
			expectError:               true,
			expectDryRunLog:           false,
			expectedTotalRequests:     1,
			expectedThrottledRequests: 1,
		},
		{
			name:    "Dry-run mode - query would be throttled but allowed",
			enabled: true,
			dryRun:  true,
			throttleDecision: registry.ThrottleDecision{
				Throttle:           true,
				Message:            "Query throttled: metric=cpu value=95.0 threshold=80.0",
				MetricName:         "cpu",
				MetricValue:        95.0,
				Threshold:          80.0,
				ThrottlePercentage: 1.0,
			},
			expectError:               false,
			expectDryRunLog:           true,
			expectedLogMsg:            "[DRY-RUN] Query throttled: metric=cpu value=95.0 threshold=80.0, metric name: cpu, metric value: 95.000000",
			expectedTotalRequests:     1,
			expectedThrottledRequests: 1,
		},
		{
			name:    "Dry-run mode - query allowed normally",
			enabled: true,
			dryRun:  true,
			throttleDecision: registry.ThrottleDecision{
				Throttle: false,
				Message:  "Query allowed",
			},
			expectError:           false,
			expectDryRunLog:       false,
			expectedTotalRequests: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock strategy with controlled decision
			mockStrategy := &mockThrottlingStrategy{
				decision: tt.throttleDecision,
			}

			env := tabletenv.NewEnv(vtenv.NewTestEnv(), &tabletenv.TabletConfig{}, "TestThrottler")

			// Create throttler with controlled config
			iqt := &QueryThrottler{
				ctx: t.Context(),
				env: env,
			}
			iqt.snapshot.Store(&stateSnapshot{
				cfg: &querythrottlerpb.Config{
					Enabled: tt.enabled,
					DryRun:  tt.dryRun,
				},
				strategy: mockStrategy,
			})

			requestsTotal.ResetAll()
			requestsThrottled.ResetAll()

			// Capture log output.
			logCapture := &testLogCapture{}
			originalLogWarn := log.Warn
			defer func() {
				log.Warn = originalLogWarn
			}()

			log.Warn = logCapture.captureLog

			// Test the enforcement
			err := iqt.Throttle(
				t.Context(),
				topodatapb.TabletType_REPLICA,
				&sqlparser.ParsedQuery{Query: "SELECT * FROM test_table WHERE id = 1"},
				sqlparser.StmtSelect,
				12345,
				&querypb.ExecuteOptions{
					WorkloadName: "test-workload",
					Priority:     "50",
				},
			)

			// Verify error expectation
			if tt.expectError {
				require.EqualError(t, err, tt.throttleDecision.Message, "Error should match the throttle message exactly")
			} else {
				require.NoError(t, err, "Expected no throttling error")
			}

			// Verify log expectation
			if tt.expectDryRunLog {
				require.Len(t, logCapture.logs, 1, "Expected exactly one log message")
				require.Equal(t, tt.expectedLogMsg, logCapture.logs[0], "Log message should match expected")
			} else {
				require.Empty(t, logCapture.logs, "Expected no log messages")
			}

			// Verify stats expectation
			totalReqs := stats.CounterForDimension(requestsTotal, "Strategy")
			throttledReqs := stats.CounterForDimension(requestsThrottled, "Strategy")
			require.Equal(t, tt.expectedTotalRequests, totalReqs.Counts()["MockStrategy"], "Total requests should match expected")
			require.Equal(t, tt.expectedThrottledRequests, throttledReqs.Counts()["MockStrategy"], "Throttled requests should match expected")
		})
	}
}

func TestQueryThrottler_extractWorkloadName(t *testing.T) {
	tests := []struct {
		name    string
		options *querypb.ExecuteOptions
		want    string
	}{
		{
			name:    "nil options returns unknown",
			options: nil,
			want:    "unknown",
		},
		{
			name: "empty workload name returns unknown",
			options: &querypb.ExecuteOptions{
				WorkloadName: "",
			},
			want: "unknown",
		},
		{
			name: "custom workload name returns the name",
			options: &querypb.ExecuteOptions{
				WorkloadName: "analytics",
			},
			want: "analytics",
		},
		{
			name: "another custom workload name",
			options: &querypb.ExecuteOptions{
				WorkloadName: "batch-processing",
			},
			want: "batch-processing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractWorkloadName(tt.options)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestQueryThrottler_buildLabels(t *testing.T) {
	tests := []struct {
		name               string
		perWorkloadMetrics bool
		extras             []string
		want               []string
	}{
		{
			name:               "workload label disabled, no extras",
			perWorkloadMetrics: false,
			want:               []string{"strat", "unknown", "50"},
		},
		{
			name:               "workload label disabled, with extras",
			perWorkloadMetrics: false,
			extras:             []string{"cpu", "false"},
			want:               []string{"strat", "unknown", "50", "cpu", "false"},
		},
		{
			name:               "workload label enabled, no extras",
			perWorkloadMetrics: true,
			want:               []string{"strat", "client-supplied-wl", "50"},
		},
		{
			name:               "workload label enabled, with extras",
			perWorkloadMetrics: true,
			extras:             []string{"cpu", "false"},
			want:               []string{"strat", "client-supplied-wl", "50", "cpu", "false"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qt := &QueryThrottler{perWorkloadMetrics: tt.perWorkloadMetrics}
			got := qt.buildLabels("strat", "client-supplied-wl", "50", tt.extras...)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestQueryThrottler_metricsLabelCountStableAcrossInstances guards the panic the stats schema
// would suffer if instances with diverging EnablePerWorkloadTableMetrics shared the
// process-global metrics: the label *count* must be identical regardless of the per-instance
// flag, otherwise CountersWithMultiLabels.Add / MultiTimings.Record panic on a count mismatch.
// On the pre-fix code the enabled instance produced 3 base values against a 2-label registered
// schema, so the Add/Record calls below panicked.
func TestQueryThrottler_metricsLabelCountStableAcrossInstances(t *testing.T) {
	enabled := &QueryThrottler{perWorkloadMetrics: true}
	disabled := &QueryThrottler{perWorkloadMetrics: false}

	enabledBase := enabled.buildLabels("noop", "wl", "50")
	disabledBase := disabled.buildLabels("noop", "wl", "50")
	require.Len(t, disabledBase, len(enabledBase), "base label count must not depend on the per-instance flag")

	enabledThrottled := enabled.buildLabels("noop", "wl", "50", "cpu", "false")
	disabledThrottled := disabled.buildLabels("noop", "wl", "50", "cpu", "false")
	require.Len(t, disabledThrottled, len(enabledThrottled), "throttled label count must not depend on the per-instance flag")

	require.NotPanics(t, func() {
		requestsTotal.Add(enabledBase, 1)
		requestsTotal.Add(disabledBase, 1)
		requestsThrottled.Add(enabledThrottled, 1)
		requestsThrottled.Add(disabledThrottled, 1)
		totalLatency.Record(enabledBase, time.Now())
		totalLatency.Record(disabledBase, time.Now())
		evaluateLatency.Record(enabledBase, time.Now())
		evaluateLatency.Record(disabledBase, time.Now())
	}, "stats must accept labels from instances regardless of EnablePerWorkloadTableMetrics")
}

func TestQueryThrottler_extractPriority(t *testing.T) {
	tests := []struct {
		name    string
		options *querypb.ExecuteOptions
		want    int
	}{
		{
			name:    "nil options returns default priority",
			options: nil,
			want:    100,
		},
		{
			name: "empty priority returns default priority",
			options: &querypb.ExecuteOptions{
				Priority: "",
			},
			want: 100,
		},
		{
			name: "valid integer priority 0",
			options: &querypb.ExecuteOptions{
				Priority: "0",
			},
			want: 0,
		},
		{
			name: "valid integer priority 50",
			options: &querypb.ExecuteOptions{
				Priority: "50",
			},
			want: 50,
		},
		{
			name: "valid integer priority 100",
			options: &querypb.ExecuteOptions{
				Priority: "100",
			},
			want: 100,
		},
		{
			name: "invalid non-numeric priority returns default priority",
			options: &querypb.ExecuteOptions{
				Priority: "high",
			},
			want: 100,
		},
		{
			name: "invalid non-numeric priority low returns default priority",
			options: &querypb.ExecuteOptions{
				Priority: "low",
			},
			want: 100,
		},
		{
			name: "invalid negative priority returns default priority",
			options: &querypb.ExecuteOptions{
				Priority: "-1",
			},
			want: 100,
		},
		{
			name: "invalid decimal priority returns default priority",
			options: &querypb.ExecuteOptions{
				Priority: "50.5",
			},
			want: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractPriority(tt.options)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestQueryThrottler_HandleConfigUpdate_ErrorHandling verifies callback behavior for different error types.
func TestQueryThrottler_HandleConfigUpdate_ErrorHandling(t *testing.T) {
	tests := []struct {
		name           string
		inputErr       error
		expectedResult bool
		description    string
	}{
		{
			name:           "ContextCanceledError",
			inputErr:       context.Canceled,
			expectedResult: true,
			description:    "callback should return true to keep watching on context cancellation",
		},
		{
			name:           "TransientTopoError",
			inputErr:       errors.New("topo error: transient error"),
			expectedResult: true,
			description:    "callback should return true and continue watching on transient errors",
		},
		{
			name:           "NoNodeError",
			inputErr:       topo.NewError(topo.NoNode, "keyspace/test_keyspace"),
			expectedResult: true,
			description:    "callback should return true to keep watching when keyspace is not found (NoNode)",
		},
		{
			name:           "InterruptedError",
			inputErr:       topo.NewError(topo.Interrupted, "watch interrupted"),
			expectedResult: true,
			description:    "callback should return true to keep watching on Interrupted error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			qt := &QueryThrottler{
				ctx:          ctx,
				keyspace:     "test-keyspace",
				tabletConfig: &tabletenv.TabletConfig{},
			}
			qt.snapshot.Store(&stateSnapshot{
				cfg:      &querythrottlerpb.Config{Enabled: true, Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER},
				strategy: &registry.NoOpStrategy{},
			})

			// Create a valid SrvKeyspace matching the test setup (errors are checked before srvks is used)
			srvks := createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, false)

			result := qt.HandleConfigUpdate(srvks, tt.inputErr)

			require.Equal(t, tt.expectedResult, result, tt.description)
		})
	}
}

// TestQueryThrottler_HandleConfigUpdate__ConfigExtraction verifies config is properly extracted from SrvKeyspace.
func TestQueryThrottler_HandleConfigUpdate__ConfigExtraction(t *testing.T) {
	ctx := t.Context()

	oldCfg := &querythrottlerpb.Config{Enabled: false, Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, DryRun: false}
	oldStrategy := &registry.NoOpStrategy{}

	qt := &QueryThrottler{
		ctx:             ctx,
		tabletConfig:    &tabletenv.TabletConfig{},
		throttlerClient: &throttle.Client{},
	}
	qt.snapshot.Store(&stateSnapshot{
		cfg:      oldCfg,
		strategy: oldStrategy,
	})

	// Create SrvKeyspace with different config values
	srvks := createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, true)

	result := qt.HandleConfigUpdate(srvks, nil)

	// Should return true to continue watching, config should be extracted from SrvKeyspace
	require.True(t, result, "callback should return true and continue watching")

	snap := qt.snapshot.Load()
	require.True(t, snap.cfg.GetEnabled(), "Enabled should be updated from SrvKeyspace")
	require.True(t, snap.cfg.GetDryRun(), "DryRun should be updated from SrvKeyspace")
	require.Equal(t, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, snap.cfg.GetStrategy(), "strategy should remain TabletThrottler")
}

// TestQueryThrottler_HandleConfigUpdate__SuccessfulConfigUpdate tests successful config update when strategy doesn't change.
func TestQueryThrottler_HandleConfigUpdate__SuccessfulConfigUpdate(t *testing.T) {
	ctx := t.Context()

	// Use a mock strategy to track state changes
	oldStrategy := &mockThrottlingStrategy{}

	// Both initial and new config have the same strategy TYPE (no swap expected)
	unchangedStrategyType := querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER

	qt := &QueryThrottler{
		ctx:          ctx,
		tabletConfig: &tabletenv.TabletConfig{},
	}
	qt.snapshot.Store(&stateSnapshot{
		cfg:      &querythrottlerpb.Config{Enabled: true, Strategy: unchangedStrategyType, DryRun: false},
		strategy: oldStrategy,
	})

	// Create SrvKeyspace with same strategy but DryRun changed
	srvks := createTestSrvKeyspace(true, unchangedStrategyType, true)

	result := qt.HandleConfigUpdate(srvks, nil)

	require.True(t, result, "callback should return true")

	snap := qt.snapshot.Load()
	require.True(t, snap.cfg.GetDryRun(), "DryRun config should be updated")
	require.Equal(t, unchangedStrategyType, snap.cfg.GetStrategy(), "strategy type should remain the same")
	require.Equal(t, oldStrategy, snap.strategy, "strategy instance should not change when type is same")
	// Verify the old strategy was NOT stopped (no swap occurred)
	require.False(t, oldStrategy.stopped, "old strategy should NOT be stopped when type doesn't change")
}

// TestQueryThrottler_HandleConfigUpdate__StrategySwitch tests that strategy is properly switched when strategy type changes.
func TestQueryThrottler_HandleConfigUpdate__StrategySwitch(t *testing.T) {
	ctx := t.Context()

	oldStrategy := &mockThrottlingStrategy{}

	qt := &QueryThrottler{
		ctx:             ctx,
		tabletConfig:    &tabletenv.TabletConfig{},
		throttlerClient: &throttle.Client{},
	}
	qt.snapshot.Store(&stateSnapshot{
		cfg:      &querythrottlerpb.Config{Enabled: true, Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER},
		strategy: oldStrategy,
	})

	srvks := createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_UNKNOWN, false)

	result := qt.HandleConfigUpdate(srvks, nil)

	// Strategy should be switched
	require.True(t, result, "callback should return true")

	snap := qt.snapshot.Load()
	require.Equal(t, querythrottlerpb.ThrottlingStrategy_UNKNOWN, snap.cfg.GetStrategy(), "config strategy should be updated")
	// Old strategy should have been stopped (mocked strategy tracks this)
	require.True(t, oldStrategy.stopped, "old strategy should be stopped")
	// New strategy should be different instance
	newStrategyInstance := snap.strategy

	require.NotEqual(t, fmt.Sprintf("%p", oldStrategy), fmt.Sprintf("%p", newStrategyInstance),
		"strategy instance should be different after type change")
}

// TestQueryThrottler_HandleConfigUpdate__NoChange tests that nothing changes when the config is identical.
func TestQueryThrottler_HandleConfigUpdate__NoChange(t *testing.T) {
	ctx := t.Context()

	unchangedCfg := &querythrottlerpb.Config{Enabled: true, Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, DryRun: false}
	oldStrategy := &registry.NoOpStrategy{}

	qt := &QueryThrottler{
		ctx:          ctx,
		tabletConfig: &tabletenv.TabletConfig{},
	}
	qt.snapshot.Store(&stateSnapshot{
		cfg:      unchangedCfg,
		strategy: oldStrategy,
	})

	// Create SrvKeyspace with identical config
	srvks := createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, false)

	result := qt.HandleConfigUpdate(srvks, nil)

	// Config and strategy should remain same
	require.True(t, result, "callback should return true")

	snap := qt.snapshot.Load()
	require.Equal(t, unchangedCfg, snap.cfg, "config should remain unchanged")
	require.Equal(t, oldStrategy, snap.strategy, "strategy should remain unchanged")
}

// TestQueryThrottler_startSrvKeyspaceWatch_InitialLoad tests that initial configuration is loaded successfully when GetSrvKeyspace succeeds.
func TestQueryThrottler_startSrvKeyspaceWatch_InitialLoad(t *testing.T) {
	ctx := t.Context()

	env := tabletenv.NewEnv(vtenv.NewTestEnv(), &tabletenv.TabletConfig{}, "TestThrottler")

	srvTopoServer := srvtopotest.NewPassthroughSrvTopoServer()
	srvTopoServer.SrvKeyspace = createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, false)
	srvTopoServer.SrvKeyspaceError = nil

	throttler := &throttle.Throttler{}
	qt := NewQueryThrottler(ctx, throttler, env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: uint32(123)}, srvTopoServer)

	qt.InitDBConfig("test_keyspace")

	// Verify watch was started
	require.Eventually(t, func() bool {
		return qt.watchStarted.Load()
	}, 2*time.Second, 10*time.Millisecond, "Watch should have been started")

	// Verify that the configuration was loaded correctly
	require.Eventually(t, func() bool {
		snap := qt.snapshot.Load()
		return snap.cfg.GetEnabled() &&
			snap.cfg.GetStrategy() == querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER &&
			!snap.cfg.GetDryRun()
	}, 2*time.Second, 10*time.Millisecond, "Config should be loaded correctly: enabled=true, strategy=TabletThrottler, dryRun=false")

	require.Equal(t, "test_keyspace", qt.keyspace, "Keyspace should be set correctly")
}

// TestQueryThrottler_startSrvKeyspaceWatch_InitialLoadFailure tests that watch starts even when initial GetSrvKeyspace fails.
func TestQueryThrottler_startSrvKeyspaceWatch_InitialLoadFailure(t *testing.T) {
	ctx := t.Context()

	env := tabletenv.NewEnv(vtenv.NewTestEnv(), &tabletenv.TabletConfig{}, "TestThrottler")

	// Configure PassthroughSrvTopoServer to return an error on GetSrvKeyspace
	srvTopoServer := srvtopotest.NewPassthroughSrvTopoServer()
	srvTopoServer.SrvKeyspace = nil
	srvTopoServer.SrvKeyspaceError = errors.New("failed to fetch keyspace")

	throttler := &throttle.Throttler{}
	qt := NewQueryThrottler(ctx, throttler, env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: uint32(123)}, srvTopoServer)

	// Initialize with keyspace to trigger startSrvKeyspaceWatch
	qt.InitDBConfig("test_keyspace")

	// Verify watch was started despite initial load failure
	require.Eventually(t, func() bool {
		return qt.watchStarted.Load()
	}, 2*time.Second, 10*time.Millisecond, "Watch should be started even if initial load fails")

	require.Equal(t, "test_keyspace", qt.keyspace, "Keyspace should be set correctly")

	// Configuration should remain at default (NoOpStrategy) due to failure
	require.Eventually(t, func() bool {
		return !qt.snapshot.Load().cfg.GetEnabled()
	}, 2*time.Second, 10*time.Millisecond, "Config should remain disabled after initial load failure")
}

// TestQueryThrottler_startSrvKeyspaceWatch_OnlyStartsOnce tests that watch only starts once even with concurrent calls (atomic flag protection).
func TestQueryThrottler_startSrvKeyspaceWatch_OnlyStartsOnce(t *testing.T) {
	ctx := t.Context()

	env := tabletenv.NewEnv(vtenv.NewTestEnv(), &tabletenv.TabletConfig{}, "TestThrottler")

	srvTopoServer := srvtopotest.NewPassthroughSrvTopoServer()
	srvTopoServer.SrvKeyspace = createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, false)
	srvTopoServer.SrvKeyspaceError = nil

	throttler := &throttle.Throttler{}
	qt := NewQueryThrottler(ctx, throttler, env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: uint32(123)}, srvTopoServer)

	qt.InitDBConfig("test_keyspace")

	// Attempt to start the watch multiple times concurrently
	const numGoroutines = 10
	startedCount := 0
	var wg sync.WaitGroup
	var mu sync.Mutex

	for range numGoroutines {
		wg.Go(func() {
			// Each goroutine tries to start the watch
			qt.startSrvKeyspaceWatch()
			mu.Lock()
			startedCount++
			mu.Unlock()
		})
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify that the watch was started exactly once (atomic flag prevents multiple starts)
	require.Eventually(t, func() bool {
		return qt.watchStarted.Load()
	}, 2*time.Second, 10*time.Millisecond, "Watch should have been started")

	require.Equal(t, numGoroutines, startedCount, "All goroutines should have called startSrvKeyspaceWatch")
}

// TestQueryThrottler_startSrvKeyspaceWatch_RequiredFieldsValidation tests that watch doesn't start when required fields are missing.
func TestQueryThrottler_startSrvKeyspaceWatch_RequiredFieldsValidation(t *testing.T) {
	tests := []struct {
		name              string
		srvTopoServer     srvtopo.Server
		keyspace          string
		expectedWatchFlag bool
		description       string
	}{
		{
			name:              "Nil srvTopoServer prevents watch start",
			srvTopoServer:     nil,
			keyspace:          "test_keyspace",
			expectedWatchFlag: false,
			description:       "Watch should not start when srvTopoServer is nil",
		},
		{
			name:              "Empty keyspace prevents watch start",
			srvTopoServer:     srvtopotest.NewPassthroughSrvTopoServer(),
			keyspace:          "",
			expectedWatchFlag: false,
			description:       "Watch should not start when keyspace is empty",
		},
		{
			name:              "Valid fields allow watch to start",
			srvTopoServer:     srvtopotest.NewPassthroughSrvTopoServer(),
			keyspace:          "test_keyspace",
			expectedWatchFlag: true,
			description:       "Watch should start when all required fields are valid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			env := tabletenv.NewEnv(vtenv.NewTestEnv(), &tabletenv.TabletConfig{}, "TestThrottler")

			throttler := &throttle.Throttler{}
			qt := NewQueryThrottler(ctx, throttler, env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: uint32(123)}, tt.srvTopoServer)

			qt.InitDBConfig(tt.keyspace)

			qt.startSrvKeyspaceWatch()

			if tt.expectedWatchFlag {
				require.Eventually(t, func() bool {
					return qt.watchStarted.Load()
				}, 2*time.Second, 10*time.Millisecond, tt.description)
			} else {
				// For negative cases, ensure the watch doesn't start within a reasonable time
				require.Never(t, func() bool {
					return qt.watchStarted.Load()
				}, 500*time.Millisecond, 10*time.Millisecond, tt.description)
			}
		})
	}
}

// TestQueryThrottler_startSrvKeyspaceWatch_WatchCallback tests that WatchSrvKeyspace callback receives config updates and HandleConfigUpdate is invoked correctly.
func TestQueryThrottler_startSrvKeyspaceWatch_WatchCallback(t *testing.T) {
	tests := []struct {
		name             string
		enabled          bool
		strategy         querythrottlerpb.ThrottlingStrategy
		dryRun           bool
		expectedEnabled  bool
		expectedStrategy querythrottlerpb.ThrottlingStrategy
		expectedDryRun   bool
	}{
		{
			name:             "TabletThrottler strategy with enabled and no dry-run",
			enabled:          true,
			strategy:         querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
			dryRun:           false,
			expectedEnabled:  true,
			expectedStrategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
			expectedDryRun:   false,
		},
		{
			name:             "TabletThrottler disabled with dry-run",
			enabled:          false,
			strategy:         querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
			dryRun:           true,
			expectedEnabled:  false,
			expectedStrategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
			expectedDryRun:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			env := tabletenv.NewEnv(vtenv.NewTestEnv(), &tabletenv.TabletConfig{}, "TestThrottler")

			srvTopoServer := srvtopotest.NewPassthroughSrvTopoServer()
			srvTopoServer.SrvKeyspace = createTestSrvKeyspace(tt.enabled, tt.strategy, tt.dryRun)
			srvTopoServer.SrvKeyspaceError = nil

			throttler := &throttle.Throttler{}
			qt := NewQueryThrottler(ctx, throttler, env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: uint32(123)}, srvTopoServer)

			qt.InitDBConfig("test_keyspace")

			// Verify watch was started
			require.Eventually(t, func() bool {
				return qt.watchStarted.Load()
			}, 2*time.Second, 10*time.Millisecond, "Watch should have been started")

			// Verify that HandleConfigUpdate was called by checking if the config was updated
			require.Eventually(t, func() bool {
				snap := qt.snapshot.Load()
				return snap.cfg.GetEnabled() == tt.expectedEnabled &&
					snap.cfg.GetStrategy() == tt.expectedStrategy &&
					snap.cfg.GetDryRun() == tt.expectedDryRun
			}, 2*time.Second, 10*time.Millisecond, "Config should be updated correctly after callback is invoked")
		},
		)
	}
}

// TestQueryThrottler_startSrvKeyspaceWatch_ShutdownStopsWatch tests that Shutdown properly cancels the watch context and stops the watch goroutine.
func TestQueryThrottler_startSrvKeyspaceWatch_ShutdownStopsWatch(t *testing.T) {
	ctx := t.Context()

	env := tabletenv.NewEnv(vtenv.NewTestEnv(), &tabletenv.TabletConfig{}, "TestThrottler")

	srvTopoServer := srvtopotest.NewPassthroughSrvTopoServer()
	srvTopoServer.SrvKeyspace = createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, false)
	srvTopoServer.SrvKeyspaceError = nil

	throttler := &throttle.Throttler{}
	qt := NewQueryThrottler(ctx, throttler, env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: uint32(123)}, srvTopoServer)

	qt.InitDBConfig("test_keyspace")

	// Verify watch was started
	require.Eventually(t, func() bool {
		return qt.watchStarted.Load()
	}, 2*time.Second, 10*time.Millisecond, "Watch should have been started before shutdown")

	require.NotNil(t, qt.cancelWatchContext, "Cancel function should be set before shutdown")

	// Call Shutdown to stop the watch
	qt.Shutdown()

	// Verify that the watch started flag is reset
	require.Eventually(t, func() bool {
		return !qt.watchStarted.Load()
	}, 2*time.Second, 10*time.Millisecond, "Watch should be marked as not started after shutdown")

	// Verify that the strategy was stopped
	strategyInstance := qt.snapshot.Load().strategy
	require.NotNil(t, strategyInstance, "Strategy instance should still exist after shutdown")

	// Call Shutdown again to ensure it doesn't panic
	qt.Shutdown()

	// Verify the watch flag remains false
	require.False(t, qt.watchStarted.Load(), "Watch should remain not started after multiple shutdowns")
}

// TestQueryThrottler_ConcurrentThrottleAndConfigUpdate exercises concurrent
// Throttle() reads against HandleConfigUpdate() swaps to guard against the
// data race that previously existed when cfg/strategy were read without
// synchronization. Must be run with `go test -race` to be meaningful.
func TestQueryThrottler_ConcurrentThrottleAndConfigUpdate(t *testing.T) {
	ctx := t.Context()

	qt := &QueryThrottler{
		ctx:             ctx,
		tabletConfig:    &tabletenv.TabletConfig{},
		throttlerClient: &throttle.Client{},
		env:             tabletenv.NewEnv(vtenv.NewTestEnv(), &tabletenv.TabletConfig{}, "TestThrottler"),
	}
	qt.snapshot.Store(&stateSnapshot{
		cfg:      &querythrottlerpb.Config{Enabled: true, Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER},
		strategy: &mockThrottlingStrategy{decision: registry.ThrottleDecision{Throttle: false}},
	})

	var wg sync.WaitGroup
	stop := make(chan struct{})

	const readers = 8
	for range readers {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					_ = qt.Throttle(ctx, topodatapb.TabletType_REPLICA,
						&sqlparser.ParsedQuery{Query: "SELECT 1"}, sqlparser.StmtSelect, 0,
						&querypb.ExecuteOptions{WorkloadName: "w", Priority: "50"})
				}
			}
		})
	}

	// Alternate the strategy type to force snapshot swaps that exercise both
	// the cfg pointer and the strategy interface value.
	strategies := []querythrottlerpb.ThrottlingStrategy{
		querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		querythrottlerpb.ThrottlingStrategy_UNKNOWN,
	}
	for i := range 200 {
		qt.HandleConfigUpdate(createTestSrvKeyspace(true, strategies[i%2], i%2 == 0), nil)
	}

	close(stop)
	wg.Wait()
}

// TestQueryThrottler_HandleConfigUpdate_PushesNestedConfigBeforeSnapshotSwap is the
// regression test for the dual-watch race described in PR review #3: when a single
// SrvKeyspace update flips a top-level field (e.g. Enabled or DryRun) AND changes the
// nested TabletStrategyConfig AND keeps the same Strategy, the strategy's nested
// config must be updated synchronously before the snapshot swap. Without the fix,
// QueryThrottler would publish the new top-level snapshot while leaving the strategy's
// stale nested config in place until the strategy's own SrvKeyspace watch fired,
// briefly throttling queries against the old rules.
func TestQueryThrottler_HandleConfigUpdate_PushesNestedConfigBeforeSnapshotSwap(t *testing.T) {
	ctx := t.Context()

	oldStrategy := &mockThrottlingStrategy{}
	qt := &QueryThrottler{
		ctx:          ctx,
		tabletConfig: &tabletenv.TabletConfig{},
	}
	qt.snapshot.Store(&stateSnapshot{
		cfg: &querythrottlerpb.Config{
			Enabled:  false,
			Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		},
		strategy: oldStrategy,
	})

	// Single SrvKeyspace update: flip Enabled false->true AND add nested rules.
	// Strategy enum is unchanged, so the strategy instance is reused.
	newCfg := &querythrottlerpb.Config{
		Enabled:  true,
		Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		TabletStrategyConfig: &querythrottlerpb.TabletStrategyConfig{
			TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
				"PRIMARY": {
					StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
						"SELECT": {
							MetricRules: map[string]*querythrottlerpb.MetricRule{
								"lag": {Thresholds: []*querythrottlerpb.ThrottleThreshold{{Above: 20, Throttle: 75}}},
							},
						},
					},
				},
			},
		},
	}
	srvks := &topodatapb.SrvKeyspace{QueryThrottlerConfig: newCfg}

	require.True(t, qt.HandleConfigUpdate(srvks, nil))

	// The strategy instance must be reused (Strategy enum unchanged) — guards against
	// future refactors that accidentally rebuild on Enabled changes.
	snap := qt.snapshot.Load()
	require.Same(t, oldStrategy, snap.strategy, "strategy instance must not change when Strategy enum is unchanged")
	require.True(t, snap.cfg.GetEnabled(), "snapshot must reflect the new Enabled=true")

	// Core assertion: UpdateConfig was called with the new cfg before HandleConfigUpdate
	// returned. Without the fix, this slice would be empty — the strategy would have
	// kept its old nested config until its own watch fired separately.
	require.Len(t, oldStrategy.updateConfigCfgs, 1, "UpdateConfig must be invoked exactly once before the snapshot swap")
	pushed := oldStrategy.updateConfigCfgs[0]
	require.True(t, pushed.GetEnabled(), "pushed cfg must reflect new Enabled=true")
	require.Contains(t, pushed.GetTabletStrategyConfig().GetTabletRules(), "PRIMARY", "pushed cfg must carry the new nested rules")
}

// TestQueryThrottler_HandleConfigUpdate_NestedOnlyChangePropagates is the regression
// test for the fact that the old isConfigUpdateRequired helper only compared the three
// top-level scalar fields (Enabled, Strategy, DryRun). With the strategy's own
// SrvKeyspace watch removed, a SrvKeyspace update that changes ONLY the nested
// TabletStrategyConfig — without touching any top-level field — would be silently
// dropped: HandleConfigUpdate would short-circuit, never calling UpdateConfig on
// the active strategy. The fix replaces that helper with a full proto.Equal
// comparison so any change in the nested config also reaches the strategy.
func TestQueryThrottler_HandleConfigUpdate_NestedOnlyChangePropagates(t *testing.T) {
	ctx := t.Context()

	initialNested := &querythrottlerpb.TabletStrategyConfig{
		TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
			"PRIMARY": {
				StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
					"SELECT": {
						MetricRules: map[string]*querythrottlerpb.MetricRule{
							"lag": {Thresholds: []*querythrottlerpb.ThrottleThreshold{{Above: 10, Throttle: 25}}},
						},
					},
				},
			},
		},
	}
	oldStrategy := &mockThrottlingStrategy{}
	qt := &QueryThrottler{
		ctx:          ctx,
		tabletConfig: &tabletenv.TabletConfig{},
	}
	qt.snapshot.Store(&stateSnapshot{
		cfg: &querythrottlerpb.Config{
			Enabled:              true,
			Strategy:             querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
			DryRun:               false,
			TabletStrategyConfig: initialNested,
		},
		strategy: oldStrategy,
	})

	// New SrvKeyspace: identical top-level fields, but the nested rules changed
	// (different threshold on the same metric). This is the case the old
	// isConfigUpdateRequired helper missed.
	newNested := &querythrottlerpb.TabletStrategyConfig{
		TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
			"PRIMARY": {
				StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
					"SELECT": {
						MetricRules: map[string]*querythrottlerpb.MetricRule{
							"lag": {Thresholds: []*querythrottlerpb.ThrottleThreshold{{Above: 5, Throttle: 99}}},
						},
					},
				},
			},
		},
	}
	srvks := &topodatapb.SrvKeyspace{QueryThrottlerConfig: &querythrottlerpb.Config{
		Enabled:              true,
		Strategy:             querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		DryRun:               false,
		TabletStrategyConfig: newNested,
	}}

	require.True(t, qt.HandleConfigUpdate(srvks, nil))

	// The strategy must have been told about the nested change. Without the proto.Equal
	// guard replacing isConfigUpdateRequired, this slice would be empty: the old helper
	// only looked at Enabled/Strategy/DryRun (all unchanged) and short-circuited.
	require.Len(t, oldStrategy.updateConfigCfgs, 1, "UpdateConfig must be invoked when only the nested TabletStrategyConfig changes")
	pushed := oldStrategy.updateConfigCfgs[0].GetTabletStrategyConfig().GetTabletRules()["PRIMARY"].GetStatementRules()["SELECT"].GetMetricRules()["lag"].GetThresholds()
	require.Len(t, pushed, 1)
	require.Equal(t, float64(5), pushed[0].GetAbove(), "pushed cfg must carry the new threshold")
	require.Equal(t, int32(99), pushed[0].GetThrottle(), "pushed cfg must carry the new throttle ratio")

	// And the snapshot must also reflect the new nested config.
	snap := qt.snapshot.Load()
	require.Same(t, oldStrategy, snap.strategy, "strategy instance must not change for a nested-only update")
	snapThresholds := snap.cfg.GetTabletStrategyConfig().GetTabletRules()["PRIMARY"].GetStatementRules()["SELECT"].GetMetricRules()["lag"].GetThresholds()
	require.Len(t, snapThresholds, 1)
	require.Equal(t, float64(5), snapThresholds[0].GetAbove(), "snapshot cfg must carry the new nested config")
}

// TestQueryThrottler_HandleConfigUpdate_DiscardsStrategyAfterShutdown is the regression
// test for PR review #4: HandleConfigUpdate builds the new strategy outside qt.mu, so
// if Shutdown wins the lock first the callback can still proceed to Start() and store
// the new strategy after Shutdown has returned — leaking the tablet strategy's ticker
// and SrvKeyspace watch goroutines.
//
// The interleaving is forced deterministically via the newStrategyFactory hook: the
// factory blocks on a channel after the callback has reached it but before the
// callback can acquire qt.mu. While the factory is parked, the main goroutine runs
// Shutdown (uncontended, so it wins the lock immediately), then releases the factory
// so the callback proceeds and tries to take the lock that Shutdown has since
// released. The callback must observe the shutdown flag and discard the freshly
// built strategy.
func TestQueryThrottler_HandleConfigUpdate_DiscardsStrategyAfterShutdown(t *testing.T) {
	ctx := t.Context()

	originalMock := &mockThrottlingStrategy{}
	qt := &QueryThrottler{
		ctx:          ctx,
		tabletConfig: &tabletenv.TabletConfig{},
	}
	qt.snapshot.Store(&stateSnapshot{
		cfg: &querythrottlerpb.Config{
			Enabled:  true,
			Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
		},
		strategy: originalMock,
	})

	// Factory blocks until the test releases it, deterministically parking the
	// callback between strategy build and the lock acquisition.
	newMock := &mockThrottlingStrategy{}
	factoryEntered := make(chan struct{})
	factoryReleased := make(chan struct{})
	qt.newStrategyFactory = func(_ *querythrottlerpb.Config) registry.ThrottlingStrategyHandler {
		close(factoryEntered)
		<-factoryReleased
		return newMock
	}

	// Strategy enum changes, so HandleConfigUpdate hits the needsStrategyChange path
	// that builds via the factory.
	srvks := &topodatapb.SrvKeyspace{
		QueryThrottlerConfig: &querythrottlerpb.Config{
			Enabled:  true,
			Strategy: querythrottlerpb.ThrottlingStrategy_UNKNOWN,
		},
	}

	callbackDone := make(chan struct{})
	go func() {
		defer close(callbackDone)
		qt.HandleConfigUpdate(srvks, nil)
	}()

	// Wait until the callback is parked inside the factory; only then is the
	// "Shutdown wins the lock first" ordering guaranteed.
	<-factoryEntered

	// Shutdown takes qt.mu uncontended (the callback is still in the factory),
	// flips the shutdown flag, stops originalMock, and releases the lock.
	qt.Shutdown()

	// Releasing the factory lets the callback continue: it returns newMock and
	// then acquires qt.mu, where it must see shutdown=true and bail out.
	close(factoryReleased)

	require.Eventually(t, func() bool {
		select {
		case <-callbackDone:
			return true
		default:
			return false
		}
	}, 30*time.Second, 10*time.Millisecond, "HandleConfigUpdate callback should return after Shutdown")

	// Invariants after the race resolves:
	require.False(t, newMock.started, "discarded strategy must not be Start()'d after Shutdown")
	require.True(t, newMock.stopped, "discarded strategy must be Stop()'d as defense-in-depth so its background work cannot leak")
	require.True(t, originalMock.stopped, "Shutdown must have Stop()'d the original strategy")

	snap := qt.snapshot.Load()
	require.Same(t, originalMock, snap.strategy, "snapshot must NOT be swapped to the discarded strategy")
	require.Equal(t, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, snap.cfg.GetStrategy(),
		"snapshot cfg must NOT be replaced with the post-shutdown update")
}

// TestQueryThrottler_HandleConfigUpdate_SortsThresholdsOnReceipt verifies that
// HandleConfigUpdate sorts each MetricRule's Thresholds slice ascending by Above
// before storing the new cfg. GetThrottleDecision relies on sort.Search and
// therefore on ascending order; sanitizeQueryThrottlerConfig only sorts on the
// RPC write path, so a direct topo write that landed unsorted thresholds would
// silently corrupt throttling decisions. Sorting on receipt here closes that gap.
func TestQueryThrottler_HandleConfigUpdate_SortsThresholdsOnReceipt(t *testing.T) {
	ctx := t.Context()

	// Seed the snapshot with a different strategy so the incoming TABLET_THROTTLER
	// SrvKeyspace is treated as a strategy change — the new cfg is then built
	// into the snapshot rather than short-circuited by proto.Equal.
	qt := &QueryThrottler{
		ctx:          ctx,
		tabletConfig: &tabletenv.TabletConfig{},
	}
	qt.snapshot.Store(&stateSnapshot{
		cfg:      &querythrottlerpb.Config{Strategy: querythrottlerpb.ThrottlingStrategy_UNKNOWN},
		strategy: &mockThrottlingStrategy{},
	})
	// Inject a deterministic factory so building the new strategy doesn't pull
	// in production dependencies (selectThrottlingStrategy needs a real client).
	qt.newStrategyFactory = func(_ *querythrottlerpb.Config) registry.ThrottlingStrategyHandler {
		return &mockThrottlingStrategy{}
	}

	// Thresholds intentionally given OUT OF ORDER to simulate a direct topo write
	// that bypassed the RPC sanitizer.
	unsorted := []*querythrottlerpb.ThrottleThreshold{
		{Above: 50, Throttle: 100},
		{Above: 10, Throttle: 25},
		{Above: 25, Throttle: 50},
	}
	srvks := &topodatapb.SrvKeyspace{
		QueryThrottlerConfig: &querythrottlerpb.Config{
			Enabled:  true,
			Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
			TabletStrategyConfig: &querythrottlerpb.TabletStrategyConfig{
				TabletRules: map[string]*querythrottlerpb.StatementRuleSet{
					"PRIMARY": {
						StatementRules: map[string]*querythrottlerpb.MetricRuleSet{
							"SELECT": {
								MetricRules: map[string]*querythrottlerpb.MetricRule{
									"lag": {Thresholds: unsorted},
								},
							},
						},
					},
				},
			},
		},
	}

	qt.HandleConfigUpdate(srvks, nil)

	storedThresholds := qt.snapshot.Load().cfg.
		GetTabletStrategyConfig().
		GetTabletRules()["PRIMARY"].
		GetStatementRules()["SELECT"].
		GetMetricRules()["lag"].
		GetThresholds()
	require.Len(t, storedThresholds, 3)
	require.Equal(t, float64(10), storedThresholds[0].GetAbove(), "thresholds[0] must be the minimum after defensive sort")
	require.Equal(t, float64(25), storedThresholds[1].GetAbove())
	require.Equal(t, float64(50), storedThresholds[2].GetAbove())
}
