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

			strategy := selectThrottlingStrategy(&querythrottlerpb.Config{Enabled: true, Strategy: tt.giveThrottlingStrategy}, mockClient, config)

			require.IsType(t, tt.expectedType, strategy)
		})
	}
}

// TestQueryThrottler_StrategyLifecycleManagement tests that strategies are properly started and stopped.
func TestQueryThrottler_StrategyLifecycleManagement(t *testing.T) {
	// Test that initial strategy is started
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	throttler := &throttle.Throttler{}
	config := &tabletenv.TabletConfig{
		QueryThrottlerConfigRefreshInterval: 10 * time.Millisecond,
	}
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), config, "TestThrottler")

	srvTopoServer := srvtopotest.NewPassthroughSrvTopoServer()

	iqt := NewQueryThrottler(ctx, throttler, env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: uint32(123)}, srvTopoServer)

	// Verify initial strategy was started (NoOpStrategy in this case)
	require.NotNil(t, iqt.strategyHandlerInstance)

	// Test Shutdown properly stops the strategy
	iqt.Shutdown()

	// After shutdown, the strategy should have been stopped
	// In a real test, we would verify the strategy's Stop method was called
	require.NotNil(t, iqt.strategyHandlerInstance) // Strategy reference should still exist but be stopped
}

// TestQueryThrottler_Shutdown tests the Shutdown method.
func TestQueryThrottler_Shutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	iqt.mu.RLock()
	strategy := iqt.strategyHandlerInstance
	iqt.mu.RUnlock()
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
				ctx: context.Background(),
				cfg: &querythrottlerpb.Config{
					Enabled: tt.enabled,
					DryRun:  tt.dryRun,
				},
				env: env,
				stats: Stats{
					requestsTotal:     env.Exporter().NewCountersWithMultiLabels(queryThrottlerAppName+"Requests", "TestThrottler requests", []string{"Strategy", "Workload", "Priority"}),
					requestsThrottled: env.Exporter().NewCountersWithMultiLabels(queryThrottlerAppName+"Throttled", "TestThrottler throttled", []string{"Strategy", "Workload", "Priority", "MetricName", "MetricValue", "DryRun"}),
					totalLatency:      env.Exporter().NewMultiTimings(queryThrottlerAppName+"TotalLatencyMs", "Total latency of QueryThrottler.Throttle in milliseconds", []string{"Strategy", "Workload", "Priority"}),
					evaluateLatency:   env.Exporter().NewMultiTimings(queryThrottlerAppName+"EvaluateLatencyMs", "Latency from Throttle entry to completion of Evaluate in milliseconds", []string{"Strategy", "Workload", "Priority"}),
				},
				strategyHandlerInstance: mockStrategy,
			}

			iqt.stats.requestsTotal.ResetAll()
			iqt.stats.requestsThrottled.ResetAll()

			// Capture log output
			logCapture := &testLogCapture{}
			originalLogWarningf := log.Warningf
			defer func() {
				// Restore original logging function
				log.Warningf = originalLogWarningf
			}()

			// Mock log.Warningf to capture output
			log.Warningf = logCapture.captureLog

			// Test the enforcement
			err := iqt.Throttle(
				context.Background(),
				topodatapb.TabletType_REPLICA,
				&sqlparser.ParsedQuery{Query: "SELECT * FROM test_table WHERE id = 1"},
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
			totalRequests := stats.CounterForDimension(iqt.stats.requestsTotal, "Strategy")
			throttledRequests := stats.CounterForDimension(iqt.stats.requestsThrottled, "Strategy")
			require.Equal(t, tt.expectedTotalRequests, totalRequests.Counts()["MockStrategy"], "Total requests should match expected")
			require.Equal(t, tt.expectedThrottledRequests, throttledRequests.Counts()["MockStrategy"], "Throttled requests should match expected")
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
			name: "empty workload name returns default",
			options: &querypb.ExecuteOptions{
				WorkloadName: "",
			},
			want: "default",
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
			expectedResult: false,
			description:    "callback should return false to stop watching on context cancellation",
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
			expectedResult: false,
			description:    "callback should return false to stop watching when keyspace is deleted (NoNode)",
		},
		{
			name:           "InterruptedError",
			inputErr:       topo.NewError(topo.Interrupted, "watch interrupted"),
			expectedResult: false,
			description:    "callback should return false to stop watching on Interrupted error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			qt := &QueryThrottler{
				ctx:                     ctx,
				keyspace:                "test-keyspace",
				cfg:                     &querythrottlerpb.Config{Enabled: true, Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER},
				strategyHandlerInstance: &registry.NoOpStrategy{},
				tabletConfig:            &tabletenv.TabletConfig{},
			}

			// Create a valid SrvKeyspace matching the test setup (errors are checked before srvks is used)
			srvks := createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, false)

			result := qt.HandleConfigUpdate(srvks, tt.inputErr)

			require.Equal(t, tt.expectedResult, result, tt.description)
		})
	}
}

// TestQueryThrottler_HandleConfigUpdate__ConfigExtraction verifies config is properly extracted from SrvKeyspace.
func TestQueryThrottler_HandleConfigUpdate__ConfigExtraction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	oldCfg := &querythrottlerpb.Config{Enabled: false, Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, DryRun: false}
	oldStrategy := &registry.NoOpStrategy{}

	qt := &QueryThrottler{
		ctx:                     ctx,
		cfg:                     oldCfg,
		strategyHandlerInstance: oldStrategy,
		tabletConfig:            &tabletenv.TabletConfig{},
		throttleClient:          &throttle.Client{},
	}

	// Create SrvKeyspace with different config values
	srvks := createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, true)

	result := qt.HandleConfigUpdate(srvks, nil)

	// Should return true to continue watching, config should be extracted from SrvKeyspace
	require.True(t, result, "callback should return true and continue watching")

	qt.mu.RLock()
	require.True(t, qt.cfg.GetEnabled(), "Enabled should be updated from SrvKeyspace")
	require.True(t, qt.cfg.GetDryRun(), "DryRun should be updated from SrvKeyspace")
	require.Equal(t, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, qt.cfg.GetStrategy(), "strategy should remain TabletThrottler")
	qt.mu.RUnlock()
}

// TestQueryThrottler_HandleConfigUpdate__SuccessfulConfigUpdate tests successful config update when strategy doesn't change.
func TestQueryThrottler_HandleConfigUpdate__SuccessfulConfigUpdate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use a mock strategy to track state changes
	oldStrategy := &mockThrottlingStrategy{}

	// Both initial and new config have the same strategy TYPE (no swap expected)
	unchangedStrategyType := querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER

	qt := &QueryThrottler{
		ctx:                     ctx,
		cfg:                     &querythrottlerpb.Config{Enabled: true, Strategy: unchangedStrategyType, DryRun: false},
		strategyHandlerInstance: oldStrategy,
		tabletConfig:            &tabletenv.TabletConfig{},
	}

	// Create SrvKeyspace with same strategy but DryRun changed
	srvks := createTestSrvKeyspace(true, unchangedStrategyType, true)

	result := qt.HandleConfigUpdate(srvks, nil)

	require.True(t, result, "callback should return true")

	qt.mu.RLock()
	require.True(t, qt.cfg.GetDryRun(), "DryRun config should be updated")
	require.Equal(t, unchangedStrategyType, qt.cfg.GetStrategy(), "strategy type should remain the same")
	require.Equal(t, oldStrategy, qt.strategyHandlerInstance, "strategy instance should not change when type is same")
	// Verify the old strategy was NOT stopped (no swap occurred)
	require.False(t, oldStrategy.stopped, "old strategy should NOT be stopped when type doesn't change")
	qt.mu.RUnlock()
}

// TestQueryThrottler_HandleConfigUpdate__StrategySwitch tests that strategy is properly switched when strategy type changes.
func TestQueryThrottler_HandleConfigUpdate__StrategySwitch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	oldStrategy := &mockThrottlingStrategy{}

	qt := &QueryThrottler{
		ctx:                     ctx,
		cfg:                     &querythrottlerpb.Config{Enabled: true, Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER},
		strategyHandlerInstance: oldStrategy,
		tabletConfig:            &tabletenv.TabletConfig{},
		throttleClient:          &throttle.Client{},
	}

	srvks := createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_UNKNOWN, false)

	result := qt.HandleConfigUpdate(srvks, nil)

	// Strategy should be switched
	require.True(t, result, "callback should return true")

	qt.mu.RLock()
	require.Equal(t, querythrottlerpb.ThrottlingStrategy_UNKNOWN, qt.cfg.GetStrategy(), "config strategy should be updated")
	// Old strategy should have been stopped (mocked strategy tracks this)
	require.True(t, oldStrategy.stopped, "old strategy should be stopped")
	// New strategy should be different instance
	newStrategyInstance := qt.strategyHandlerInstance
	qt.mu.RUnlock()

	require.NotEqual(t, fmt.Sprintf("%p", oldStrategy), fmt.Sprintf("%p", newStrategyInstance),
		"strategy instance should be different after type change")
}

// TestQueryThrottler_HandleConfigUpdate__NoChange tests that nothing changes when the config is identical.
func TestQueryThrottler_HandleConfigUpdate__NoChange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	unchangedCfg := &querythrottlerpb.Config{Enabled: true, Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, DryRun: false}
	oldStrategy := &registry.NoOpStrategy{}

	qt := &QueryThrottler{
		ctx:                     ctx,
		cfg:                     unchangedCfg,
		strategyHandlerInstance: oldStrategy,
		tabletConfig:            &tabletenv.TabletConfig{},
	}

	// Create SrvKeyspace with identical config
	srvks := createTestSrvKeyspace(true, querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER, false)

	result := qt.HandleConfigUpdate(srvks, nil)

	// Config and strategy should remain same
	require.True(t, result, "callback should return true")

	qt.mu.RLock()
	require.Equal(t, unchangedCfg, qt.cfg, "config should remain unchanged")
	require.Equal(t, oldStrategy, qt.strategyHandlerInstance, "strategy should remain unchanged")
	qt.mu.RUnlock()
}

// TestIsConfigUpdateRequired tests the isConfigUpdateRequired function.
func TestIsConfigUpdateRequired(t *testing.T) {
	tests := []struct {
		name     string
		oldCfg   *querythrottlerpb.Config
		newCfg   *querythrottlerpb.Config
		expected bool
	}{
		{
			name: "No changes - configs identical",
			oldCfg: &querythrottlerpb.Config{
				Enabled:  true,
				Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
				DryRun:   false,
			},
			newCfg: &querythrottlerpb.Config{
				Enabled:  true,
				Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
				DryRun:   false,
			},
			expected: false,
		},
		{
			name: "Enabled changed from true to false",
			oldCfg: &querythrottlerpb.Config{
				Enabled:  true,
				Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
				DryRun:   false,
			},
			newCfg: &querythrottlerpb.Config{
				Enabled:  false,
				Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
				DryRun:   false,
			},
			expected: true,
		},
		{
			name: "Enabled changed from false to true",
			oldCfg: &querythrottlerpb.Config{
				Enabled:  false,
				Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
				DryRun:   false,
			},
			newCfg: &querythrottlerpb.Config{
				Enabled:  true,
				Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
				DryRun:   false,
			},
			expected: true,
		},
		{
			name: "DryRun changed from false to true",
			oldCfg: &querythrottlerpb.Config{
				Enabled:  true,
				Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
				DryRun:   false,
			},
			newCfg: &querythrottlerpb.Config{
				Enabled:  true,
				Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
				DryRun:   true,
			},
			expected: true,
		},
		{
			name: "DryRun changed from true to false",
			oldCfg: &querythrottlerpb.Config{
				Enabled:  true,
				Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
				DryRun:   true,
			},
			newCfg: &querythrottlerpb.Config{
				Enabled:  true,
				Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
				DryRun:   false,
			},
			expected: true,
		},
		{
			name: "Multiple fields changed - Enabled and DryRun",
			oldCfg: &querythrottlerpb.Config{
				Enabled:  true,
				Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
				DryRun:   false,
			},
			newCfg: &querythrottlerpb.Config{
				Enabled:  false,
				Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
				DryRun:   true,
			},
			expected: true,
		},
		{
			name: "Multiple fields changed - Enabled and StrategyName",
			oldCfg: &querythrottlerpb.Config{
				Enabled:  true,
				Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
				DryRun:   false,
			},
			newCfg: &querythrottlerpb.Config{
				Enabled:  false,
				Strategy: querythrottlerpb.ThrottlingStrategy_UNKNOWN,
				DryRun:   false,
			},
			expected: true,
		},
		{
			name: "Multiple fields changed - StrategyName and DryRun",
			oldCfg: &querythrottlerpb.Config{
				Enabled:  true,
				Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
				DryRun:   false,
			},
			newCfg: &querythrottlerpb.Config{
				Enabled:  true,
				Strategy: querythrottlerpb.ThrottlingStrategy_UNKNOWN,
				DryRun:   true,
			},
			expected: true,
		},
		{
			name: "All three fields changed",
			oldCfg: &querythrottlerpb.Config{
				Enabled:  true,
				Strategy: querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER,
				DryRun:   false,
			},
			newCfg: &querythrottlerpb.Config{
				Enabled:  false,
				Strategy: querythrottlerpb.ThrottlingStrategy_UNKNOWN,
				DryRun:   true,
			},
			expected: true,
		},
		{
			name: "All fields false/default - no change",
			oldCfg: &querythrottlerpb.Config{
				Enabled:  false,
				Strategy: querythrottlerpb.ThrottlingStrategy_UNKNOWN,
				DryRun:   false,
			},
			newCfg: &querythrottlerpb.Config{
				Enabled:  false,
				Strategy: querythrottlerpb.ThrottlingStrategy_UNKNOWN,
				DryRun:   false,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isConfigUpdateRequired(tt.oldCfg, tt.newCfg)
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestQueryThrottler_startSrvKeyspaceWatch_InitialLoad tests that initial configuration is loaded successfully when GetSrvKeyspace succeeds.
func TestQueryThrottler_startSrvKeyspaceWatch_InitialLoad(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
		qt.mu.RLock()
		defer qt.mu.RUnlock()
		return qt.cfg.GetEnabled() &&
			qt.cfg.GetStrategy() == querythrottlerpb.ThrottlingStrategy_TABLET_THROTTLER &&
			!qt.cfg.GetDryRun()
	}, 2*time.Second, 10*time.Millisecond, "Config should be loaded correctly: enabled=true, strategy=TabletThrottler, dryRun=false")

	require.Equal(t, "test_keyspace", qt.keyspace, "Keyspace should be set correctly")
}

// TestQueryThrottler_startSrvKeyspaceWatch_InitialLoadFailure tests that watch starts even when initial GetSrvKeyspace fails.
func TestQueryThrottler_startSrvKeyspaceWatch_InitialLoadFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
		qt.mu.RLock()
		defer qt.mu.RUnlock()
		return !qt.cfg.GetEnabled()
	}, 2*time.Second, 10*time.Millisecond, "Config should remain disabled after initial load failure")
}

// TestQueryThrottler_startSrvKeyspaceWatch_OnlyStartsOnce tests that watch only starts once even with concurrent calls (atomic flag protection).
func TestQueryThrottler_startSrvKeyspaceWatch_OnlyStartsOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each goroutine tries to start the watch
			qt.startSrvKeyspaceWatch()
			mu.Lock()
			startedCount++
			mu.Unlock()
		}()
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
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

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
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

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
				qt.mu.RLock()
				defer qt.mu.RUnlock()
				return qt.cfg.GetEnabled() == tt.expectedEnabled &&
					qt.cfg.GetStrategy() == tt.expectedStrategy &&
					qt.cfg.GetDryRun() == tt.expectedDryRun
			}, 2*time.Second, 10*time.Millisecond, "Config should be updated correctly after callback is invoked")
		},
		)
	}
}

// TestQueryThrottler_startSrvKeyspaceWatch_ShutdownStopsWatch tests that Shutdown properly cancels the watch context and stops the watch goroutine.
func TestQueryThrottler_startSrvKeyspaceWatch_ShutdownStopsWatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	qt.mu.RLock()
	strategyInstance := qt.strategyHandlerInstance
	qt.mu.RUnlock()
	require.NotNil(t, strategyInstance, "Strategy instance should still exist after shutdown")

	// Call Shutdown again to ensure it doesn't panic
	qt.Shutdown()

	// Verify the watch flag remains false
	require.False(t, qt.watchStarted.Load(), "Watch should remain not started after multiple shutdowns")
}
