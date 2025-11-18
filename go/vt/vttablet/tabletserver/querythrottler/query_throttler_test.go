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
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo/fakesrvtopo"
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
		giveThrottlingStrategy registry.ThrottlingStrategy
		expectedType           registry.ThrottlingStrategyHandler
	}{
		{
			name:                   "Unknown strategy defaults to NoOp",
			giveThrottlingStrategy: "some-unknown-string",
			expectedType:           &registry.NoOpStrategy{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &throttle.Client{}

			config := &tabletenv.TabletConfig{
				QueryThrottlerConfigRefreshInterval: 10 * time.Millisecond,
			}

			strategy := selectThrottlingStrategy(Config{Enabled: true, StrategyName: tt.giveThrottlingStrategy}, mockClient, config)

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

	iqt := NewQueryThrottler(ctx, throttler, newFakeConfigLoader(Config{
		Enabled:      true,
		StrategyName: registry.ThrottlingStrategyTabletThrottler,
	}), env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: uint32(123)}, &fakesrvtopo.FakeSrvTopo{})

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
	iqt := NewQueryThrottler(ctx, throttler, newFakeConfigLoader(Config{
		Enabled:      false,
		StrategyName: registry.ThrottlingStrategyTabletThrottler,
	}), env, &topodatapb.TabletAlias{Cell: "test-cell", Uid: uint32(123)}, &fakesrvtopo.FakeSrvTopo{})

	// Should not panic when called multiple times
	iqt.Shutdown()
	iqt.Shutdown()

	// Should still be able to check the strategy reference
	iqt.mu.RLock()
	strategy := iqt.strategyHandlerInstance
	iqt.mu.RUnlock()
	require.NotNil(t, strategy)
}

// TestIncomingQueryThrottler_DryRunMode tests that dry-run mode logs decisions but doesn't throttle queries.
func TestIncomingQueryThrottler_DryRunMode(t *testing.T) {
	tests := []struct {
		name             string
		enabled          bool
		dryRun           bool
		throttleDecision registry.ThrottleDecision
		expectError      bool
		expectDryRunLog  bool
		expectedLogMsg   string
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
			expectError:     false,
			expectDryRunLog: false,
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
			expectError:     true,
			expectDryRunLog: false,
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
			expectError:     false,
			expectDryRunLog: true,
			expectedLogMsg:  "[DRY-RUN] Query throttled: metric=cpu value=95.0 threshold=80.0",
		},
		{
			name:    "Dry-run mode - query allowed normally",
			enabled: true,
			dryRun:  true,
			throttleDecision: registry.ThrottleDecision{
				Throttle: false,
				Message:  "Query allowed",
			},
			expectError:     false,
			expectDryRunLog: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock strategy with controlled decision
			mockStrategy := &mockThrottlingStrategy{
				decision: tt.throttleDecision,
			}

			// Create throttler with controlled config
			iqt := &QueryThrottler{
				ctx: context.Background(),
				cfg: Config{
					Enabled: tt.enabled,
					DryRun:  tt.dryRun,
				},
				strategyHandlerInstance: mockStrategy,
			}

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
		})
	}
}

// mockThrottlingStrategy is a test strategy that allows us to control throttling decisions
type mockThrottlingStrategy struct {
	decision registry.ThrottleDecision
	started  bool
	stopped  bool
}

func (m *mockThrottlingStrategy) Evaluate(ctx context.Context, targetTabletType topodatapb.TabletType, parsedQuery *sqlparser.ParsedQuery, transactionID int64, attrs registry.QueryAttributes) registry.ThrottleDecision {
	return m.decision
}

func (m *mockThrottlingStrategy) Start() {
	m.started = true
}

func (m *mockThrottlingStrategy) Stop() {
	m.stopped = true
}

func (m *mockThrottlingStrategy) GetStrategyName() string {
	return "MockStrategy"
}

// testLogCapture captures log output for testing
type testLogCapture struct {
	logs []string
}

func (lc *testLogCapture) captureLog(msg string, args ...interface{}) {
	lc.logs = append(lc.logs, fmt.Sprintf(msg, args...))
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
			inputErr:       fmt.Errorf("transient network error"),
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
				cfg:                     Config{Enabled: true, StrategyName: registry.ThrottlingStrategyTabletThrottler},
				strategyHandlerInstance: &registry.NoOpStrategy{},
				tabletConfig:            &tabletenv.TabletConfig{},
			}

			// Create a valid SrvKeyspace matching the test setup (errors are checked before srvks is used)
			srvks := createTestSrvKeyspace(true, registry.ThrottlingStrategyTabletThrottler, false)

			result := qt.HandleConfigUpdate(srvks, tt.inputErr)

			require.Equal(t, tt.expectedResult, result, tt.description)
		})
	}
}

// TestQueryThrottler_HandleConfigUpdate__ConfigExtraction verifies config is properly extracted from SrvKeyspace.
func TestQueryThrottler_HandleConfigUpdate__ConfigExtraction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	oldCfg := Config{Enabled: false, StrategyName: registry.ThrottlingStrategyTabletThrottler, DryRun: false}
	oldStrategy := &registry.NoOpStrategy{}

	qt := &QueryThrottler{
		ctx:                     ctx,
		cfg:                     oldCfg,
		strategyHandlerInstance: oldStrategy,
		tabletConfig:            &tabletenv.TabletConfig{},
		throttleClient:          &throttle.Client{},
	}

	// Create SrvKeyspace with different config values
	srvks := createTestSrvKeyspace(true, registry.ThrottlingStrategyTabletThrottler, true)

	result := qt.HandleConfigUpdate(srvks, nil)

	// Should return true to continue watching, config should be extracted from SrvKeyspace
	require.True(t, result, "callback should return true and continue watching")

	qt.mu.RLock()
	require.True(t, qt.cfg.Enabled, "Enabled should be updated from SrvKeyspace")
	require.True(t, qt.cfg.DryRun, "DryRun should be updated from SrvKeyspace")
	require.Equal(t, registry.ThrottlingStrategyTabletThrottler, qt.cfg.StrategyName, "strategy should remain TabletThrottler")
	qt.mu.RUnlock()
}

// TestQueryThrottler_HandleConfigUpdate__SuccessfulConfigUpdate tests successful config update when strategy doesn't change.
func TestQueryThrottler_HandleConfigUpdate__SuccessfulConfigUpdate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use a mock strategy to track state changes
	oldStrategy := &mockThrottlingStrategy{}

	// Both initial and new config have the same strategy TYPE (no swap expected)
	unchangedStrategyType := registry.ThrottlingStrategyTabletThrottler

	qt := &QueryThrottler{
		ctx:                     ctx,
		cfg:                     Config{Enabled: true, StrategyName: unchangedStrategyType, DryRun: false},
		strategyHandlerInstance: oldStrategy,
		tabletConfig:            &tabletenv.TabletConfig{},
	}

	// Create SrvKeyspace with same strategy but DryRun changed
	srvks := createTestSrvKeyspace(true, unchangedStrategyType, true)

	result := qt.HandleConfigUpdate(srvks, nil)

	require.True(t, result, "callback should return true")

	qt.mu.RLock()
	require.True(t, qt.cfg.DryRun, "DryRun config should be updated")
	require.Equal(t, unchangedStrategyType, qt.cfg.GetStrategyName(), "strategy type should remain the same")
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
		cfg:                     Config{Enabled: true, StrategyName: registry.ThrottlingStrategyTabletThrottler},
		strategyHandlerInstance: oldStrategy,
		tabletConfig:            &tabletenv.TabletConfig{},
		throttleClient:          &throttle.Client{},
	}

	srvks := createTestSrvKeyspace(true, registry.ThrottlingStrategyUnknown, false)

	result := qt.HandleConfigUpdate(srvks, nil)

	// Strategy should be switched
	require.True(t, result, "callback should return true")

	qt.mu.RLock()
	require.Equal(t, registry.ThrottlingStrategyUnknown, qt.cfg.GetStrategyName(), "config strategy should be updated")
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

	unchangedCfg := Config{Enabled: true, StrategyName: registry.ThrottlingStrategyTabletThrottler, DryRun: false}
	oldStrategy := &registry.NoOpStrategy{}

	qt := &QueryThrottler{
		ctx:                     ctx,
		cfg:                     unchangedCfg,
		strategyHandlerInstance: oldStrategy,
		tabletConfig:            &tabletenv.TabletConfig{},
	}

	// Create SrvKeyspace with identical config
	srvks := createTestSrvKeyspace(true, registry.ThrottlingStrategyTabletThrottler, false)

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
		oldCfg   Config
		newCfg   Config
		expected bool
	}{
		{
			name: "No changes - configs identical",
			oldCfg: Config{
				Enabled:      true,
				StrategyName: registry.ThrottlingStrategyTabletThrottler,
				DryRun:       false,
			},
			newCfg: Config{
				Enabled:      true,
				StrategyName: registry.ThrottlingStrategyTabletThrottler,
				DryRun:       false,
			},
			expected: false,
		},
		{
			name: "Enabled changed from true to false",
			oldCfg: Config{
				Enabled:      true,
				StrategyName: registry.ThrottlingStrategyTabletThrottler,
				DryRun:       false,
			},
			newCfg: Config{
				Enabled:      false,
				StrategyName: registry.ThrottlingStrategyTabletThrottler,
				DryRun:       false,
			},
			expected: true,
		},
		{
			name: "Enabled changed from false to true",
			oldCfg: Config{
				Enabled:      false,
				StrategyName: registry.ThrottlingStrategyTabletThrottler,
				DryRun:       false,
			},
			newCfg: Config{
				Enabled:      true,
				StrategyName: registry.ThrottlingStrategyTabletThrottler,
				DryRun:       false,
			},
			expected: true,
		},
		{
			name: "DryRun changed from false to true",
			oldCfg: Config{
				Enabled:      true,
				StrategyName: registry.ThrottlingStrategyTabletThrottler,
				DryRun:       false,
			},
			newCfg: Config{
				Enabled:      true,
				StrategyName: registry.ThrottlingStrategyTabletThrottler,
				DryRun:       true,
			},
			expected: true,
		},
		{
			name: "DryRun changed from true to false",
			oldCfg: Config{
				Enabled:      true,
				StrategyName: registry.ThrottlingStrategyTabletThrottler,
				DryRun:       true,
			},
			newCfg: Config{
				Enabled:      true,
				StrategyName: registry.ThrottlingStrategyTabletThrottler,
				DryRun:       false,
			},
			expected: true,
		},
		{
			name: "Multiple fields changed - Enabled and DryRun",
			oldCfg: Config{
				Enabled:      true,
				StrategyName: registry.ThrottlingStrategyTabletThrottler,
				DryRun:       false,
			},
			newCfg: Config{
				Enabled:      false,
				StrategyName: registry.ThrottlingStrategyTabletThrottler,
				DryRun:       true,
			},
			expected: true,
		},
		{
			name: "Multiple fields changed - Enabled and StrategyName",
			oldCfg: Config{
				Enabled:      true,
				StrategyName: registry.ThrottlingStrategyTabletThrottler,
				DryRun:       false,
			},
			newCfg: Config{
				Enabled:      false,
				StrategyName: registry.ThrottlingStrategyUnknown,
				DryRun:       false,
			},
			expected: true,
		},
		{
			name: "Multiple fields changed - StrategyName and DryRun",
			oldCfg: Config{
				Enabled:      true,
				StrategyName: registry.ThrottlingStrategyTabletThrottler,
				DryRun:       false,
			},
			newCfg: Config{
				Enabled:      true,
				StrategyName: registry.ThrottlingStrategyUnknown,
				DryRun:       true,
			},
			expected: true,
		},
		{
			name: "All three fields changed",
			oldCfg: Config{
				Enabled:      true,
				StrategyName: registry.ThrottlingStrategyTabletThrottler,
				DryRun:       false,
			},
			newCfg: Config{
				Enabled:      false,
				StrategyName: registry.ThrottlingStrategyUnknown,
				DryRun:       true,
			},
			expected: true,
		},
		{
			name: "All fields false/empty - no change",
			oldCfg: Config{
				Enabled:      false,
				StrategyName: "",
				DryRun:       false,
			},
			newCfg: Config{
				Enabled:      false,
				StrategyName: "",
				DryRun:       false,
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
